from types import SimpleNamespace

import pyarrow as pa
import pytest

from ear_db_export.config import ConfigError
from ear_db_export.read import arrow_schema, col_type
from ear_db_export.read.sql import SqlReader, mariadb, profile as schema
from ear_db_export.read.sql.queries import Queries, bisect_id


def live_from(profile, drop=()):
    """A fake information_schema built from a profile (all types right)."""
    live = {}
    for t, cols in profile.columns.items():
        types = {"int": "int", "float": "float", "str": "varchar"}
        live[profile.tables[t]] = {p: types[col_type(t, c)]
                                   for c, p in cols.items() if p not in drop}
    return live


@pytest.mark.parametrize("name", schema.available_profiles())
def test_bundled_profiles_are_valid(name):
    schema.load_profile(name)


def test_detect_picks_unique_profile():
    for name in ("ear4", "ear6", "ear7"):
        live = live_from(schema.load_profile(name))
        assert schema.detect(live).name == name


def test_detect_fails_on_no_match():
    with pytest.raises(ConfigError, match="no profile"):
        schema.detect({})


def test_bind_drops_missing_optional():
    p = schema.load_profile("ear6")
    p.bind(live_from(p, drop=("GPU_energy",)))
    assert "gpu_energy" not in p.columns["periodic_metrics"]


def test_diff_reports_missing_and_type():
    p = schema.load_profile("ear6")
    live = live_from(p, drop=("DC_energy",))
    live["Jobs"]["policy"] = "int"
    live["Jobs"]["new_col"] = "int"
    lines, ok = schema.diff(p, live, mariadb.TYPE_FAMILY)
    text = "\n".join(lines)
    assert not ok
    assert "missing: DC_energy" in text and "policy is int" in text and "new_col" in text


def test_invalid_profile(tmp_path):
    f = tmp_path / "bad.toml"
    f.write_text('[tables.jobs]\nname = "Jobs"\n[tables.jobs.columns]\nnope = "x"\n')
    with pytest.raises(ConfigError, match="unknown column jobs.nope"):
        schema.load_profile(str(f))


def test_select_aliases_physical_to_logical():
    p = schema.load_profile("ear6")
    sql, arrow = Queries(p, mariadb.quote).by_end_time("jobs", 10, 20)[::2]
    assert "`app_id` AS `app_name`" in sql and sql.endswith("WHERE `end_time` >= %s AND `end_time` < %s")
    assert arrow.names[0] == "job_id"


def test_bisect_id_finds_neighbours():
    # ids with gaps; end_time not monotonic within the margin
    rows = {1: 100, 2: 110, 5: 105, 6: 200, 9: 210}
    probe = lambda i: next(((k, rows[k]) for k in sorted(rows) if k >= i), None)
    assert bisect_id(probe, 150, 1, 9) == (5, 6)
    assert bisect_id(probe, 50, 1, 9) == (None, 1)
    assert bisect_id(probe, 300, 1, 9) == (9, None)


def test_gpu_signatures_use_the_galera_id_step():
    # Signature 7 has GPU ids 11..17 with step 2: 11, 13, 15, 17; 13 has no row
    r = SqlReader.__new__(SqlReader)  # no connection: only gpu_signatures is exercised
    r.conn, r.dry_run = None, False
    r.engine = SimpleNamespace(id_step=lambda conn: 2)
    looked_up = []

    def fetch_in(table, col, keys):
        looked_up.extend(keys)
        rows = [{"id": i, "gpu_power": float(i)} for i in (11, 15, 17)]
        return pa.Table.from_pylist(rows, schema=arrow_schema("gpu_signatures"))
    r._fetch_in = fetch_in
    sigs = pa.table({"id": [7, 8], "min_gpu_sig_id": [11, None], "max_gpu_sig_id": [17, None]})
    out = r.gpu_signatures(sigs)
    assert looked_up == [11, 13, 15, 17]
    assert out.select(["signature_id", "gpu_index", "gpu_power"]).to_pylist() == [
        {"signature_id": 7, "gpu_index": 0, "gpu_power": 11.0},
        {"signature_id": 7, "gpu_index": 2, "gpu_power": 15.0},
        {"signature_id": 7, "gpu_index": 3, "gpu_power": 17.0}]
