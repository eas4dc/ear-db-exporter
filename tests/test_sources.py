"""Sources: SqlSource on a fake SQLite engine, BundleSource on Parquet files, and the SQL they share."""
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ear_db_export.config import ConfigError
from ear_db_export.read import bundle, sql
from ear_db_export.read.bundle import BundleSource
from ear_db_export.read.sql import SqlSource, queries

import fakedata
from fakedb import DBCONF, FakeEngine, make_engine, physical_tables

T0 = fakedata.T0
q = lambda name: "`%s`" % name


def source(profile="ear6", **kw):
    return SqlSource(DBCONF, engine=make_engine(profile), **kw)


def rows(tables, col):
    return sorted(v for t in tables for v in t[col].to_pylist())


def test_where_sql():
    sql_, params = queries.where(q, [[("a", "=", 1), ("b", "in", [2, 3]), ("c", "like", "x%")],
                                     [("d", "!=", 4), ("e", "in", []), ("f", "not in", [])]])
    assert sql_ == "(`a` = %s AND `b` IN (%s, %s) AND `c` LIKE %s) OR (`d` <> %s AND 1=0 AND 1=1)"
    assert params == [1, 2, 3, "x%", 4]
    assert queries.where(q, []) == ("", []) and queries.where(q, [[]]) == ("1=1", [])


@pytest.mark.parametrize("page_rows", [1, 2, 100])
def test_scan_pages_every_row_once(monkeypatch, page_rows):
    monkeypatch.setattr(sql, "PAGE_ROWS", page_rows)
    src = source()
    pages = list(src.scan("Periodic_metrics"))
    assert rows(pages, "id") == [1, 2, 3, 4]
    assert pages[0].schema.field("id").type == pa.uint64()
    assert pages[0].schema.field("DC_energy").type == pa.uint32()


def test_scan_time_window_goes_through_an_id_window():
    src = source()
    where = [("end_time", ">=", T0 + 100), ("end_time", "<", T0 + 200)]
    got = list(src.scan("Periodic_metrics", where))
    assert rows(got, "id") == [1, 3]
    data_query = [s for s, _ in src.engine.queries if s.startswith("SELECT `id`, `start_time`")][-1]
    assert "`id` BETWEEN %s AND %s AND (" in data_query


def test_scan_maps_logical_names():
    src = source("ear7")
    got = list(src.scan("Jobs", [("user_id", "in", ["bob"])]))
    assert rows(got, "job_id") == [8] and "user_name" in got[0].column_names
    with pytest.raises(ConfigError, match="job_type"):
        list(src.scan("Jobs", [("job_type", "=", 0)]))  # gone in EAR 7


def test_scan_without_profile_reads_tables_as_stored():
    def unknown_version():
        eng = make_engine()
        eng.db.execute("ALTER TABLE `Jobs` RENAME COLUMN `user_acc` TO `account`")
        eng.db.commit()
        eng.ddl["Jobs"] = [("account" if c[0] == "user_acc" else c[0],) + c[1:]
                           for c in eng.ddl["Jobs"]]
        return eng
    with pytest.raises(ConfigError, match="autodetect"):
        SqlSource(DBCONF, engine=unknown_version())
    src = SqlSource(DBCONF, engine=unknown_version(), need_profile=False)
    assert src.profile is None and rows(src.scan("Jobs"), "job_id") == [7, 7, 8]
    assert not src.check()[1]
    with pytest.raises(ConfigError, match="profile"):
        list(src.scan("Jobs", [("user_id", "=", "x")]))


def test_lookup_batches(monkeypatch):
    monkeypatch.setattr(sql, "IN_BATCH", 2)
    src = source()
    got = list(src.lookup("Signatures", "id", [102, 100, None, 101, 100]))
    assert len(got) == 2 and rows(got, "id") == [100, 101, 102]
    assert rows(src.lookup("Signatures", "id", []), "id") == []


def test_dry_run_logs_and_reads_nothing(caplog):
    src = source(dry_run=True)
    caplog.set_level("INFO")
    got = list(src.scan("Periodic_metrics", [("end_time", ">=", T0)]))
    assert [t.num_rows for t in got] == [0]
    assert "rows: 4" in caplog.text and "EXPLAIN" in caplog.text


def test_info_and_oldest_day():
    src = source()
    assert src.info() == {"engine": "fake", "server": "fake-1.0", "database": "EAR",
                          "profile": "ear6", "id_step": 2}
    assert src.oldest_day() == fakedata.DAY


def make_bundle(path, profile="ear6", tables=None, tags=None):
    """A bundle of the fakedata records, written by hand (as export writes it, without checksums)."""
    files = {}
    for table, t in (tables or physical_tables(profile)).items():
        rel = table + ".parquet"
        pq.write_table(t, os.path.join(path, rel))
        files[table] = {"files": {rel: {"rows": t.num_rows}}, "rows": t.num_rows}
    manifest = {"format": 1, "files": "parquet", "pseudonymized": False,
                "source": {"engine": "fake", "profile": profile, "id_step": fakedata.ID_STEP},
                "node_tags": {"n3": "gpu"} if tags is None else tags, "tables": files}
    with open(os.path.join(path, bundle.MANIFEST), "w") as f:
        json.dump(manifest, f)
    return path


def test_bundle_scan_and_lookup(tmp_path):
    src = BundleSource(make_bundle(str(tmp_path), "ear7"))
    assert src.id_step == 2 and src.tags == {"n3": "gpu"} and "Jobs" in src.provides
    got = list(src.scan("Jobs", [[("user_id", "in", ["alice"]), ("app_name", "like", "lmp\\_%")],
                                 [("step_id", "=", 4294967291)]]))
    assert rows(got, "job_id") == [7, 8]
    assert got[0].schema.field("job_id").type == pa.uint32()
    assert rows(src.lookup("GPU_signatures", "id", [56, 50, 99]), "id") == [50, 56]
    assert [t.num_rows for t in src.scan("Jobs", [("user_id", "=", "nobody")])] == [0]


def test_bundle_needs_manifest_and_parquet(tmp_path):
    with pytest.raises(ConfigError, match="not a complete bundle"):
        BundleSource(str(tmp_path))
    make_bundle(str(tmp_path))
    m = json.loads((tmp_path / bundle.MANIFEST).read_text())
    (tmp_path / bundle.MANIFEST).write_text(json.dumps(dict(m, files="csv")))
    with pytest.raises(ConfigError, match="Parquet"):
        BundleSource(str(tmp_path))


def test_fake_engine_matches_every_profile():
    for profile in ("ear4", "ear6", "ear7"):
        assert SqlSource(DBCONF, engine=FakeEngine(profile)).profile.name == profile
