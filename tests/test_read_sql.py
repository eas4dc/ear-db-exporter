import pyarrow as pa
import pytest

from ear_db_export.config import ConfigError
from ear_db_export.read import arrow_schema, col_type, is_optional
from ear_db_export.read.sql import mariadb, profile as schema
from ear_db_export.read.sql.queries import bisect_id


def live_from(profile, drop=()):
    """A fake information_schema built from a profile (all types right)."""
    live = {}
    for t, cols in profile.columns.items():
        types = {"int": "int", "float": "float", "str": "varchar", "ts": "timestamp"}
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


def test_bisect_id_finds_neighbours():
    # ids with gaps; end_time not monotonic within the margin
    rows = {1: 100, 2: 110, 5: 105, 6: 200, 9: 210}
    probe = lambda i: next(((k, rows[k]) for k in sorted(rows) if k >= i), None)
    assert bisect_id(probe, 150, 1, 9) == (5, 6)
    assert bisect_id(probe, 50, 1, 9) == (None, 1)
    assert bisect_id(probe, 300, 1, 9) == (9, None)


@pytest.mark.parametrize("column_type, expected", [
    ("int(10) unsigned", pa.uint32()), ("bigint(20) unsigned", pa.uint64()), ("int(11)", pa.int32()),
    ("smallint(5) unsigned", pa.uint16()), ("float", pa.float32()), ("double", pa.float64()),
    ("varchar(64)", pa.string()), ("uuid", pa.string()), ("timestamp", pa.timestamp("us", "UTC")),
])
def test_arrow_type(column_type, expected):
    assert mariadb.arrow_type(column_type) == expected


# == Profiles are total over EAR's DDL =============================================

@pytest.mark.parametrize("name", ["ear4", "ear6", "ear7"])
def test_profile_covers_ddl(name):
    """Every column EAR creates is mapped, every required mapped column exists, types agree."""
    p = schema.load_profile(name)
    ddl = schema.ddl_schema(name)
    assert sorted(ddl) == sorted(p.tables.values())
    for t, phys in p.tables.items():
        cols = {c: typ for c, typ, _, _ in ddl[phys]}
        mapped = p.columns[t]
        assert sorted(set(cols) - set(mapped.values())) == [], phys
        assert [c for c, pc_ in mapped.items() if pc_ not in cols and not is_optional(t, c)] == [], phys
        base = lambda typ: typ.split("(")[0].split()[0]
        assert [c for c, pc_ in mapped.items() if pc_ in cols
                and mariadb.TYPE_FAMILY[base(cols[pc_])] != col_type(t, c)] == [], phys


def test_ddl_schema_types_keys_and_nullability():
    jobs = {c[0]: c[1:] for c in schema.ddl_schema("ear7")["Jobs"]}
    assert jobs["job_id"] == ("int unsigned", False, True)
    assert jobs["user_name"] == ("varchar(128)", True, False)
    ge = {c[0]: c[1:] for c in schema.ddl_schema("ear6")["Global_energy"]}
    assert ge["time"] == ("timestamp", False, True)
    assert mariadb.arrow_type(ge["warning_level"][0]) == pa.uint32()
    apps = {c[0]: c[1:] for c in schema.ddl_schema("ear6")["Applications"]}
    assert apps["node_id"] == ("varchar(64)", False, True)  # in the primary key: NOT NULL


def test_like_copies_columns():
    p = schema.load_profile("ear4")
    assert p.columns["learning_jobs"] == p.columns["jobs"] and p.columns["jobs"]["job_id"] == "id"


def test_missing_optional_table_is_not_a_mismatch():
    p = schema.load_profile("ear6")
    live = live_from(p)
    del live["Loops"], live["Learning_jobs"]
    assert schema.detect(live).name == "ear6"
    lines, ok = schema.diff(p, live, mariadb.TYPE_FAMILY)
    assert ok and "Loops (loops): TABLE MISSING (optional)" in lines
    p.bind(live)
    assert "loops" not in p.tables and "jobs" in p.tables


def test_logical_float32_keeps_the_decimal_value():
    p = schema.load_profile("ear6")
    phys = pa.table({"id": pa.array([1], pa.uint32()), "DC_power": pa.array([310.523], pa.float32()),
                     "time": pa.array([0.1], pa.float32())})
    out = p.logical("power_signatures", phys, ["id", "dc_power", "time", "edp"])
    assert out.to_pylist() == [{"id": 1, "dc_power": 310.523, "time": 0.1, "edp": None}]
    assert out.schema.field("dc_power").type == pa.float64()
    assert p.logical("power_signatures", phys.slice(0, 0)).num_rows == 0


def test_physical_inverts_logical_and_where_maps_names():
    p = schema.load_profile("ear7")
    t = pa.table({"job_id": [1], "local_id": [2], "app_name": ["x"], "user_id": ["u"]})
    phys = p.physical("jobs", t)
    assert phys.column_names == ["job_id", "app_id", "app_name", "user_name"]
    assert p.logical("jobs", phys, t.column_names).equals(t)
    assert p.where("jobs", [("user_id", "in", ["u"])]) == [[("user_name", "in", ["u"])]]
    with pytest.raises(ConfigError, match="job_type"):
        p.where("jobs", [("job_type", "=", 0)])  # gone in EAR 7
