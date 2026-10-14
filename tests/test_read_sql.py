"""Schema profiles, check, the id search and SQL types (no database)."""
import subprocess
import sys

import pyarrow as pa
import pytest

from ear_db_export.config import ConfigError
from ear_db_export.read import Cond, col_type, is_optional
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
    """Every bundled profile loads and maps every logical table."""
    schema.load_profile(name)


def test_detect_picks_unique_profile():
    """Each EAR version's columns autodetect its profile."""
    for name in ("ear4", "ear6", "ear7"):
        live = live_from(schema.load_profile(name))
        assert schema.detect(live).name == name


def test_detect_fails_on_no_match():
    """No matching profile is a configuration error."""
    with pytest.raises(ConfigError, match="no profile"):
        schema.detect({})


def test_bind_drops_missing_optional():
    """An optional column the database lacks is dropped (read as NULL)."""
    p = schema.load_profile("ear6")
    p.bind(live_from(p, drop=("GPU_energy",)))
    assert "gpu_energy" not in p.columns["periodic_metrics"]


def test_diff_reports_missing_and_type():
    """check reports missing columns, wrong types and extra columns."""
    p = schema.load_profile("ear6")
    live = live_from(p, drop=("DC_energy",))
    live["Jobs"]["policy"] = "int"
    live["Jobs"]["new_col"] = "int"
    lines, ok = schema.diff(p, live, mariadb.TYPE_FAMILY)
    text = "\n".join(lines)
    assert not ok
    assert "missing: DC_energy" in text and "policy is int" in text and "new_col" in text


def test_invalid_profile(tmp_path):
    """A typo in a profile is an error, not a silent gap."""
    f = tmp_path / "bad.toml"
    f.write_text('[tables.jobs]\nname = "Jobs"\n[tables.jobs.columns]\nnope = "x"\n')
    with pytest.raises(ConfigError, match="unknown column jobs.nope"):
        schema.load_profile(str(f))


def test_bisect_id_finds_neighbours():
    """The id search finds the rows on each side of a time, with id gaps."""
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
    """Each SQL column type maps to an Arrow type that holds it exactly."""
    assert mariadb.arrow_type(column_type) == expected


def test_dropped_stream_rows_are_an_error():
    """pymysql only warns when it drops a streamed result's unread rows: with mariadb imported it is an error.

    Run in a new interpreter: pytest resets warning filters inside each test.
    """
    code = ("import warnings, ear_db_export.read.sql.mariadb\n"
            "warnings.warn('Previous unbuffered result was left incomplete')")
    run = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert run.returncode != 0 and "UserWarning: Previous unbuffered" in run.stderr


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
    """EAR's DDL parses into column types, keys and nullability."""
    jobs = {c[0]: c[1:] for c in schema.ddl_schema("ear7")["Jobs"]}
    assert jobs["job_id"] == ("int unsigned", False, True)
    assert jobs["user_name"] == ("varchar(128)", True, False)
    ge = {c[0]: c[1:] for c in schema.ddl_schema("ear6")["Global_energy"]}
    assert ge["time"] == ("timestamp", False, True)
    assert mariadb.arrow_type(ge["warning_level"][0]) == pa.uint32()
    apps = {c[0]: c[1:] for c in schema.ddl_schema("ear6")["Applications"]}
    assert apps["node_id"] == ("varchar(64)", False, True)  # in the primary key: NOT NULL


def test_like_copies_columns():
    """like = copies another table's columns (Learning_*)."""
    p = schema.load_profile("ear4")
    assert p.columns["learning_jobs"] == p.columns["jobs"] and p.columns["jobs"]["job_id"] == "id"


def test_missing_optional_table_is_not_a_mismatch():
    """A missing optional table is reported but matches."""
    p = schema.load_profile("ear6")
    live = live_from(p)
    del live["Loops"], live["Learning_jobs"]
    assert schema.detect(live).name == "ear6"
    lines, ok = schema.diff(p, live, mariadb.TYPE_FAMILY)
    assert ok and "Loops (loops): TABLE MISSING (optional)" in lines
    p.bind(live)
    assert "loops" not in p.tables and "jobs" in p.tables


def test_renames_are_reversible_and_where_maps_names():
    """to_logical/to_physical only rename (extra columns kept); where maps each condition's column."""
    p = schema.load_profile("ear7")
    phys = pa.table({"job_id": [1], "app_id": [2], "app_name": ["x"], "user_name": ["u"],
                     "pod_uuid": ["k8s"]})
    logical = p.to_logical("jobs", phys)
    assert logical.column_names == ["job_id", "local_id", "app_name", "user_id", "pod_uuid"]
    assert p.to_physical("jobs", logical).equals(phys)
    assert p.where("jobs", [Cond("user_id", "in", ["u"])]) == [Cond("user_name", "in", ["u"])]
    with pytest.raises(ConfigError, match="job_type"):
        p.where("jobs", [Cond("job_type", "=", 0)])  # gone in EAR 7


def test_bind_refuses_unmapped_column_with_a_logical_name():
    """An extra column named like a logical column would make two columns with one name."""
    p = schema.load_profile("ear7")
    live = live_from(p)
    live["Jobs"]["local_id"] = "int"  # EAR 7 maps local_id to app_id
    with pytest.raises(ConfigError, match="local_id"):
        p.bind(live)
