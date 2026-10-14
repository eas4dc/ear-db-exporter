"""Sources: SqlSource on a fake SQLite engine, BundleSource on Parquet files, EacctSource on CSV, and the SQL they share."""
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ear_db_export.clean import pseudonym
from ear_db_export.config import ConfigError
from ear_db_export.output import MANIFEST
from ear_db_export.read import Cond, concat, time_conds
from ear_db_export.read import sql
from ear_db_export.read.bundle import BundleSource
from ear_db_export.read.eacct import HEADER, EacctSource
from ear_db_export.read.pseudonymized import Pseudonymized
from ear_db_export.read.sql import SqlSource, queries

import fakedata
from fakedb import DBCONF, FakeEngine, make_engine, physical_tables

T0 = fakedata.T0
q = lambda name: "`%s`" % name  # noqa: E731


def source(profile="ear6", **kw):
    """A SqlSource on a fake database holding the fakedata records."""
    return SqlSource(DBCONF, engine=make_engine(profile), **kw)


def values(tables, col):
    """The sorted values of one column across the tables a read yields."""
    return sorted(v for t in tables for v in t[col].to_pylist())


def make_bundle(path, profile="ear6", tables=None, tags=None, fingerprint=None):
    """A raw bundle of the fakedata records, written by hand (as `export raw` writes it, without checksums)."""
    outputs = {}
    for table, t in (tables or physical_tables(profile)).items():
        rel = table + ".parquet"
        pq.write_table(t, os.path.join(path, rel))
        outputs[table] = {"files": {rel: {"rows": t.num_rows}}, "rows": t.num_rows}
    manifest = {"format": 2, "layout": "raw", "salt_fingerprint": fingerprint,
                "source": {"engine": "fake", "profile": profile, "id_step": fakedata.ID_STEP},
                "node_tags": {"n3": "gpu"} if tags is None else tags, "outputs": outputs}
    with open(os.path.join(path, MANIFEST), "w") as f:
        json.dump(manifest, f)
    return path


# == SQL text ===========================================================================

def test_where_sql():
    """Every operator renders as one condition; empty lists are always false, not invalid SQL."""
    sql_, params = queries.where(q, [
        Cond("a", "=", 1), Cond("b", "in", [2, 3]), Cond("c", "like_any", ["x%", "y"]),
        Cond("d", "ranges", [(5, 5), (7, 9)]), Cond("e", ">=", 10), Cond("f", "<", 11)])
    assert sql_ == ("`a` = %s AND `b` IN (%s, %s) AND (`c` LIKE %s OR `c` LIKE %s) AND "
                    "(`d` IN (%s) OR `d` BETWEEN %s AND %s) AND `e` >= %s AND `f` < %s")
    assert params == [1, 2, 3, "x%", "y", 5, 7, 9, 10, 11]
    assert queries.where(q, [Cond("a", "in", []), Cond("b", "ranges", [])]) == ("1=0 AND 1=0", [])
    assert queries.where(q, []) == ("", [])


def test_select_orders_and_limits():
    """ORDER BY takes pyarrow-style (column, direction) pairs."""
    assert queries.select(q, "Jobs", ["a"], ["`a` > %s"], [("end_time", "descending")], 3) == \
        "SELECT `a` FROM `Jobs` WHERE `a` > %s ORDER BY `end_time` DESC LIMIT 3"


def test_time_conds_types_the_bounds():
    """Epoch columns take seconds; Global_energy's TIMESTAMP takes a naive UTC datetime."""
    assert time_conds("events", 100, None) == [Cond("timestamp", ">=", 100)]
    (c,) = time_conds("global_energy", None, T0)
    assert (c.column, c.op, c.value.isoformat()) == ("time", "<", "2026-07-04T00:00:00")


# == SqlSource ===========================================================================

@pytest.mark.parametrize("page_rows", [1, 2, 100])
def test_dump_pages_every_row_once_as_stored(monkeypatch, page_rows):
    """dump reads primary-key pages, with physical names and exact types."""
    monkeypatch.setattr(sql, "PAGE_ROWS", page_rows)
    pages = list(source().dump("Periodic_metrics"))
    assert values(pages, "id") == [1, 2, 3, 4]
    assert pages[0].schema.field("id").type == pa.uint64()
    assert pages[0].schema.field("DC_energy").type == pa.uint32()


def test_scan_renames_to_logical_and_keeps_stored_types():
    """On EAR 7 the logical user_id is the physical user_name; types stay as stored."""
    got = list(source("ear7").scan("jobs", [Cond("user_id", "in", ["bob"])]))
    assert values(got, "job_id") == [8] and "user_id" in got[0].column_names
    assert got[0].schema.field("job_id").type == pa.uint32()
    with pytest.raises(ConfigError, match="job_type"):
        list(source("ear7").scan("jobs", [Cond("job_type", "=", 0)]))  # gone in EAR 7


def test_scan_order_and_limit():
    """--limit's read: newest first, one streamed SELECT."""
    got = concat(source().scan("jobs", order=[("end_time", "descending")], limit=2))
    assert got["end_time"].to_pylist() == [T0 + 7000, T0 + 300]


def test_window_goes_through_an_id_range():
    """A time window on Periodic_metrics reads an id range, with the exact time condition."""
    src = source()
    got = list(src.window("periodic_metrics", T0 + 100, T0 + 200))
    assert values(got, "id") == [1, 3]
    data = [s for s, _ in src.engine.queries if s.startswith("SELECT `id`, `start_time`")][-1]
    assert "`id` BETWEEN %s AND %s AND (" in data


def test_window_with_nodes_and_without_bounds():
    """Extra conditions go in the same query; no bound reads the whole table."""
    src = source()
    assert values(src.window("periodic_metrics", None, None, [Cond("node_id", "in", ["n2"])]),
                  "id") == [1, 3]
    assert values(src.window("events", None, None), "id") == []  # empty table: one empty batch


def test_without_profile_only_dump_works():
    """An unknown EAR version: dump works, logical reads refuse with the reason."""
    def unknown_version():
        """A fake database whose Jobs has a column no profile knows."""
        eng = make_engine()
        eng.db.execute("ALTER TABLE `Jobs` RENAME COLUMN `user_acc` TO `account`")
        eng.db.commit()
        eng.ddl["Jobs"] = [("account" if c[0] == "user_acc" else c[0],) + c[1:]
                           for c in eng.ddl["Jobs"]]
        return eng
    src = SqlSource(DBCONF, engine=unknown_version())
    assert src.profile is None and not src.check()[1] and not src.has("jobs")
    assert values(src.dump("Jobs"), "job_id") == [7, 7, 8]
    with pytest.raises(ConfigError, match="autodetect"):
        list(src.scan("jobs"))


def test_a_stream_stays_complete_while_other_queries_run():
    """Lookups while a streamed read is open must not cut it short.

    MariaDB sends an unbuffered result over its connection; pymysql drops its
    unread rows when that connection runs another query. A job selection looks
    up each chunk's applications while the ``Jobs`` stream is open, so the
    stream needs a connection of its own (the fake engine streams 2 rows per
    batch and refuses a query on a connection with an open stream).
    """
    src = source()
    steps = 0
    for jobs in src.scan("jobs"):
        steps += jobs.num_rows
        concat(src.lookup("applications", "job_id", jobs["job_id"].to_pylist()))
    assert steps == 3


def test_lookup_batches(monkeypatch):
    """Keys go in IN batches; None and duplicates are dropped; no keys gives one empty table."""
    monkeypatch.setattr(sql, "IN_BATCH", 2)
    src = source()
    got = list(src.lookup("signatures", "id", [102, 100, None, 101, 100]))
    assert len(got) == 2 and values(got, "id") == [100, 101, 102]
    assert [t.num_rows for t in src.lookup("signatures", "id", [])] == [0]


def test_dry_run_logs_and_reads_nothing(caplog):
    """A dry run logs SQL, EXPLAIN and the row count, and yields empty tables."""
    src = source(dry_run=True)
    caplog.set_level("INFO")
    got = list(src.window("periodic_metrics", T0, None))
    assert [t.num_rows for t in got] == [0]
    assert "rows: 4" in caplog.text and "EXPLAIN" in caplog.text


def test_info_and_has():
    """info() describes the database for manifests; has() answers by logical name."""
    src = source()
    assert src.info() == {"engine": "fake", "server": "fake-1.0", "database": "EAR",
                          "profile": "ear6", "id_step": 2}
    assert src.has("jobs") and not src.has("nope")


def test_fake_engine_matches_every_profile():
    """The fake engine builds each EAR version's tables, and autodetection finds it."""
    for profile in ("ear4", "ear6", "ear7"):
        assert SqlSource(DBCONF, engine=FakeEngine(profile)).profile.name == profile


# == BundleSource ========================================================================

def test_bundle_reads_like_the_database(tmp_path):
    """scan/window/lookup filter by logical names on Parquet, with stored types."""
    src = BundleSource(make_bundle(str(tmp_path), "ear7"))
    assert src.id_step == 2 and src.tags == {"n3": "gpu"} and "Jobs" in src.provides
    got = list(src.scan("jobs", [Cond("user_id", "in", ["alice"]),
                                 Cond("app_name", "like_any", ["lmp\\_%"])]))
    assert values(got, "local_id") == [5] and got[0].schema.field("job_id").type == pa.uint32()
    assert values(src.scan("jobs", [Cond("job_id", "ranges", [(8, 9)])]), "job_id") == [8]
    assert values(src.lookup("gpu_signatures", "id", [56, 50, 99]), "id") == [50, 56]
    assert values(src.window("periodic_metrics", T0 + 100, T0 + 200), "id") == [1, 3]
    assert concat(src.scan("jobs", order=[("end_time", "descending")], limit=1))["job_id"].to_pylist() == [8]
    assert [t.num_rows for t in src.scan("jobs", [Cond("user_id", "=", "nobody")])] == [0]


def test_bundle_needs_a_raw_manifest(tmp_path):
    """No manifest: incomplete; another layout's bundle: not EAR's tables."""
    with pytest.raises(ConfigError, match="not a complete bundle"):
        BundleSource(str(tmp_path))
    make_bundle(str(tmp_path))
    m = json.loads((tmp_path / MANIFEST).read_text())
    (tmp_path / MANIFEST).write_text(json.dumps(dict(m, layout="eacct")))
    with pytest.raises(ConfigError, match="export raw"):
        BundleSource(str(tmp_path))


# == EacctSource =========================================================================

def eacct_file(path, rows):
    """Write an eacct -l -c file with the given ``{header: value}`` rows (other columns 0)."""
    names = HEADER.split(";")
    lines = [HEADER] + [";".join(str(r.get(h, 0)) for h in names) for r in rows]
    path.write_text("\n".join(lines) + "\n")
    return str(path)


def test_eacct_source_rebuilds_the_tables(tmp_path):
    """One CSV row per step and node becomes jobs, applications, signatures, power signatures and GPUs."""
    f = eacct_file(tmp_path / "a.csv", [
        dict(JOBID=7, STEPID=0, APPID=5, USERID="alice", NODENAME="n1", JOB_END_TIME=T0,
             EARL_ENABLED=1, CPI=0.5, NODEMGR_DC_NODE_POWER_W=300.5, GPU0_POWER_W=200,
             GPU1_POWER_W=210),
        dict(JOBID=7, STEPID=0, APPID=5, USERID="alice", NODENAME="n2", JOB_END_TIME=T0,
             EARL_ENABLED=0, NODEMGR_DC_NODE_POWER_W=290.0)])
    src = EacctSource([f])
    assert src.has("jobs") and not src.has("periodic_metrics") and not src.provides
    assert concat(src.scan("jobs"))["user_id"].to_pylist() == ["alice"]  # one step
    apps = concat(src.scan("applications"))
    assert apps["node_id"].to_pylist() == ["n1", "n2"] and apps["signature_id"].to_pylist() == [1, None]
    assert concat(src.lookup("power_signatures", "id", [2]))["dc_power"].to_pylist() == [290.0]
    sig = concat(src.lookup("signatures", "id", [1])).to_pylist()[0]
    assert (sig["cpi"], sig["min_gpu_sig_id"], sig["max_gpu_sig_id"]) == (0.5, 1, 2)
    with pytest.raises(ConfigError, match="no periodic_metrics"):
        list(src.window("periodic_metrics", None, None))
    with pytest.raises(ConfigError, match="raw"):
        list(src.dump("Jobs"))


# == Pseudonymized =======================================================================

def test_pseudonymized_filters_in_clear_and_yields_pseudonyms():
    """--users compares clear names; every user field read, logical or physical, is a pseudonym."""
    src = Pseudonymized(source("ear7"), b"k")
    jobs = concat(src.scan("jobs", [Cond("user_id", "in", ["bob"])]))
    assert jobs["user_id"].to_pylist() == [pseudonym(b"k", "bob")]
    assert jobs["user_acc"].to_pylist() == [pseudonym(b"k", "acc2")]
    dumped = concat(src.dump("Jobs"))
    assert pseudonym(b"k", "alice") in dumped["user_name"].to_pylist()
    assert src.salt_fingerprint == pseudonym(b"k", "ear-db-export") and src.has("jobs")
