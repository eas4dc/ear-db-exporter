"""Against a live EAR DB. Run: EAR_EXPORT_TEST_CONF=$EAR_ETC/ear/ear.conf pytest -m db

EAR_EXPORT_TEST_DAY (YYYY-MM-DD) and EAR_EXPORT_TEST_MONTH (YYYY-MM) pick
partitions with data. Counts are checked against plain full-scan SQL, which
also proves the PK id window of `periodic` misses nothing.
"""
import datetime as dt
import glob
import json
import os
import random
import re

import pyarrow.parquet as pq
import pytest

from ear_db_export import cli, config
from ear_db_export.canonical import NODE_METRICS, pseudonym
from ear_db_export.read.sql import SqlReader, mariadb as db
from ear_db_export.schedule import day_epoch

CONF = os.environ.get("EAR_EXPORT_TEST_CONF")
DAY = dt.date.fromisoformat(os.environ.get("EAR_EXPORT_TEST_DAY", "2026-07-04"))
MONTH = os.environ.get("EAR_EXPORT_TEST_MONTH", "2026-06")
pytestmark = [pytest.mark.db, pytest.mark.skipif(not CONF, reason="EAR_EXPORT_TEST_CONF unset")]


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    out = tmp_path_factory.mktemp("out")
    salt = out / "salt"
    salt.write_bytes(b"test-salt")
    reader = SqlReader(config.load(CONF)[0])
    yield out, ["--conf", CONF, "--out", str(out), "--salt-file", str(salt)], reader.conn, reader.profile
    reader.close()


def sql_count(conn, sql, params):
    return db.scalar(conn, sql, params)


def jobs_apps_sql(p, select, extra=""):
    """Reference query: Jobs JOIN Applications [extra joins] WHERE Jobs.end_time in window."""
    j, a = p.columns["jobs"], p.columns["applications"]
    on = " AND ".join("j.`%s` = a.`%s`" % (j[k], a[k]) for k in ("job_id", "step_id", "local_id")
                      if k in j)
    return "SELECT %s FROM `%s` j JOIN `%s` a ON %s %s WHERE j.`%s` >= %%s AND j.`%s` < %%s" % (
        select, p.tables["jobs"], p.tables["applications"], on, extra, j["end_time"], j["end_time"])


def one(pattern):
    files = glob.glob(pattern)
    assert len(files) == 1, files
    return files[0]


def test_check_schema(env, capsys):
    assert cli.main(env[1] + ["check-schema"]) == 0
    out = capsys.readouterr().out
    assert "session read-only: yes" in out and "GRANT" in out and "IDENTIFIED" not in out


def test_session_is_read_only(env):
    assert db.read_only(env[2])


def test_dump_tables(env, tmp_path):
    """Dump two tables as stored: row counts, exact types, schema.sql, pseudonymised users."""
    out, base, conn, p = env
    args = base[:]
    args[args.index("--out") + 1] = str(tmp_path)
    tables = [p.tables["jobs"], p.tables["power_signatures"]]
    count = lambda t: db.scalar(conn, "SELECT COUNT(*) FROM `%s`" % t)
    before = {t: count(t) for t in tables}
    assert cli.main(args + ["dump", "--tables", ",".join(tables)]) == 0
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    ddl = (tmp_path / "schema.sql").read_text()
    for t in tables:
        meta = pq.read_metadata(tmp_path / (t + ".parquet"))
        assert before[t] <= meta.num_rows == manifest["tables"][t]["rows"] <= count(t)
        assert "CREATE TABLE `%s`" % t in ddl
    jobs = pq.read_table(tmp_path / (tables[0] + ".parquet"))
    users = [u for u in jobs[p.columns["jobs"]["user_id"]].to_pylist() if u is not None]
    assert users and all(re.fullmatch("[0-9a-f]{16}", u) for u in users)


def test_dry_run_writes_nothing(env, tmp_path):
    args = env[1][:]
    args[args.index("--out") + 1] = str(tmp_path)
    assert cli.main(args + ["--dry-run", "periodic", "--from", str(DAY), "--to", str(DAY)]) == 0
    assert os.listdir(tmp_path) == []


def test_periodic_day(env, tmp_path):
    out, base, conn, p = env
    days = ["periodic", "--from", str(DAY), "--to", str(DAY)]
    run = base + ["--format", "node_metrics=long", "--format", "job_nodes=allocations",
                  "--format-opt", "service=test"] + days
    assert cli.main(run) == 0
    m_file = one(str(out / "node_metrics" / ("date=%s" % DAY) / "*.parquet"))
    a_file = one(str(out / "job_nodes" / ("date=%s" % DAY) / "*.parquet"))
    pm = p.columns["periodic_metrics"]
    terms = []
    for col, _, drop_zero in NODE_METRICS:
        if col in pm:
            terms.append("SUM(`%s` IS NOT NULL%s)" % (pm[col], " AND `%s` <> 0" % pm[col] if drop_zero else ""))
    # One scan: interval count (wide rows) and valid values (long rows)
    intervals, expected = db.rows(conn, "SELECT COUNT(*), %s FROM `%s` WHERE `%s` >= %%s AND `%s` < %%s" % (
        " + ".join(terms), p.tables["periodic_metrics"], pm["end_time"], pm["end_time"]),
        (day_epoch(DAY), day_epoch(DAY + dt.timedelta(1))))[0]
    metrics = pq.read_table(m_file)
    assert metrics.num_rows == int(expected) > 0

    # Default format: one wide row per interval
    wide_out = base[:]
    wide_out[wide_out.index("--out") + 1] = str(tmp_path)
    assert cli.main(wide_out + days) == 0
    wide = pq.read_table(one(str(tmp_path / "node_metrics" / ("date=%s" % DAY) / "*.parquet")))
    assert wide.num_rows == intervals

    expected = sql_count(conn, jobs_apps_sql(p, "COUNT(*)"),
                         (day_epoch(DAY), day_epoch(DAY + dt.timedelta(1))))
    alloc = pq.read_table(a_file)
    assert alloc.num_rows == expected

    # Re-run: same file content, overwritten not duplicated
    assert cli.main(run) == 0
    assert pq.read_table(m_file).equals(metrics) and pq.read_table(a_file).equals(alloc)
    assert len(glob.glob(str(out / "node_metrics" / "**" / "*.parquet"), recursive=True)) == 1


def _month_window():
    start = dt.date.fromisoformat(MONTH + "-01")
    end = (start.replace(day=28) + dt.timedelta(4)).replace(day=1)
    return start, end - dt.timedelta(1), (day_epoch(start), day_epoch(end))


def test_power_signatures_month(env):
    out, base, conn, p = env
    frm, to, window = _month_window()
    assert cli.main(base + ["power-signatures", "--from", str(frm), "--to", str(to), "--overwrite"]) == 0
    t = pq.read_table(one(str(out / "power_signatures" / ("month=" + MONTH) / "*")))
    assert t.num_rows == sql_count(conn, jobs_apps_sql(p, "COUNT(*)"), window) > 0

    # Spot-check 5 random rows field by field against SQL
    j, a, ps = p.columns["jobs"], p.columns["applications"], p.columns["power_signatures"]
    sql = jobs_apps_sql(
        p, "j.`%s`, j.`%s`, a.`%s`, %s" % (j["user_id"], j["end_time"], a["node_id"],
                                           ", ".join("s.`%s`" % ps[c] for c in ps if c != "id")),
        "LEFT JOIN `%s` s ON s.`%s` = a.`%s`" % (p.tables["power_signatures"], ps["id"],
                                                 a["power_signature_id"]))
    sql += " AND j.`%s`=%%s AND j.`%s`=%%s AND j.`%s`=%%s AND a.`%s`=%%s" % (
        j["job_id"], j["step_id"], j["local_id"], a["node_id"])
    rows = t.to_pylist()
    for r in random.Random(0).sample(rows, min(5, len(rows))):
        keys = [r["job_job_id"], r["job_step_id"], r["job_local_id"], r["node"]]
        db_row = db.rows(conn, sql, list(window) + keys)[0]
        assert r["job_user_id"] == pseudonym(b"test-salt", db_row[0])
        assert int(r["job_end_time"].timestamp()) == db_row[1] and r["node"] == db_row[2]
        got = [r["ps_" + c] for c in ps if c != "id"]
        assert got == pytest.approx(list(db_row[3:]), rel=1e-6)


def test_app_signatures_month(env):
    out, base, conn, p = env
    frm, to, window = _month_window()
    assert cli.main(base + ["app-signatures", "--from", str(frm), "--to", str(to), "--overwrite"]) == 0
    t = pq.read_table(one(str(out / "app_signatures" / ("month=" + MONTH) / "*")))
    g = pq.read_table(one(str(out / "gpu_signatures" / ("month=" + MONTH) / "*")))
    a, s, gq = p.columns["applications"], p.columns["signatures"], p.columns["gpu_signatures"]
    sig_join = "JOIN `%s` s ON s.`%s` = a.`%s`" % (p.tables["signatures"], s["id"], a["signature_id"])
    assert t.num_rows == sql_count(conn, jobs_apps_sql(p, "COUNT(*)", sig_join), window) > 0
    step = db.id_step(conn)
    gpu_join = sig_join + (" JOIN `%s` g ON g.`%s` BETWEEN s.`%s` AND s.`%s`"
                           " AND MOD(g.`%s` - s.`%s`, %d) = 0") % (
        p.tables["gpu_signatures"], gq["id"], s["min_gpu_sig_id"], s["max_gpu_sig_id"],
        gq["id"], s["min_gpu_sig_id"], step)
    assert g.num_rows == sql_count(conn, jobs_apps_sql(p, "COUNT(*)", gpu_join), window)
