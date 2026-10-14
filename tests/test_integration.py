"""Against a live EAR DB, read-only. Run: EAR_EXPORT_TEST_CONF=$EAR_ETC/ear/ear.conf pytest -m db

EAR_EXPORT_TEST_DAY (YYYY-MM-DD) picks a day with data; EAR_EXPORT_TEST_DB
overrides ear.conf's database (e.g. EAR70). Every reference query is bounded by
a primary-key range or reads ``Jobs`` (small): test databases may have no
``Periodic_metrics`` time index, and a full scan there takes minutes.
"""
import dataclasses
import datetime as dt
import json
import os
import subprocess

import pyarrow.parquet as pq
import pytest

from ear_db_export import cli, config
from ear_db_export.clean import typed
from ear_db_export.read import concat
from ear_db_export.read.bundle import BundleSource
from ear_db_export.read.sql import SLACK, SqlSource, mariadb as db
from ear_db_export.selection import epoch

CONF = os.environ.get("EAR_EXPORT_TEST_CONF")
DAY = os.environ.get("EAR_EXPORT_TEST_DAY", "2026-07-04")
DB = os.environ.get("EAR_EXPORT_TEST_DB")
pytestmark = [pytest.mark.db, pytest.mark.skipif(not CONF, reason="EAR_EXPORT_TEST_CONF unset")]


def dbconf():
    """The DbConf of EAR_EXPORT_TEST_CONF, with EAR_EXPORT_TEST_DB's database if set."""
    conf = config.load(CONF)[0]
    return dataclasses.replace(conf, database=DB) if DB else conf


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    """``(out dir, CLI flags for the test database with a salt, an open SqlSource)``."""
    out = tmp_path_factory.mktemp("out")
    salt = out / "salt"
    salt.write_bytes(b"test-salt")
    c = dbconf()
    pw = out / "pw"
    pw.write_text(c.password)
    flags = ["--conf", CONF, "--db-host", c.host, "--db-port", str(c.port), "--db-name", c.database,
             "--db-user", c.user, "--db-password-file", str(pw), "--salt-file", str(salt)]
    src = SqlSource(c)
    yield out, flags, src
    src.close()


def test_check(env, capsys):
    """check reports a read-only session and the grants, without password hashes."""
    assert cli.main(["check"] + env[1][:-2]) == 0
    out = capsys.readouterr().out
    assert "session read-only: yes" in out and "GRANT" in out and "IDENTIFIED" not in out


def test_session_is_read_only(env):
    """The server refuses writes in the session."""
    assert db.read_only(env[2].conn)


def test_raw_tables_as_stored(env, tmp_path):
    """Two tables whole: row counts, exact types, schema.sql, checksums, pseudonymised users."""
    _, flags, src = env
    p = src.profile
    tables = [p.tables["jobs"], p.tables["power_signatures"]]
    count = lambda t: db.scalar(src.conn, "SELECT COUNT(*) FROM `%s`" % t)  # noqa: E731
    before = {t: count(t) for t in tables}
    assert cli.main(["export", "raw", *tables, "--out", str(tmp_path)] + flags) == 0
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    for t in tables:
        meta = pq.read_metadata(tmp_path / (t + ".parquet"))
        assert before[t] <= meta.num_rows == manifest["outputs"][t]["rows"] <= count(t)
    users = pq.read_table(tmp_path / (tables[0] + ".parquet"), columns=[p.col("jobs", "user_id")])
    assert all(len(u) == 16 for u in users.column(0).to_pylist() if u)
    assert subprocess.run(["sha256sum", "--quiet", "-c", "SHA256SUMS"], cwd=tmp_path).returncode == 0


def test_raw_newest_job_keeps_float_values(env, tmp_path):
    """One job and its links; FLOAT values read back as pymysql returns them."""
    _, flags, src = env
    p = src.profile
    job = db.scalar(src.conn, "SELECT MAX(`%s`) FROM `%s`" % (p.col("jobs", "job_id"), p.tables["jobs"]))
    tables = [p.tables[t] for t in ("jobs", "applications", "power_signatures")]
    assert cli.main(["export", "raw", *tables, "--jobs", str(job), "--out", str(tmp_path)] + flags) == 0
    b = BundleSource(str(tmp_path))
    assert set(concat(b.scan("jobs"))["job_id"].to_pylist()) == {job}
    ps = typed("power_signatures", concat(b.scan("power_signatures")), ["id", "dc_power"]).to_pylist()
    for r in ps[:5]:
        want = db.scalar(src.conn, "SELECT `%s` FROM `%s` WHERE `id` = %%s" % (
            p.col("power_signatures", "dc_power"), p.tables["power_signatures"]), (r["id"],))
        assert r["dc_power"] == pytest.approx(want, rel=1e-6) and \
            repr(r["dc_power"]) == repr(float(want)), r


def test_raw_day_misses_no_row(env, tmp_path):
    """Periodic_metrics of one day equals a count in a primary-key window 7x wider than SLACK."""
    _, flags, src = env
    p, q = src.profile, src.engine.quote
    table = p.tables["periodic_metrics"]
    assert cli.main(["export", "raw", table, "--from", DAY, "--to", DAY, "--out", str(tmp_path)]
                    + flags) == 0
    rows = pq.read_metadata(tmp_path / (table + ".parquet")).num_rows
    start = epoch(dt.date.fromisoformat(DAY))
    key, end = p.col("periodic_metrics", "id"), p.col("periodic_metrics", "end_time")
    lo, hi = src._id_window(table, key, end, start - 6 * SLACK, start + 86400 + 6 * SLACK)
    want = db.scalar(src.conn, "SELECT COUNT(*) FROM %s WHERE %s BETWEEN %%s AND %%s AND %s >= %%s "
                     "AND %s < %%s" % (q(table), q(key), q(end), q(end)), (lo, hi, start, start + 86400))
    assert rows == want


def test_dry_run_writes_nothing(env, tmp_path):
    """A dry run on the live database prints its queries and writes nothing."""
    out = tmp_path / "dry"
    assert cli.main(["export", "node_metrics", "--service", "test", "--dry-run", "--from", DAY,
                     "--to", DAY, "--hive", "--out", str(out)] + env[1]) == 0
    assert not out.exists()
