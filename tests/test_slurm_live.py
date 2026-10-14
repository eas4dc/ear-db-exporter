"""Against a live slurmdbd database, read-only. Run:

    SLURM_EXPORT_TEST_ARGS="--db-host slurmmaster --db-user RO_USER --db-password-file ~/.slurm_ro_pw" \\
        SLURM_EXPORT_TEST_CLUSTER=lenox.hu uv run pytest -m db tests/test_slurm_live.py

SLURM_EXPORT_TEST_DAY (YYYY-MM-DD, default yesterday) picks the day exported.
Every reference query is bounded by a ``job_db_inx`` range: ``time_submit``
leads no index, so a plain count would scan the whole job table.
"""
import datetime as dt
import json
import os
import shlex

import pyarrow.parquet as pq
import pytest

from slurm_db_export import cli
from slurm_db_export.read import SLACK, SlurmSource
from ear_db_export.selection import epoch

ARGS = shlex.split(os.path.expanduser(os.environ.get("SLURM_EXPORT_TEST_ARGS", "")))
CLUSTER = os.environ.get("SLURM_EXPORT_TEST_CLUSTER")
DAY = os.environ.get("SLURM_EXPORT_TEST_DAY",
                     (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)).isoformat())
pytestmark = [pytest.mark.db, pytest.mark.skipif(not (ARGS and CLUSTER),
                                                 reason="SLURM_EXPORT_TEST_ARGS/_CLUSTER unset")]


@pytest.fixture(scope="module")
def src():
    """An open SlurmSource with the test flags."""
    args = cli.parser().parse_args(["check"] + ARGS)
    s = SlurmSource(cli.dbconf(args), args.profile)
    yield s
    s.close()


def test_check(capsys):
    """check: a read-only session, grants without password hashes, the profile matches."""
    assert cli.main(["check", "--cluster", CLUSTER] + ARGS) == 0
    out = capsys.readouterr().out
    assert "session read-only: yes" in out and "GRANT" in out and "IDENTIFIED" not in out


def test_session_is_read_only(src):
    """The server refuses writes in the session."""
    assert src.engine.read_only(src.conn)


def test_day_misses_no_job(src, tmp_path):
    """One day of jobs equals a count in a job_db_inx window 6x wider than SLACK; jobs decodes it."""
    raw = tmp_path / "raw"
    assert cli.main(["export", "raw", "--cluster", CLUSTER, "--from", DAY, "--to", DAY,
                     "--path", "{table}.{ext}", "--no-pseudonymize", "--out", str(raw)] + ARGS) == 0
    rows = pq.read_metadata(raw / CLUSTER / "job_table.parquet").num_rows
    start = epoch(dt.date.fromisoformat(DAY))
    table, key, q = "%s_job_table" % CLUSTER, "job_db_inx", src.engine.quote
    lo, hi = src._id_range(table, key, "time_submit", start - 5 * SLACK, start + 86400 + 5 * SLACK)
    want = src.engine.scalar(src.conn, "SELECT COUNT(*) FROM %s WHERE %s BETWEEN %%s AND %%s AND "
                             "time_submit >= %%s AND time_submit < %%s" % (q(table), q(key)),
                             (lo, hi, start, start + 86400))
    assert rows == want
    manifest = json.loads((raw / "manifest.json").read_text())
    assert manifest["source"]["profile"] == src.profile.name
    assert cli.main(["export", "jobs", "--bundle", str(raw), "--path", "{table}.{ext}",
                     "--out", str(tmp_path / "jobs")]) == 0
    jobs = pq.read_table(tmp_path / "jobs" / CLUSTER / "jobs.parquet")
    assert jobs.num_rows == rows and "state_name" in jobs.column_names


def test_dry_run_writes_nothing(tmp_path):
    """A dry run prints its queries and EXPLAIN, and writes nothing."""
    out = tmp_path / "dry"
    first = dt.date.fromisoformat(DAY).replace(day=1).isoformat()
    assert cli.main(["export", "raw", "--cluster", CLUSTER, "--from", first, "--to", DAY,
                     "--dry-run", "--out", str(out)] + ARGS) == 0
    assert not out.exists()
