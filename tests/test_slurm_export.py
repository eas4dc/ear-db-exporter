"""The slurm-db-export command line end to end, on the fake database: raw bundle, then jobs."""
import json
import logging
import os
import subprocess

import pyarrow.parquet as pq
import pytest

from ear_db_export.clean import pseudonym
from slurm_db_export import cli

from slurmfake import SlurmFakeEngine

WINDOW = ["--from", "2026-09-01", "--to", "2026-10-13"]


@pytest.fixture
def db(tmp_path, monkeypatch):
    """The --db-* flags of the fake database (Slurm 25.05), and a salt file."""
    engine = SlurmFakeEngine()
    monkeypatch.setattr(cli, "ENGINE", engine)
    pw, salt = tmp_path / "pw", tmp_path / "salt"
    pw.write_text("secret\n")
    salt.write_bytes(b"s3cret")
    return ["--db-host", "fake", "--db-user", "ro", "--db-password-file", str(pw),
            "--salt-file", str(salt)]


def main(*argv):
    """Run the command line; return its exit code."""
    return cli.main([str(a) for a in argv])


def tables(root):
    """``{relative path: rows as dicts}`` of every Parquet file under ``root``."""
    return {os.path.relpath(os.path.join(d, f), root): pq.read_table(os.path.join(d, f)).to_pylist()
            for d, _, files in os.walk(root) for f in files if f.endswith(".parquet")}


def test_raw_bundle_by_cluster_table_and_month(tmp_path, db):
    """Files per cluster/table/month, censored and deleted jobs kept, steps with their job, ids hashed."""
    out = tmp_path / "raw"
    assert main("export", "raw", *WINDOW, "--with-scripts", "--with-usage-hour", "--out", out,
                *db) == 0
    files = tables(out)
    jobs = files["lenox.hu/job_table/year=2026/month=09/part-0.parquet"]
    assert [j["job_db_inx"] for j in jobs] == [2, 3, 4, 5]
    assert [j["job_db_inx"] for j in files["lenox.hu/job_table/year=2026/month=10/part-0.parquet"]] == [6]
    assert jobs[0]["id_user"] == pseudonym(b"s3cret", "1000") and jobs[0]["account"] != "physics"
    steps = files["lenox.hu/step_table/year=2026/month=09/part-0.parquet"]
    assert sorted((s["job_db_inx"], s["id_step"]) for s in steps) == [(2, -5), (2, 0)]
    assert [e["reason"] for e in files["lenox.hu/event_table/year=2026/month=09/part-0.parquet"]] == \
        ["bad dimm"]
    assert len(files["lenox.hu/resv_table/year=2026/month=09/part-0.parquet"]) == 1
    assert len(files["lenox.hu/assoc_usage_hour_table/year=2026/month=09/part-0.parquet"]) == 1
    assert files["lenox.hu/job_script_table/year=2026/month=09/part-0.parquet"][0]["hash_inx"] == 1
    assoc = files["lenox.hu/assoc_table/year=2026/month=10/part-0.parquet"]  # snapshot: window's end
    assert "lineage" not in assoc[0] and "alice" not in {a["user"] for a in assoc}
    assert len(files["tres_table/year=2026/month=10/part-0.parquet"]) == 6
    m = json.loads((out / "manifest.json").read_text())
    assert m["tool"].startswith("slurm-db-export ") and m["layout"] == "raw"
    assert m["source"]["profile"] == "slurm20" and m["source"]["schema_version"] == 16
    assert m["source"]["columns"]["lenox.hu_job_table"]["missing"] == []
    assert m["outputs"]["lenox.hu/job_table"]["rows"] == 5
    assert m["filters"]["window"]["from"] == "2026-09-01T00:00:00+00:00"
    assert subprocess.run(["sha256sum", "--quiet", "-c", "SHA256SUMS"], cwd=out).returncode == 0


def test_rerun_rewrites_its_months(tmp_path, db):
    """Re-exporting a month replaces its file: a job that changed is not duplicated."""
    out = tmp_path / "raw"
    assert main("export", "raw", *WINDOW, "--out", out, *db) == 0
    cli.ENGINE.db.execute("UPDATE `lenox.hu_job_table` SET state = 1, time_start = 1 WHERE job_db_inx = 3")
    cli.ENGINE.db.commit()
    assert main("export", "raw", "--from", "2026-09-01", "--to", "2026-09-30", "--out", out, *db) == 0
    jobs = tables(out)["lenox.hu/job_table/year=2026/month=09/part-0.parquet"]
    assert [(j["job_db_inx"], j["state"]) for j in jobs if j["job_db_inx"] == 3] == [(3, 1)]
    assert len(tables(out)["lenox.hu/job_table/year=2026/month=10/part-0.parquet"]) == 1  # kept


def test_from_must_start_a_month(tmp_path, db):
    """With {month} in the path, a window from the 15th would truncate a month file."""
    assert main("export", "raw", "--from", "2026-09-15", "--to", "2026-10-13",
                "--out", tmp_path / "x", *db) == 2
    assert main("export", "raw", "--from", "2026-09-15", "--to", "2026-10-13", "--path",
                "{table}/{date}.parquet", "--out", tmp_path / "y", *db) == 0


def test_salt_or_explicit_opt_out(tmp_path, db):
    """Without --salt-file the run needs --no-pseudonymize; then ids are as stored."""
    flags = db[:-2]
    assert main("export", "raw", *WINDOW, "--out", tmp_path / "a", *flags) == 2
    assert main("export", "raw", *WINDOW, "--out", tmp_path / "b", "--no-pseudonymize", *flags) == 0
    jobs = tables(tmp_path / "b")["lenox.hu/job_table/year=2026/month=09/part-0.parquet"]
    assert jobs[0]["id_user"] == 1000 and jobs[0]["account"] == "physics"


def test_dry_run_writes_nothing(tmp_path, db, caplog):
    """A dry run logs SQL, EXPLAIN and counts, and creates no directory."""
    out = tmp_path / "dry"
    with caplog.at_level(logging.INFO):
        assert main("export", "raw", *WINDOW, "--dry-run", "--out", out, *db) == 0
    assert not out.exists()
    assert "[dry-run] lenox.hu_job_table rows: 5" in caplog.messages


def test_jobs_from_the_raw_bundle(tmp_path, db):
    """jobs: decoded TRES, state names, wait/run times, by month; the same pseudonyms."""
    raw = tmp_path / "raw"
    assert main("export", "raw", *WINDOW, "--out", raw, *db) == 0
    out = tmp_path / "jobs"
    assert main("export", "jobs", "--bundle", raw, "--out", out) == 0
    files = tables(out)
    sept = {j["job_db_inx"]: j for j in files["lenox.hu/jobs/year=2026/month=09/part-0.parquet"]}
    j = sept[2]
    assert (j["wait_s"], j["run_s"], j["timelimit_s"], j["state_name"]) == (540, 3600, 7200, "COMPLETED")
    assert (j["req_cpu"], j["alloc_billing"], j["alloc_gres_gpu"], j["mem_req_mb"]) == (4, 4, None, 8000)
    assert sept[3]["run_s"] is None and sept[3]["state_name"] == "PENDING"
    assert sept[4]["time_start_ts"] is None and sept[4]["state_name"] == "CANCELLED"
    october = files["lenox.hu/jobs/year=2026/month=10/part-0.parquet"][0]
    assert (october["alloc_gres_gpu"], october["state_name"], october["timelimit_s"]) == \
        (2, "REQUEUED", None)
    assert j["id_user"] == pseudonym(b"s3cret", "1000")
    m = json.loads((out / "manifest.json").read_text())
    assert m["layout"] == "jobs" and m["salt_fingerprint"] == pseudonym(b"s3cret", "slurm-db-export")


def test_jobs_from_a_slurm17_bundle(tmp_path, db, monkeypatch):
    """A 17.11 database: raw keeps pack_job_id, jobs names it het_job_id."""
    monkeypatch.setattr(cli, "ENGINE", SlurmFakeEngine(release="17.11", version=5))
    raw, out = tmp_path / "raw", tmp_path / "jobs"
    assert main("export", "raw", *WINDOW, "--out", raw, *db) == 0
    assert "pack_job_id" in tables(raw)["lenox.hu/job_table/year=2026/month=09/part-0.parquet"][0]
    assert main("export", "jobs", "--bundle", raw, "--out", out) == 0
    assert "het_job_id" in tables(out)["lenox.hu/jobs/year=2026/month=09/part-0.parquet"][0]


def test_jobs_refuses_a_non_bundle(tmp_path):
    """jobs needs a finished raw bundle of slurm-db-export."""
    assert main("export", "jobs", "--bundle", tmp_path, "--out", tmp_path / "o") == 2


def test_check(db, capsys):
    """check: read-only session, grants, the clusters and the profile comparison."""
    assert main("check", *db[:-2]) == 0
    text = capsys.readouterr().out
    assert "session read-only: yes" in text and "clusters: lenox.hu" in text
    assert "lenox.hu_job_table: OK" in text
