"""The command line end to end, on the fake database (EAR 6): bundles, round trips, resume, errors."""
import json
import os
import subprocess
import sys

import pyarrow.parquet as pq
import pymysql
import pytest

from ear_db_export import cli
from ear_db_export.clean import pseudonym
from ear_db_export.config import DbConf, DbError
from ear_db_export.output import LOCK_FILE, MANIFEST, SUMS, Output, read_checkpoint
from ear_db_export.read.eacct import HEADER
from ear_db_export.read.sql import mariadb
from ear_db_export.selection import Window, epoch

import fakedata
from fakedb import make_engine

DAY = ["--from", "2026-07-04", "--to", "2026-07-04"]


@pytest.fixture
def db(tmp_path, monkeypatch):
    """CLI flags reading the fake database (EAR 6), with its ear.conf and a salt file."""
    monkeypatch.setattr(cli, "ENGINE", make_engine("ear6"))
    conf, salt = tmp_path / "ear.conf", tmp_path / "salt"
    conf.write_text(fakedata.EAR_CONF)
    salt.write_bytes(b"s3cret")
    return ["--conf", str(conf), "--salt-file", str(salt)]


def main(*argv):
    """Run the command line with these arguments; return its exit code."""
    return cli.main([str(a) for a in argv])


def manifest(out):
    """The parsed manifest of a bundle."""
    return json.loads((out / MANIFEST).read_text())


def parquet_files(root):
    """``{relative path: table}`` of every Parquet file under ``root``."""
    return {os.path.relpath(os.path.join(d, f), root): pq.ParquetFile(os.path.join(d, f)).read()
            for d, _, files in os.walk(root) for f in files if f.endswith(".parquet")}


def checksums_ok(out):
    """Tell whether ``sha256sum -c SHA256SUMS`` accepts the bundle."""
    return subprocess.run(["sha256sum", "--quiet", "-c", SUMS], cwd=out).returncode == 0


def test_raw_dump_is_a_complete_bundle(tmp_path, db):
    """Every table as stored, schema.sql, checksums; in clear here, so a salted run is refused after."""
    out = tmp_path / "b"
    assert main("export", "raw", "--out", out, "--conf", db[1], "--no-pseudonymize") == 0
    m = manifest(out)
    assert m["layout"] == "raw" and m["checkpoint"] is None and m["salt_fingerprint"] is None
    assert "Loops" in m["outputs"] and m["outputs"]["Periodic_metrics"]["rows"] == 4
    assert m["source"]["profile"] == "ear6" and m["source"]["id_step"] == 2
    assert pq.read_table(out / "Jobs.parquet")["user_id"].to_pylist()[0] == "alice"
    assert "CREATE TABLE `Jobs`" in (out / "schema.sql").read_text() and checksums_ok(out)
    assert main("export", "raw", "--out", out, *db) == 2  # another salt


def test_raw_linked_dump(tmp_path, db):
    """bob's job step, every row it points to, and its node metrics; names as pseudonyms."""
    out = tmp_path / "b"
    tables = ["Jobs", "Applications", "Signatures", "Power_signatures", "GPU_signatures",
              "Periodic_metrics"]
    assert main("export", "raw", *tables, "--users", "bob", "--out", out, *db) == 0
    got = {t: pq.read_table(out / (t + ".parquet")) for t in tables}
    assert got["Jobs"]["user_id"].to_pylist() == [pseudonym(b"s3cret", "bob")]
    assert got["Applications"]["node_id"].to_pylist() == ["n3"]
    assert got["Signatures"]["id"].to_pylist() == [102]
    assert got["GPU_signatures"]["id"].to_pylist() == [50, 54, 56]
    assert got["Periodic_metrics"]["id"].to_pylist() == [4]
    assert manifest(out)["filters"]["users"] == pseudonym(b"s3cret", "bob")  # no clear name anywhere
    assert checksums_ok(out)
    assert main("export", "raw", "--users", "bob", "--out", tmp_path / "all", *db) == 0
    assert "Loops" in manifest(tmp_path / "all")["outputs"]  # a filtered `all`: EAR's linked tables,
    assert "Learning_jobs" not in manifest(tmp_path / "all")["outputs"]  # but not Learning_*
    assert main("export", "raw", "Learning_jobs", "--users", "bob", "--out", tmp_path / "l", *db) == 0
    assert manifest(tmp_path / "l")["outputs"]["Learning_jobs"]["rows"] == 0  # named: copied whole


@pytest.mark.parametrize("layout, extra", [
    ("node_metrics", ["--service", "s"]), ("job_nodes", []), ("power_signatures", []),
    ("eacct", []), ("raw", ["Jobs", "Applications", "Periodic_metrics"])])
def test_layouts_from_a_bundle_equal_from_the_database(tmp_path, db, layout, extra):
    """A layout written from a raw bundle equals the same layout written from the database."""
    bundle, a, b = tmp_path / "bundle", tmp_path / "a", tmp_path / "b"
    assert main("export", "raw", "--out", bundle, *db) == 0
    assert main("export", layout, *extra, *DAY, "--out", a, *db) == 0
    assert main("export", layout, *extra, *DAY, "--out", b, "--bundle", bundle) == 0  # pseudonyms already
    got, want = parquet_files(b), parquet_files(a)
    assert sorted(got) == sorted(want) and all(got[p].equals(want[p]) for p in want)


def test_node_metrics_hive_and_resume(tmp_path, db):
    """--resume starts at --from, then at the checkpoint; up to date does nothing."""
    out = tmp_path / "nm"
    flags = ["--service", "s", "--hive", "--resume", "--from", "2026-07-04", "--out", out, *db]
    assert main("export", "node_metrics", "--to", "2026-07-05", *flags) == 0
    assert sorted(parquet_files(out)) == ["node_metrics/date=2026-07-04/part-0.parquet"]
    assert read_checkpoint(str(out)) == epoch(fakedata.DAY) + 2 * 86400
    finished = manifest(out)["finished"]
    assert main("export", "node_metrics", "--to", "2026-07-05", *flags) == 0  # up to date
    assert manifest(out)["finished"] == finished
    assert main("export", "node_metrics", "--to", "2026-07-06", *flags) == 0  # no rows that day
    assert read_checkpoint(str(out)) == epoch(fakedata.DAY) + 3 * 86400
    assert len(parquet_files(out)) == 1 and checksums_ok(out)
    rows = pq.read_table(out / "node_metrics", partitioning="hive")
    assert set(rows["service"].to_pylist()) == {"s"} and rows.num_rows == 20  # 24 values, 4 empty


def test_eacct_csv_and_the_eacct_source(tmp_path, db):
    """eacct's CSV layout; reading it back with --eacct writes the same file."""
    a, b = tmp_path / "a", tmp_path / "b"
    assert main("export", "eacct", "--as", "csv", "--out", a, *db) == 0
    text = (a / "eacct.csv").read_text()
    assert text.splitlines()[0] == HEADER and len(text.splitlines()) == 5
    assert main("export", "eacct", "--as", "csv", "--eacct", a / "eacct.csv", "--no-pseudonymize",
                "--out", b) == 0
    assert (b / "eacct.csv").read_text() == text
    assert main("export", "raw", "--eacct", a / "eacct.csv", "--no-pseudonymize",
                "--out", tmp_path / "c") == 2  # eacct files hold no tables as stored


def test_dry_run_writes_nothing(tmp_path, db):
    """A dry run reads no rows and creates no file or directory."""
    out = tmp_path / "d"
    assert main("export", "raw", "--dry-run", "--users", "bob", "--out", out, *db) == 0
    assert main("export", "node_metrics", "--service", "s", "--dry-run", *DAY, "--out", out, *db) == 0
    assert not out.exists()


@pytest.mark.parametrize("argv", [
    ["export", "node_metrics", "--service", "s", "--no-salt"],              # argparse: unknown flag
    ["export", "job_nodes", "--users", "bob"],                               # not a job_nodes filter
])
def test_flags_a_layout_does_not_take_are_refused(tmp_path, db, argv):
    """argparse refuses a flag the layout does not list (exit 2), instead of ignoring it."""
    with pytest.raises(SystemExit) as e:
        main(*argv, "--out", tmp_path, *db)
    assert e.value.code == 2


def test_errors_exit_2(tmp_path, db):
    """Configuration errors are one line and exit code 2."""
    out = tmp_path / "e"
    assert main("export", "job_nodes", "--out", out, "--conf", db[1]) == 2          # no salt
    assert main("export", "raw", "Nope", "--out", out, *db) == 2                    # no such table
    assert main("export", "raw", "--hive", "--out", out, *db) == 2                  # one file per table
    assert main("export", "raw", "--as", "csv", "--out", out, *db) == 2             # Parquet only
    assert main("export", "job_nodes", "--resume", "--out", out, *db) == 2         # needs --to
    assert main("export", "job_nodes", "--path", "../x.{ext}", "--out", out, *db) == 2
    assert main("export", "node_metrics", "--service", "s", "--as", "csv", "--out", out, *db) == 2
    with Output(str(out), "job_nodes", "{layout}.{ext}", "parquet", Window()):
        assert main("export", "job_nodes", "--out", out, *db) == 2                 # locked
    assert (out / LOCK_FILE).exists()


def test_user_filters_on_a_pseudonymised_bundle(tmp_path, db):
    """Names need the bundle's salt (then they match as the database does); pseudonyms need none."""
    bundle, want = tmp_path / "bundle", tmp_path / "want"
    assert main("export", "raw", "--out", bundle, *db) == 0
    assert main("export", "power_signatures", "--users", "bob", "--out", want, *db) == 0
    rows = pq.read_table(want / "power_signatures.parquet")
    assert rows.num_rows == 1
    by_name, by_pseudonym = tmp_path / "n", tmp_path / "p"
    assert main("export", "power_signatures", "--users", "bob", "--bundle", bundle, "--out", by_name,
                "--salt-file", db[3]) == 0
    assert main("export", "power_signatures", "--users", pseudonym(b"s3cret", "bob"),
                "--bundle", bundle, "--out", by_pseudonym) == 0
    assert pq.read_table(by_name / "power_signatures.parquet").equals(rows)
    assert pq.read_table(by_pseudonym / "power_signatures.parquet").equals(rows)
    assert manifest(by_name)["filters"]["users"] == pseudonym(b"s3cret", "bob")
    assert main("export", "power_signatures", "--users", "bob", "--bundle", bundle,
                "--out", tmp_path / "x") == 2                                    # a name, no salt
    other = tmp_path / "other"
    other.write_bytes(b"other")
    assert main("export", "power_signatures", "--users", "bob", "--bundle", bundle,
                "--out", tmp_path / "x", "--salt-file", other) == 2               # not the bundle's salt


def test_database_errors_are_one_line(tmp_path, db, monkeypatch, caplog):
    """A refused connection is one error line, exit 1; the driver's error is wrapped in DbError."""
    def refuse(*a, **kw):
        raise pymysql.err.OperationalError(1045, "Access denied for user 'u'@'h'")
    monkeypatch.setattr(pymysql, "connect", refuse)
    with pytest.raises(DbError, match="cannot connect to h:3306/EAR as u: Access denied"):
        mariadb.connect(DbConf("h", 3306, "EAR", "u", "pw"))
    monkeypatch.setattr(cli.ENGINE, "connect", lambda dbconf: mariadb.connect(dbconf))
    assert main("check", "--conf", db[1]) == 1
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1 and errors[0].exc_info is None and "Access denied" in errors[0].message


def test_the_command_uses_the_system_allocator():
    """Importing the command line before pyarrow makes Arrow use the system allocator, unless one is chosen."""
    code = "import ear_db_export.cli, pyarrow as pa; print(pa.default_memory_pool().backend_name)"
    env = {k: v for k, v in os.environ.items() if k != "ARROW_DEFAULT_MEMORY_POOL"}

    def pool(**extra):
        return subprocess.run([sys.executable, "-c", code], env=dict(env, **extra),
                              capture_output=True, text=True, check=True).stdout.strip()
    assert pool() == "system" and pool(ARROW_DEFAULT_MEMORY_POOL="mimalloc") == "mimalloc"


def test_version(capsys):
    """--version prints the program and its version."""
    with pytest.raises(SystemExit) as e:
        main("--version")
    assert e.value.code == 0 and capsys.readouterr().out.startswith("ear-db-export ")


def test_check(db, capsys):
    """check prints the session, the grants and the profile comparison."""
    assert main("check", "--conf", db[1]) == 0
    text = capsys.readouterr().out
    assert "session read-only: yes" in text and "GRANT SELECT" in text and "profile ear6" in text


def test_flags_from_a_file(tmp_path, db):
    """@FILE reads flags from a file, one per line."""
    args = tmp_path / "daily.args"
    args.write_text("--service=s\n--out=%s\n--conf=%s\n--salt-file=%s\n" % (tmp_path / "o", db[1], db[3]))
    assert main("export", "node_metrics", "@%s" % args, *DAY) == 0
    assert (tmp_path / "o" / "node_metrics.parquet").exists()
