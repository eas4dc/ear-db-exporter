"""Commands end to end on the fake database: export bundles, view, periods and watermark."""
import datetime as dt
import json
import os
import subprocess
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ear_db_export import cli, schedule, write
from ear_db_export.canonical import pseudonym
from ear_db_export.config import ConfigError

import fakedata
from fakedb import make_engine
from test_golden import EAR_CONF, parquet_files

DAY = fakedata.DAY


@pytest.fixture
def db(tmp_path, monkeypatch):
    """CLI flags reading the fake database (ear6), with a salt file."""
    monkeypatch.setattr(cli, "ENGINE", make_engine("ear6"))
    conf, salt = tmp_path / "ear.conf", tmp_path / "salt"
    conf.write_text(EAR_CONF)
    salt.write_bytes(b"s3cret")
    return ["--conf", str(conf), "--salt-file", str(salt)]


def main(*argv):
    return cli.main([str(a) for a in argv])


# == export ===========================================================================

def test_export_is_a_complete_checked_bundle(tmp_path, db):
    out = tmp_path / "b"
    assert main("export", "--users", "bob", "--tables", "jobs,periodic", "--out", out, *db) == 0
    m = json.loads((out / write.MANIFEST).read_text())
    assert sorted(m["tables"]) == ["Applications", "GPU_signatures", "Jobs", "Periodic_metrics",
                                   "Power_signatures", "Signatures"]
    assert m["source"]["profile"] == "ear6" and m["source"]["id_step"] == 2
    assert m["selection"]["users"] == [pseudonym(b"s3cret", "bob")]  # no clear name anywhere
    assert m["pseudonymized"] and m["salt_fingerprint"] == pseudonym(b"s3cret", "ear-db-export")
    jobs = pq.read_table(out / "Jobs.parquet")
    assert jobs["user_id"].to_pylist() == [pseudonym(b"s3cret", "bob")]
    assert subprocess.run(["sha256sum", "--quiet", "-c", write.SUMS], cwd=out).returncode == 0
    assert "CREATE TABLE `Jobs`" in (out / write.SCHEMA).read_text()


def test_export_round_trip_views_are_equal(tmp_path, db):
    """A view from a bundle equals the same view from the database."""
    bundle, a, b = tmp_path / "b", tmp_path / "a", tmp_path / "c"
    day = ["--every", "day", "--from", DAY, "--to", DAY]
    assert main("export", "--tables", "jobs,periodic", "--out", bundle, *day, *db) == 0
    assert (bundle / "Jobs" / "date=2026-07-04" / "part-0.parquet").exists()
    names = ["node_metrics", "job_nodes", "app_signatures", "gpu_signatures"]
    assert main("view", *names, "--out", a, *day, *db) == 0
    assert main("view", *names, "--out", b, *day, "--bundle", bundle) == 0  # no salt: pseudonyms
    got, want = parquet_files(b), parquet_files(a)
    assert sorted(got) == sorted(want) and all(got[p].equals(want[p]) for p in want)


def test_export_without_filters_needs_no_profile_or_salt_in_clear(tmp_path, db):
    out = tmp_path / "b"
    assert main("export", "--tables", "all", "--out", out, "--conf", db[1], "--no-pseudonymize") == 0
    m = json.loads((out / write.MANIFEST).read_text())
    assert "Loops" in m["tables"] and m["tables"]["Periodic_metrics"]["rows"] == 4
    assert not m["pseudonymized"]
    assert pq.read_table(out / "Jobs.parquet")["user_id"].to_pylist()[0] == "alice"
    # adding to this bundle with a salt is refused
    assert main("export", "--out", out, *db) == 2


def test_csv_output(tmp_path, db):
    out = tmp_path / "c"
    assert main("view", "job_nodes", "--as", "csv", "--from", DAY, "--to", DAY, "--out", out,
                *db) == 0
    assert (out / "job_nodes.csv").read_text().startswith("job_id,step_id,local_id,")
    assert main("view", "node_metrics", "--as", "csv", "--format", "node_metrics=long",
                "--format-opt", "service=x", "--out", out, *db) == 2  # map column: not CSV


def test_dry_run_writes_nothing(tmp_path, db):
    out = tmp_path / "d"
    assert main("export", "--dry-run", "--users", "bob", "--out", out, *db) == 0
    assert main("view", "node_metrics", "--dry-run", "--every", "day", "--from", DAY, "--to", DAY,
                "--out", out, *db) == 0
    assert not out.exists()


def test_second_run_on_same_out_is_refused(tmp_path, db):
    with write.lock(str(tmp_path)):
        assert main("view", "node_metrics", "--out", tmp_path, *db) == 2


def test_errors_exit_2(tmp_path, db):
    assert main("view", "node_metrics", "--out", tmp_path, "--conf", db[1]) == 2  # no salt
    assert main("export", "--tables", "nope", "--out", tmp_path, *db) == 2
    assert main("view", "job_nodes", "--format", "node_metrics=long", "--out", tmp_path, *db) == 2


# == periods and watermark ==============================================================

def args(**kw):
    base = dict(every=None, incremental=False, from_=None, to=None, lag_days=2, state=None,
                update_state=False, overwrite=False, dry_run=False)
    return SimpleNamespace(**dict(base, **kw))


SOURCE = SimpleNamespace(oldest_day=lambda: dt.date(2026, 10, 1))
TODAY = dt.date(2026, 10, 9)


def days(out, **kw):
    return [p.partition.get("date") or p.partition.get("month")
            for p in schedule.periods(args(**kw), SOURCE, str(out), ["node_metrics"], today=TODAY)]


def test_incremental_starts_at_horizon_and_moves_watermark(tmp_path):
    got = days(tmp_path, every="day", incremental=True)
    assert got[0] == dt.date(2026, 10, 1) and got[-1] == dt.date(2026, 10, 7)  # lag 2
    assert schedule.load_watermark(str(tmp_path / schedule.STATE_FILE)) == dt.date(2026, 10, 7)
    assert days(tmp_path, every="day", incremental=True) == []  # up to date


def test_watermark_never_moves_backwards_and_only_on_request(tmp_path):
    schedule.save_watermark(str(tmp_path / schedule.STATE_FILE), dt.date(2026, 10, 5))
    days(tmp_path, every="day", from_=dt.date(2026, 10, 2), to=dt.date(2026, 10, 3),
         update_state=True)
    days(tmp_path, every="day", from_=dt.date(2026, 10, 6), to=dt.date(2026, 10, 6))
    assert schedule.load_watermark(str(tmp_path / schedule.STATE_FILE)) == dt.date(2026, 10, 5)


def test_existing_partitions_are_skipped_unless_overwrite(tmp_path):
    path = write.partition_path(str(tmp_path), "node_metrics", {"date": dt.date(2026, 10, 2)})
    os.makedirs(os.path.dirname(path))
    open(path, "w").close()
    kw = dict(every="day", from_=dt.date(2026, 10, 2), to=dt.date(2026, 10, 3))
    assert days(tmp_path, **kw) == [dt.date(2026, 10, 3)]
    assert len(days(tmp_path, overwrite=True, **kw)) == 2


def test_months_whole_and_finished_default_last_12(tmp_path):
    got = days(tmp_path, every="month")
    assert got[0] == dt.date(2025, 10, 1) and got[-1] == dt.date(2026, 9, 1) and len(got) == 12
    got = days(tmp_path, every="month", from_=dt.date(2026, 9, 15), to=TODAY)
    assert got == [dt.date(2026, 9, 1)]  # October has not ended


@pytest.mark.parametrize("kw, match", [
    (dict(incremental=True), "need --every"),
    (dict(update_state=True), "need --every"),
    (dict(every="day"), "needs --from and --to"),
    (dict(every="day", incremental=True, from_=DAY), "no --from"),
    (dict(every="day", from_=dt.date(2026, 7, 5), to=DAY), "after"),
    (dict(every="day", from_=dt.datetime(2026, 7, 4, 1), to=DAY), "whole days"),
])
def test_period_flag_errors(tmp_path, kw, match):
    with pytest.raises(ConfigError, match=match):
        days(tmp_path, **kw)


def test_no_every_is_one_output(tmp_path):
    assert list(schedule.periods(args(), SOURCE, str(tmp_path), [])) == [
        schedule.Period(None, None, {})]


def test_months_and_defaults():
    assert schedule.default_range(dt.date(2026, 10, 1)) == (dt.date(2025, 10, 1), dt.date(2026, 9, 30))
    assert schedule.months(dt.date(2025, 12, 15), dt.date(2026, 2, 3)) == [
        dt.date(2025, 12, 1), dt.date(2026, 1, 1), dt.date(2026, 2, 1)]


def test_parquet_atomic_overwrite(tmp_path):
    path = str(tmp_path / "a" / "x.parquet")
    t = pa.table({"v": [1, 2]})
    assert write.parquet(path, [t])[0] == 2
    write.parquet(path, [t.slice(0, 1)])
    assert pq.read_table(path).num_rows == 1

    def boom():
        yield t
        raise RuntimeError
    with pytest.raises(RuntimeError):
        write.parquet(path, boom())
    assert pq.read_table(path).num_rows == 1 and not (tmp_path / "a" / "x.parquet.tmp").exists()


def test_partition_path():
    assert write.partition_path("o", "Jobs") == "o/Jobs.parquet"
    assert write.partition_path("o", "node_metrics", {"date": dt.date(2026, 7, 4)}, "csv") == \
        "o/node_metrics/date=2026-07-04/part-0.csv"
    assert write.partition_path("o", "app_signatures", {"month": dt.date(2026, 6, 15)}) == \
        "o/app_signatures/month=2026-06/part-0.parquet"
