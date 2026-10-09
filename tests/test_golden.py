"""The view files must stay what 0.3.0 wrote (tests/golden/), byte for byte in content.

tests/golden/ was written by the 0.3.0 pipeline from the records of fakedata.py
(commit "tests: golden 0.3.0 view outputs"); here the 0.4.0 commands that
replace 0.3.0's must reproduce it, from a bundle and from the database.
"""
import os

import pyarrow.parquet as pq

from ear_db_export import cli

import fakedata
from fakedb import make_engine
from test_sources import make_bundle

GOLDEN = fakedata.GOLDEN
EAR_CONF = """DBIp=fake
DBCommandsUser=reader
DBCommandsPassw=pw
Island=0 Nodes=n2 tag=cpu
Island=1 Nodes=n3 tag=gpu
"""


def parquet_files(root):
    """``{relative path: table}`` of every Parquet file under ``root`` (no partition columns)."""
    out = {}
    for d, _, files in os.walk(root):
        for f in files:
            if f.endswith(".parquet"):
                path = os.path.join(d, f)
                out[os.path.relpath(path, root)] = pq.ParquetFile(path).read()
    return out


def assert_same_files(out, golden):
    got, want = parquet_files(out), parquet_files(golden)
    assert sorted(got) == sorted(want)
    for path, table in want.items():
        assert got[path].schema.equals(table.schema, check_metadata=True), path
        assert got[path].equals(table), path


def run_0_4_commands(tmp_path, source):
    """The 0.4.0 commands that replace 0.3.0's periodic, power-signatures and app-signatures."""
    salt = tmp_path / "salt"
    salt.write_bytes(fakedata.KEY)
    common = source + ["--salt-file", str(salt)]
    day = ["--every", "day", "--from", "2026-07-04", "--to", "2026-07-04"]
    month = ["--every", "month", "--from", "2026-07-01", "--to", "2026-07-31"]
    wide, long = str(tmp_path / "wide"), str(tmp_path / "long")
    for argv in (["view", "node_metrics", "job_nodes"] + day + ["--out", wide],
                 ["view", "power_signatures"] + month + ["--out", wide],
                 ["view", "app_signatures", "gpu_signatures"] + month + ["--out", wide],
                 ["view", "node_metrics", "job_nodes"] + day +
                 ["--out", long, "--format", "node_metrics=long", "--format", "job_nodes=allocations",
                  "--format-opt", "service=snellius"]):
        assert cli.main(argv + common) == 0, argv
    return wide, long


def test_views_from_a_bundle_match_0_3(tmp_path):
    (tmp_path / "b").mkdir()
    bundle = make_bundle(str(tmp_path / "b"), "ear6", tags=fakedata.TAGS)
    wide, long = run_0_4_commands(tmp_path, ["--bundle", bundle])
    assert_same_files(wide, os.path.join(GOLDEN, "wide"))
    assert_same_files(long, os.path.join(GOLDEN, "long"))


def test_views_from_the_database_match_0_3(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "ENGINE", make_engine("ear6"))
    conf = tmp_path / "ear.conf"
    conf.write_text(EAR_CONF)
    wide, long = run_0_4_commands(tmp_path, ["--conf", str(conf)])
    assert_same_files(wide, os.path.join(GOLDEN, "wide"))
    assert_same_files(long, os.path.join(GOLDEN, "long"))
