"""The view files must stay what 0.3.0 wrote (tests/golden/, made by make_golden.py)."""
import os

import pyarrow.parquet as pq

import make_golden

GOLDEN = make_golden.GOLDEN


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


def test_old_pipeline_matches_golden(tmp_path, monkeypatch):
    monkeypatch.setattr(make_golden, "GOLDEN", str(tmp_path))
    make_golden.main()
    assert_same_files(tmp_path, GOLDEN)
