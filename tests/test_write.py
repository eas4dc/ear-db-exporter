"""write: CSV files and the bundle files (schema.sql, SHA256SUMS, manifest.json)."""
import hashlib
import json

import pyarrow as pa
import pytest

from ear_db_export import write
from ear_db_export.config import ConfigError


def test_csv_plain_header_and_atomic(tmp_path):
    path = str(tmp_path / "d" / "x.csv")
    t = pa.table({"JOBID": [1, 2], "NODENAME": ["n1", "n;2"]})
    assert write.csv(path, [t, t.slice(0, 1)], delimiter=";") == (3, (tmp_path / "d" / "x.csv").stat().st_size)
    lines = (tmp_path / "d" / "x.csv").read_text().splitlines()
    assert lines[0] == "JOBID;NODENAME" and lines[1] == '1;"n1"' and lines[2] == '2;"n;2"'
    assert not (tmp_path / "d" / "x.csv.tmp").exists()


def write_table(out, rel, rows):
    path = str(out / rel)
    nrows, nbytes = write.parquet(path, [pa.table({"id": list(range(rows))})])
    return {rel: {"rows": nrows, "bytes": nbytes, "sha256": write.sha256(path)}}


def test_finish_bundle_writes_checksums_and_manifest_last(tmp_path):
    files = {"Jobs": write_table(tmp_path, "Jobs.parquet", 3)}
    m = write.finish_bundle(str(tmp_path), {"format": 1, "created": "t0"}, "CREATE TABLE x;\n", files)
    on_disk = json.loads((tmp_path / write.MANIFEST).read_text())
    assert on_disk == m and m["tables"]["Jobs"]["rows"] == 3
    # SHA256SUMS in coreutils format, covering the data files and schema.sql
    for line in (tmp_path / write.SUMS).read_text().splitlines():
        digest, rel = line.split("  ")
        assert hashlib.sha256((tmp_path / rel).read_bytes()).hexdigest() == digest
    assert "schema.sql" in (tmp_path / write.SUMS).read_text()


def test_finish_bundle_merges_runs(tmp_path):
    p1 = write_table(tmp_path, "Periodic_metrics/date=2026-07-04/part-0.parquet", 2)
    write.finish_bundle(str(tmp_path), {"created": "t0"}, "", {"Periodic_metrics": p1})
    p2 = write_table(tmp_path, "Periodic_metrics/date=2026-07-05/part-0.parquet", 5)
    m = write.finish_bundle(str(tmp_path), {"created": "t1"}, "", {"Periodic_metrics": p2})
    assert m["created"] == "t0" and m["tables"]["Periodic_metrics"]["rows"] == 7
    assert len(m["tables"]["Periodic_metrics"]["files"]) == 2
    assert len((tmp_path / write.SUMS).read_text().splitlines()) == 2  # no schema.sql


def test_check_bundle_refuses_another_salt(tmp_path):
    write.check_bundle(str(tmp_path), "abc")  # no bundle yet
    write.finish_bundle(str(tmp_path), {"salt_fingerprint": "abc"}, "", {})
    write.check_bundle(str(tmp_path), "abc")
    with pytest.raises(ConfigError, match="another salt"):
        write.check_bundle(str(tmp_path), "def")
    with pytest.raises(ConfigError, match="another salt"):
        write.check_bundle(str(tmp_path), None)
