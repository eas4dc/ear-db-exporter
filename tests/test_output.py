"""Output: path templates, Hive splitting, CSV, checkpoint, merged runs, lock and failures."""
import json
import os
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ear_db_export.config import ConfigError
from ear_db_export.output import CHECKPOINT, HIVE, MANIFEST, SUMS, Output, read_checkpoint, \
    template_for
from ear_db_export.selection import Window

D0 = 1783123200  # 2026-07-04 00:00 UTC
DAY = 86400


def rows(*days):
    """A small table with one row per day offset, and its epoch times."""
    t = pa.table({"v": list(range(len(days))), "t": [D0 + d * DAY + 10 for d in days]})
    return t, t["t"]


def run(out, template, *tables, window=Window(D0, D0 + 2 * DAY), ext="parquet", **kw):
    """One run into ``out``: write each ``(table, times)`` under the name ``nm``, then finish."""
    with Output(str(out), "nm", template, ext, window, **kw) as o:
        for t, times in tables:
            o.write("nm", t, times=times)
        return o.finish({"source": {"engine": "test"}})


def test_templates_are_checked():
    """Defaults, --hive, and the refused templates."""
    assert template_for("nm") == "{layout}.{ext}" and template_for("raw") == "{table}.{ext}"
    assert template_for("nm", hive=True) == HIVE
    for layout, path, match in (("nm", "{nope}.x", "unknown"), ("nm", "../x.{ext}", "inside"),
                                ("nm", "/x.{ext}", "inside"), ("raw", "x/{date}.{ext}", "raw"),
                                ("raw", "x.{ext}", "raw"), ("nm", "{table}.{ext}", "only for raw")):
        with pytest.raises(ConfigError, match=match):
            template_for(layout, path)
    with pytest.raises(ConfigError, match="--hive"):
        template_for("raw", hive=True)


def test_flat_file_checksums_manifest_and_checkpoint(tmp_path):
    """A flat run: one file, SHA256SUMS that sha256sum accepts, a manifest pointing to the checkpoint."""
    m = run(tmp_path, "{layout}.{ext}", rows(0, 1))
    assert pq.read_table(tmp_path / "nm.parquet").num_rows == 2
    assert m["layout"] == "nm" and m["checkpoint"] == CHECKPOINT and m["format"] == 2
    assert m["outputs"]["nm"]["rows"] == 2 and m["source"] == {"engine": "test"}
    assert read_checkpoint(str(tmp_path)) == D0 + 2 * DAY
    assert subprocess.run(["sha256sum", "--quiet", "-c", SUMS], cwd=tmp_path).returncode == 0
    assert json.loads((tmp_path / MANIFEST).read_text()) == m
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]


def test_hive_splits_rows_by_day(tmp_path):
    """--hive: one file per day of the rows' times; a row without a time in Hive's null partition."""
    t, times = rows(0, 1, 1)
    times = pa.array([D0 + 10, D0 + DAY + 10, None], pa.int64())
    run(tmp_path, HIVE, (t, times))
    got = {os.path.relpath(os.path.join(d, f), tmp_path): pq.read_metadata(os.path.join(d, f)).num_rows
           for d, _, fs in os.walk(tmp_path) for f in fs if f.endswith(".parquet")}
    assert got == {"nm/date=2026-07-04/part-0.parquet": 1, "nm/date=2026-07-05/part-0.parquet": 1,
                   "nm/date=__HIVE_DEFAULT_PARTITION__/part-0.parquet": 1}


def test_run_fields_and_month_partitions(tmp_path):
    """{from}/{to} give the window as given (--to's last day); {year}/{month} split by month."""
    window = Window(D0, D0 + 31 * DAY)
    run(tmp_path, "{layout}_{from}_{to}/year={year}/month={month}.{ext}", rows(0, 30), window=window)
    assert sorted(os.listdir(tmp_path / "nm_2026-07-04_2026-08-03")) == ["year=2026"]
    assert sorted(os.listdir(tmp_path / "nm_2026-07-04_2026-08-03" / "year=2026")) == \
        ["month=07.parquet", "month=08.parquet"]
    with pytest.raises(ConfigError, match="give --from"):
        Output(str(tmp_path), "nm", "{from}.{ext}", "parquet", Window(None, D0))


def test_csv_header_and_delimiter(tmp_path):
    """CSV: a plain header line and the layout's delimiter; types CSV cannot hold are refused."""
    with Output(str(tmp_path), "nm", "{layout}.{ext}", "csv", Window()) as o:
        o.write("nm", pa.table({"a": [1, 2], "b": ["x", "y"]}), delimiter=";")
        o.finish({})
    assert (tmp_path / "nm.csv").read_text() == 'a;b\n1;"x"\n2;"y"\n'
    with pytest.raises(ConfigError, match="CSV"):
        with Output(str(tmp_path / "m"), "nm", "{layout}.{ext}", "csv", Window()) as o:
            o.write("nm", pa.table({"m": pa.array([[("k", "v")]], pa.map_(pa.string(), pa.string()))}))


def test_runs_merge_and_the_checkpoint_never_moves_back(tmp_path):
    """A second run keeps the first's files; an older window does not move the checkpoint back."""
    run(tmp_path, HIVE, rows(0), window=Window(D0, D0 + DAY))
    run(tmp_path, HIVE, rows(1), window=Window(D0 + DAY, D0 + 2 * DAY))
    m = run(tmp_path, HIVE, rows(0), window=Window(D0, D0 + DAY))
    assert sorted(m["outputs"]["nm"]["files"]) == ["nm/date=2026-07-04/part-0.parquet",
                                                   "nm/date=2026-07-05/part-0.parquet"]
    assert read_checkpoint(str(tmp_path)) == D0 + 2 * DAY
    assert subprocess.run(["sha256sum", "--quiet", "-c", SUMS], cwd=tmp_path).returncode == 0


def test_another_layout_or_salt_is_refused(tmp_path):
    """A bundle holds one layout, pseudonymised with one salt (or in clear)."""
    run(tmp_path, "{layout}.{ext}", rows(0), fingerprint="abc")
    with pytest.raises(ConfigError, match="another salt"):
        run(tmp_path, "{layout}.{ext}", rows(0), fingerprint="def")
    with pytest.raises(ConfigError, match="layout nm"):
        with Output(str(tmp_path), "eacct", "{layout}.{ext}", "parquet", Window(), "abc"):
            pass


def test_lock_refuses_a_second_run(tmp_path):
    """Two runs never write the same bundle at once."""
    with Output(str(tmp_path), "nm", "{layout}.{ext}", "parquet", Window()):
        with pytest.raises(ConfigError, match="another ear-db-export run"):
            with Output(str(tmp_path), "nm", "{layout}.{ext}", "parquet", Window()):
                pass


def test_failure_keeps_the_previous_files(tmp_path):
    """A failed run removes its .tmp files; the bundle stays as it was."""
    run(tmp_path, "{layout}.{ext}", rows(0, 1))
    with pytest.raises(RuntimeError):
        with Output(str(tmp_path), "nm", "{layout}.{ext}", "parquet", Window(D0, D0 + DAY)) as o:
            o.write("nm", *rows(0))
            raise RuntimeError
    assert pq.read_table(tmp_path / "nm.parquet").num_rows == 2
    assert not (tmp_path / "nm.parquet.tmp").exists()
    assert read_checkpoint(str(tmp_path)) == D0 + 2 * DAY


def test_two_names_cannot_share_a_file(tmp_path):
    """A template that sends two tables to one path is an error, not a mixed file."""
    with pytest.raises(ConfigError, match="both write"):
        with Output(str(tmp_path), "raw", "all.{ext}", "parquet", Window()) as o:
            o.write("Jobs", pa.table({"a": [1]}))
            o.write("Signatures", pa.table({"a": [1]}))


def test_dry_run_writes_nothing(tmp_path, caplog):
    """A dry run takes no lock, creates nothing, and logs where files would go."""
    caplog.set_level("INFO")
    out = tmp_path / "d"
    with Output(str(out), "nm", HIVE, "parquet", Window(), dry_run=True) as o:
        o.write("nm", *rows(0))
        assert o.finish({}) is None
    assert not out.exists() and "would write" in caplog.text
