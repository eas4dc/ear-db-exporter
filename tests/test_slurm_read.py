"""SlurmSource on the fake database: profile choice, missing columns, id windows, lookups."""
import logging

import pytest

from ear_db_export.config import ConfigError
from ear_db_export.read import concat
from slurm_db_export.read import SlurmSource

from slurmfake import CLUSTER, DBCONF, SlurmFakeEngine, T0

SEPT = (T0, T0 + 30 * 86400)


def source(**kw):
    """A SlurmSource on a fresh fake database (``kw`` go to SlurmFakeEngine)."""
    return SlurmSource(DBCONF, engine=SlurmFakeEngine(**kw), page_rows=2)


def test_opens_with_version_clusters_and_profile():
    """Deleted clusters are left out; schema version 16 is slurm20, 25.05 by its columns."""
    src = source()
    assert (src.clusters, src.version, src.profile.name, src.release) == \
        ([CLUSTER], 16, "slurm20", "25.05 or later")


def test_window_reads_jobs_by_submit_time_in_pages():
    """September's jobs, censored and deleted ones included, through an id range, 2 rows a page."""
    src = source()
    jobs = concat(src.window("job", CLUSTER, "time_submit", *SEPT))
    assert jobs["job_db_inx"].to_pylist() == [2, 3, 4, 5]
    assert jobs["deleted"].to_pylist() == [0, 0, 0, 1] and 0 in jobs["time_start"].to_pylist()
    pages = [sql for sql, _ in src.engine.queries if "LIMIT 2" in sql and "BETWEEN" in sql]
    assert len(pages) == 3  # 2 + 2 rows, then an empty page


def test_window_columns_are_the_profile_s():
    """job_table: only the profile's columns; event_table: all of them."""
    src = source()
    jobs = concat(src.window("job", CLUSTER, "time_submit", *SEPT))
    assert "work_dir" not in jobs.column_names and "submit_line" in jobs.column_names
    events = concat(src.window("event", CLUSTER, "time_start", *SEPT))
    assert events["reason"].to_pylist() == ["bad dimm"] and "instance_type" in events.column_names


def test_missing_optional_column_warns(caplog):
    """A 25.05 database without submit_line: warned once, left out, recorded for the manifest."""
    src = source(drop={"job": ["submit_line"]})
    with caplog.at_level(logging.WARNING):
        jobs = concat(src.window("job", CLUSTER, "time_submit", *SEPT))
        concat(src.window("job", CLUSTER, "time_submit", *SEPT))
    assert "submit_line" not in jobs.column_names
    assert [r.message for r in caplog.records if "submit_line" in r.message] == \
        ["lenox.hu_job_table has no submit_line column (Slurm 25.05 or later): not exported"]
    assert src.report["lenox.hu_job_table"]["missing"] == ["submit_line"]


def test_missing_required_column_is_refused():
    """Without time_submit the profile does not match."""
    src = source(drop={"job": ["time_submit"]})
    with pytest.raises(ConfigError, match="time_submit"):
        concat(src.window("job", CLUSTER, "time_submit", *SEPT))


def test_steps_by_job_db_inx():
    """Steps are looked up by their job's job_db_inx (batch step -5 included)."""
    src = source()
    steps = concat(src.lookup("step", CLUSTER, "job_db_inx", [2, 6, 2, None]))
    assert sorted(zip(steps["job_db_inx"].to_pylist(), steps["id_step"].to_pylist())) == \
        [(2, -5), (2, 0), (6, 0)]


def test_slurm17_database():
    """A 17.11 database: slurm17, pack_job_id read as stored, old step usage columns."""
    src = source(release="17.11", version=5)
    assert src.profile.name == "slurm17" and src.release == "17.11"
    jobs = concat(src.window("job", CLUSTER, "time_submit", *SEPT))
    assert "pack_job_id" in jobs.column_names
    steps = concat(src.lookup("step", CLUSTER, "job_db_inx", [2]))
    assert "max_rss" in steps.column_names and "tres_usage_in_max" not in steps.column_names


def test_detect_without_version_row():
    """No convert_version_table row: the profile comes from the job table's columns."""
    src = source(release="20.02", version=None)
    assert (src.version, src.profile.name) == (None, "slurm20")


def test_check_reports_each_table():
    """check: OK lines, missing optional columns, and a missing optional table on 20.02."""
    lines, ok = source(release="20.02", version=8).check([CLUSTER])
    assert ok
    assert "lenox.hu_job_script_table: TABLE MISSING (optional)" in lines
    assert "  missing optional (not exported): submit_line, script_hash_inx" in lines
    assert "tres_table: OK, 5 columns" in lines
