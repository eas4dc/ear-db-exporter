"""Selections reading the fake database (EAR 6 and 7) or a bundle: chunks, links, loops, time series."""
import pyarrow as pa
import pytest

from ear_db_export.config import ConfigError
from ear_db_export.read import concat
from ear_db_export.read.bundle import BundleSource
from ear_db_export.read.sql import SqlSource
from ear_db_export.selection import Window
from ear_db_export.selection import jobs as jobs_module
from ear_db_export.selection.jobs import BATCH_STEPS, JobKeys, JobSelection, gpu_ids, parse_jobs
from ear_db_export.selection.timeseries import TimeSelection

import fakedata
from fakedb import DBCONF, make_engine
from test_sources import make_bundle

T0 = fakedata.T0


@pytest.fixture(params=["ear6", "ear7", "bundle"])
def source(request, tmp_path):
    """The fakedata records in an EAR 6 or EAR 7 database, or in a bundle."""
    if request.param == "bundle":
        return BundleSource(make_bundle(str(tmp_path), "ear7"))
    return SqlSource(DBCONF, engine=make_engine(request.param))


def read(sel, source):
    """All the chunks of a JobSelection, as one table per chunk attribute."""
    chunks = list(sel.chunks(source))
    return {name: concat([getattr(c, name) for c in chunks])
            for name in ("jobs", "applications", "power_signatures", "signatures", "gpu_signatures")}


def test_job_filters_follow_the_links(source):
    """bob's one job step, its application, and every row they point to."""
    got = read(JobSelection(users=("bob",)), source)
    assert got["jobs"]["job_id"].to_pylist() == [8]
    assert got["applications"]["node_id"].to_pylist() == ["n3"]
    assert got["signatures"]["id"].to_pylist() == [102]
    assert got["power_signatures"]["id"].to_pylist() == [13]
    assert got["gpu_signatures"]["id"].to_pylist() == [50, 54, 56]  # range 50..56, step 2


def test_window_is_on_end_time(source):
    """A step is in the window it ended in."""
    got = read(JobSelection(window=Window(T0 + 250, T0 + 3600)), source)
    assert got["jobs"]["local_id"].to_pylist() == [6]  # ended at T0+300; local_id 5 at T0+200


def test_steps_batch_steps_and_limit(source):
    """--jobs matches named steps exactly; --batch-steps keeps the batch step; --limit the newest."""
    assert sorted(read(JobSelection(jobs=parse_jobs("8.sb,7.0")), source)["jobs"]["job_id"]
                  .to_pylist()) == [7, 7, 8]
    assert read(JobSelection(jobs=parse_jobs("7.1")), source)["jobs"].num_rows == 0
    assert read(JobSelection(batch_steps=True), source)["jobs"]["step_id"].to_pylist() == \
        [BATCH_STEPS[0]]
    assert read(JobSelection(limit=1), source)["jobs"]["job_id"].to_pylist() == [8]


def test_nodes_keep_their_applications_and_steps(source):
    """--nodes n4: only that node's application, and the one step that ran there."""
    got = read(JobSelection(nodes=("n4",)), source)
    assert got["applications"]["node_id"].to_pylist() == ["n4"]
    assert got["jobs"]["local_id"].to_pylist() == [5]


def test_chunks_cut_the_steps_and_never_come_empty_handed(monkeypatch):
    """Chunks hold at most CHUNK steps; no match still gives one empty chunk (its schema)."""
    monkeypatch.setattr(jobs_module, "CHUNK", 1)
    src = SqlSource(DBCONF, engine=make_engine())
    chunks = list(JobSelection().chunks(src))
    assert [c.jobs.num_rows for c in chunks] == [1, 1, 1]
    assert sum(c.applications.num_rows for c in chunks) == 4  # each step's own applications
    (empty,) = JobSelection(users=("nobody",)).chunks(src)
    assert empty.jobs.num_rows == 0 and empty.per_node.num_rows == 0
    assert empty.power_signatures.num_rows == 0 and empty.gpus.num_rows == 0


def test_per_node_and_gpus(source):
    """per_node: one typed row per step and node; gpus: GPU positions kept when one has no row."""
    chunks = list(JobSelection().chunks(source))  # the fake engine streams 2 rows per batch
    t = concat([c.per_node for c in chunks])
    assert [(r["local_id"], r["node_id"]) for r in t.select(["local_id", "node_id"]).to_pylist()] == \
        [(5, "n2"), (5, "n4"), (6, "n2"), (0, "n3")]  # by job, step, local_id, node
    assert t["job_id"].type == pa.int64() and "threshold" in t.column_names
    g = concat([c.gpus for c in chunks])
    assert g.select(["signature_id", "gpu_index"]).to_pylist() == [
        {"signature_id": 102, "gpu_index": i} for i in (0, 2, 3)]  # id 52 has no row
    assert g["gpu_power"].to_pylist() == [200.5, 220.25, 180.0]


def test_loops_are_read_once_and_matched_exactly():
    """Loops of the selected steps only, matched by (job, step, local_id)."""
    eng = make_engine()
    eng.db.execute("INSERT INTO `Loops` (`event`, `size`, `level`, `job_id`, `step_id`, `local_id`, "
                   "`node_id`, `total_iterations`, `signature_id`) VALUES "
                   "(1, 2, 0, 7, 0, 6, 'n2', 10, 101), (1, 2, 0, 7, 0, 9, 'n2', 10, 101), "
                   "(1, 2, 0, 9, 0, 0, 'n9', 10, 102)")
    eng.db.commit()
    src = SqlSource(DBCONF, engine=eng)
    sel = JobSelection(users=("alice",))
    keys = JobKeys(by_step=False)
    for chunk in sel.chunks(src):
        keys.add(chunk.jobs)
    loops = concat(sel.loops(src, keys))
    assert loops.select(["job_id", "local_id"]).to_pylist() == [{"job_id": 7, "local_id": 6}]
    assert sum(1 for s, _ in eng.queries if s.startswith("SELECT `event`")) == 1


def test_time_series_by_window_and_nodes(source):
    """The table's own time column, and node; idle rows (job 0) are kept."""
    rows = lambda sel: concat(sel.batches(source))["id"].to_pylist()  # noqa: E731
    assert sorted(rows(TimeSelection("periodic_metrics", Window(T0, T0 + 101)))) == [1, 2]
    assert sorted(rows(TimeSelection("periodic_metrics", nodes=("n2",)))) == [1, 3]


def test_time_series_of_job_steps(source):
    """With job keys: the jobs' span and their rows; by step when --jobs named steps."""
    keys = JobKeys(by_step=False)
    for chunk in JobSelection(users=("bob",)).chunks(source):
        keys.add(chunk.jobs)
    sel = TimeSelection("periodic_metrics", Window(T0, T0 + 1), jobs=keys)  # window replaced by span
    assert concat(sel.batches(source))["id"].to_pylist() == [4]
    none = TimeSelection("periodic_metrics", jobs=JobKeys(by_step=False))
    assert concat(none.batches(source)).num_rows == 0
    assert keys.span(3600) == Window(T0 + 3000, T0 + 7000 + 3600)


def test_batch_steps_keep_every_periodic_row_of_the_job():
    """--batch-steps matches time-series rows by job; --jobs 8.0 by step."""
    eng = make_engine()
    eng.db.execute("UPDATE `Periodic_metrics` SET `step_id` = 0 WHERE `id` = 4")  # an srun step
    eng.db.commit()
    src = SqlSource(DBCONF, engine=eng)
    for sel, want in ((JobSelection(batch_steps=True), [4]), (JobSelection(jobs=parse_jobs("8.sb")), [])):
        keys = JobKeys(by_step=sel.steps_named())
        for chunk in sel.chunks(src):
            keys.add(chunk.jobs)
        assert concat(TimeSelection("periodic_metrics", jobs=keys).batches(src))["id"].to_pylist() == want


def test_time_series_need_the_table():
    """A source without the table (eacct files, an optional table) is refused clearly."""
    src = SqlSource(DBCONF, engine=make_engine())
    del src.profile.tables["events"]
    with pytest.raises(ConfigError, match="no events"):
        list(TimeSelection("events").batches(src))


def test_gpu_ids_use_the_step():
    """A signature's GPU ids are min, min+step, …, max; NULL ranges have none."""
    assert gpu_ids([(7, 11, 17), (8, None, None)], 2) == ([7, 7, 7, 7], [0, 1, 2, 3], [11, 13, 15, 17])
