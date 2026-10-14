"""Selections without a database: the time window, the node and job flags, and the filter on Jobs."""
import datetime as dt
from types import SimpleNamespace

import pytest

from ear_db_export.config import ConfigError
from ear_db_export.read import Cond
from ear_db_export.selection import Window, epoch, nodes_of, when, window_from_args
from ear_db_export.selection.jobs import BATCH_STEPS, JobSelection, glob_to_like, parse_jobs

DAY = dt.date(2026, 7, 4)
D0 = epoch(DAY)
NOW = epoch(dt.date(2026, 10, 14))


def flags(**kw):
    """Parsed window flags, unset unless given."""
    return SimpleNamespace(**dict(dict(from_=None, to=None, resume=False), **kw))


def test_dates_include_the_day_datetimes_are_exact():
    """--to DAY includes the day; a time is exact; both are UTC."""
    assert epoch(DAY) == 1783123200 and epoch(DAY, end=True) == 1783123200 + 86400
    t = when("2026-07-04T10:30")
    assert t.tzinfo == dt.timezone.utc and epoch(t, end=True) == epoch(t) == 1783123200 + 37800
    assert when("2026-07-04") == DAY and when("2026-07-04 10:30:00Z") == t


def test_window():
    """A window renders conditions on any column, and knows when it is empty."""
    w = Window(D0, D0 + 86400)
    assert w.conds("end_time") == [Cond("end_time", ">=", D0), Cond("end_time", "<", D0 + 86400)]
    assert Window().conds("x") == [] and not Window().empty and Window(5, 5).empty
    assert w.describe() == {"from": "2026-07-04T00:00:00+00:00", "to": "2026-07-05T00:00:00+00:00"}


def test_window_from_flags():
    """--from/--to give the window; --to in the future and an inverted window are refused."""
    assert window_from_args(flags(from_=DAY, to=DAY), now=NOW) == Window(D0, D0 + 86400)
    assert window_from_args(flags(), now=NOW) == Window()
    with pytest.raises(ConfigError, match="future"):
        window_from_args(flags(to=dt.date(2026, 10, 14)), now=NOW)  # today has not ended
    with pytest.raises(ConfigError, match="not before"):
        window_from_args(flags(from_=dt.date(2026, 7, 5), to=DAY), now=NOW)


def test_resume_starts_at_the_checkpoint():
    """--resume: --from on the first run, then the checkpoint; up to date is an empty window."""
    resume = dict(resume=True, from_=DAY, to=dt.date(2026, 7, 10))
    assert window_from_args(flags(**resume), None, now=NOW).start == D0
    assert window_from_args(flags(**resume), D0 + 3 * 86400, now=NOW) == \
        Window(D0 + 3 * 86400, D0 + 7 * 86400)
    assert window_from_args(flags(**resume), D0 + 7 * 86400, now=NOW).empty
    with pytest.raises(ConfigError, match="needs --to"):
        window_from_args(flags(resume=True), now=NOW)
    with pytest.raises(ConfigError, match="first run"):
        window_from_args(flags(resume=True, to=DAY), None, now=NOW)


def test_nodes_and_tags():
    """--nodes is a hostlist, --tags the ear.conf islands; both together are a union."""
    tags = {"g1": "gpu", "c1": "cpu"}
    assert nodes_of("n[1-2]", "gpu", tags) == ("g1", "n1", "n2")
    assert nodes_of(None, None, tags) == ()
    with pytest.raises(ConfigError, match="give --conf"):
        nodes_of(None, "gpu", {})
    with pytest.raises(ConfigError, match="no node"):
        nodes_of(None, "nope", tags)


def test_parse_jobs():
    """eacct -j syntax plus ranges; .sb is the batch step."""
    assert parse_jobs("123,456.0,1000-2000,789.sb") == (
        (123, 123, ()), (456, 456, (0,)), (1000, 2000, ()), (789, 789, BATCH_STEPS))
    for bad in ("x", "5-3", "1.2.3"):
        with pytest.raises(ConfigError):
            parse_jobs(bad)


def test_glob_to_like_escapes():
    """* and ? become % and _; LIKE's own wildcards in a name are escaped."""
    assert glob_to_like("lmp_*") == "lmp\\_%" and glob_to_like("a%b?") == "a\\%b_"


def test_job_where_is_and_across_flags_or_within():
    """Each flag is one condition on Jobs; values of one flag are alternatives."""
    sel = JobSelection(window=Window(D0, None), users=("alice", "bob"), apps=("lmp*",),
                       jobs=parse_jobs("7,100-200"))
    assert sel.where() == [Cond("end_time", ">=", D0), Cond("user_id", "in", ["alice", "bob"]),
                           Cond("app_name", "like_any", ["lmp%"]),
                           Cond("job_id", "ranges", [(7, 7), (100, 200)])]
    assert JobSelection().where() == [] and not JobSelection().filters()


def test_job_where_pushes_steps_only_when_every_item_names_them():
    """--jobs 7.0,8.sb pushes the steps (a superset); --jobs 7.0,8 cannot."""
    named = JobSelection(jobs=parse_jobs("7.0,8.sb")).where()
    assert named[-1] == Cond("step_id", "in", sorted((0,) + BATCH_STEPS))
    assert [c.column for c in JobSelection(jobs=parse_jobs("7.0,8")).where()] == ["job_id"]
    assert JobSelection(batch_steps=True).where() == [Cond("step_id", "in", list(BATCH_STEPS))]


def test_what_counts_as_a_filter():
    """filters_jobs: anything but window and nodes; filters: anything at all."""
    assert JobSelection(limit=3).filters_jobs()
    assert not JobSelection(nodes=("n1",)).filters_jobs() and JobSelection(nodes=("n1",)).filters()
    assert JobSelection(window=Window(None, D0)).filters()
    assert JobSelection(jobs=parse_jobs("7.0")).steps_named()


def test_from_args_reads_only_the_flags_a_layout_has():
    """job_nodes has no job filters: they read as unset; --limit must be positive."""
    args = SimpleNamespace(nodes="n1", tags=None)
    sel = JobSelection.from_args(args, Window(), {})
    assert sel.nodes == ("n1",) and not sel.filters_jobs()
    with pytest.raises(ConfigError, match="positive"):
        JobSelection.from_args(SimpleNamespace(nodes=None, tags=None, limit=0), Window(), {})
