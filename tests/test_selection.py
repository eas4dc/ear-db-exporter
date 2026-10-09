"""Selection: parsing the filter flags and turning them into DNF filters on Jobs."""
import datetime as dt
from types import SimpleNamespace

import pytest

from ear_db_export.canonical import pseudonym
from ear_db_export.config import ConfigError
from ear_db_export.selection import (BATCH_STEPS, Selection, from_args, glob_to_like,
                                     parse_jobs, when)


def args(**kw):
    base = dict(from_=None, to=None, jobs=None, users=None, accounts=None, groups=None,
                apps=None, etags=None, policies=None, nodes=None, tags=None, batch_steps=False,
                sample=None, seed=None, limit=None, tables=None)
    return SimpleNamespace(**dict(base, **kw))


def test_parse_jobs():
    assert parse_jobs("123, 456.0,1000-2000,789.sb,5.sbatch") == (
        (123, 123, ()), (456, 456, (0,)), (1000, 2000, ()), (789, 789, BATCH_STEPS),
        (5, 5, BATCH_STEPS))
    for bad in ("x", "1.2.3", "5-3", "1-"):
        with pytest.raises(ConfigError, match="--jobs"):
            parse_jobs(bad)


def test_glob_to_like_escapes():
    assert glob_to_like("lmp_*") == "lmp\\_%"
    assert glob_to_like("a%b?c\\") == "a\\%b_c\\\\"


def test_dates_include_the_day_datetimes_are_exact():
    assert when("2026-07-04") == dt.date(2026, 7, 4)
    assert when("2026-07-04T10:00Z") == dt.datetime(2026, 7, 4, 10, tzinfo=dt.timezone.utc)
    s = from_args(args(from_=when("2026-07-04"), to=when("2026-07-04")), {})
    assert s.end - s.start == 86400
    s = from_args(args(from_=when("2026-07-04 10:00"), to=when("2026-07-04T11:00:00")), {})
    assert s.end - s.start == 3600
    with pytest.raises(ConfigError, match="not before"):
        from_args(args(from_=when("2026-07-05"), to=when("2026-07-04")), {})


def test_job_where_is_and_across_flags_or_within():
    s = from_args(args(from_=when("2026-07-04"), to=when("2026-07-04"), users="a,b",
                       jobs="7,10-20.0", apps="lmp*,python"), {})
    where = s.job_where()
    assert len(where) == 4  # 2 job alternatives x 2 app alternatives
    base = [("end_time", ">=", s.start), ("end_time", "<", s.end), ("user_id", "in", ["a", "b"])]
    assert where[0] == base + [("job_id", "=", 7), ("app_name", "like", "lmp%")]
    assert where[3] == base + [("job_id", ">=", 10), ("job_id", "<=", 20), ("step_id", "in", [0]),
                               ("app_name", "=", "python")]
    assert s.steps_named() and s.job_filters()


def test_empty_selection_selects_everything():
    s = Selection()
    assert s.job_where() == [[]] and not s.filters() and s.tables == ("jobs", "periodic")


def test_batch_steps():
    where = Selection(batch_steps=True).job_where()
    assert where == [[("step_id", "in", list(BATCH_STEPS))]]


def test_nodes_and_tags():
    tags = {"n1": "cpu", "n2": "gpu", "n3": "gpu"}
    s = from_args(args(nodes="m[1-2]", tags="gpu"), tags)
    assert s.nodes == ("m1", "m2", "n2", "n3") and not s.job_filters() and s.filters()
    with pytest.raises(ConfigError, match="--conf"):
        from_args(args(tags="gpu"), {})
    with pytest.raises(ConfigError, match="no node"):
        from_args(args(tags="fpga"), tags)


def test_between_intersects():
    s = Selection(start=100, end=500)
    assert (s.between(0, 200).start, s.between(0, 200).end) == (100, 200)
    assert Selection().between(10, 20).start == 10


def test_to_json_pseudonymises_users():
    s = Selection(users=("alice",), groups=("g",), jobs=parse_jobs("1"))
    out = s.to_json(b"k")
    assert out["users"] == [pseudonym(b"k", "alice")] and out["groups"] == [pseudonym(b"k", "g")]
    assert s.to_json()["users"] == ("alice",)
