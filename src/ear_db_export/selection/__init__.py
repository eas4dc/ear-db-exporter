"""Selections: which rows a layout reads, and the flags that choose them.

EAR's tables fall in two families, selected differently, so there is one class
per family. Each adds its own flags to a layout's command line
(``add_arguments``), builds itself from the parsed flags (``from_args``), and
reads its rows from a source, batch by batch:

- :class:`~ear_db_export.selection.timeseries.TimeSelection`: the rows of one
  time-series table (``Periodic_metrics``, ``Events``, ``Periodic_aggregations``,
  ``Global_energy``), by its own time column and by node.
- :class:`~ear_db_export.selection.jobs.JobSelection`: job steps, by ``Jobs``
  columns and job ids, then every row they point to (``Applications``, the
  signatures, the GPUs, ``Loops``), by id.

Where each filter applies:

=================================  =============================  ==========================================
Flag                               TimeSelection                  JobSelection
=================================  =============================  ==========================================
``--from --to``                    the table's time column        ``Jobs.end_time`` in ``[from, to)``
``--nodes --tags``                 ``node_id``                    ``Applications.node_id``, and the steps
                                                                  that ran there
``--jobs``                         —                              ``Jobs (job_id, step_id)``
``--users`` … ``--policies``       —                              ``Jobs`` columns (``--accounts``,
                                                                  ``--groups``, ``--etags`` too)
``--apps``                         —                              ``Jobs.app_name``, with ``*``/``?`` wildcards
``--batch-steps``                  —                              ``Jobs.step_id``: the batch step only
``--limit N``                      —                              the newest N steps by ``Jobs.end_time``
=================================  =============================  ==========================================

This module has what both families share: the time window (:class:`Window`,
``--from/--to/--resume``) and node names (``--nodes/--tags``).
"""
import dataclasses
import datetime as dt
import logging
from typing import Optional

from ..config import ConfigError, expand_hostlist
from ..read import Cond

log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class Window:
    """A time window ``[start, end)`` in Unix seconds; either bound may be ``None`` (open).

    Example, ``--from 2026-07-04 --to 2026-07-04`` (the whole day)::

        w = Window(1783123200, 1783209600)
        w.conds("end_time")
        # [Cond("end_time", ">=", 1783123200), Cond("end_time", "<", 1783209600)]
        w.describe()
        # {"from": "2026-07-04T00:00:00+00:00", "to": "2026-07-05T00:00:00+00:00"}
    """

    start: Optional[int] = None
    end: Optional[int] = None

    @property
    def empty(self):
        """Tell whether no second fits in the window (``start >= end``): nothing to read."""
        return self.start is not None and self.end is not None and self.start >= self.end

    def conds(self, column):
        """Return the conditions ``start <= column < end`` (none for an open bound)."""
        out = []
        if self.start is not None:
            out.append(Cond(column, ">=", self.start))
        if self.end is not None:
            out.append(Cond(column, "<", self.end))
        return out

    def describe(self):
        """Return the window as ISO 8601 UTC times, ``{"from": ..., "to": ...}`` (``None`` when open), for a manifest."""
        return {"from": iso(self.start), "to": iso(self.end)}


def add_window_arguments(parser, meaning):
    """Add the time window flags, ``--from``, ``--to`` and ``--resume``, to a layout's parser.

    :param parser: the layout's ``argparse`` parser.
    :param meaning: what the window selects in this layout, shown in ``--help``,
        for example ``"job steps that ended in [--from, --to)"``.
    """
    g = parser.add_argument_group("time window: %s" % meaning)
    g.add_argument("--from", dest="from_", type=when, metavar="DATE",
                   help="first day YYYY-MM-DD, or a UTC time YYYY-MM-DDTHH:MM[:SS]")
    g.add_argument("--to", type=when, metavar="DATE",
                   help="last day (included), or a UTC time (excluded); not in the future")
    g.add_argument("--resume", action="store_true",
                   help="start where the last run into --out ended (its checkpoint.json); "
                        "--from is then only the start of the first run")


def window_from_args(args, checkpoint=None, now=None):
    """Work out the time window of a run from ``--from``, ``--to`` and ``--resume``.

    :param args: parsed flags (``from_``, ``to``, ``resume``).
    :param checkpoint: the end of the last finished run into ``--out``, Unix
        seconds, or ``None`` (see :func:`ear_db_export.output.read_checkpoint`).
    :param now: the current Unix time (default: now), for tests.
    :returns: a :class:`Window`. With ``--resume`` and a checkpoint at or after
        ``--to``, an empty window: the bundle is up to date.
    :raises ConfigError: if ``--to`` is in the future, ``--from`` is not before
        ``--to``, or ``--resume`` lacks ``--to`` (or ``--from`` on a first run).

    Example: ``--resume --from 2026-10-01 --to 2026-10-12`` gives
    ``Window(<2026-10-01 00:00>, <2026-10-13 00:00>)`` on the first run, and
    ``Window(<checkpoint>, <2026-10-13 00:00>)`` on the next ones.
    """
    start, end = epoch(args.from_), epoch(args.to, end=True)
    now = int(dt.datetime.now(dt.timezone.utc).timestamp()) if now is None else now
    if end is not None and end > now:
        raise ConfigError("--to %s is in the future: export only windows that have ended" % args.to)
    if args.resume:
        if end is None:
            raise ConfigError("--resume needs --to")
        if checkpoint is not None:
            start = checkpoint
        elif start is None:
            raise ConfigError("--resume: --out has no checkpoint yet: give --from for the first run")
        if start >= end:
            log.info("up to date: the checkpoint %s is at or after --to", iso(start))
            return Window(start, end)
    if start is not None and end is not None and start >= end:
        raise ConfigError("--from %s is not before --to %s" % (args.from_, args.to))
    return Window(start, end)


def when(text):
    """Parse a ``--from``/``--to`` value: a day, or a date and time in UTC (the argparse type of both flags).

    :param text: ``YYYY-MM-DD``, or ``YYYY-MM-DDTHH:MM[:SS]`` (a space instead
        of ``T`` and a trailing ``Z`` are accepted).
    :returns: a ``datetime.date``, or a timezone-aware ``datetime.datetime`` in UTC.
    :raises ValueError: if it is neither (argparse reports it).
    """
    text = text.strip()
    if len(text) == 10:
        return dt.date.fromisoformat(text)
    t = dt.datetime.fromisoformat(text[:-1] if text.endswith("Z") else text)
    return t.replace(tzinfo=dt.timezone.utc) if t.tzinfo is None else t.astimezone(dt.timezone.utc)


def epoch(value, end=False):
    """Return the Unix time of a :func:`when` value.

    :param value: a date or an aware datetime, or ``None``.
    :param end: for a date, return the end of that day (the next midnight), so
        ``--to`` includes the whole day.
    :returns: seconds, int, or ``None``.

    Example: ``epoch(date(2026, 7, 4))`` is 1783123200 (00:00 UTC);
    ``epoch(date(2026, 7, 4), end=True)`` is 1783209600 (the next midnight).
    """
    if value is None:
        return None
    if not isinstance(value, dt.datetime):
        value = dt.datetime(value.year, value.month, value.day, tzinfo=dt.timezone.utc) + \
            dt.timedelta(days=1 if end else 0)
    return int(value.timestamp())


def iso(seconds):
    """Format Unix seconds as an ISO 8601 UTC time (``2026-07-04T00:00:00+00:00``), or ``None`` for ``None``."""
    if seconds is None:
        return None
    return dt.datetime.fromtimestamp(seconds, dt.timezone.utc).isoformat()


def add_node_arguments(parser):
    """Add the node flags, ``--nodes`` and ``--tags``, to a layout's parser."""
    g = parser.add_argument_group("nodes")
    g.add_argument("--nodes", metavar="HOSTLIST", help="nodes, as a Slurm hostlist: n[01-04],gpu1")
    g.add_argument("--tags", metavar="LIST", help="the nodes of these ear.conf Island tags")


def nodes_of(hostlist, tags, node_tags):
    """Resolve ``--nodes`` and ``--tags`` into node names.

    :param hostlist: ``--nodes`` value (a Slurm-style hostlist) or ``None``.
    :param tags: ``--tags`` value (comma-separated tags) or ``None``.
    :param node_tags: ``{node: tag}`` from ear.conf.
    :returns: sorted node names; empty means no node filter.
    :raises ConfigError: on a bad hostlist, or ``--tags`` without any tag in ear.conf.

    Example: ``nodes_of("n[1-2]", "gpu", {"g1": "gpu", "c1": "cpu"})`` is ``("g1", "n1", "n2")``.
    """
    nodes = set()
    if hostlist:
        try:
            nodes.update(expand_hostlist(hostlist))
        except ValueError as e:
            raise ConfigError("--nodes: %s" % e)
    if tags:
        if not node_tags:
            raise ConfigError("--tags needs the Island tags of ear.conf (give --conf)")
        wanted = set(split(tags))
        tagged = {n for n, t in node_tags.items() if t in wanted}
        if not tagged:
            raise ConfigError("--tags %s: no node has these tags in ear.conf" % tags)
        nodes.update(tagged)
    return tuple(sorted(nodes))


def split(value):
    """Split a comma-separated flag value into its non-empty, stripped items (``()`` for ``None``)."""
    return tuple(v.strip() for v in (value or "").split(",") if v.strip())
