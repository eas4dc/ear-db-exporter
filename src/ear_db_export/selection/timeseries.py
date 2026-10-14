"""Select the rows of a time-series table (``Periodic_metrics``, ``Events``, …) by time and node.

``Periodic_metrics`` (one row per node and interval), ``Events``,
``Periodic_aggregations`` and ``Global_energy`` each have a time column
(:data:`~ear_db_export.read.TIME_COLUMNS`). :class:`TimeSelection` reads one of
them through the source's ``window``: on EAR's database the time window becomes
an id range found by binary search on the primary key, then pages of that range,
so the cost is the window's rows whatever the table's size, with or without a
time index.

How each flag reaches the database:

- ``--from/--to``: ``<time> >= from AND <time> < to``, inside the id range.
- ``--nodes/--tags``: ``node_id IN (...)``, in the same query (tables with a node).

In a linked dump (``export raw`` with job filters) the selection also carries
the selected job steps (:class:`~ear_db_export.selection.jobs.JobKeys`): it then
reads over the steps' time span instead, with ``job_id IN (...)`` in the query,
and keeps only those jobs' rows.
"""
from dataclasses import dataclass
from typing import Optional

from ..config import ConfigError
from ..read import TABLES, Cond
from . import Window, add_node_arguments, add_window_arguments, nodes_of
from .jobs import IN_PUSHDOWN, JobKeys

#: Seconds after a job step's end in which its rows may still end: a node reports
#: its last interval after the step ends.
TAIL = 3600  # ponytail: one PowerMon period is ~60 s; widen if a site reports hourly


@dataclass(frozen=True)
class TimeSelection:
    """The rows of one time-series table: in a time window, on some nodes, maybe of some job steps only.

    :ivar table: logical table name, a key of :data:`~ear_db_export.read.TIME_COLUMNS`.
    :ivar window: :class:`~ear_db_export.selection.Window` on the table's time column.
    :ivar nodes: node names (empty: every node); ignored by tables without nodes.
    :ivar jobs: :class:`~ear_db_export.selection.jobs.JobKeys` to keep only those
        job steps' rows, read over their time span instead of ``window``; or ``None``.

    Example, node metrics of n1 and n2 that end on 2026-07-04::

        sel = TimeSelection("periodic_metrics", Window(1783123200, 1783209600), ("n1", "n2"))
        for batch in sel.batches(source):     # one primary-key page at a time
            ...
    """

    table: str
    window: Window = Window()
    nodes: tuple = ()
    jobs: Optional[JobKeys] = None

    @staticmethod
    def add_arguments(parser, meaning="rows that end in [--from, --to)"):
        """Add the flags of the time-series family to a layout's parser: the time window and the nodes.

        :param parser: the layout's ``argparse`` parser.
        :param meaning: what the time window selects, shown in ``--help``.
        """
        add_window_arguments(parser, meaning)
        add_node_arguments(parser)

    @classmethod
    def from_args(cls, table, args, window, node_tags):
        """Build the selection of a table from the parsed flags.

        :param table: logical table name.
        :param args: parsed flags (``nodes``, ``tags``).
        :param window: the run's :class:`~ear_db_export.selection.Window`.
        :param node_tags: ``{node: tag}`` from ear.conf, for ``--tags``.
        """
        return cls(table, window, nodes_of(args.nodes, args.tags, node_tags))

    def batches(self, source):
        """Yield the selected rows, one table per read batch (a primary-key page on EAR's database).

        :param source: a source (see :mod:`ear_db_export.read`).
        :returns: an iterator of tables with logical names and the types as
            stored; at least one (empty if no row matches).
        :raises ConfigError: if the source has no such table.
        """
        if not source.has(self.table):
            raise ConfigError("the source has no %s table" % self.table)
        columns, window, where = TABLES[self.table], self.window, []
        if self.nodes and "node_id" in columns:
            where.append(Cond("node_id", "in", list(self.nodes)))
        if self.jobs is not None:
            window = self.jobs.span(TAIL) or Window(0, 0)  # no step selected: an empty window
            ids = self.jobs.job_ids()
            if "job_id" in columns and len(ids) <= IN_PUSHDOWN:
                where.append(Cond("job_id", "in", ids))
        for t in source.window(self.table, window.start, window.end, where):
            yield self.jobs.match(t) if self.jobs is not None and "job_id" in columns else t
