"""Select job steps by their ``Jobs`` columns, then read the rows they point to in the other tables.

EAR has no foreign keys, only ids that point to other tables:

.. mermaid::

    flowchart TD
        J[Jobs] -- "job, step, local_id" --> A["Applications<br/>(one per step and node)"]
        J -- "job, step, local_id" --> L[Loops]
        A -- signature_id --> S[Signatures]
        L -- signature_id --> S
        A -- power_signature_id --> P[Power_signatures]
        S -- "GPU id range" --> G[GPU_signatures]

:class:`JobSelection` reads the selected ``Jobs`` rows (one ``scan``, with every
job filter in its ``WHERE``) and cuts them into chunks of :data:`CHUNK` steps.
For each chunk, :class:`JobChunk` looks up its ``Applications`` by ``job_id``
(the first column of their primary key), and the signatures, power signatures
and GPUs they point to by id, on first use. So a layout holds one chunk at a
time, whatever the number of job steps.

How each flag reaches the database:

- ``--from/--to``: ``Jobs.end_time >= from AND end_time < to`` (``Jobs`` has no
  time index: a full scan, but ``Jobs`` is small).
- ``--users --accounts --groups --etags --policies``: ``IN (...)`` on ``Jobs``
  columns; ``--apps``: ``LIKE`` patterns on ``Jobs.app_name``.
- ``--jobs``: job ids and ranges on ``Jobs.job_id``, the first column of its
  primary key; named steps are matched exactly in memory.
- ``--batch-steps``: ``Jobs.step_id IN`` (the batch step ids).
- ``--limit N``: ``ORDER BY end_time DESC LIMIT N``.
- ``--nodes/--tags``: in memory, on each chunk's ``Applications``; then only the
  steps that ran on those nodes are kept.
"""
import logging
import re
from dataclasses import dataclass
from functools import cached_property

import pyarrow as pa
import pyarrow.compute as pc

from ..clean import typed
from ..config import ConfigError
from ..read import Cond, concat
from . import Window, add_node_arguments, add_window_arguments, nodes_of, split

log = logging.getLogger(__name__)

#: The batch step of a job, which EAR stores per job: ``SLURM_BATCH_SCRIPT``
#: (Slurm >= 20.11) and ``NULL_STEPID`` (older Slurm, PBS, no scheduler).
BATCH_STEPS = (0xFFFFFFFB, 0xFFFFFFFE)
#: Job steps per chunk: the unit a layout holds in memory, and the number of
#: job ids per ``Applications`` lookup.
CHUNK = 5000
#: Most job ids pushed into a query as ``job_id IN (...)``; with more, rows are
#: matched in memory only.
IN_PUSHDOWN = 5000
#: The columns that identify a job step, in ``Jobs``, ``Applications`` and ``Loops``
#: (``local_id`` exists from EAR 5.0).
KEYS = ("job_id", "step_id", "local_id")
#: The columns :attr:`JobChunk.gpus` puts in front of the ``gpu_signatures``
#: columns: the signature the GPU belongs to, and its position (0, 1, …) in it.
GPU_LINK = ("signature_id", "gpu_index")
_JOB = re.compile(r"(\d+)(?:-(\d+))?(?:\.(\d+|sb|sbatch))?")


@dataclass(frozen=True)
class JobSelection:
    """The job steps a run selects with the job flags (``--jobs``, ``--users``, ``--from``, …). Every field is optional.

    :ivar window: :class:`~ear_db_export.selection.Window` on ``Jobs.end_time``.
    :ivar jobs: ``((first job id, last job id, step ids), ...)`` from :func:`parse_jobs`;
        empty step ids mean every step.
    :ivar users, accounts, groups, etags, policies: values of the ``Jobs`` columns
        ``user_id``, ``user_acc``, ``user_group``, ``e_tag``, ``policy``.
    :ivar apps: ``app_name`` patterns (``*`` and ``?`` wildcards).
    :ivar nodes: node names: only applications on these nodes, and their steps.
    :ivar batch_steps: only the batch step of each job (job granularity).
    :ivar limit: only the newest this many steps, by end time (0: all).

    Example, alice's job steps that ended on 2026-07-04, on nodes n1 and n2::

        sel = JobSelection(window=Window(1783123200, 1783209600), users=("alice",),
                           nodes=("n1", "n2"))
        for chunk in sel.chunks(source):           # up to CHUNK steps at a time
            chunk.jobs                             # their Jobs rows
            chunk.applications                     # one row per step and node (n1, n2 only)
            chunk.power_signatures                 # looked up by id on first use
    """

    window: Window = Window()
    jobs: tuple = ()
    users: tuple = ()
    accounts: tuple = ()
    groups: tuple = ()
    apps: tuple = ()
    etags: tuple = ()
    policies: tuple = ()
    nodes: tuple = ()
    batch_steps: bool = False
    limit: int = 0

    @staticmethod
    def add_arguments(parser, meaning="job steps that ended in [--from, --to)", job_filters=True):
        """Add the flags of the job family to a layout's parser.

        :param parser: the layout's ``argparse`` parser.
        :param meaning: what the time window selects, shown in ``--help``.
        :param job_filters: also add ``--jobs``, the ``Jobs`` attribute flags,
            ``--batch-steps`` and ``--limit``; without them only the window and
            the nodes select steps.
        """
        add_window_arguments(parser, meaning)
        add_node_arguments(parser)
        if not job_filters:
            return
        g = parser.add_argument_group("job steps (AND across flags, OR within a list)")
        g.add_argument("--jobs", metavar="LIST",
                       help="job ids: 123, 123.0 (a step), 1000-2000 (a range), 789.sb (batch step)")
        for name, what in (("users", "user names"), ("accounts", "accounts"), ("groups", "groups"),
                           ("etags", "energy tags"), ("policies", "EAR policies")):
            g.add_argument("--" + name, metavar="LIST", help="comma-separated %s" % what)
        g.add_argument("--apps", metavar="PATTERNS",
                       help="comma-separated application names; * and ? wildcards")
        g.add_argument("--batch-steps", action="store_true",
                       help="only the batch step of each job (job granularity)")
        g.add_argument("--limit", type=int, metavar="N",
                       help="only the newest N job steps, by end time (applied in the query, "
                            "before --nodes)")

    @classmethod
    def from_args(cls, args, window, node_tags):
        """Build the selection of the parsed flags.

        Job filter flags the layout did not add (``job_filters=False``) read as unset.

        :param args: parsed flags.
        :param window: the run's :class:`~ear_db_export.selection.Window`.
        :param node_tags: ``{node: tag}`` from ear.conf, for ``--tags``.
        :raises ConfigError: on an invalid ``--jobs``, ``--nodes``, ``--tags`` or ``--limit``.
        """
        get = lambda name: getattr(args, name, None)  # noqa: E731
        limit = get("limit")
        if limit is not None and limit < 1:
            raise ConfigError("--limit takes a positive number, got %d" % limit)
        return cls(window=window, jobs=parse_jobs(get("jobs")) if get("jobs") else (),
                   users=split(get("users")), accounts=split(get("accounts")),
                   groups=split(get("groups")), apps=split(get("apps")),
                   etags=split(get("etags")), policies=split(get("policies")),
                   nodes=nodes_of(args.nodes, args.tags, node_tags),
                   batch_steps=bool(get("batch_steps")), limit=limit or 0)

    def filters_jobs(self):
        """Tell whether a filter other than the window and the nodes picks job steps (``--jobs``, ``--users``, …)."""
        return bool(self.jobs or self.users or self.accounts or self.groups or self.apps or
                    self.etags or self.policies or self.batch_steps or self.limit)

    def filters(self):
        """Tell whether the selection filters anything at all (an empty one is the whole database)."""
        bounded = self.window.start is not None or self.window.end is not None
        return self.filters_jobs() or bounded or bool(self.nodes)

    def steps_named(self):
        """Tell whether ``--jobs`` names steps (``123.0``, ``789.sb``), so rows must match steps, not only jobs."""
        return any(steps for _, _, steps in self.jobs)

    def where(self):
        """Return the filter on ``Jobs``, as a list of :class:`~ear_db_export.read.Cond` (all must hold).

        Example: ``--users alice,bob --apps 'lmp*' --jobs 7,100-200`` gives::

            [Cond("user_id", "in", ["alice", "bob"]),
             Cond("app_name", "like_any", ["lmp%"]),
             Cond("job_id", "ranges", [(7, 7), (100, 200)])]
        """
        conds = self.window.conds("end_time")
        for col, values in (("user_id", self.users), ("user_acc", self.accounts),
                            ("user_group", self.groups), ("e_tag", self.etags),
                            ("policy", self.policies)):
            if values:
                conds.append(Cond(col, "in", list(values)))
        if self.apps:
            conds.append(Cond("app_name", "like_any", [glob_to_like(a) for a in self.apps]))
        if self.jobs:
            conds.append(Cond("job_id", "ranges", [(lo, hi) for lo, hi, _ in self.jobs]))
            if all(steps for _, _, steps in self.jobs):  # a superset: matched exactly in memory
                conds.append(Cond("step_id", "in", sorted({s for _, _, st in self.jobs for s in st})))
        if self.batch_steps:
            conds.append(Cond("step_id", "in", list(BATCH_STEPS)))
        return conds

    def chunks(self, source):
        """Yield the selected job steps, :data:`CHUNK` at a time, each as a :class:`JobChunk`.

        One ``scan`` of ``Jobs`` with :meth:`where` (sorted newest first with
        ``--limit``); for each chunk, one ``lookup`` of ``Applications`` by
        ``job_id``. At least one chunk is yielded, empty if no step matches, so
        a layout always writes its schema.

        :param source: a source (see :mod:`ear_db_export.read`).
        :returns: an iterator of :class:`JobChunk`.
        """
        order = [("end_time", "descending")] if self.limit else None
        last, yielded = None, False
        for jobs in source.scan("jobs", self.where(), order=order, limit=self.limit or None):
            jobs = last = self._exact_steps(jobs)
            for i in range(0, jobs.num_rows, CHUNK):
                chunk = self._chunk(source, jobs.slice(i, CHUNK))
                yielded = True
                yield chunk
        if not yielded:
            yield self._chunk(source, last.slice(0, 0))

    def loops(self, source, keys):
        """Yield the ``Loops`` rows of the selected job steps, and only those.

        ``Loops`` has no primary key and no index, so it is read in **one** scan
        for the whole run (never per chunk), with ``job_id IN (...)`` in the query
        when there are at most :data:`IN_PUSHDOWN` jobs, and matched exactly
        (job, step, local_id) in memory.

        :param source: a source.
        :param keys: the :class:`JobKeys` of every selected step (collected from the chunks).
        :returns: an iterator of tables (logical names, stored types), none if
            the source has no ``Loops`` table.
        """
        if not source.has("loops"):
            return
        where, ids = [], keys.job_ids()
        if len(ids) <= IN_PUSHDOWN:
            where.append(Cond("job_id", "in", ids))
        if self.nodes:
            where.append(Cond("node_id", "in", list(self.nodes)))
        for t in source.scan("loops", where):
            yield keys.match(t, exact=True)

    def _chunk(self, source, jobs):
        """Build the :class:`JobChunk` of some ``Jobs`` rows: look up their applications and apply ``--nodes``."""
        apps = concat(source.lookup("applications", "job_id", distinct(jobs["job_id"])))
        keys = join_keys(jobs, apps)
        apps = semi_join(apps, keys, jobs, keys)  # the lookup returned every step of those jobs
        if self.nodes:
            apps = apps.filter(pc.is_in(apps["node_id"], value_set=pa.array(self.nodes, pa.string())))
            jobs = semi_join(jobs, keys, apps, keys)
        return JobChunk(jobs, apps, source)

    def _exact_steps(self, jobs):
        """Keep the ``Jobs`` rows that match a ``--jobs`` item exactly, job and step.

        The query only matched job ids (and all named steps together), a
        superset: ``--jobs 7.0,8.1`` also matches step 1 of job 7.
        """
        if not self.steps_named():
            return jobs
        job, step = pc.cast(jobs["job_id"], pa.int64()), pc.cast(jobs["step_id"], pa.int64())
        mask = pa.array([False] * jobs.num_rows, pa.bool_())
        for lo, hi, steps in self.jobs:
            m = pc.and_(pc.greater_equal(job, lo), pc.less_equal(job, hi))
            if steps:
                m = pc.and_(m, pc.is_in(step, value_set=pa.array(steps, pa.int64())))
            mask = pc.or_(mask, m)
        return jobs.filter(mask)


class JobChunk:
    """Up to :data:`CHUNK` selected job steps with their applications, and the rows they point to.

    ``jobs`` and ``applications`` are read when the chunk is made; the other
    tables are looked up by id the first time they are used, and kept. Every
    table has logical column names and the types as stored, except
    :attr:`per_node` and :attr:`gpus`, which have logical types.

    :param jobs: ``Jobs`` rows.
    :param applications: their ``Applications`` rows (one per step and node).
    :param source: the source to look the rest up in.

    Example::

        chunk.per_node                 # one row per step and node: jobs columns + node_id + signature ids
        chunk.power_signatures         # the Power_signatures rows the applications point to
        chunk.gpus                     # one row per GPU of their signatures, with gpu_index
    """

    def __init__(self, jobs, applications, source):
        """Keep the chunk's rows and source, and work out the columns that link them."""
        self.jobs, self.applications, self.source = jobs, applications, source
        #: The columns that link ``jobs`` and ``applications`` (``local_id`` from EAR 5.0).
        self.keys = join_keys(jobs, applications)

    @cached_property
    def per_node(self):
        """One row per job step and node it ran on, with logical types.

        Every ``jobs`` column, then ``node_id``, ``signature_id`` and
        ``power_signature_id``, sorted by job, step, local_id and node. Steps
        without applications are left out.
        """
        jobs = typed("jobs", self.jobs)
        apps = typed("applications", self.applications).select(
            self.keys + ["node_id", "signature_id", "power_signature_id"])
        out = jobs.join(apps, keys=self.keys, join_type="inner")
        return out.sort_by([(k, "ascending") for k in self.keys + ["node_id"]])

    @cached_property
    def power_signatures(self):
        """The ``Power_signatures`` rows the applications point to (``power_signature_id``)."""
        ids = distinct(self.applications["power_signature_id"])
        return concat(self.source.lookup("power_signatures", "id", ids))

    @cached_property
    def signatures(self):
        """The ``Signatures`` (EARL's application signatures) the applications point to (``signature_id``)."""
        ids = distinct(self.applications["signature_id"])
        return concat(self.source.lookup("signatures", "id", ids))

    @cached_property
    def gpu_signatures(self):
        """The ``GPU_signatures`` rows of :attr:`signatures`, as stored (see :func:`gpu_signatures_of`)."""
        return gpu_signatures_of(self.source, self.signatures)

    @cached_property
    def gpus(self):
        """One row per GPU of :attr:`signatures`, with logical types: :data:`GPU_LINK`, then the GPU columns.

        A signature's GPUs are ``min_gpu_sig_id, min + step, …, max_gpu_sig_id``
        (:func:`gpu_ids`); ``gpu_index`` is the position in that list. A GPU
        without a ``GPU_signatures`` row is left out (and counted in a warning),
        and the next GPUs keep their index.

        Example: signature 102 with GPU ids 50..56, step 2, and no row for id 52::

            signature_id  gpu_index  gpu_power  ...
            102           0          200.5
            102           2          220.25
            102           3          180.0
        """
        s = self.signatures
        sig_ids, index, ids = gpu_ids(zip(s["id"].to_pylist(), s["min_gpu_sig_id"].to_pylist(),
                                          s["max_gpu_sig_id"].to_pylist()), self.source.id_step)
        gpus = typed("gpu_signatures", self.gpu_signatures)
        pos = pc.index_in(pa.array(ids, pa.int64()), value_set=gpus["id"])
        cols = [c for c in gpus.column_names if c != "id"]
        out = pa.table([pa.array(sig_ids, pa.int64()), pa.array(index, pa.int64())] +
                       [pc.take(gpus[c], pos) for c in cols], names=list(GPU_LINK) + cols)
        out = out.filter(pc.is_valid(pos))
        missing = len(ids) - out.num_rows
        if missing:
            log.warning("%d GPU ids in min/max_GPU_sig_id ranges (step %d) have no GPU_signatures "
                        "row (skipped; gpu_index keeps its position)", missing, self.source.id_step)
        return out


class JobKeys:
    """The keys and times of every job step a run selected, collected chunk by chunk.

    Linked dumps (``export raw`` with job filters) need them once the chunks are
    done: ``Loops`` is read in one scan and matched against them, and the
    time-series tables are read over the steps' time span and matched by job.
    Only integers are kept: ``job_id, step_id[, local_id], start_time, end_time``
    per step.

    :param by_step: match time-series rows by job **and** step (when ``--jobs``
        names steps); otherwise by job only, so ``--batch-steps`` keeps every
        row of the job.

    Example::

        keys = JobKeys(by_step=False)
        for chunk in sel.chunks(source):
            keys.add(chunk.jobs)
        keys.span(3600)        # Window(first start, last end + 1 h)
        keys.match(events)     # the Events rows of those jobs
    """

    def __init__(self, by_step):
        """Start with no job steps; ``by_step`` decides how :meth:`match` matches time-series rows."""
        self.by_step = by_step
        self._parts = []

    def add(self, jobs):
        """Add the job steps of some ``Jobs`` rows."""
        cols = [c for c in KEYS + ("start_time", "end_time") if c in jobs.column_names]
        self._parts.append(pa.table({c: pc.cast(jobs[c], pa.int64()) for c in cols}))

    @property
    def table(self):
        """The collected keys and times, one row per step (empty if none)."""
        if not self._parts:
            names = ("job_id", "step_id", "start_time", "end_time")
            return pa.table({c: pa.array([], pa.int64()) for c in names})
        return pa.concat_tables(self._parts)

    def job_ids(self):
        """Return the distinct job ids, sorted."""
        return distinct(self.table["job_id"])

    def span(self, tail):
        """Return the time the steps cover, from the first start to the last end plus ``tail`` seconds.

        :param tail: seconds after a step's end in which its rows may still end
            (a node reports its last interval after the step ends).
        :returns: a :class:`~ear_db_export.selection.Window`, or ``None`` if no step was selected.
        """
        t = self.table
        if t.num_rows == 0:
            return None
        return Window(pc.min(t["start_time"]).as_py(), pc.max(t["end_time"]).as_py() + tail)

    def match(self, t, exact=False):
        """Keep the rows of ``t`` that belong to the collected job steps.

        :param t: a table with a ``job_id`` column (and ``step_id``, ``local_id``).
        :param exact: match every key both have (job, step, local_id), as for
            ``Loops``; otherwise by job, and by step too if ``by_step``.
        :returns: the matching rows, in their order.
        """
        if exact:
            cols = [c for c in KEYS if c in t.column_names and c in self.table.column_names]
        else:
            cols = ["job_id"] + (["step_id"] if self.by_step else [])
        return semi_join(t, cols, self.table, cols)


def parse_jobs(text):
    """Parse a ``--jobs`` value, in ``eacct -j`` syntax plus ranges.

    :param text: comma-separated ``JOB``, ``JOB.STEP``, ``FIRST-LAST`` or
        ``JOB.sb`` (the batch step, also ``.sbatch``).
    :returns: ``((first, last, steps), ...)``: ``steps`` is ``()`` for every
        step, ``(step,)``, or :data:`BATCH_STEPS`.
    :raises ConfigError: on a token that is not one of these forms.

    Example::

        parse_jobs("123,456.0,1000-2000,789.sb")
        # ((123, 123, ()), (456, 456, (0,)), (1000, 2000, ()), (789, 789, (4294967291, 4294967294)))
    """
    out = []
    for token in (t.strip() for t in text.split(",")):
        m = _JOB.fullmatch(token)
        if not m:
            raise ConfigError("--jobs: %r is not JOB, JOB.STEP, FIRST-LAST or JOB.sb" % token)
        lo, hi, step = m.groups()
        lo, hi = int(lo), int(hi or lo)
        if hi < lo:
            raise ConfigError("--jobs: descending range %r" % token)
        steps = () if step is None else BATCH_STEPS if step in ("sb", "sbatch") else (int(step),)
        out.append((lo, hi, steps))
    return tuple(out)


def glob_to_like(glob):
    """Turn a shell-style pattern (``*``, ``?``) into a SQL ``LIKE`` pattern.

    ``%``, ``_`` and ``\\`` in the name are escaped with ``\\``, the escape
    character of MariaDB's ``LIKE`` and of ``pyarrow.compute.match_like``. A
    name without wildcards matches only itself.

    Example: ``glob_to_like("lmp_*")`` is ``"lmp\\_%"``.
    """
    escaped = glob.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped.replace("*", "%").replace("?", "_")


def gpu_ids(ranges, step):
    """Expand signatures' GPU id ranges: EAR stores a signature's GPUs as ids ``min, min+step, …, max``.

    ``step`` is the server's auto-increment step (Galera uses steps > 1), as in
    EAR's ``mysql_io_functions.c``.

    :param ranges: ``(signature id, min GPU id, max GPU id)`` per signature (NULL ranges skipped).
    :param step: the auto-increment step.
    :returns: three lists, one item per GPU: signature id, GPU index, GPU id.

    Example: ``gpu_ids([(7, 11, 17)], 2)`` is ``([7, 7, 7, 7], [0, 1, 2, 3], [11, 13, 15, 17])``.
    """
    sig_ids, index, ids = [], [], []
    for sig, lo, hi in ranges:
        if lo is None or hi is None:
            continue
        for i, gid in enumerate(range(lo, hi + 1, step)):
            sig_ids.append(sig)
            index.append(i)
            ids.append(gid)
    return sig_ids, index, ids


def gpu_signatures_of(source, signatures):
    """Look up the ``GPU_signatures`` rows of some signatures, by their id ranges (:func:`gpu_ids`).

    :param source: a source.
    :param signatures: ``Signatures`` rows (``id``, ``min_gpu_sig_id``, ``max_gpu_sig_id``).
    :returns: one table of ``GPU_signatures`` rows, as stored (logical names).
    """
    ids = gpu_ids(zip(signatures["id"].to_pylist(), signatures["min_gpu_sig_id"].to_pylist(),
                      signatures["max_gpu_sig_id"].to_pylist()), source.id_step)[2]
    return concat(source.lookup("gpu_signatures", "id", ids))


def join_keys(a, b):
    """Return the job-step key columns (:data:`KEYS`) that both tables have, in order."""
    return [c for c in KEYS if c in a.column_names and c in b.column_names]


def semi_join(t, cols, other, other_cols):
    """Keep the rows of ``t`` whose ``cols`` values appear in ``other``'s ``other_cols``, in their order.

    Integer keys are compared as int64, so ``uint32`` and ``int64`` columns match.

    Example: ``t`` with ``job_id`` 7, 8, 9 and ``other`` with ``job_id`` 9, 7
    keeps the rows of jobs 7 and 9, in that order.
    """
    if t.num_rows == 0 or not cols:
        return t
    if other.num_rows == 0:
        return t.slice(0, 0)
    norm = lambda c: pc.cast(c, pa.int64()) if pa.types.is_integer(c.type) else c  # noqa: E731
    names = ["_k%d" % i for i in range(len(cols))]
    left = pa.table([norm(t[c]) for c in cols] + [pa.array(range(t.num_rows), pa.int64())],
                    names=names + ["_row"])
    right = pa.table([norm(other[c]) for c in other_cols], names=names)
    kept = left.join(right.group_by(names).aggregate([]), keys=names, join_type="left semi")["_row"]
    return t.take(pc.take(kept, pc.sort_indices(kept)))


def distinct(col):
    """Return the sorted distinct non-null values of a column, as Python values."""
    return sorted(v for v in pc.unique(col).to_pylist() if v is not None)
