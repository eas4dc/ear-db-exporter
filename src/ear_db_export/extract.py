"""Read what a Selection selects, table by table, following EAR's links between tables.

EAR has no foreign keys, only ids that point to other tables. Starting from
the job steps a :class:`~ear_db_export.selection.Selection` picks,
:func:`extract` reads, in this order (:data:`ORDER`):

1. ``Jobs``: one scan, with the job filters in the WHERE.
2. ``Applications`` (one row per step and node), by job id; with ``--nodes``
   only those nodes' rows, and the jobs that ran there.
3. ``Loops``: one streamed scan (no primary key, no index), kept for the
   selected steps.
4. ``Power_signatures``, ``Signatures`` (of applications and loops) and
   ``GPU_signatures`` (each signature's id range), by primary key.
5. ``Periodic_metrics``, ``Events``: by time and node; with job filters only
   the rows of the selected jobs. ``Periodic_aggregations``, ``Global_energy``:
   by time.
6. ``Learning_*``: whole.

Without any filter it reads every requested table whole, as stored, which
needs no schema profile (so any EAR version can be exported). Every table is
yielded as an iterator of ``pyarrow.Table`` with its physical columns; the
caller must consume each before asking for the next, because later tables use
ids collected from earlier ones.

:class:`Records` gives the datasets (:mod:`ear_db_export.canonical`) the same
rows with logical names.
"""
import datetime as dt
import logging
import random
from functools import cached_property

import pyarrow as pa
import pyarrow.compute as pc

from .canonical import INPUTS
from .config import ConfigError
from .read import GPU_LINK, TIME_COLUMNS
from .selection import time_where

log = logging.getLogger(__name__)

#: ``--tables`` groups: physical tables (the same names in every EAR version).
GROUPS = {
    "jobs": ("Jobs", "Applications", "Power_signatures", "Signatures", "GPU_signatures"),
    "loops": ("Loops", "Signatures", "GPU_signatures"),
    "periodic": ("Periodic_metrics",),
    "events": ("Events",),
    "energy": ("Periodic_aggregations", "Global_energy"),
    "learning": ("Learning_applications", "Learning_jobs", "Learning_signatures"),
}
#: Order in which :func:`extract` reads tables: every table after those it points to.
ORDER = ("Jobs", "Applications", "Loops", "Power_signatures", "Signatures", "GPU_signatures",
         "Periodic_metrics", "Events", "Periodic_aggregations", "Global_energy",
         "Learning_applications", "Learning_jobs", "Learning_signatures")
#: Tables always read whole: filters don't apply (their GPU rows are not followed).
WHOLE = GROUPS["learning"]
#: Keys per pushed-down ``IN`` list when filtering a scan by job id.
IN_PUSHDOWN = 5000
#: Seconds after a job's end in which its periodic rows and events may still end.
TAIL = 3600  # ponytail: one PowerMon period is ~60 s; widen if a site reports hourly
_LOGICAL = {"Jobs": "jobs", "Applications": "applications", "Loops": "loops",
            "Power_signatures": "power_signatures", "Signatures": "signatures",
            "GPU_signatures": "gpu_signatures", "Periodic_metrics": "periodic_metrics",
            "Events": "events", "Periodic_aggregations": "periodic_aggregations",
            "Global_energy": "global_energy"}


def tables(sel, provides):
    """Resolve ``--tables`` into the physical tables to read, in :data:`ORDER`.

    :param sel: the :class:`~ear_db_export.selection.Selection`.
    :param provides: physical tables the source has.
    :returns: list of physical table names. ``all`` is every table of the
        source (EAR's first, in order, then any other one).
    :raises ConfigError: on an unknown group or table.
    """
    wanted = []
    for name in sel.tables:
        if name == "all":
            wanted += [t for t in ORDER if t in provides] + sorted(set(provides) - set(ORDER))
        elif name in GROUPS:
            wanted += [t for t in GROUPS[name] if t in provides]
        elif name in provides:
            wanted.append(name)
        else:
            raise ConfigError("--tables: %r is neither a group (%s, all) nor a table of the source"
                              % (name, ", ".join(GROUPS)))
    order = {t: i for i, t in enumerate(ORDER)}
    return sorted(set(wanted), key=lambda t: (order.get(t, len(ORDER)), t))


def extract(source, sel):
    """Read the rows a Selection picks, table by table.

    :param source: a source (:class:`~ear_db_export.read.sql.SqlSource` or
        :class:`~ear_db_export.read.bundle.BundleSource`).
    :param sel: the :class:`~ear_db_export.selection.Selection`.
    :returns: an iterator of ``(physical table, iterator of pyarrow.Table)``.
    :raises ConfigError: if the Selection filters but the source has no profile.
    """
    names = tables(sel, source.provides)
    if not sel.filters():
        for t in names:
            yield t, source.scan(t)
        return
    if source.profile is None:
        raise ConfigError("filters need a schema profile: pass --profile")
    walk = _Walk(source, sel)
    for t in names:
        yield t, walk.read(t)


class _Walk:
    """The state of one filtered extract: the selected job steps and the ids collected so far."""

    def __init__(self, source, sel):
        self.source, self.sel, self.p = source, sel, source.profile
        self.sig_ids, self.gpu_ranges = set(), []
        self.signatures_read = False

    @cached_property
    def job_side(self):
        return job_side(self.source, self.sel)

    def col(self, logical, c):
        return self.p.col(logical, c)

    def read(self, table):
        if table in WHOLE or table not in _LOGICAL:
            return self.source.scan(table)
        return getattr(self, "_" + _LOGICAL[table])()

    def _jobs(self):
        yield self.job_side[0]

    def _applications(self):
        yield self.job_side[1]

    def _loops(self):
        jobs = self.job_side[0]
        ids = _distinct(jobs[self.col("jobs", "job_id")])
        where = [("job_id", "in", ids)] if len(ids) <= IN_PUSHDOWN else []
        if self.sel.nodes:
            where.append(("node_id", "in", list(self.sel.nodes)))
        keys = _keys(self.p, "loops", "jobs")
        sig = self.col("loops", "signature_id")
        for t in self.source.scan(self.p.tables["loops"], where):
            t = semi_join(t, [k[0] for k in keys], jobs, [k[1] for k in keys])
            self.sig_ids.update(_distinct(t[sig]))
            yield t

    def _power_signatures(self):
        ids = self.job_side[1][self.col("applications", "power_signature_id")]
        return self.source.lookup(self.p.tables["power_signatures"], "id", _distinct(ids))

    def _signatures(self):
        ids = set(_distinct(self.job_side[1][self.col("applications", "signature_id")]))
        ids |= self.sig_ids
        lo, hi = self.col("signatures", "min_gpu_sig_id"), self.col("signatures", "max_gpu_sig_id")
        for t in self.source.lookup(self.p.tables["signatures"], "id", sorted(ids)):
            if lo and hi:
                self.gpu_ranges += zip(t[self.col("signatures", "id")].to_pylist(),
                                       t[lo].to_pylist(), t[hi].to_pylist())
            yield t
        self.signatures_read = True

    def _gpu_signatures(self):
        if not self.signatures_read:  # GPU_signatures asked without Signatures
            for _ in self._signatures():
                pass
        ids = gpu_ids(self.gpu_ranges, self.source.id_step)[2]
        return self.source.lookup(self.p.tables["gpu_signatures"], "id", ids)

    def _periodic_metrics(self):
        return self._rows("periodic_metrics")

    def _events(self):
        return self._rows("events")

    def _periodic_aggregations(self):
        return self._rows("periodic_aggregations", by_node=False)

    def _global_energy(self):
        return self.source.scan(self.p.tables["global_energy"], time_where(
            "time", _utc(self.sel.start), _utc(self.sel.end)))

    def _rows(self, logical, by_node=True):
        """Rows of a table with a time and (maybe) a node and a job: periodic metrics, events."""
        return rows_of(self.source, self.sel, logical, self.job_side[0] if
                       self.sel.job_filters() and by_node else None, by_node)


def rows_of(source, sel, logical, jobs=None, by_node=True):
    """Read the rows of a time table (``periodic_metrics``, ``events``, ``periodic_aggregations``).

    :param source: the source.
    :param sel: the Selection: its time window and nodes.
    :param logical: logical table name.
    :param jobs: physical ``Jobs`` rows when the Selection filters jobs: then
        only those jobs' rows, from the first job start to the last job end
        (+ :data:`TAIL`), whatever the Selection's window.
    :param by_node: the table has a node column (not ``periodic_aggregations``).
    :returns: an iterator of physical tables.
    """
    p = source.profile
    table, tcol = p.tables[logical], TIME_COLUMNS[logical]
    if jobs is None:
        where = time_where(tcol, sel.start, sel.end)
        if by_node and sel.nodes:
            where.append(("node_id", "in", list(sel.nodes)))
        yield from source.scan(table, where)
        return
    jc = lambda c: p.col("jobs", c)
    ids = _distinct(jobs[jc("job_id")])
    if not ids:  # no job selected: no rows (an impossible filter keeps the schema)
        yield from source.scan(table, [("job_id", "in", [])])
        return
    start = pc.min(jobs[jc("start_time")]).as_py()
    end = pc.max(jobs[jc("end_time")]).as_py() + TAIL
    where = time_where(tcol, start, end)
    if sel.nodes:
        where.append(("node_id", "in", list(sel.nodes)))
    if len(ids) <= IN_PUSHDOWN:
        where.append(("job_id", "in", ids))
    keys = ["job_id", "step_id"] if sel.steps_named() else ["job_id"]
    for t in source.scan(table, where):
        yield semi_join(t, [p.col(logical, k) for k in keys], jobs, [jc(k) for k in keys])


def job_side(source, sel):
    """Read the selected job steps and their applications.

    :returns: ``(jobs, applications)``, physical tables. ``--nodes`` keeps only
        those nodes' applications, and the job steps that have one;
        ``--sample`` and ``--limit`` then pick jobs (all steps of a job).
    """
    p = source.profile
    jobs = _concat(source.scan(p.tables["jobs"], sel.job_where()))
    jc = lambda c: p.col("jobs", c)
    apps = _concat(source.lookup(p.tables["applications"], "job_id", _distinct(jobs[jc("job_id")])))
    keys = _keys(p, "applications", "jobs")
    apps = semi_join(apps, [k[0] for k in keys], jobs, [k[1] for k in keys])
    if sel.nodes:
        apps = apps.filter(pc.is_in(apps[p.col("applications", "node_id")],
                                    value_set=pa.array(sel.nodes, pa.string())))
        jobs = semi_join(jobs, [k[1] for k in keys], apps, [k[0] for k in keys])
    if sel.sample or sel.limit:
        ids = _distinct(jobs[jc("job_id")])
        if sel.sample:
            ids = sorted(random.Random(sel.seed).sample(ids, min(sel.sample, len(ids))))
        if sel.limit:
            last = jobs.group_by(jc("job_id")).aggregate([(jc("end_time"), "max")])
            last = last.filter(pc.is_in(last[jc("job_id")], value_set=pa.array(ids, last[jc("job_id")].type)))
            last = last.sort_by([(jc("end_time") + "_max", "descending"), (jc("job_id"), "descending")])
            ids = sorted(last[jc("job_id")].to_pylist()[:sel.limit])
        keep = lambda t, c: t.filter(pc.is_in(t[c], value_set=pa.array(ids, t[c].type)))
        jobs, apps = keep(jobs, jc("job_id")), keep(apps, p.col("applications", "job_id"))
    return jobs, apps


def gpu_ids(ranges, step):
    """Expand signatures' GPU id ranges: EAR stores GPUs ``min, min+step, …, max``.

    ``step`` is the server's auto-increment step (Galera uses steps > 1), as in
    EAR's ``mysql_io_functions.c``.

    :param ranges: ``(signature id, min GPU id, max GPU id)`` per signature (NULLs skipped).
    :param step: the auto-increment step.
    :returns: three lists, one item per GPU: signature id, GPU index, GPU id.
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


def semi_join(t, cols, other, other_cols):
    """Keep the rows of ``t`` whose ``cols`` values appear in ``other``'s ``other_cols``, in order.

    Integer keys are compared as int64, so ``uint32`` and ``int64`` columns match.
    """
    if t.num_rows == 0 or not cols:
        return t
    if other.num_rows == 0:
        return t.slice(0, 0)
    norm = lambda c: pc.cast(c, pa.int64()) if pa.types.is_integer(c.type) else c
    names = ["_k%d" % i for i in range(len(cols))]
    left = pa.table([norm(t[c]) for c in cols] + [pa.array(range(t.num_rows), pa.int64())],
                    names=names + ["_row"])
    right = pa.table([norm(other[c]) for c in other_cols], names=names)
    kept = left.join(right.group_by(names).aggregate([]), keys=names, join_type="left semi")["_row"]
    return t.take(pc.take(kept, pc.sort_indices(kept)))


def _keys(p, logical, other):
    """``[(column of logical, column of other)]`` of the job-step key both tables have."""
    out = []
    for c in ("job_id", "step_id", "local_id"):
        a, b = p.col(logical, c), p.col(other, c)
        if a and b:
            out.append((a, b))
    return out


def _distinct(col):
    """Sorted distinct non-null values of a column, as Python values."""
    return sorted(v for v in pc.unique(col).to_pylist() if v is not None)


def _concat(tables):
    """Concatenate an iterator of tables (at least one)."""
    tables = list(tables)
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def _utc(epoch):
    """A naive UTC datetime for a TIMESTAMP column filter (the session time zone is UTC)."""
    return None if epoch is None else \
        dt.datetime.fromtimestamp(epoch, dt.timezone.utc).replace(tzinfo=None)


class Records:
    """The records of one Selection with logical names, read on first use, for the datasets.

    Each attribute is one record table, with the columns of
    :data:`ear_db_export.canonical.INPUTS`.

    :param source: a source with a profile.
    :param sel: the Selection (usually one period of it).
    """

    def __init__(self, source, sel):
        if source.profile is None:
            raise ConfigError("datasets need a schema profile: pass --profile")
        self.source, self.sel, self.p = source, sel, source.profile
        self.join_keys = ["job_id", "step_id"] + (
            ["local_id"] if self.p.col("jobs", "local_id") else [])

    def _logical(self, logical, tables):
        return self.p.logical(logical, _concat(tables), INPUTS[logical])

    @cached_property
    def _job_side(self):
        return job_side(self.source, self.sel)

    @cached_property
    def periodic_metrics(self):
        """Node metrics records of the Selection (only the selected jobs' rows when it filters jobs)."""
        jobs = self._job_side[0] if self.sel.job_filters() else None
        return self._logical("periodic_metrics", rows_of(self.source, self.sel,
                                                         "periodic_metrics", jobs))

    @cached_property
    def jobs(self):
        """Job step records."""
        return self.p.logical("jobs", self._job_side[0], INPUTS["jobs"])

    @cached_property
    def applications(self):
        """Application records (one per step and node) of :attr:`jobs`."""
        return self.p.logical("applications", self._job_side[1], INPUTS["applications"])

    @cached_property
    def power_signatures(self):
        """Power signatures that :attr:`applications` point to."""
        ids = _distinct(self.applications["power_signature_id"])
        return self._logical("power_signatures", self.source.lookup(
            self.p.tables["power_signatures"], "id", ids))

    @cached_property
    def signatures(self):
        """EARL application signatures that :attr:`applications` point to."""
        ids = _distinct(self.applications["signature_id"])
        return self._logical("signatures", self.source.lookup(self.p.tables["signatures"], "id", ids))

    @cached_property
    def gpu_signatures(self):
        """One row per GPU of :attr:`signatures`: :data:`~ear_db_export.read.GPU_LINK`, then
        the ``gpu_signatures`` columns without ``id``. GPUs without a row are left
        out; ``gpu_index`` keeps its position."""
        cols = INPUTS["gpu_signatures"][1:]
        s = self.signatures
        sig_ids, index, ids = gpu_ids(zip(s["id"].to_pylist(), s["min_gpu_sig_id"].to_pylist(),
                                          s["max_gpu_sig_id"].to_pylist()), self.source.id_step)
        gpus = self._logical("gpu_signatures", self.source.lookup(
            self.p.tables["gpu_signatures"], "id", ids))
        pos = pc.index_in(pa.array(ids, pa.int64()), value_set=gpus["id"])
        out = pa.table([pa.array(sig_ids, pa.int64()), pa.array(index, pa.int64())] +
                       [pc.take(gpus[c], pos) for c in cols], names=list(GPU_LINK) + cols)
        out = out.filter(pc.is_valid(pos))
        missing = len(ids) - out.num_rows
        if missing:
            log.warning(
                "%d GPU ids in min/max_GPU_sig_id ranges (step %d) have no GPU_signatures row "
                "(skipped; gpu_index keeps its position)", missing, self.source.id_step)
        return out
