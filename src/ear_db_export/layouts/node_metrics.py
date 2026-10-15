"""``node_metrics``: EAR's node metrics (``Periodic_metrics``) in long format, one row per node, interval and metric.

EAR's daemon writes one ``Periodic_metrics`` row per node and interval (about
60 s); each becomes one row per metric that has a value::

    node_id  job_id  step_id  start_time  end_time    dc_energy  temp  gpu_energy
    n2       42      0        1783136226  1783136286  1100       50    0

    timestamp            duration_s  service   source  metric_name                value   labels
    2026-07-04 03:38:06  60          snellius  ear     ear_node_dc_energy_joules  1100.0  {node=n2, job_steps=42.0,42.sb}
    2026-07-04 03:38:06  60          snellius  ear     ear_node_temp_celsius        50.0  {node=n2, job_steps=42.0,42.sb}

- ``timestamp`` is the interval's end (UTC), ``duration_s`` its length.
- A metric without a value is left out: a null, or EAR's 0 for "no value"
  (:data:`METRICS`; a valid DC energy is never 0, so its zeros are kept).
- ``labels`` has the node and, when steps ran on it, the steps that ran on the
  node during the interval, from ``Jobs`` and ``Applications`` (:func:`steps_by_node`),
  as ``--job-labels`` says:

  - ``all`` (default): ``job_steps``, every step, ``job.step`` (``job.sb`` for
    the batch step);
  - ``sb``: ``job_ids``, the jobs whose batch step ran there (jobs without a
    batch step, ``salloc`` + ``srun``, are not listed);
  - ``table``: the one ``job_id`` and ``step_id`` the ``Periodic_metrics`` row
    holds (none when ``job_id`` is 0, idle); ``Jobs``/``Applications`` are not read.

  It tells who was on the node, not what each step consumed (that is
  ``power_signatures``). Steps are in ``Jobs`` only once they end: export a
  period once its jobs have ended.
- Rows come in ``Periodic_metrics`` id order, which is roughly time order: one
  table per metric for each page read, so a month never sits in memory.

Flags: the time window (rows whose interval ends in it), ``--nodes``/``--tags``,
``--service`` and ``--job-labels``. ``--hive`` writes one file per day.

Flow, as a UML sequence diagram:

.. mermaid::

    sequenceDiagram
        participant CLI as cli.run
        participant L as node_metrics layout
        participant J as JobSelection
        participant T as TimeSelection
        participant S as Source
        participant O as Output
        CLI->>L: run(source, args, out)
        L->>T: from_args("periodic_metrics", args, out.window, source.tags)
        opt --job-labels all or sb
            L->>J: chunks(source), overlap=True (sb: batch_steps=True)
            J->>S: scan("jobs", ran in the window), lookup("applications", job_id)
            J-->>L: steps per node (steps_by_node)
        end
        L->>T: batches(source)
        T->>S: window("periodic_metrics", start, end, [node_id in nodes])
        loop each primary-key page (1 M rows)
            S-->>T: Periodic_metrics rows
            T-->>L: batch
            L->>L: typed(), then long_rows(): labels, one table per metric
            L->>O: write("node_metrics", rows, times=timestamp)
        end
"""
from bisect import bisect_left

import pyarrow as pa
import pyarrow.compute as pc

from ..clean import TS, to_ts, typed, zero_to_null
from ..selection.jobs import BATCH_STEPS, JobSelection
from ..selection.timeseries import TimeSelection

NAME = "node_metrics"
HELP = "EAR's node metrics in long format: one row per node, interval and metric"
READS = "Periodic_metrics; Jobs and Applications unless --job-labels table"

#: The metrics, in output order: ``(periodic_metrics column, metric_name, EAR's 0 means no value)``.
#: Names follow the Prometheus conventions (namespace prefix, base unit suffix).
#: Units, checked in EAR's ``src/daemon/power_monitor.c``: energies are average
#: power (W) times interval seconds, so joules; ``avg_f`` is kHz; ``temp`` is the
#: hottest socket in Celsius. EAR writes 0 when a sensor is unavailable (GPU
#: energy on CPU-only nodes, DRAM energy on AMD nodes without DRAM RAPL).
METRICS = [
    ("dc_energy", "ear_node_dc_energy_joules", False),
    ("dram_energy", "ear_node_dram_energy_joules", True),
    ("pck_energy", "ear_node_pck_energy_joules", True),
    ("gpu_energy", "ear_node_gpu_energy_joules", True),
    ("avg_f", "ear_node_avg_cpu_freq_khz", True),
    ("temp", "ear_node_temp_celsius", True),
]
_DICT = pa.dictionary(pa.int32(), pa.string())  # repeated strings stored once per file
#: Type of the ``labels`` column: string → string map.
LABELS = pa.map_(pa.string(), pa.string())
#: Schema of the rows this layout writes.
SCHEMA = pa.schema([
    ("timestamp", TS), ("duration_s", pa.int32()), ("service", _DICT), ("source", _DICT),
    ("metric_name", _DICT), ("value", pa.float64()), ("labels", LABELS),
])


def add_arguments(parser):
    """Add the flags of ``node_metrics``: the time window and nodes, and ``--service``."""
    TimeSelection.add_arguments(parser, "node metrics whose interval ends in [--from, --to)")
    g = parser.add_argument_group("long format")
    g.add_argument("--service", required=True, metavar="NAME",
                   help="the value of the service column, for example the cluster's name")
    g.add_argument("--job-labels", choices=("all", "sb", "table"), default="all",
                   help="all (default): job_steps, every job step (job.step, job.sb for the batch "
                        "step) that ran on the node in the interval, from Jobs (only steps that "
                        "have ended); sb: job_ids, the jobs whose batch step ran there; table: the "
                        "one job and step the Periodic_metrics row holds, without reading Jobs")


def run(source, args, out):
    """Write the node metrics of the window in long format, one page of ``Periodic_metrics`` at a time."""
    sel = TimeSelection.from_args("periodic_metrics", args, out.window, source.tags)
    steps = None
    if args.job_labels != "table":
        sb = args.job_labels == "sb"
        jobs = JobSelection(window=sel.window, nodes=sel.nodes, batch_steps=sb, overlap=True)
        steps = steps_by_node(jobs.chunks(source), jobs_only=sb)
    for batch in sel.batches(source):
        for rows in long_rows(typed("periodic_metrics", batch), args.service, steps):
            out.write(NAME, rows, times=rows["timestamp"])


def steps_by_node(chunks, jobs_only=False):
    """Index the job steps of some :class:`~ear_db_export.selection.jobs.JobChunk` by the node they ran on.

    Steps without a start time (EAR's 0) or with ``end_time <= start_time`` are left out.

    :param chunks: job chunks, from a ``JobSelection`` with ``overlap``.
    :param jobs_only: name the steps by their job only (``--job-labels sb``).
    :returns: ``(label, {node: (starts, steps, longest)})``: ``label`` is
        ``job_ids`` or ``job_steps``; ``steps`` a list of ``(start_time, end_time,
        key)`` sorted by start, ``key`` being ``(job_id,)`` or ``(job_id, step_id)``;
        ``starts`` their start times; ``longest`` the longest step's duration (for :func:`_present`).
    """
    by_node = {}
    for chunk in chunks:
        r = chunk.per_node
        for job, step, start, end, node in zip(*(r[c].to_pylist() for c in
                                                 ("job_id", "step_id", "start_time", "end_time", "node_id"))):
            if start and end > start:  # EAR's 0 (no start time) would list the step since 1970
                by_node.setdefault(node, []).append((start, end, (job,) if jobs_only else (job, step)))
    out = {}
    for node, steps in by_node.items():
        steps.sort()
        out[node] = ([s[0] for s in steps], steps, max(e - s for s, e, _ in steps))
    return ("job_ids" if jobs_only else "job_steps"), out


def _present(steps, start, end):
    """Return the names of the steps that ran during ``[start, end)``, comma-separated, or ``None``.

    Names: ``job`` (``--job-labels sb``), ``job.step``, or ``job.sb`` for a batch step.

    :param steps: one node's ``(starts, steps, longest)`` from :func:`steps_by_node`, or ``None``.

    A step that started before ``start - longest`` ended before ``start``, so only
    the steps that started in ``[start - longest, end)`` are looked at.
    """
    if steps is None:
        return None
    starts, rows, longest = steps
    ran = {key for s, e, key in rows[bisect_left(starts, start - longest):bisect_left(starts, end)]
           if e > start}
    return ",".join(_name(key) for key in sorted(ran)) or None


def _name(key):
    """Name a step key of :func:`steps_by_node`: ``42``, ``42.0`` or ``42.sb`` (``--jobs`` syntax)."""
    if len(key) == 1:
        return str(key[0])
    return "%d.sb" % key[0] if key[1] in BATCH_STEPS else "%d.%d" % key


def long_rows(metrics, service, steps=None):
    """Turn ``periodic_metrics`` rows (logical types) into long rows: one table per metric that has values.

    :param metrics: ``periodic_metrics`` rows from :func:`ear_db_export.clean.typed`.
    :param service: the value of the ``service`` column.
    :param steps: ``(label, steps per node)`` from :func:`steps_by_node`;
        ``None``: the row's own ``job_id`` and ``step_id``.
    :returns: an iterator of :data:`SCHEMA` tables; at least one (empty if no
        metric has a value), so the file always has its schema.

    Example: 2 rows, n1 idle (``temp`` 51, ``gpu_energy`` 0) and n2 running job
    42 (``temp`` 50, ``gpu_energy`` 5), give a ``dc_energy`` table of 2 rows,
    a ``gpu_energy`` table of 1 row (n1's 0 is left out), a ``temp`` table of
    2 rows, and so on; only n2's labels have ``job_steps`` (or ``job_ids``, or ``job_id`` and ``step_id``).
    """
    seconds = lambda c: pc.cast(metrics[c], pa.int64())  # noqa: E731
    if steps is None:
        labels = _labels(metrics["node_id"], metrics["job_id"], metrics["step_id"])
    else:
        labels = _step_labels(metrics["node_id"], metrics["start_time"], metrics["end_time"], steps)
    base = pa.table({
        "timestamp": to_ts(metrics["end_time"]),
        "duration_s": pc.cast(pc.subtract(seconds("end_time"), seconds("start_time")), pa.int32()),
        "labels": labels,
    })
    written = False
    for column, metric, zero_means_none in METRICS:
        values = zero_to_null(metrics[column]) if zero_means_none else metrics[column]
        rows = base.append_column("value", pc.cast(values, pa.float64())).filter(pc.is_valid(values))
        n = rows.num_rows
        if n == 0:
            continue
        written = True
        yield pa.Table.from_arrays([rows["timestamp"], rows["duration_s"], _const(service, n),
                                    _const("ear", n), _const(metric, n), rows["value"],
                                    rows["labels"]], schema=SCHEMA)
    if not written:
        yield SCHEMA.empty_table()


def _labels(nodes, job_ids, step_ids):
    """Build the ``labels`` of each row: ``node``, plus ``job_id`` and ``step_id`` when a job ran (job_id not 0)."""
    out = []
    for node, job, step in zip(nodes.to_pylist(), job_ids.to_pylist(), step_ids.to_pylist()):
        labels = [("node", node)]
        if job:
            labels += [("job_id", str(job)), ("step_id", str(step))]
        out.append(labels)
    return pa.array(out, LABELS)


def _step_labels(nodes, starts, ends, steps):
    """Build the ``labels`` of each row: ``node``, plus ``job_steps``/``job_ids`` when steps ran on it (:func:`_present`)."""
    label, steps = steps
    # ponytail: per-row Python bisect; vectorise if a month takes too long on Snellius
    out = []
    for node, start, end in zip(nodes.to_pylist(), starts.to_pylist(), ends.to_pylist()):
        labels = [("node", node)]
        ran = _present(steps.get(node), start, end)
        if ran:
            labels.append((label, ran))
        out.append(labels)
    return pa.array(out, LABELS)


def _const(value, n):
    """Return a dictionary-encoded string array with ``value`` repeated ``n`` times."""
    return pa.DictionaryArray.from_arrays(pa.array([0] * n, pa.int32()), pa.array([value]))
