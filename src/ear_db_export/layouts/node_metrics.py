"""``node_metrics``: EAR's node metrics (``Periodic_metrics``) in long format, one row per node, interval and metric.

EAR's daemon writes one ``Periodic_metrics`` row per node and interval (about
60 s); each becomes one row per metric that has a value::

    node_id  job_id  step_id  start_time  end_time    dc_energy  temp  gpu_energy
    n2       42      0        1783136226  1783136286  1100       50    0

    timestamp            duration_s  service   source  metric_name                value   labels
    2026-07-04 03:38:06  60          snellius  ear     ear_node_dc_energy_joules  1100.0  {node: n2, job_id: 42, step_id: 0}
    2026-07-04 03:38:06  60          snellius  ear     ear_node_temp_celsius        50.0  {node: n2, job_id: 42, step_id: 0}

- ``timestamp`` is the interval's end (UTC), ``duration_s`` its length.
- A metric without a value is left out: a null, or EAR's 0 for "no value"
  (:data:`METRICS`; a valid DC energy is never 0, so its zeros are kept).
- ``labels`` has the node, and the job and step when a job ran (``job_id`` 0
  means an idle node).
- Rows come in ``Periodic_metrics`` id order, which is roughly time order: one
  table per metric for each page read, so a month never sits in memory.

Flags: the time window (rows whose interval ends in it), ``--nodes``/``--tags``,
and ``--service``. ``--hive`` writes one file per day.

Flow, as a UML sequence diagram:

.. mermaid::

    sequenceDiagram
        participant CLI as cli.run
        participant L as node_metrics layout
        participant T as TimeSelection
        participant S as Source
        participant O as Output
        CLI->>L: run(source, args, out)
        L->>T: from_args("periodic_metrics", args, out.window, source.tags)
        L->>T: batches(source)
        T->>S: window("periodic_metrics", start, end, [node_id in nodes])
        loop each primary-key page (1 M rows)
            S-->>T: Periodic_metrics rows
            T-->>L: batch
            L->>L: typed(), then long_rows(): one table per metric
            L->>O: write("node_metrics", rows, times=timestamp)
        end
"""
import pyarrow as pa
import pyarrow.compute as pc

from ..clean import TS, to_ts, typed, zero_to_null
from ..selection.timeseries import TimeSelection

NAME = "node_metrics"
HELP = "EAR's node metrics in long format: one row per node, interval and metric"
READS = "Periodic_metrics"

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


def run(source, args, out):
    """Write the node metrics of the window in long format, one page of ``Periodic_metrics`` at a time."""
    sel = TimeSelection.from_args("periodic_metrics", args, out.window, source.tags)
    for batch in sel.batches(source):
        for rows in long_rows(typed("periodic_metrics", batch), args.service):
            out.write(NAME, rows, times=rows["timestamp"])


def long_rows(metrics, service):
    """Turn ``periodic_metrics`` rows (logical types) into long rows: one table per metric that has values.

    :param metrics: ``periodic_metrics`` rows from :func:`ear_db_export.clean.typed`.
    :param service: the value of the ``service`` column.
    :returns: an iterator of :data:`SCHEMA` tables; at least one (empty if no
        metric has a value), so the file always has its schema.

    Example: 2 rows, n1 idle (``temp`` 51, ``gpu_energy`` 0) and n2 running job
    42 (``temp`` 50, ``gpu_energy`` 5), give a ``dc_energy`` table of 2 rows,
    a ``gpu_energy`` table of 1 row (n1's 0 is left out), a ``temp`` table of
    2 rows, and so on; only n2's labels have ``job_id`` and ``step_id``.
    """
    seconds = lambda c: pc.cast(metrics[c], pa.int64())  # noqa: E731
    base = pa.table({
        "timestamp": to_ts(metrics["end_time"]),
        "duration_s": pc.cast(pc.subtract(seconds("end_time"), seconds("start_time")), pa.int32()),
        "labels": _labels(metrics["node_id"], metrics["job_id"], metrics["step_id"]),
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


def _const(value, n):
    """Return a dictionary-encoded string array with ``value`` repeated ``n`` times."""
    return pa.DictionaryArray.from_arrays(pa.array([0] * n, pa.int32()), pa.array([value]))
