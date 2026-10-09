"""Step 3, formats: shape one canonical dataset the way its consumer wants it.

A *format function* takes one canonical dataset (step 2,
:mod:`ear_db_export.canonical`) and the ``--format-opt`` values, and returns
what step 4 writes::

    def my_format(table, opts):     # table: e.g. node_metrics; opts: {"service": "x"}
        return out_table            # or yield several tables that share one schema

The module has three parts:

1. **Format functions**: the bundled formats, :func:`wide`, :func:`long` and
   :func:`allocations`.
2. **Choosing a format**: :data:`FORMATS` lists which names each dataset
   accepts, :func:`resolve` turns ``--format DATASET=NAME`` into a function,
   and :func:`apply` runs it. A new format is a function added to :data:`FORMATS`.
3. **Helpers of** ``long``: its metric names and schema, and private helpers.
   They are not formats.
"""
import itertools

import pyarrow as pa
import pyarrow.compute as pc

from .canonical import TS
from .config import ConfigError


# == 1. Format functions: (table, opts) -> table, or iterator of tables ============

def wide(table, opts):
    """The canonical dataset as it is: one column per variable. The default for every dataset.

    :param table: any canonical dataset.
    :param opts: unused.
    :returns: ``table``.
    """
    return table


def long(table, opts):
    """``node_metrics`` in long ("tidy") format: one row per node, interval and metric.

    One ``node_metrics`` row becomes one row per metric that has a value::

        node  end                  dc_energy_j  temp_c        timestamp            metric_name                value   labels
        n2    2026-07-04 03:38:06  1100         50      →     2026-07-04 03:38:06  ear_node_dc_energy_joules  1100.0  {node: n2, job_id: 42, step_id: 0}
                                                              2026-07-04 03:38:06  ear_node_temp_celsius        50.0  {node: n2, job_id: 42, step_id: 0}

    Columns (:data:`LONG_SCHEMA`): ``timestamp`` (interval end),
    ``duration_s``, ``service``, ``source``, ``metric_name``
    (:data:`LONG_METRICS`), ``value`` (float64) and ``labels`` (``node``, and
    ``job_id``/``step_id`` when a job ran). Missing values are left out. Rows
    come sorted by ``(metric_name, node, timestamp)``, one table per metric, so
    the whole long table is never held in memory.

    :param table: ``node_metrics``, sorted by node and end time.
    :param opts: ``service`` (required) and ``source`` (default ``ear``), the
        values of the ``service`` and ``source`` columns.
    :returns: an iterator of :data:`LONG_SCHEMA` tables.
    :raises ConfigError: if ``service`` is not given.
    """
    if not opts.get("service"):
        raise ConfigError("format long needs --format-opt service=NAME")
    service, source = opts["service"], opts.get("source", "ear")
    # Columns shared by every metric: interval end, length and labels
    seconds = lambda c: pc.cast(table[c], pa.int64())
    base = pa.table({
        "timestamp": table["end"],
        "duration_s": pc.cast(pc.subtract(seconds("end"), seconds("start")), pa.int32()),
        "labels": _labels(table["node"], table["job_id"], table["step_id"]),
    })
    written = False
    for col, name in LONG_METRICS:
        values = table[col]
        rows = base.append_column("value", pc.cast(values, pa.float64())).filter(
            pc.is_valid(values))
        n = rows.num_rows
        if n == 0:
            continue
        written = True
        yield pa.Table.from_arrays([
            rows["timestamp"], rows["duration_s"], _const(service, n), _const(source, n),
            _const(name, n), rows["value"], rows["labels"],
        ], schema=LONG_SCHEMA)
    if not written:  # an empty file still needs its schema
        yield LONG_SCHEMA.empty_table()


def allocations(table, opts):
    """``job_nodes`` as Slurm-style allocation records: who ran on which node, and when.

    Keeps and renames a subset of the columns::

        job_id  step_id  local_id  node  start                end                  user_id  …  procs  node_tag
        42      0        5         n2    2026-07-04 03:37:06  2026-07-04 04:37:06  3f2a…    …  64     gpu

    Columns: ``job_id, step_id, local_id, node, start, end, user_id, user_acc,
    user_group, app_name, policy, job_type, procs, node_tag``, where ``start``
    and ``end`` are the step's ``start_time`` and ``end_time``.

    :param table: ``job_nodes``.
    :param opts: unused.
    :returns: the selected and renamed columns.
    """
    t = table
    return pa.table({
        "job_id": t["job_id"], "step_id": t["step_id"], "local_id": t["local_id"],
        "node": t["node"], "start": t["start_time"], "end": t["end_time"],
        "user_id": t["user_id"], "user_acc": t["user_acc"], "user_group": t["user_group"],
        "app_name": t["app_name"], "policy": t["policy"], "job_type": t["job_type"],
        "procs": t["procs"], "node_tag": t["node_tag"],
    })


# == 2. Choosing a format: --format DATASET=NAME ====================================

#: Bundled format names per dataset. ``wide`` is the default everywhere.
FORMATS = {
    "node_metrics": {"wide": wide, "long": long},
    "job_nodes": {"wide": wide, "allocations": allocations},
    "power_signatures": {"wide": wide},
    "app_signatures": {"wide": wide},
    "gpu_signatures": {"wide": wide},
    "eacct": {"wide": wide},
}


def resolve(dataset, name):
    """Find the format function for a dataset.

    :param dataset: canonical dataset name, a key of :data:`FORMATS`.
    :param name: a format name of that dataset.
    :returns: the format function.
    :raises ConfigError: if the dataset has no format of that name.
    """
    if name not in FORMATS[dataset]:
        raise ConfigError("unknown format %s for %s (one of: %s)"
                          % (name, dataset, ", ".join(FORMATS[dataset])))
    return FORMATS[dataset][name]


def apply(dataset, name, table, opts):
    """Run a format on a canonical dataset, and hand step 4 a non-empty iterator of tables.

    :param dataset: canonical dataset name.
    :param name: format name, as for :func:`resolve`.
    :param table: the canonical dataset.
    :param opts: the ``--format-opt`` values.
    :returns: an iterator of tables; the first one is always there and gives
        the file's schema.
    :raises ConfigError: if the format is unknown or rejects its options.
    :raises ValueError: if the format yields no table.
    """
    out = resolve(dataset, name)(table, opts)
    chunks = iter([out] if isinstance(out, pa.Table) else out)
    first = next(chunks, None)
    if first is None:
        raise ValueError("format %s yielded no table for %s" % (name, dataset))
    return itertools.chain([first], chunks)


# == 3. Helpers of long (not formats) ===============================================

#: ``long`` metrics: ``(node_metrics column, metric_name)``, in output order.
#: Names follow the Prometheus conventions: namespace prefix, base unit suffix.
LONG_METRICS = [
    ("dc_energy_j", "ear_node_dc_energy_joules"),
    ("dram_energy_j", "ear_node_dram_energy_joules"),
    ("pck_energy_j", "ear_node_pck_energy_joules"),
    ("gpu_energy_j", "ear_node_gpu_energy_joules"),
    ("avg_cpu_freq_khz", "ear_node_avg_cpu_freq_khz"),
    ("temp_c", "ear_node_temp_celsius"),
]
_DICT = pa.dictionary(pa.int32(), pa.string())  # repeated strings stored once per file
#: Type of the ``labels`` column of ``long``: string → string map.
LABELS = pa.map_(pa.string(), pa.string())
#: Schema of the ``long`` format.
LONG_SCHEMA = pa.schema([
    ("timestamp", TS), ("duration_s", pa.int32()), ("service", _DICT), ("source", _DICT),
    ("metric_name", _DICT), ("value", pa.float64()), ("labels", LABELS),
])


def _labels(nodes, job_ids, step_ids):
    """Build the ``labels`` map of each ``long`` row: ``node``, plus ``job_id``/``step_id`` when a job ran.

    To switch to JSON strings: return
    ``pa.array([json.dumps(dict(l)) for l in out])`` and change :data:`LABELS`
    to ``pa.string()``.
    """
    out = []
    for node, job, step in zip(nodes.to_pylist(), job_ids.to_pylist(), step_ids.to_pylist()):
        labels = [("node", node)]
        if job is not None:
            labels += [("job_id", str(job)), ("step_id", str(step))]
        out.append(labels)
    return pa.array(out, LABELS)


def _const(value, n):
    """Return a dictionary array with the same string repeated ``n`` times."""
    return pa.DictionaryArray.from_arrays(pa.array([0] * n, pa.int32()), pa.array([value]))
