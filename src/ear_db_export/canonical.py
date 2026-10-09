"""Step 2, canonical datasets: EAR's records reorganised for reading, one table per subject.

EAR's database is designed for EAR to write fast, not for reading: one row
per node and minute with ``0`` meaning "no value", jobs split over ``Jobs``
and ``Applications``, signatures behind id pointers. A *canonical dataset* is
the same data reorganised for reading: one table per subject, already joined,
with UTC timestamps, NULLs and units in the column names. Every reader
produces the records it is built from, and every format (step 3) starts from
it, so its schema is the fixed point between the database and the files.

Each dataset is built by one function ``build(records, tags, key)``:

- ``records``: the :class:`ear_db_export.read.Records` of one period (a day
  or a month). The function reads only the record tables listed in its
  :data:`DATASETS` entry.
- ``tags``: ``{node: tag}`` from ear.conf, for the ``node_tag`` column.
- ``key``: the pseudonymisation key, or ``None`` to keep user fields in clear.
  User fields are pseudonymised only here, so no format ever sees them in clear.

The examples in the docstrings show column subsets; the column lists are in
the *Output* lines and in :doc:`/data`.
"""
import hashlib
import hmac
import logging
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

log = logging.getLogger(__name__)

#: Timestamp type of every exported time column: seconds, UTC.
TS = pa.timestamp("s", tz="UTC")
#: The record columns the datasets read, in this order. The dataset schemas are
#: a public contract, so logical columns added later (EAR 7's DCGM GPU metrics,
#: ``def_gpu_f``) stay out of them until a dataset adds them on purpose.
INPUTS = {
    "periodic_metrics": ["id", "start_time", "end_time", "node_id", "job_id", "step_id",
                         "dc_energy", "avg_f", "temp", "dram_energy", "pck_energy", "gpu_energy"],
    "jobs": ["job_id", "step_id", "local_id", "user_id", "user_acc", "user_group", "app_name",
             "start_time", "end_time", "start_mpi_time", "end_mpi_time", "policy", "threshold",
             "procs", "job_type", "def_f", "e_tag"],
    "applications": ["job_id", "step_id", "local_id", "node_id", "signature_id",
                     "power_signature_id"],
    "power_signatures": ["id", "dc_power", "dram_power", "pck_power", "edp", "max_dc_power",
                         "min_dc_power", "time", "avg_f", "def_f"],
    "signatures": ["id", "dc_power", "dram_power", "pck_power", "edp", "gbs", "io_mbs", "tpi",
                   "cpi", "gflops", "time", "perc_mpi", "l1_misses", "l2_misses", "l3_misses"]
                  + ["flops_%d" % i for i in range(1, 9)]
                  + ["instructions", "cycles", "avg_f", "avg_imc_f", "def_f", "cpu_util",
                     "min_gpu_sig_id", "max_gpu_sig_id"],
    "gpu_signatures": ["id", "gpu_power", "gpu_freq", "gpu_mem_freq", "gpu_util", "gpu_mem_util",
                       "gpu_gflops"],
}
#: Columns that identify a user and are pseudonymised.
USER_FIELDS = ("user_id", "user_acc", "user_group")
#: ``Jobs`` columns holding epoch seconds, converted to timestamps.
JOB_TIMES = ("start_time", "end_time", "start_mpi_time", "end_mpi_time")
#: ``Signatures`` columns that are pointers to other rows, not data; not exported.
SIG_POINTERS = ("id", "min_gpu_sig_id", "max_gpu_sig_id")
#: ``node_metrics`` value columns: ``(periodic_metrics column, output column, zero means missing)``.
#: Units, checked in EAR's ``src/daemon/power_monitor.c``: energies are average
#: power (W) times interval seconds, so joules; ``avg_f`` is kHz; ``temp`` is
#: the maximum socket temperature in Celsius. EAR writes ``0`` when a sensor
#: is unavailable (GPU energy on CPU-only nodes, DRAM energy on AMD nodes
#: without DRAM RAPL, ``avg_f``/``temp`` when node metrics fail). DC energy is
#: never 0 for a valid sample, so its zeros are kept.
NODE_METRICS = [
    ("dc_energy", "dc_energy_j", False),
    ("dram_energy", "dram_energy_j", True),
    ("pck_energy", "pck_energy_j", True),
    ("gpu_energy", "gpu_energy_j", True),
    ("avg_f", "avg_cpu_freq_khz", True),
    ("temp", "temp_c", True),
]


# == Datasets: build(records, tags, key) -> table ===============================

def node_metrics(records, tags, key):
    """``node_metrics``: one row per node and interval, sorted by ``node, end``.

    Input, ``records.periodic_metrics``: one row per node and interval (~60 s)::

        node_id  job_id  step_id  start_time  end_time    dc_energy  avg_f    temp  gpu_energy
        n1       0       0        1783136226  1783136286  900        0        51    0
        n2       42      0        1783136226  1783136286  1100       2100000  50    5

    Output::

        node  node_tag  job_id  step_id  start                end                  dc_energy_j  avg_cpu_freq_khz  temp_c  gpu_energy_j
        n1    cpu       null    null     2026-07-04 03:37:06  2026-07-04 03:38:06  900          null              51      null
        n2    gpu       42      0        2026-07-04 03:37:06  2026-07-04 03:38:06  1100         2100000           50      5

    What changes:

    - ``job_id``/``step_id`` 0 (the node was idle) become null.
    - Epoch seconds become UTC timestamps (``start``, ``end``).
    - A sensor's 0 becomes null, except DC energy (:data:`NODE_METRICS`), and
      every value column gets its unit in the name.
    - ``node_tag`` is added from ear.conf; ``id`` is dropped.

    Output columns: ``node, node_tag, job_id, step_id, start, end,
    dc_energy_j, dram_energy_j, pck_energy_j, gpu_energy_j, avg_cpu_freq_khz,
    temp_c``.
    """
    wide = records.periodic_metrics.sort_by([("node_id", "ascending"), ("end_time", "ascending")])
    idle = pc.equal(wide["job_id"], 0)
    no_job = lambda c: pc.if_else(idle, pa.scalar(None, c.type), c)
    out = pa.table({
        "node": wide["node_id"], "node_tag": node_tags(wide["node_id"], tags),
        "job_id": no_job(wide["job_id"]), "step_id": no_job(wide["step_id"]),
        "start": to_ts(wide["start_time"]), "end": to_ts(wide["end_time"]),
    })
    for col, name, zero_missing in NODE_METRICS:
        values = wide[col]
        out = out.append_column(name, _zero_to_null(values) if zero_missing else values)
    return out


def job_nodes(records, tags, key):
    """``job_nodes``: one row per job step and node it ran on, sorted by job, step and node.

    Input, ``records.jobs`` (one row per job step) and ``records.applications``
    (one row per step and node)::

        jobs:          job_id  step_id  local_id  user_id  start_time  end_time    …
                       42      0        5         alice    1783136226  1783139826

        applications:  job_id  step_id  local_id  node_id  signature_id  power_signature_id
                       42      0        5         n2       100           10
                       42      0        5         n3       101           11

    Output::

        job_id  step_id  local_id  user_id           start_time           end_time             …  node  node_tag
        42      0        5         3f2a9c1e0b7d4e61  2026-07-04 03:37:06  2026-07-04 04:37:06  …  n2    gpu
        42      0        5         3f2a9c1e0b7d4e61  2026-07-04 03:37:06  2026-07-04 04:37:06  …  n3    gpu

    What changes:

    - ``Jobs`` and ``Applications`` are joined on ``records.join_keys``
      (``job_id, step_id``, plus ``local_id`` from EAR 5.0); steps without
      applications are left out.
    - Time columns become UTC timestamps, with 0 as null.
    - User fields are pseudonymised (:func:`pseudonymize`).
    - ``node_id`` becomes ``node``; ``node_tag`` is added; the signature ids are dropped.

    Output columns: every ``jobs`` column (``job_id, step_id, local_id,
    user_id, user_acc, user_group, app_name, start_time, end_time,
    start_mpi_time, end_mpi_time, policy, threshold, procs, job_type, def_f,
    e_tag``), then ``node, node_tag``.
    """
    jobs = records.jobs
    t = jobs_x_apps(clean_jobs(jobs, key), records.applications, records.join_keys)
    out = t.select(jobs.column_names)
    out = out.append_column("node", t["node_id"])
    return out.append_column("node_tag", node_tags(t["node_id"], tags))


def power_signatures(records, tags, key):
    """``power_signatures``: one row per job step and node, with its power signature.

    Input, ``records.jobs`` and ``records.applications`` as in
    :func:`job_nodes`, and ``records.power_signatures``, the rows
    ``applications.power_signature_id`` points to::

        power_signatures:  id  dc_power  pck_power  time  …
                           10  310.5     180.2      3600

    Output::

        job_job_id  job_step_id  …  node  node_tag  ps_dc_power  ps_pck_power  ps_time  …
        42          0            …  n2    gpu       310.5        180.2         3600
        42          0            …  n3    gpu       null         null          null

    What changes: the ``jobs`` columns are cleaned as in :func:`job_nodes` and
    prefixed ``job_``; the power signature columns are prefixed ``ps_``
    (without ``id``). Rows without a power signature are kept with null
    ``ps_*`` and counted in a warning.

    Output columns: ``job_*``, ``node, node_tag``, ``ps_*``.
    """
    base, t = _signature_base(records.jobs, records.applications, tags, key, records.join_keys)
    out, pos = _left_join_by_id(base, t["power_signature_id"], records.power_signatures,
                                "ps_", ("id",))
    if pos.null_count:
        log.warning("%d (job, step, node) rows have no power signature (kept, ps_* NULL)",
                    pos.null_count)
    return out


def app_signatures(records, tags, key):
    """``app_signatures``: one row per job step and node that has an EARL application signature.

    Input, ``records.jobs`` and ``records.applications`` as in
    :func:`job_nodes`, and ``records.signatures``, the rows
    ``applications.signature_id`` points to::

        signatures:  id   cpi   gbs   gflops  min_gpu_sig_id  max_gpu_sig_id  …
                     100  0.61  12.3  85.1    50              52

    Output::

        job_job_id  job_step_id  …  node  node_tag  sig_cpi  sig_gbs  sig_gflops  …
        42          0            …  n2    gpu       0.61     12.3     85.1

    What changes: the ``jobs`` columns are cleaned as in :func:`job_nodes` and
    prefixed ``job_``; the signature columns are prefixed ``sig_``, without
    the pointer columns :data:`SIG_POINTERS`. Rows without a signature are
    left out.

    Output columns: ``job_*``, ``node, node_tag``, ``sig_*`` (including
    ``sig_flops_1`` … ``sig_flops_8``).
    """
    apps = _with_signature(records.applications)
    base, t = _signature_base(records.jobs, apps, tags, key, records.join_keys)
    out, pos = _left_join_by_id(base, t["signature_id"], records.signatures, "sig_",
                                SIG_POINTERS)
    return out.filter(pc.is_valid(pos))


def gpu_signatures(records, tags, key):
    """``gpu_signatures``: one row per GPU of each application signature.

    Input, ``records.jobs``, ``records.applications`` and
    ``records.signatures`` as in :func:`app_signatures`, and
    ``records.gpu_signatures``, which the reader has already linked to its
    signature (:meth:`ear_db_export.read.Reader.gpu_signatures`)::

        gpu_signatures:  signature_id  gpu_index  gpu_power  gpu_util  …
                         100           0          200.0      97
                         100           2          220.0      95

    Output, in the order of the :func:`app_signatures` rows, then by ``gpu_index``::

        job_id  step_id  local_id  node  gpu_index  gpu_power  gpu_util  …
        42      0        5         n2    0          200.0      97
        42      0        5         n2    2          220.0      95

    What changes: each GPU row gets the job step and node of its signature;
    ``signature_id`` is dropped. ``gpu_index`` keeps its position when a GPU
    in the middle has no row (index 1 above).

    Output columns: ``job_id, step_id`` (``local_id`` from EAR 5.0), ``node,
    gpu_index``, then the ``gpu_signatures`` columns without ``id``.
    """
    keys = records.join_keys
    t = jobs_x_apps(records.jobs, _with_signature(records.applications), keys)
    refs = t.select(keys + ["node_id", "signature_id"]).filter(
        pc.is_in(t["signature_id"], value_set=records.signatures["id"]))
    refs = refs.append_column("row", pa.array(range(refs.num_rows), pa.int64()))
    gpus = records.gpu_signatures
    joined = refs.join(gpus, keys="signature_id", join_type="inner").sort_by(
        [("row", "ascending"), ("gpu_index", "ascending")])
    gpu_cols = [c for c in gpus.column_names if c not in ("signature_id", "gpu_index")]
    out = joined.select(keys + ["node_id", "gpu_index"] + gpu_cols)
    return out.rename_columns(["node" if c == "node_id" else c for c in out.column_names])


@dataclass(frozen=True)
class Dataset:
    """How to build one canonical dataset.

    :ivar inputs: the record tables :attr:`build` reads (see :class:`ear_db_export.read.Records`).
    :ivar build: ``build(records, tags, key) -> pyarrow.Table``.
    :ivar empty_warning: logged when a written partition has no rows, if set.
    """

    inputs: tuple
    build: object
    empty_warning: str = ""


#: Every canonical dataset, by name.
DATASETS = {
    "node_metrics": Dataset(("periodic_metrics",), node_metrics,
                            "no periodic metrics (pruned, or EAR not reporting?)"),
    "job_nodes": Dataset(("jobs", "applications"), job_nodes),
    "power_signatures": Dataset(("jobs", "applications", "power_signatures"), power_signatures),
    "app_signatures": Dataset(("jobs", "applications", "signatures"), app_signatures),
    "gpu_signatures": Dataset(("jobs", "applications", "signatures", "gpu_signatures"),
                              gpu_signatures),
}


# == Helpers =====================================================================

def to_ts(arr):
    """Convert Unix epoch seconds to UTC timestamps. EAR's ``0`` ("unset") becomes null.

    :param arr: integer array or chunked array of epoch seconds.
    :returns: the same values as ``timestamp[s, UTC]``.
    """
    return pc.cast(_zero_to_null(arr), TS)


def _zero_to_null(arr):
    """Replace the zeros of an array by nulls."""
    return pc.if_else(pc.equal(arr, 0), pa.scalar(None, arr.type), arr)


def pseudonym(key, value):
    """Return the pseudonym of one value: ``HMAC-SHA256(key, value)``, first 16 hex digits.

    The same key and value always give the same pseudonym, so pseudonymised
    datasets stay joinable across runs and exporters that share the key.

    :param key: the secret key (contents of ``--salt-file``), bytes.
    :param value: the value to hide, str.
    :returns: 16 lowercase hex characters.
    """
    return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:16]


def pseudonymize(table, key, columns=USER_FIELDS):
    """Replace user-identifying columns by their pseudonyms.

    Each distinct value is hashed once, then mapped back onto the rows. Nulls
    stay null. Columns missing from the table are skipped.

    :param table: a ``pyarrow.Table``.
    :param key: the secret key, bytes; ``None`` returns the table unchanged.
    :param columns: names of the columns to replace.
    :returns: the table with those columns as pseudonym strings.
    """
    if key is None:
        return table
    for name in columns:
        if name not in table.column_names:
            continue
        col = table[name]
        # Hash each distinct value once, then look every row up in that list
        uniq = pc.unique(col).to_pylist()
        mapping = pa.array([None if v is None else pseudonym(key, v) for v in uniq], pa.string())
        idx = pc.index_in(col, value_set=pa.array(uniq, col.type))
        table = table.set_column(table.schema.get_field_index(name), name, mapping.take(idx))
    return table


def node_tags(nodes, tags):
    """Look up the ear.conf tag of each node.

    :param nodes: array of node names.
    :param tags: ``{node: tag}`` from :func:`ear_db_export.config.load`.
    :returns: string array of tags, null for nodes not in ear.conf.
    """
    pos = pc.index_in(nodes, value_set=pa.array(list(tags), pa.string()))
    return pc.take(pa.array(list(tags.values()), pa.string()), pos)


def jobs_x_apps(jobs, apps, keys):
    """Inner-join job steps with their applications: one row per (job, step[, local_id], node).

    :param jobs: ``jobs`` records.
    :param apps: ``applications`` records.
    :param keys: join columns (``Records.join_keys``).
    :returns: the jobs columns plus ``node_id`` and the signature ids, sorted by
        the keys and the node.
    """
    if "local_id" not in keys:  # all-NULL column on both sides: never a join key
        apps = apps.drop(["local_id"])
    out = jobs.join(apps, keys=keys, join_type="inner")
    return out.sort_by([(k, "ascending") for k in keys + ["node_id"]])


def clean_jobs(jobs, key):
    """Convert the ``jobs`` time columns to UTC timestamps and pseudonymise the user fields.

    :param jobs: ``jobs`` records.
    :param key: pseudonymisation key, or ``None`` to keep user fields in clear.
    :returns: the cleaned table.
    """
    for name in JOB_TIMES:
        jobs = jobs.set_column(jobs.schema.get_field_index(name), name, to_ts(jobs[name]))
    return pseudonymize(jobs, key)


def _with_signature(apps):
    """Keep the ``applications`` rows that point to an EARL signature."""
    return apps.filter(pc.is_valid(apps["signature_id"]))


def _signature_base(jobs, apps, tags, key, keys):
    """Build the common part of every signature row: ``job_*`` columns, ``node`` and ``node_tag``.

    :returns: ``(base, joined)``: the output columns so far, and the full join,
        which still has the signature ids.
    """
    t = jobs_x_apps(clean_jobs(jobs, key), apps, keys)
    out = _prefixed(t.select(jobs.column_names), "job_")
    out = out.append_column("node", t["node_id"])
    out = out.append_column("node_tag", node_tags(t["node_id"], tags))
    return out, t


def _prefixed(table, prefix):
    """Rename every column to ``prefix + name``."""
    return table.rename_columns([prefix + c for c in table.column_names])


def _left_join_by_id(base, ids, right, prefix, drop):
    """Append the columns of the ``right`` rows whose ``id`` matches ``ids``, keeping base's row order.

    :param base: table to extend.
    :param ids: one id per base row (may contain nulls).
    :param right: table with an ``id`` column.
    :param prefix: prefix for the appended column names.
    :param drop: ``right`` columns not to append.
    :returns: ``(table, positions)``: the extended table, and for each base row
        the matched row index in ``right`` (null where there was no match).
    """
    pos = pc.index_in(ids, value_set=right["id"])
    for name in right.column_names:
        if name not in drop:
            base = base.append_column(prefix + name, pc.take(right[name], pos))
    return base, pos
