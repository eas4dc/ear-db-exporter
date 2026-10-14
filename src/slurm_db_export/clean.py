"""Turn slurmdbd's stored job values into analysis-ready columns (the ``jobs`` layout), and hide user ids.

slurmdbd stores what is quickest for it to write: resources as TRES strings
(``1=64,2=256000,1001=4``: TRES id = count, ids in ``tres_table``), memory with a
per-CPU flag in its top bit, states as integers with flag bits, times as epoch
seconds with ``0`` for "not yet", and ``NO_VAL``/``INFINITE`` sentinels. The
constants come from ``slurm/slurm.h`` (Slurm 25.05; unchanged since 17.11).
"""
import pyarrow as pa
import pyarrow.compute as pc

from ear_db_export.clean import pseudonymize, to_ts

#: ``slurm.h``: "no value" for 32 and 64-bit fields, and "unlimited".
NO_VAL, NO_VAL64, INFINITE, INFINITE64 = 0xfffffffe, 0xfffffffffffffffe, 0xffffffff, \
    0xffffffffffffffff
#: ``slurm.h``: the top bit of ``mem_req``: the request is per CPU, not per node.
MEM_PER_CPU = 0x8000000000000000
#: ``enum job_states`` (``state & JOB_STATE_BASE``), named as ``sacct`` names them.
STATES = ("PENDING", "RUNNING", "SUSPENDED", "COMPLETED", "CANCELLED", "FAILED", "TIMEOUT",
          "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "OUT_OF_MEMORY")
JOB_STATE_BASE = 0xff
#: State flags, in the order ``job_state_string()`` tests them: a set flag names the state.
STATE_FLAGS = ((1 << 15, "COMPLETING"), (1 << 23, "STAGE_OUT"), (1 << 14, "CONFIGURING"),
               (1 << 13, "RESIZING"), (1 << 10, "REQUEUED"), (1 << 20, "REQUEUE_FED"),
               (1 << 11, "REQUEUE_HOLD"), (1 << 12, "SPECIAL_EXIT"), (1 << 16, "STOPPED"),
               (1 << 19, "REVOKED"), (1 << 21, "RESV_DEL_HOLD"), (1 << 22, "SIGNALING"))
#: Epoch-second columns of the job table that get a ``<column>_ts`` timestamp.
JOB_TIMES = ("time_submit", "time_eligible", "time_start", "time_end", "mod_time")
#: Columns that identify a person, pseudonymised in each raw table that has them.
USER_COLUMNS = {"job": ("id_user", "id_group", "account", "kill_requid"),
                "assoc": ("user", "acct", "parent_acct"),
                "event": ("reason_uid",)}
#: Columns dropped when pseudonymising: they hold names that cannot be hashed in place.
NAME_COLUMNS = {"assoc": ("lineage",)}  # "/root/acct/0-alice/"


def hide(table, t, key):
    """Pseudonymise the user columns of a raw table (:data:`USER_COLUMNS`); drop :data:`NAME_COLUMNS`.

    Integer ids are hashed as their decimal text, so ``id_user`` 1000 becomes
    ``HMAC(key, "1000")``: the same pseudonym on every run with the same salt.

    :param table: logical table name.
    :param t: ``pyarrow.Table`` with physical names.
    :param key: the salt (bytes); ``None`` returns ``t`` unchanged.
    """
    if key is None:
        return t
    cols = [c for c in USER_COLUMNS.get(table, ()) if c in t.column_names]
    for c in cols:
        if not pa.types.is_string(t[c].type):
            t = t.set_column(t.schema.get_field_index(c), c, pc.cast(t[c], pa.string()))
    t = t.drop_columns([c for c in NAME_COLUMNS.get(table, ()) if c in t.column_names])
    return pseudonymize(t, key, cols)


def tres_names(tres):
    """Return ``{TRES id: column-safe name}`` from ``tres_table`` rows.

    The name is ``type`` or ``type/name``, with ``/``, ``:`` and ``-`` turned into
    ``_``: ``cpu``, ``mem``, ``gres_gpu``, ``gres_gpu_a100``, ``fs_disk``.

    :param tres: ``pyarrow.Table`` with ``id``, ``type``, ``name``.
    """
    out = {}
    for r in tres.select(["id", "type", "name"]).to_pylist():
        full = r["type"] + ("/" + r["name"] if r["name"] else "")
        out[r["id"]] = full.replace("/", "_").replace(":", "_").replace("-", "_")
    return out


def parse_tres(text):
    """Parse one TRES string into ``{id: count}``.

    Example: ``parse_tres("1=64,2=256000,1001=4")`` is ``{1: 64, 2: 256000, 1001: 4}``;
    ``""`` and ``None`` give ``{}``.
    """
    out = {}
    for item in (text or "").split(","):
        if "=" in item:
            k, v = item.split("=", 1)
            out[int(k)] = int(v)
    return out


def tres_columns(arr, names, prefix):
    """Decode a column of TRES strings into one int64 column per TRES of ``tres_table``.

    Every TRES of ``names`` gets a column, null where the string lacks it, so
    every batch has the same columns.

    :param arr: array of TRES strings.
    :param names: ``{id: name}`` from :func:`tres_names`.
    :param prefix: ``"req_"`` or ``"alloc_"``.
    :returns: ``{prefix + name: pyarrow.Array}``.

    Example: ``["1=64,1001=4"]`` with ``{1: "cpu", 1001: "gres_gpu"}`` and ``"req_"``
    gives ``{"req_cpu": [64], "req_gres_gpu": [4]}``.
    """
    parsed = [parse_tres(s) for s in arr.to_pylist()]
    return {prefix + name: pa.array([p.get(i) for p in parsed], pa.int64())
            for i, name in sorted(names.items())}


def decode_mem_req(arr):
    """Split ``mem_req`` into the requested MB and whether it is per CPU.

    :param arr: ``mem_req`` (uint64).
    :returns: ``(mem_req_mb, mem_per_cpu)``: int64 MB (null for ``NO_VAL64`` or
        ``INFINITE64``) and bool.

    Example: ``[0x8000000000000800, 4096]`` gives ``([2048, 4096], [True, False])``.
    """
    arr = pc.cast(arr, pa.uint64())
    unset = pc.or_(pc.equal(arr, pa.scalar(NO_VAL64, pa.uint64())),
                   pc.equal(arr, pa.scalar(INFINITE64, pa.uint64())))
    flag = pa.scalar(MEM_PER_CPU, pa.uint64())
    per_cpu = pc.not_equal(pc.bit_wise_and(arr, flag), pa.scalar(0, pa.uint64()))
    mb = pc.bit_wise_and(arr, pa.scalar(MEM_PER_CPU - 1, pa.uint64()))
    mb = pc.if_else(unset, pa.scalar(None, pa.uint64()), mb)
    return pc.cast(mb, pa.int64()), pc.if_else(unset, pa.scalar(None, pa.bool_()), per_cpu)


def state_name(state):
    """Return the ``sacct`` name of one stored state: a set flag first, else the base state.

    Example: ``state_name(3)`` is ``"COMPLETED"``; ``state_name(1024)`` is ``"REQUEUED"``;
    ``state_name(99)`` is ``"UNKNOWN_99"``.
    """
    for bit, name in STATE_FLAGS:
        if state & bit:
            return name
    base = state & JOB_STATE_BASE
    return STATES[base] if base < len(STATES) else "UNKNOWN_%d" % base


def state_names(arr):
    """Map a column of stored states to their names (:func:`state_name`), each distinct value once."""
    values = pc.unique(arr).to_pylist()
    names = pa.array([None if v is None else state_name(v) for v in values], pa.string())
    return names.take(pc.index_in(arr, value_set=pa.array(values, arr.type)))


def _positive(arr):
    """Return the column as int64, null where it is 0 (Slurm's "not yet") or a ``NO_VAL``/``INFINITE`` sentinel."""
    arr = pc.cast(arr, pa.int64())
    bad = pc.or_(pc.less_equal(arr, 0), pc.greater_equal(arr, NO_VAL))
    return pc.if_else(bad, pa.scalar(None, pa.int64()), arr)


def clean_jobs(t, names):
    """Add the decoded and derived columns to job rows (logical names); the stored columns are kept.

    Added: ``req_<tres>`` and ``alloc_<tres>`` (:func:`tres_columns`),
    ``mem_req_mb`` and ``mem_per_cpu``, ``state_name``, a ``<time>_ts`` UTC
    timestamp per :data:`JOB_TIMES` column, and:

    - ``wait_s = time_start - time_eligible`` (null until the job starts),
    - ``run_s = time_end - time_start`` (null until it ends; null for jobs that never started),
    - ``timelimit_s = timelimit * 60`` (null for ``INFINITE``/``NO_VAL``),
    - ``runtime_ratio = run_s / timelimit_s``.

    :param t: ``pyarrow.Table`` of ``job_table`` rows, logical names.
    :param names: ``{TRES id: name}`` from :func:`tres_names`.
    :returns: the table with the added columns.
    """
    cols = {}
    for src, prefix in (("tres_req", "req_"), ("tres_alloc", "alloc_")):
        cols.update(tres_columns(t[src], names, prefix))
    cols["mem_req_mb"], cols["mem_per_cpu"] = decode_mem_req(t["mem_req"])
    cols["state_name"] = state_names(t["state"])
    for c in JOB_TIMES:
        cols[c + "_ts"] = to_ts(t[c])
    eligible, start, end = (_positive(t[c]) for c in ("time_eligible", "time_start", "time_end"))
    cols["wait_s"] = pc.subtract(start, eligible)
    cols["run_s"] = pc.subtract(end, start)
    cols["timelimit_s"] = pc.multiply(_positive(t["timelimit"]), 60)
    cols["runtime_ratio"] = pc.divide(pc.cast(cols["run_s"], pa.float64()),
                                      pc.cast(cols["timelimit_s"], pa.float64()))
    for name, values in cols.items():
        t = t.append_column(name, values)
    return t
