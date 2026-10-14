"""Sources: where EAR's tables are read from, and the list of those tables.

A *source* reads EAR's tables from wherever they are:

- :class:`~ear_db_export.read.sql.SqlSource`: EAR's SQL database, read-only.
- :class:`~ear_db_export.read.bundle.BundleSource`: a bundle written by ``export raw``.
- :class:`~ear_db_export.read.eacct.EacctSource`: ``eacct -l -c`` CSV files.
- :class:`~ear_db_export.read.pseudonymized.Pseudonymized` wraps any of them and
  replaces user names by pseudonyms.

Every source has the same methods, so the selections and layouts work on any
of them. Each read method is one *access path*, one way of reading a table:

======================================  ====================================================
Method                                  How it reads EAR's database
======================================  ====================================================
``scan(table, where, order, limit)``    primary-key pages, or one streamed ``SELECT``
``window(table, start, end, where)``    the time window mapped to an id range by binary
                                        search, then primary-key pages of that range
``lookup(table, column, keys)``         ``WHERE column IN (...)`` batches on the primary key
``dump(table)``                         primary-key pages of a physical table, as stored
======================================  ====================================================

``scan``, ``window`` and ``lookup`` take logical table and column names, and
yield ``pyarrow.Table`` batches with logical column names and the column types
as stored (``uint32``, ``float32``, ...); :func:`ear_db_export.clean.typed` gives
them the logical types. ``dump`` takes and yields physical names. Every read
yields at least one table (empty if no row matches), which gives the schema.
``where`` is a list of :class:`Cond`, all of which must hold.

Besides the reads, a source says what it holds and where it comes from:

- ``has(table)``: whether it holds a logical table; ``provides``: its physical tables.
- ``profile``: its schema profile, or ``None`` when the EAR version is unknown.
- ``id_step``: the database's auto-increment step, to expand GPU id ranges.
- ``tags``: each node's tag from ear.conf, ``{node: tag}``.
- ``salt_fingerprint``: not ``None`` when its user fields are already pseudonyms.
- ``info()``: a description for the bundle's manifest; ``ddl(tables)``: the
  ``CREATE TABLE`` statements; ``close()``.

EAR renames columns between versions (EAR 7 renamed most of them), so the
code uses one set of names for every version: :data:`TABLES` lists each table
and column under these *logical* names, and a schema profile
(:mod:`~ear_db_export.read.sql.profile`) maps them to the *physical* names of
one EAR version. The values stay as EAR stores them: times in Unix seconds,
``0`` where EAR had no value, and ids that point to rows of other tables.
"""
import datetime as dt
from typing import NamedTuple

import pyarrow as pa


#: Every table EAR creates, with every column: ``{table: {column: type}}``.
#: These are the *logical names*: the same for every EAR version, mapped to
#: each version's own names by the SQL reader's profiles. The type is ``int``,
#: ``float``, ``str`` or ``ts`` (a SQL ``TIMESTAMP``). A trailing ``?`` marks a
#: column that may be absent in a deployment: EAR drops some columns when
#: ``DBReportNodeDetail`` / ``DBReportSigDetail`` are 0, some only exist in
#: newer versions, and EAR 7 removed ``Jobs.job_type`` and ``Signatures.EDP``.
#: An absent optional column reads as NULL.
TABLES = {
    "periodic_metrics": {
        "id": "int", "start_time": "int", "end_time": "int", "node_id": "str",
        "job_id": "int", "step_id": "int", "dc_energy": "int",
        "avg_f": "int?", "temp": "int?", "dram_energy": "int?", "pck_energy": "int?",
        "gpu_energy": "int?",
    },
    "jobs": {
        "job_id": "int", "step_id": "int", "local_id": "int?",
        "user_id": "str", "user_acc": "str", "user_group": "str", "app_name": "str",
        "start_time": "int", "end_time": "int", "start_mpi_time": "int", "end_mpi_time": "int",
        "policy": "str", "threshold": "float", "procs": "int", "job_type": "int?",
        "def_f": "int", "e_tag": "str", "def_gpu_f": "int?",
    },
    "applications": {
        "job_id": "int", "step_id": "int", "local_id": "int?", "node_id": "str",
        "signature_id": "int", "power_signature_id": "int",
    },
    "power_signatures": {
        "id": "int", "dc_power": "float", "dram_power": "float", "pck_power": "float",
        "edp": "float", "max_dc_power": "float", "min_dc_power": "float", "time": "float",
        "avg_f": "int", "def_f": "int",
    },
    "signatures": dict(
        {"id": "int", "dc_power": "float", "dram_power": "float", "pck_power": "float",
         "edp": "float?", "gbs": "float", "io_mbs": "float", "tpi": "float", "cpi": "float",
         "gflops": "float", "time": "float", "perc_mpi": "float",
         "l1_misses": "int?", "l2_misses": "int?", "l3_misses": "int?"},
        **{"flops_%d" % i: "int?" for i in range(1, 9)},
        **{"instructions": "int?", "cycles": "int?", "avg_f": "int", "avg_imc_f": "int",
           "def_f": "int", "cpu_util": "int?", "min_gpu_sig_id": "int", "max_gpu_sig_id": "int"}),
    "gpu_signatures": dict(
        {"id": "int", "gpu_power": "float", "gpu_freq": "int", "gpu_mem_freq": "int",
         "gpu_util": "int", "gpu_mem_util": "int", "gpu_gflops": "float?"},
        # EAR 7: DCGM metrics
        **{c: "float?" for c in ("gr_engine_active", "sm_active", "sm_occupancy", "tensor_active",
                                 "dram_active", "fp64_active", "fp32_active", "fp16_active")},
        **{c: "int?" for c in ("pcie_tx_bytes", "pcie_rx_bytes", "nvlink_tx_bytes",
                               "nvlink_rx_bytes")}),
    "loops": {
        "event": "int", "size": "int", "level": "int", "job_id": "int", "step_id": "int",
        "local_id": "int?", "node_id": "str", "total_iterations": "int?", "timestamp": "int?",
        "signature_id": "int",
    },
    "events": {
        "id": "int", "timestamp": "int", "event_type": "int", "job_id": "int", "step_id": "int",
        "value": "int", "node_id": "str",
    },
    "periodic_aggregations": {
        "id": "int", "start_time": "int", "end_time": "int", "dc_energy": "int",
        "eardbd_host": "str",
    },
    "global_energy": {
        "energy_percent": "float", "warning_level": "int", "time": "ts", "inc_th": "float",
        "p_state": "int", "energy_consumed_t1": "int", "energy_consumed_t2": "int",
        "energy_limit": "int", "energy_period_t1": "int", "energy_period_t2": "int",
        "energy_policy": "str",
    },
}
# Learning_* are copies of the tables EAR writes outside the learning phase
for _t in ("applications", "jobs", "signatures"):
    TABLES["learning_" + _t] = TABLES[_t]

#: Tables a deployment may lack (other EAR builds, or never created): a
#: missing one is not an error. If present, its required columns are required.
OPTIONAL_TABLES = frozenset({"loops", "events", "periodic_aggregations", "global_energy",
                             "learning_applications", "learning_jobs", "learning_signatures"})
#: The time column of each time-series table: what ``window`` reads by. Only
#: ``Global_energy``'s is a key (its primary key); the others have no time index
#: in EAR's schema, so the SQL source reads them through an id range.
TIME_COLUMNS = {"periodic_metrics": "end_time", "events": "timestamp",
                "periodic_aggregations": "end_time", "global_energy": "time"}

#: Arrow type of each logical type of :data:`TABLES`.
ARROW = {"int": pa.int64(), "float": pa.float64(), "str": pa.string(),
         "ts": pa.timestamp("us", tz="UTC")}


class Cond(NamedTuple):
    """One condition of a filter, on one column. A filter is a list of them, all of which must hold.

    ``op`` is one of:

    - ``"="``, ``">="``, ``"<"``: compare the column with ``value``.
    - ``"in"``: the column is one of the values of the list ``value``.
    - ``"ranges"``: the column is in one of the inclusive ``(lo, hi)`` pairs of ``value``.
    - ``"like_any"``: the column matches one of the SQL ``LIKE`` patterns of ``value``.

    There is no OR between columns: each condition is about a single column, so a
    filter reads as a list of plain sentences. Example, alice's job steps that
    ended on or after 2026-07-04 00:00 UTC, of jobs 100 to 200 or 300::

        [Cond("user_id", "in", ["alice"]),
         Cond("end_time", ">=", 1783123200),
         Cond("job_id", "ranges", [(100, 200), (300, 300)])]
    """

    column: str
    op: str
    value: object


def time_conds(table, start, end):
    """Return the conditions ``start <= time < end`` on the time column of a time-series table.

    The time column is ``TIME_COLUMNS[table]``. Its values are Unix seconds, except
    ``global_energy.time``, a SQL ``TIMESTAMP``: its bounds become naive UTC
    datetimes (the SQL session's time zone is UTC).

    :param table: logical table name, a key of :data:`TIME_COLUMNS`.
    :param start: first second, Unix time (included), or ``None`` for no lower bound.
    :param end: Unix time (excluded), or ``None`` for no upper bound.
    :returns: a list of 0, 1 or 2 :class:`Cond`.

    Example::

        time_conds("events", 100, 200)
        # [Cond("timestamp", ">=", 100), Cond("timestamp", "<", 200)]
    """
    column = TIME_COLUMNS[table]
    as_value = _utc_datetime if col_type(table, column) == "ts" else int
    out = []
    if start is not None:
        out.append(Cond(column, ">=", as_value(start)))
    if end is not None:
        out.append(Cond(column, "<", as_value(end)))
    return out


def _utc_datetime(epoch):
    """Turn Unix seconds into a naive ``datetime`` in UTC, the value of a ``TIMESTAMP`` condition."""
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).replace(tzinfo=None)


def concat(tables):
    """Concatenate the tables a read yields into one table.

    :param tables: an iterable of ``pyarrow.Table`` with one schema, at least one
        (every read yields at least one).
    :returns: one ``pyarrow.Table``.
    """
    tables = list(tables)
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def col_type(table, col):
    """Return the logical type of a column, without the optional marker.

    :param table: logical table name.
    :param col: logical column name.
    :returns: ``"int"``, ``"float"``, ``"str"`` or ``"ts"``.
    """
    return TABLES[table][col].rstrip("?")


def is_optional(table, col):
    """Tell whether a column may be missing from a deployment (``?`` in :data:`TABLES`).

    :param table: logical table name.
    :param col: logical column name.
    :returns: ``True`` if the column is optional.
    """
    return TABLES[table][col].endswith("?")


def arrow_schema(table, columns=None):
    """Return the pyarrow schema of one of EAR's tables, with its logical types.

    :param table: logical table name.
    :param columns: logical columns, in order; default all of the table.
    :returns: a ``pyarrow.Schema``.
    """
    return pa.schema([(c, ARROW[col_type(table, c)]) for c in columns or TABLES[table]])
