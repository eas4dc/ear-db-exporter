"""Read EAR's tables from wherever they are: its SQL database, or a bundle.

A *record* is a row of one of EAR's own tables (``Periodic_metrics``,
``Jobs``, ``Applications``, the signature tables, ...), as EAR stores it: epoch
seconds, ``0`` for "no value", ids that point to other tables. :data:`TABLES`
lists every table and column with *logical* names, the same for every EAR
version; schema profiles (:mod:`~ear_db_export.read.sql.profile`) map them to
each version's physical names.

A *source* reads tables as stored (physical names, exact types). Two exist,
with the same methods, so everything after them works on either:

- :class:`ear_db_export.read.sql.SqlSource`: EAR's SQL database, read-only.
- :class:`ear_db_export.read.bundle.BundleSource`: a bundle written by ``export``.

Their interface: ``scan(table, where)`` and ``lookup(table, col, keys)`` yield
``pyarrow.Table`` s of a physical table (``where`` is a DNF filter on logical
names, see :mod:`ear_db_export.selection`); ``profile`` is the schema profile
(or ``None``), ``provides`` the physical tables, ``id_step`` the auto-increment
step, ``info()`` a description for manifests, ``ddl(tables)`` the
``CREATE TABLE`` statements, ``oldest_day()`` the prune horizon (database
only), ``close()``.
"""
import pyarrow as pa


#: Every table EAR creates, with every column: ``{table: {column: type}}``.
#: These are the *logical names*: the same for every EAR version, mapped to
#: each version's own names by the SQL reader's profiles. The type is ``int``,
#: ``float``, ``str`` or ``ts`` (a SQL ``TIMESTAMP``). A trailing ``?`` marks a
#: column that may be absent in a deployment: EAR drops some columns when
#: ``DBReportNodeDetail`` / ``DBReportSIGDetail`` are 0, some only exist in
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
#: The time column of the tables read by time window through their primary key
#: (no time index in EAR's schema).
TIME_COLUMNS = {"periodic_metrics": "end_time", "events": "timestamp",
                "periodic_aggregations": "end_time"}

#: Arrow type of each logical type of :data:`TABLES`.
ARROW = {"int": pa.int64(), "float": pa.float64(), "str": pa.string(),
         "ts": pa.timestamp("us", tz="UTC")}
#: Columns that :attr:`ear_db_export.extract.Records.gpu_signatures` adds in front of the
#: ``gpu_signatures`` columns: which signature the GPU belongs to, and its
#: position (0, 1, …) in that signature.
GPU_LINK = ("signature_id", "gpu_index")


def col_type(table, col):
    """Return the logical type of a column, without the optional marker.

    :param table: logical table name.
    :param col: logical column name.
    :returns: ``"int"``, ``"float"`` or ``"str"``.
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
    """Return the pyarrow schema of a record table.

    :param table: logical table name.
    :param columns: logical columns, in order; default all of the table.
    :returns: a ``pyarrow.Schema``.
    """
    return pa.schema([(c, ARROW[col_type(table, c)]) for c in columns or TABLES[table]])
