"""Step 1, read: get EAR's records for a period, from whatever store holds them.

A *record* is a row of one of EAR's own tables (``Periodic_metrics``,
``Jobs``, ``Applications``, the signature tables), as EAR stores it: epoch
seconds, ``0`` for "no value", ids that point to other tables. Every reader
returns records with the same *logical* column names and types, listed in
:data:`TABLES`, whatever the store and the EAR version. That is the contract
between step 1 and step 2 (:mod:`ear_db_export.canonical`).

- :class:`Reader` is the interface a data store implements, one table per
  method. :class:`ear_db_export.read.sql.SqlReader` implements it for EAR's
  SQL database.
- :class:`Records` holds the records of one period. Each table is read the
  first time a dataset asks for it, and kept for the other datasets of the
  same period, so ``app_signatures`` and ``gpu_signatures`` read ``Jobs`` once.
"""
from functools import cached_property

import pyarrow as pa

from ..canonical import INPUTS

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
#: Columns that :meth:`Reader.gpu_signatures` adds in front of the
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


class Reader:
    """Step 1 interface: one subclass per data store.

    Every method returns a ``pyarrow.Table`` with the logical columns and
    types of :data:`TABLES` (all of them, NULL where the store lacks an
    optional column). A store that cannot provide a table leaves the method
    unimplemented and leaves the table out of :attr:`provides`; the pipeline
    then refuses, before reading anything, a command that needs it. Time
    windows are Unix seconds, ``start`` included, ``end`` excluded.

    :ivar provides: the record tables this reader can read.
    :ivar join_keys: columns that link ``Jobs`` and ``Applications``:
        ``["job_id", "step_id"]``, plus ``"local_id"`` from EAR 5.0.
    """

    provides = frozenset()
    join_keys = ["job_id", "step_id"]

    def oldest_day(self):
        """Return the day of the oldest ``periodic_metrics`` record, or ``None`` if there is none.

        EAR deletes old ``Periodic_metrics`` rows, so this is the *prune
        horizon*: nothing older can be exported any more.
        """
        raise NotImplementedError

    def periodic_metrics(self, start, end):
        """Return the ``periodic_metrics`` records whose interval ends in ``[start, end)``."""
        raise NotImplementedError

    def jobs(self, start, end):
        """Return the ``jobs`` records (one per job step) that end in ``[start, end)``."""
        raise NotImplementedError

    def applications(self, job_ids):
        """Return the ``applications`` records (one per step and node) of these jobs."""
        raise NotImplementedError

    def power_signatures(self, ids):
        """Return the ``power_signatures`` records with these ids."""
        raise NotImplementedError

    def signatures(self, ids):
        """Return the ``signatures`` records (EARL application signatures) with these ids."""
        raise NotImplementedError

    def gpu_signatures(self, signatures):
        """Return one row per GPU of these ``signatures`` records.

        How a signature points to its GPUs is a storage detail (EAR's SQL
        database stores an id range), so the reader resolves it: the result
        has the :data:`GPU_LINK` columns, then the ``gpu_signatures`` columns
        without ``id``. GPUs whose row is missing are left out, and
        ``gpu_index`` keeps its position.
        """
        raise NotImplementedError

    def close(self):
        """Release the connection, if any."""


class Records:
    """The EAR records of one period, read on first use and kept for the period.

    Each attribute is one record table. Reads that depend on other records
    follow EAR's links: ``applications`` are those of ``jobs``,
    ``power_signatures`` and ``signatures`` those the ``applications`` point
    to, ``gpu_signatures`` those of ``signatures``.

    :param reader: a :class:`Reader`.
    :param start: period start, Unix seconds (included).
    :param end: period end, Unix seconds (excluded).
    """

    def __init__(self, reader, start, end):
        self.reader, self.start, self.end = reader, start, end
        self.join_keys = reader.join_keys

    @cached_property
    def periodic_metrics(self):
        """Node metrics records whose interval ends in the period."""
        return _inputs("periodic_metrics", self.reader.periodic_metrics(self.start, self.end))

    @cached_property
    def jobs(self):
        """Job step records that end in the period."""
        return _inputs("jobs", self.reader.jobs(self.start, self.end))

    @cached_property
    def applications(self):
        """Application records (one per step and node) of :attr:`jobs`."""
        return _inputs("applications", self.reader.applications(self.jobs["job_id"].to_pylist()))

    @cached_property
    def power_signatures(self):
        """Power signatures that :attr:`applications` point to."""
        return _inputs("power_signatures", self.reader.power_signatures(
            self.applications["power_signature_id"].to_pylist()))

    @cached_property
    def signatures(self):
        """EARL application signatures that :attr:`applications` point to."""
        return _inputs("signatures", self.reader.signatures(
            self.applications["signature_id"].to_pylist()))

    @cached_property
    def gpu_signatures(self):
        """GPU signatures of :attr:`signatures`, one row per GPU."""
        gpus = self.reader.gpu_signatures(self.signatures)
        return gpus.select(list(GPU_LINK) + INPUTS["gpu_signatures"][1:])


def _inputs(table, t):
    """Keep the columns the datasets read (:data:`ear_db_export.canonical.INPUTS`)."""
    return t.select([c for c in INPUTS[table] if c in t.column_names])
