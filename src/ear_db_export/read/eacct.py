"""Read ``eacct -l -c`` CSV files as if they were EAR's database; also eacct's CSV columns (:data:`COLUMNS`).

eacct writes one row per job step and node: the 153 columns of :data:`COLUMNS`,
with the step's job fields, its EARL signature and GPUs, and EARD's power
signature. :class:`EacctSource` turns the rows back into the logical tables
``jobs``, ``applications``, ``signatures``, ``power_signatures`` and
``gpu_signatures``, as EAR would have stored them, so every job layout reads
them as it reads the database:

- ``jobs``: one row per job step and application (``JOBID``, ``STEPID``, ``APPID``);
- ``applications``: one row per CSV row, pointing to its signatures;
- ``power_signatures``: one per row, from ``NODEMGR_*`` (EARD's measurement);
- ``signatures`` and ``gpu_signatures``: for rows with an EARL signature
  (``EARL_ENABLED``), GPUs ``0 .. last present`` as one id range, as EAR stores them.

Ids are assigned from 1, in file order, with step 1. eacct versions: 6 and 7
headers (``JOBID;STEPID;APPID;…``, 7 adds DCGM columns per GPU) and 5
(``NODENAME;JOBID;STEPID;…``, other names: :data:`V5_NAMES`). Only the epoch
``*_TIME`` columns are read: ``*_DATE`` are in the site's local time.

:data:`COLUMNS` is the layout of EAR 6 and 7 built with workflow support
(``WF_SUPPORT``, ``APPID``), the extended signature and 4 sockets / 8 GPUs, as
``application_create_header_str`` in EAR's ``src/common/types/application.c``
writes it. Each column is ``(header, table, column, gpu)``:

- ``table`` is a logical table of :data:`ear_db_export.read.TABLES`
  (``jobs``, ``applications``, ``signatures``, ``power_signatures``,
  ``gpu_signatures``) and ``column`` one of its logical columns; ``gpu`` is the
  GPU's position for the ``GPU<i>_*`` columns, else ``None``.
- ``table`` ``derived``: computed from other columns (the dates, ``EARL_ENABLED``).
- ``table`` ``None``: a value EAR's database does not store (stalls, cache hits
  and rates, per-socket temperature and power, GPU temperature, the learning
  flag); eacct prints 0 for them when it reads the database.

The ``eacct`` layout (:mod:`ear_db_export.layouts.eacct`) writes the same columns.
"""
import logging
import os
import re

import pyarrow as pa
import pyarrow.csv as pacsv

from ..config import ConfigError
from . import TABLES, Cond, arrow_schema, col_type
from .bundle import expression

log = logging.getLogger(__name__)

#: ``(header, table, column, gpu)`` of every column, in order.
COLUMNS = (
    [("JOBID", "jobs", "job_id", None), ("STEPID", "jobs", "step_id", None),
     ("APPID", "jobs", "local_id", None), ("USERID", "jobs", "user_id", None),
     ("GROUPID", "jobs", "user_group", None), ("ACCOUNTID", "jobs", "user_acc", None),
     ("JOBNAME", "jobs", "app_name", None), ("ENERGY_TAG", "jobs", "e_tag", None),
     ("JOB_START_TIME", "jobs", "start_time", None), ("JOB_END_TIME", "jobs", "end_time", None),
     ("JOB_START_DATE", "derived", "start_time", None), ("JOB_END_DATE", "derived", "end_time", None),
     ("JOB_EARL_START_TIME", "jobs", "start_mpi_time", None),
     ("JOB_EARL_END_TIME", "jobs", "end_mpi_time", None), ("POLICY", "jobs", "policy", None),
     ("POLICY_TH", "jobs", "threshold", None), ("JOB_NPROCS", "jobs", "procs", None),
     ("JOB_TYPE", "jobs", "job_type", None), ("JOB_DEF_FREQ", "jobs", "def_f", None),
     ("EARL_ENABLED", "derived", "signature_id", None), ("EAR_LEARNING_PHASE", None, None, None),
     ("NODENAME", "applications", "node_id", None)]
    + [(h, "signatures", c, None) for h, c in (
        ("AVG_CPUFREQ_KHZ", "avg_f"), ("AVG_IMCFREQ_KHZ", "avg_imc_f"), ("DEF_FREQ_KHZ", "def_f"),
        ("TIME_SEC", "time"), ("CPI", "cpi"), ("TPI", "tpi"), ("MEM_GBS", "gbs"),
        ("IO_MBS", "io_mbs"), ("PERC_MPI", "perc_mpi"), ("DC_NODE_POWER_W", "dc_power"),
        ("DRAM_POWER_W", "dram_power"), ("PCK_POWER_W", "pck_power"), ("CYCLES", "cycles"),
        ("INSTRUCTIONS", "instructions"))]
    + [(h, None, None, None) for h in ("STALLS_FETCH_DECODE", "STALLS_RESOURCES", "STALLS_MEMORY")]
    + [("CPU_GFLOPS", "signatures", "gflops", None), ("CPU_UTIL", "signatures", "cpu_util", None)]
    # extended signature
    + [("L%d_MISSES" % i, "signatures", "l%d_misses" % i, None) for i in (1, 2, 3)]
    + [(h, None, None, None) for h in
       ["LL_MISSES"] + ["%s_%s" % (lvl, kind) for kind in ("HITS", "ACCESSES", "MISSRATE", "HITRATE")
                        for lvl in ("L1", "L2", "L3", "LL")]]
    + [("%sPOPS_%s" % (p, w), "signatures", "flops_%d" % (i + 1), None)
       for i, (p, w) in enumerate((p, w) for p in "SD" for w in ("SINGLE", "128", "256", "512"))]
    # per socket
    + [(h % s, None, None, None) for s in range(4) for h in ("TEMP%d", "CPU_POWER%d", "DRAM_POWER%d")]
    # per GPU
    + [("GPU%d_%s" % (g, h), "gpu_signatures" if c else None, c, g if c else None)
       for g in range(8) for h, c in (("POWER_W", "gpu_power"), ("FREQ_KHZ", "gpu_freq"),
                                      ("MEM_FREQ_KHZ", "gpu_mem_freq"), ("UTIL_PERC", "gpu_util"),
                                      ("MEM_UTIL_PERC", "gpu_mem_util"), ("GFLOPS", "gpu_gflops"),
                                      ("TEMP", None), ("MEMTEMP", None))]
    # power signature (EARD's, for every application)
    + [("NODEMGR_" + h, "power_signatures", c, None) for h, c in (
        ("DC_NODE_POWER_W", "dc_power"), ("DRAM_POWER_W", "dram_power"),
        ("PCK_POWER_W", "pck_power"), ("MAX_DC_POWER_W", "max_dc_power"),
        ("MIN_DC_POWER_W", "min_dc_power"), ("TIME_SEC", "time"),
        ("AVG_CPUFREQ_KHZ", "avg_f"), ("DEF_FREQ_KHZ", "def_f"))]
)
#: The CSV header line (without newline), ``;``-separated as eacct writes it.
HEADER = ";".join(h for h, _, _, _ in COLUMNS)

#: The logical tables an eacct file holds.
EACCT_TABLES = ("jobs", "applications", "signatures", "power_signatures", "gpu_signatures")
#: eacct 5 header names → eacct 6/7 names.
V5_NAMES = {"USER_ACC": "ACCOUNTID", "START_TIME": "JOB_START_TIME", "END_TIME": "JOB_END_TIME",
            "START_DATE": "JOB_START_DATE", "END_DATE": "JOB_END_DATE",
            "CPU-GFLOPS": "CPU_GFLOPS", "DP_256": "DPOPS_256"}
#: Columns every eacct ``-l -c`` file has, which identify its rows.
REQUIRED = ("JOBID", "STEPID", "NODENAME")
#: EAR 7's DCGM columns per GPU (``GPU0_sm_active``…), named as the logical column.
DCGM = [c for c in TABLES["gpu_signatures"] if not c.startswith("gpu_") and c != "id"]
_GPU = re.compile(r"GPU(\d+)_(.+)")
#: ``POWER_W`` → ``gpu_power``, …: the logical GPU column of each eacct GPU suffix.
_GPU_FIELDS = dict({h.split("_", 1)[1]: c for h, t, c, g in COLUMNS
                    if t == "gpu_signatures" and g == 0}, **{c: c for c in DCGM})


class EacctSource:
    """``eacct -l -c`` CSV files, read like a database.

    The files are read once, when the source is opened; filters are applied in
    memory. There are no tables as stored, so ``export raw`` cannot read it.

    :param paths: paths of ``eacct -l -c`` files (``;`` or ``,`` separated).
    :param tags: ``{node: tag}`` from ear.conf, for ``--tags`` and ``node_tag``.
    :raises ConfigError: if a file lacks the columns that identify its rows.

    Example::

        src = EacctSource(["july.csv"])
        jobs = concat(src.scan("jobs", [Cond("user_id", "in", ["alice"])]))
    """

    profile = None
    provides = frozenset()
    id_step = 1
    salt_fingerprint = None
    dry_run = False

    def __init__(self, paths, tags=None):
        """Read every file once and rebuild the five tables (ids from 1, in file order)."""
        self.paths = list(paths)
        self.tags = dict(tags or {})
        rows = {t: [] for t in EACCT_TABLES}
        seen = set()
        for path in self.paths:
            for row in _rows(path):
                _application(row, rows, seen, path)
        self._tables = {t: _typed(t, r) for t, r in rows.items()}

    def has(self, table):
        """Tell whether the files hold a logical table (the five of :data:`EACCT_TABLES`)."""
        return table in self._tables

    def info(self):
        """Describe the source for a bundle manifest: ``{engine: "eacct", files: [names]}``."""
        return {"engine": "eacct", "files": [os.path.basename(p) for p in self.paths]}

    def ddl(self, tables):
        """eacct files have no ``CREATE TABLE`` statements: return ``""``."""
        return ""

    def close(self):
        """Nothing to release."""

    def dump(self, table):
        """Refuse: eacct files hold no tables as stored."""
        raise ConfigError("eacct files hold no tables as stored: `export raw` cannot read them")

    def scan(self, table, where=(), order=None, limit=None):
        """Read the rows of a table that match a filter (see :meth:`SqlSource.scan`); types are logical."""
        if not self.has(table):
            raise ConfigError("eacct files have no %s table" % table)
        t = self._tables[table]
        e = expression(list(where), t.schema)
        if e is not None:
            t = t.filter(e)
        if order:
            t = t.sort_by(list(order))
        yield t.slice(0, limit) if limit else t

    def window(self, table, start, end, where=()):
        """Refuse for every table: eacct files have no time-series tables."""
        raise ConfigError("eacct files have no %s table" % table)

    def lookup(self, table, column, keys):
        """Read the rows whose column is one of ``keys`` (see :meth:`SqlSource.lookup`)."""
        return self.scan(table, [Cond(column, "in", sorted(set(k for k in keys if k is not None)))])


def read_csv(path):
    """Read a CSV file with every column as text, ``;`` or ``,`` separated (whichever the header uses more).

    :returns: ``(columns, rows)``: the header names, and one ``{column: text or None}``
        per row (empty and blank cells are ``None``, other cells are stripped).
    """
    with open(path) as f:
        header = f.readline()
    sep = ";" if header.count(";") >= header.count(",") else ","
    names = [h.strip() for h in header.strip().split(sep)]
    t = pacsv.read_csv(path, parse_options=pacsv.ParseOptions(delimiter=sep),
                       convert_options=pacsv.ConvertOptions(
                           column_types={n: pa.string() for n in names}, strings_can_be_null=True))
    columns = t.column_names
    values = [[None if v is None or not v.strip() else v.strip() for v in t[n].to_pylist()]
              for n in columns]
    return columns, [dict(zip(columns, r)) for r in zip(*values)]


def _rows(path):
    """Return the rows of an eacct file, after checking it has the :data:`REQUIRED` columns.

    An eacct 5 file (``NODENAME;JOBID;…``) gets eacct 6/7 names (:data:`V5_NAMES`).
    """
    columns, rows = read_csv(path)
    if columns[:2] == ["NODENAME", "JOBID"]:
        columns = [V5_NAMES.get(c, c) for c in columns]
        rows = [{V5_NAMES.get(h, h): v for h, v in r.items()} for r in rows]
    missing = [h for h in REQUIRED if h not in columns]
    if missing:
        raise ConfigError("%s: no column %s: not an `eacct -l -c` file" % (path, ", ".join(missing)))
    return rows


def _application(row, rows, seen, path):
    """Add one CSV row to ``rows``: its job step (once per step), power signature, and EARL signature with GPUs.

    :param row: ``{eacct header: text}``.
    :param rows: ``{logical table: [row dicts]}``, filled in place.
    :param seen: the ``(job_id, step_id, local_id)`` already added to ``jobs``.
    :param path: the file, for the learning-phase warning.
    """
    job = {c: row.get(h) for h, t, c, _ in COLUMNS if t == "jobs"}
    key = (job["job_id"], job["step_id"], job["local_id"])
    if key not in seen:
        seen.add(key)
        rows["jobs"].append(job)
    if row.get("EAR_LEARNING_PHASE") not in (None, "0"):
        log.warning("%s: job %s.%s is from EAR's learning phase: read as a normal application",
                    path, job["job_id"], job["step_id"])
    rows["applications"].append({
        "job_id": job["job_id"], "step_id": job["step_id"], "local_id": job["local_id"],
        "node_id": row["NODENAME"], "power_signature_id": _power_signature(row, rows),
        "signature_id": _signature(row, rows) if _earl(row) else None})


def _earl(row):
    """Tell whether the application ran with EARL: ``EARL_ENABLED``, or (eacct 5) any EARL counter."""
    if "EARL_ENABLED" in row:
        return row["EARL_ENABLED"] not in (None, "0")
    return any(_num(row.get(h)) for h in ("CPI", "CYCLES", "INSTRUCTIONS"))


def _power_signature(row, rows):
    """Add the row's EARD power signature (``NODEMGR_*``) to ``rows`` and return its new id.

    eacct 5 has no ``NODEMGR_*`` columns: the EARL signature's power and time are used.
    """
    ps = {c: row.get(h) for h, t, c, _ in COLUMNS if t == "power_signatures"}
    if not any(_num(v) for v in ps.values()):
        sig = {c: row.get(h) for h, t, c, _ in COLUMNS if t == "signatures"}
        ps = {c: sig[c] for c in ("dc_power", "dram_power", "pck_power", "time", "avg_f", "def_f")}
        ps["max_dc_power"] = ps["min_dc_power"] = ps["dc_power"]
    ps["edp"] = _edp(ps)
    ps["id"] = len(rows["power_signatures"]) + 1
    rows["power_signatures"].append(ps)
    return ps["id"]


def _signature(row, rows):
    """Add the row's EARL signature and its GPUs to ``rows``; return the signature's new id.

    GPUs ``0 .. last present`` get consecutive ids, recorded as the signature's
    ``min_gpu_sig_id .. max_gpu_sig_id`` range, as EAR stores them (idle GPUs included).
    """
    sig = {c: row.get(h) for h, t, c, _ in COLUMNS if t == "signatures"}
    sig["edp"] = _edp(sig)
    gpus = {}
    for h, v in row.items():
        m = _GPU.fullmatch(h)
        if m and m.group(2) in _GPU_FIELDS:
            gpus.setdefault(int(m.group(1)), {})[_GPU_FIELDS[m.group(2)]] = v
    present = [g for g, f in gpus.items() if _num(f.get("gpu_power")) or _num(f.get("gpu_freq"))]
    sig["min_gpu_sig_id"] = sig["max_gpu_sig_id"] = None
    for g in range(max(present) + 1 if present else 0):
        gpu = dict(gpus.get(g, {}), id=len(rows["gpu_signatures"]) + 1)
        rows["gpu_signatures"].append(gpu)
        sig["min_gpu_sig_id"] = sig["min_gpu_sig_id"] or gpu["id"]
        sig["max_gpu_sig_id"] = gpu["id"]
    sig["id"] = len(rows["signatures"]) + 1
    rows["signatures"].append(sig)
    return sig["id"]


def _edp(sig):
    """Return the energy-delay product as EAR computes it, node power × time², or ``None`` if either is missing."""
    p, t = _num(sig.get("dc_power")), _num(sig.get("time"))
    return None if p is None or t is None else p * t * t


def _num(v):
    """Return a number from CSV text (or a number as it is), or ``None`` if it is empty or not a number."""
    if v is None or isinstance(v, (int, float)):
        return v
    try:
        return float(v)
    except ValueError:
        return None


def _int(v):
    """Return an integer from CSV text, exactly (no float for 64-bit counters), or ``None``.

    ``"4.000000"`` (an integer printed as a float) gives 4.
    """
    if v is None or isinstance(v, int):
        return v
    try:
        return int(v)
    except ValueError:
        f = _num(v)
        return None if f is None else int(f)


def _typed(table, rows):
    """Turn rows (dicts of text) into a table with every logical column of ``table``, with its logical type."""
    cols = []
    for name in TABLES[table]:
        values = [r.get(name) for r in rows]
        kind = col_type(table, name)
        if kind == "str":
            cols.append(pa.array(values, pa.string()))
        elif kind == "float":
            cols.append(pa.array([_num(v) for v in values], pa.float64()))
        else:
            cols.append(pa.array([_int(v) for v in values], pa.int64()))
    return pa.Table.from_arrays(cols, schema=arrow_schema(table))
