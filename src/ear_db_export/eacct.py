"""The CSV layout of ``eacct -l -c`` (EAR 6 and 7, one row per job step and node), as data.

:data:`COLUMNS` lists the 153 columns in eacct's order, each with where its
value comes from: ``(header, table, column, gpu)``.

- ``table`` is a logical table of :data:`ear_db_export.read.TABLES`
  (``jobs``, ``applications``, ``signatures``, ``power_signatures``,
  ``gpu_signatures``) and ``column`` one of its logical columns; ``gpu`` is the
  GPU's position for the ``GPU<i>_*`` columns, else ``None``.
- ``table`` ``derived``: computed from other columns (the dates, ``EARL_ENABLED``).
- ``table`` ``None``: a value EAR's database does not store (stalls, cache hits
  and rates, per-socket temperature and power, GPU temperature, the learning
  flag); eacct prints 0 for them when it reads the database.

The layout is that of EAR 6 and 7 built with workflow support (``WF_SUPPORT``,
``APPID``), the extended signature and 4 sockets / 8 GPUs, as
``application_create_header_str`` in EAR's ``src/common/types/application.c``
writes it. ear-db-synth reads it the other way, eacct CSV to tables.
"""

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
