"""The eacct dataset: eacct -l -c's CSV layout from the records."""
import pyarrow.csv as pacsv

from ear_db_export import canonical, eacct, extract
from ear_db_export.read.sql import SqlSource
from ear_db_export.selection import Selection

import fakedata
from fakedb import DBCONF, make_engine
from test_pipeline import db, main  # noqa: F401 (fixture)

# The header as EAR 6's application_create_header_str writes it (WF_SUPPORT, extended,
# MAX_SOCKETS_SUPPORTED 4, MAX_GPUS_SUPPORTED 8), rebuilt from its C string pieces.
HEADER_JOB = ("JOBID;STEPID;APPID;USERID;GROUPID;ACCOUNTID;JOBNAME;"
              "ENERGY_TAG;JOB_START_TIME;JOB_END_TIME;JOB_START_DATE;"
              "JOB_END_DATE;JOB_EARL_START_TIME;JOB_EARL_END_TIME;POLICY;"
              "POLICY_TH;JOB_NPROCS;JOB_TYPE;JOB_DEF_FREQ")
HEADER_BASE = ";EARL_ENABLED;EAR_LEARNING_PHASE;NODENAME"
HEADER_SIGN = (";AVG_CPUFREQ_KHZ;AVG_IMCFREQ_KHZ;DEF_FREQ_KHZ;TIME_SEC;CPI;TPI;MEM_GBS;"
               "IO_MBS;PERC_MPI;DC_NODE_POWER_W;DRAM_POWER_W;PCK_POWER_W;"
               "CYCLES;INSTRUCTIONS;STALLS_FETCH_DECODE;STALLS_RESOURCES;"
               "STALLS_MEMORY;CPU_GFLOPS;CPU_UTIL")
EXT = (";L1_MISSES;L2_MISSES;L3_MISSES;LL_MISSES;L1_HITS;L2_HITS;L3_HITS;LL_HITS;"
       "L1_ACCESSES;L2_ACCESSES;L3_ACCESSES;LL_ACCESSES;"
       "L1_MISSRATE;L2_MISSRATE;L3_MISSRATE;LL_MISSRATE;"
       "L1_HITRATE;L2_HITRATE;L3_HITRATE;LL_HITRATE;"
       "SPOPS_SINGLE;SPOPS_128;SPOPS_256;SPOPS_512;DPOPS_SINGLE;DPOPS_128;DPOPS_256;DPOPS_512")
SOCKETS = "".join(";TEMP%u;CPU_POWER%u;DRAM_POWER%u" % (s, s, s) for s in range(4))
GPUS = "".join((";GPU%d_POWER_W;GPU%d_FREQ_KHZ;GPU%d_MEM_FREQ_KHZ;GPU%d_UTIL_PERC;"
                "GPU%d_MEM_UTIL_PERC;GPU%d_GFLOPS;GPU%d_TEMP;GPU%d_MEMTEMP") % ((i,) * 8)
               for i in range(8))
POWER = (";NODEMGR_DC_NODE_POWER_W;NODEMGR_DRAM_POWER_W;NODEMGR_PCK_POWER_W;NODEMGR_MAX_DC_POWER_W;"
         "NODEMGR_MIN_DC_POWER_W;NODEMGR_TIME_SEC;NODEMGR_AVG_CPUFREQ_KHZ;NODEMGR_DEF_FREQ_KHZ")


def test_header_matches_ear6():
    assert eacct.HEADER == HEADER_JOB + HEADER_BASE + HEADER_SIGN + EXT + SOCKETS + GPUS + POWER
    assert len(eacct.COLUMNS) == 153


def build(key=None):
    src = SqlSource(DBCONF, engine=make_engine())
    records = extract.Records(src, Selection(start=fakedata.T0, end=fakedata.T0 + 86400))
    return {(r["JOBID"], r["APPID"], r["NODENAME"]): r
            for r in canonical.eacct(records, {}, key).to_pylist()}


def test_rows_values_and_zeros():
    rows = build()
    r = rows[(7, 5, "n2")]
    assert (r["USERID"], r["JOBNAME"], r["EARL_ENABLED"], r["CPI"]) == ("alice", "lmp_mpi", 1, 0.61)
    assert r["JOB_START_DATE"] == "2026-07-04 00:00:10" and r["NODEMGR_DC_NODE_POWER_W"] == 310.523
    assert r["STALLS_MEMORY"] == 0 and r["GPU0_POWER_W"] == 0 and r["ENERGY_TAG"] == ""
    post = rows[(7, 6, "n2")]  # no EARL signature: zeros, but its power signature
    assert (post["EARL_ENABLED"], post["CPI"], post["NODEMGR_DC_NODE_POWER_W"]) == (0, 0, 120.1)


def test_gpu_pivot_keeps_index():
    r = build()[(8, 0, "n3")]
    assert [r["GPU%d_POWER_W" % i] for i in range(5)] == [200.5, 0, 220.25, 180.0, 0]  # GPU 1: no row


def test_users_pseudonymised():
    r = build(b"k")[(8, 0, "n3")]
    assert r["USERID"] == canonical.pseudonym(b"k", "bob") and r["ACCOUNTID"] != "acc2"


def test_csv_semicolon_unquoted_header(tmp_path, db):  # noqa: F811
    assert main("view", "eacct", "--as", "csv", "--from", fakedata.DAY, "--to", fakedata.DAY,
                "--out", tmp_path, *db) == 0
    text = (tmp_path / "eacct.csv").read_text()
    assert text.splitlines()[0] == eacct.HEADER
    t = pacsv.read_csv(tmp_path / "eacct.csv", parse_options=pacsv.ParseOptions(delimiter=";"))
    assert t.num_rows == 4 and t.column_names == [h for h, _, _, _ in eacct.COLUMNS]
