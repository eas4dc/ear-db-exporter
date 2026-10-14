"""Layouts: the rows each one builds from the fake database's records."""
import pytest

from ear_db_export.clean import TS, typed
from ear_db_export.config import ConfigError
from ear_db_export.layouts import LAYOUTS, eacct, job_nodes, node_metrics, power_signatures, raw
from ear_db_export.read import concat
from ear_db_export.read.eacct import COLUMNS, HEADER
from ear_db_export.read.sql import SqlSource
from ear_db_export.selection.jobs import JobSelection

import fakedata
from fakedb import DBCONF, make_engine

T0 = fakedata.T0

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


@pytest.fixture(params=["ear6", "ear7"])
def source(request):
    """The fakedata records in an EAR 6 or EAR 7 database."""
    return SqlSource(DBCONF, engine=make_engine(request.param))


def chunks(source, **filters):
    """The JobChunks of a JobSelection with these filters."""
    return list(JobSelection(**filters).chunks(source))


def test_every_layout_has_the_five_names():
    """The layout protocol: NAME, HELP, READS, add_arguments, run."""
    for name, module in LAYOUTS.items():
        assert module.NAME == name and module.HELP and module.READS
        assert callable(module.add_arguments) and callable(module.run)


def test_node_metrics_long_rows():
    """One row per metric with a value; EAR's 0 left out (but DC energy's kept); job labels only when a job ran."""
    pm = typed("periodic_metrics", fakedata.tables()["periodic_metrics"])
    out = concat(list(node_metrics.long_rows(pm, "snellius")))
    assert out.schema == node_metrics.SCHEMA
    by_metric = {}
    for r in out.to_pylist():
        by_metric.setdefault(r["metric_name"], []).append(r)
    assert len(by_metric["ear_node_dc_energy_joules"]) == 4
    assert [r["value"] for r in by_metric["ear_node_gpu_energy_joules"]] == [5.0, 900.0]  # zeros out
    assert [r["value"] for r in by_metric["ear_node_temp_celsius"]] == [50.0, 51.0, 55.0]  # null out
    idle = next(r for r in by_metric["ear_node_dc_energy_joules"] if dict(r["labels"])["node"] == "n1")
    assert dict(idle["labels"]) == {"node": "n1"} and idle["duration_s"] == 60
    assert idle["service"] == "snellius" and idle["source"] == "ear"
    empty = list(node_metrics.long_rows(pm.slice(0, 0), "x"))
    assert [t.num_rows for t in empty] == [0]  # the schema, for an empty file


def test_job_nodes_rows(source):
    """Every jobs column with timestamps, then node and node_tag; one row per step and node."""
    rows = concat([job_nodes.steps_on_nodes(c.per_node, fakedata.TAGS) for c in chunks(source)])
    assert rows.column_names[-2:] == ["node", "node_tag"] and rows["end_time"].type == TS
    assert [(r["local_id"], r["node"], r["node_tag"]) for r in rows.to_pylist()] == \
        [(5, "n2", "cpu"), (5, "n4", None), (6, "n2", "cpu"), (0, "n3", "gpu")]
    assert rows["start_mpi_time"].null_count == 2  # EAR's 0: no MPI


def test_power_signatures_keep_rows_without_one(source):
    """job_*, node, ps_*; a row whose signature is missing has nulls and is counted."""
    eng = source.engine
    eng.db.execute("DELETE FROM `Power_signatures` WHERE `id` = 11")
    eng.db.commit()
    out, missing = [], 0
    for c in chunks(source):
        rows, n = power_signatures.with_power_signature(
            c.per_node, typed("power_signatures", c.power_signatures), {})
        out.append(rows)
        missing += n
    rows = concat(out)
    assert missing == 1 and "ps_id" not in rows.column_names and rows["job_end_time"].type == TS
    assert rows["ps_dc_power"].to_pylist() == [310.523, None, 120.1, 900.25]


def test_eacct_header_matches_ear6():
    """The 153 columns, in the order and names of EAR 6/7's eacct -l -c."""
    assert HEADER == HEADER_JOB + HEADER_BASE + HEADER_SIGN + EXT + SOCKETS + GPUS + POWER
    assert len(COLUMNS) == 153


def test_eacct_rows(source):
    """Values from each table, zeros for what is not stored or missing, GPUs by position."""
    rows = {(r["JOBID"], r["APPID"], r["NODENAME"]): r
            for c in chunks(source) for r in eacct.eacct_rows(c).to_pylist()}
    r = rows[(7, 5, "n2")]
    assert (r["USERID"], r["JOBNAME"], r["EARL_ENABLED"], r["CPI"]) == ("alice", "lmp_mpi", 1, 0.61)
    assert r["JOB_START_DATE"] == "2026-07-04 00:00:10" and r["NODEMGR_DC_NODE_POWER_W"] == 310.523
    assert r["STALLS_MEMORY"] == 0 and r["GPU0_POWER_W"] == 0 and r["ENERGY_TAG"] == ""
    post = rows[(7, 6, "n2")]  # no EARL signature: zeros, but its power signature
    assert (post["EARL_ENABLED"], post["CPI"], post["NODEMGR_DC_NODE_POWER_W"]) == (0, 0, 120.1)
    gpu = rows[(8, 0, "n3")]
    assert [gpu["GPU%d_POWER_W" % i] for i in range(5)] == [200.5, 0, 220.25, 180.0, 0]  # GPU 1: no row


def test_raw_resolves_table_names():
    """all is every table (only the filterable ones with filters); names keep EAR's order."""
    provides = {"Jobs", "Signatures", "Learning_jobs", "Extra"}
    assert raw.resolve_tables([], provides, False) == ["Jobs", "Signatures", "Learning_jobs", "Extra"]
    assert raw.resolve_tables(["all"], provides, True) == ["Jobs", "Signatures"]
    assert raw.resolve_tables(["Signatures", "Jobs", "Learning_jobs"], provides, True) == \
        ["Jobs", "Signatures", "Learning_jobs"]
    with pytest.raises(ConfigError, match="not a table"):
        raw.resolve_tables(["Nope"], provides, False)


def test_job_nodes_accepts_only_its_filters():
    """A layout's parser has only the flags of its selection: job_nodes has no --users."""
    from ear_db_export.cli import parser
    with pytest.raises(SystemExit):
        parser().parse_args(["export", "job_nodes", "--users", "x", "--out", "o"])
    args = parser().parse_args(["export", "power_signatures", "--users", "x", "--out", "o"])
    assert args.users == "x"  # every job filter on power_signatures
