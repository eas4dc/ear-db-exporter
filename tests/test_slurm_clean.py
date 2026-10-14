"""slurm_db_export.clean: TRES strings, mem_req, states, derived times, pseudonyms."""
import pyarrow as pa
import pytest

from ear_db_export.clean import pseudonym
from slurm_db_export.clean import (INFINITE, MEM_PER_CPU, NO_VAL64, clean_jobs, decode_mem_req,
                                   hide, parse_tres, state_name, state_names, tres_columns,
                                   tres_names)

NAMES = {1: "cpu", 2: "mem", 4: "node", 1001: "gres_gpu"}


def test_parse_tres():
    """id=count pairs; empty and NULL strings give nothing."""
    assert parse_tres("1=64,2=256000,1001=4") == {1: 64, 2: 256000, 1001: 4}
    assert parse_tres("") == {} and parse_tres(None) == {}


def test_tres_names():
    """type, or type/name, made column-safe."""
    t = pa.table({"id": [1, 1001, 1002, 6], "type": ["cpu", "gres", "gres", "fs"],
                  "name": ["", "gpu", "gpu:a100", "disk"]})
    assert tres_names(t) == {1: "cpu", 1001: "gres_gpu", 1002: "gres_gpu_a100", 6: "fs_disk"}


def test_tres_columns_have_every_tres():
    """One column per TRES of tres_table, null where the string lacks it."""
    cols = tres_columns(pa.array(["1=64,1001=4", "", None]), NAMES, "req_")
    assert list(cols) == ["req_cpu", "req_mem", "req_node", "req_gres_gpu"]
    assert cols["req_cpu"].to_pylist() == [64, None, None]
    assert cols["req_gres_gpu"].to_pylist() == [4, None, None]


def test_decode_mem_req():
    """The top bit means per CPU; NO_VAL64 is null."""
    mb, per_cpu = decode_mem_req(pa.array([MEM_PER_CPU | 2048, 4096, NO_VAL64, 0], pa.uint64()))
    assert mb.to_pylist() == [2048, 4096, None, 0]
    assert per_cpu.to_pylist() == [True, False, None, False]


@pytest.mark.parametrize("state, name", [(0, "PENDING"), (3, "COMPLETED"), (11, "OUT_OF_MEMORY"),
                                         (1024, "REQUEUED"), (8192 | 1, "RESIZING"),
                                         (1 << 19, "REVOKED"), (42, "UNKNOWN_42")])
def test_state_name(state, name):
    """Flags first, as job_state_string() does, then the base state."""
    assert state_name(state) == name


def test_state_names_column():
    """A column maps value by value, nulls kept."""
    assert state_names(pa.array([3, 0, 3, None], pa.uint32())).to_pylist() == \
        ["COMPLETED", "PENDING", "COMPLETED", None]


def test_clean_jobs_derived_columns():
    """wait/run/timelimit/ratio, null for jobs that never started and for INFINITE limits."""
    t = pa.table({
        "tres_req": ["1=4", "1=8"], "tres_alloc": ["1=4,1001=2", ""],
        "mem_req": pa.array([8000, MEM_PER_CPU | 1000], pa.uint64()),
        "state": pa.array([3, 0], pa.uint32()),
        "time_submit": [100, 200], "time_eligible": [160, 200], "time_start": [400, 0],
        "time_end": [3400, 0], "mod_time": [3400, 200],
        "timelimit": pa.array([100, INFINITE], pa.uint32())})
    r = clean_jobs(t, NAMES).to_pylist()
    assert (r[0]["wait_s"], r[0]["run_s"], r[0]["timelimit_s"], r[0]["runtime_ratio"]) == \
        (240, 3000, 6000, 0.5)
    assert (r[1]["wait_s"], r[1]["run_s"], r[1]["timelimit_s"], r[1]["runtime_ratio"]) == \
        (None, None, None, None)
    assert r[0]["alloc_gres_gpu"] == 2 and r[1]["alloc_cpu"] is None and r[1]["req_cpu"] == 8
    assert (r[1]["mem_req_mb"], r[1]["mem_per_cpu"], r[1]["state_name"]) == (1000, True, "PENDING")
    assert r[0]["time_start_ts"].timestamp() == 400 and r[1]["time_start_ts"] is None
    assert r[0]["time_start"] == 400  # stored columns kept


def test_hide_hashes_ids_and_drops_lineage():
    """Integer ids are hashed as text; assoc lineage, which holds names, is dropped."""
    jobs = hide("job", pa.table({"id_user": pa.array([1000], pa.uint32()), "account": ["phys"],
                                 "id_job": [7]}), b"k")
    assert jobs.to_pylist() == [{"id_user": pseudonym(b"k", "1000"),
                                 "account": pseudonym(b"k", "phys"), "id_job": 7}]
    assoc = hide("assoc", pa.table({"user": ["alice"], "lineage": ["/p/0-alice/"]}), b"k")
    assert assoc.column_names == ["user"]
    assert hide("job", jobs, None) is jobs
