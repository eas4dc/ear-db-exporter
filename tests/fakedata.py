"""One day of EAR records in logical names, shared by the golden, bundle and round-trip tests.

Float values have few significant digits (0.1, 310.523): pymysql returns MariaDB
FLOAT columns as their decimal text, so the views must show 0.1, not the
float32 value 0.10000000149011612.
"""
import datetime as dt

import pyarrow as pa

from ear_db_export.read import GPU_LINK, TABLES, arrow_schema

DAY = dt.date(2026, 7, 4)
T0 = int(dt.datetime(2026, 7, 4, tzinfo=dt.timezone.utc).timestamp())
INT_MAX = 2147483647  # EAR's "unset" for end_mpi_time of non-MPI steps


def _table(name, rows):
    """A record table with every logical column of ``name``, NULL where ``rows`` lack it."""
    schema = arrow_schema(name)
    return pa.Table.from_pylist([{c: r.get(c) for c in schema.names} for r in rows], schema=schema)


def tables():
    """The logical record tables: ``{table: pyarrow.Table}``."""
    t = T0
    periodic = [
        dict(id=1, start_time=t + 40, end_time=t + 100, node_id="n2", job_id=7, step_id=0,
             dc_energy=1000, avg_f=2000000, temp=50, dram_energy=10, pck_energy=600, gpu_energy=0),
        dict(id=2, start_time=t, end_time=t + 60, node_id="n1", job_id=0, step_id=0,
             dc_energy=900, avg_f=0, temp=51, dram_energy=11, pck_energy=500, gpu_energy=0),
        dict(id=3, start_time=t + 100, end_time=t + 160, node_id="n2", job_id=7, step_id=0,
             dc_energy=1100, avg_f=2100000, temp=None, dram_energy=12, pck_energy=610, gpu_energy=5),
        dict(id=4, start_time=t + 3600, end_time=t + 3660, node_id="n3", job_id=8,
             step_id=4294967291, dc_energy=1200, avg_f=2200000, temp=55, dram_energy=13,
             pck_energy=620, gpu_energy=900),
    ]
    jobs = [
        dict(job_id=7, step_id=0, local_id=5, user_id="alice", user_acc="acc1", user_group="g1",
             app_name="lmp_mpi", start_time=t + 10, end_time=t + 200, start_mpi_time=t + 12,
             end_mpi_time=t + 198, policy="min_energy", threshold=0.1, procs=64, job_type=0,
             def_f=2400000, e_tag=None),
        dict(job_id=7, step_id=0, local_id=6, user_id="alice", user_acc="acc1", user_group="g1",
             app_name="post.sh", start_time=t + 201, end_time=t + 300, start_mpi_time=0,
             end_mpi_time=INT_MAX, policy="monitoring", threshold=0.0, procs=1, job_type=0,
             def_f=2400000, e_tag="tagA"),
        dict(job_id=8, step_id=4294967291, local_id=0, user_id="bob", user_acc="acc2",
             user_group="g2", app_name="python", start_time=t + 3000, end_time=t + 7000,
             start_mpi_time=0, end_mpi_time=INT_MAX, policy="min_time", threshold=0.05, procs=8,
             job_type=0, def_f=2000000, e_tag=None),
    ]
    apps = [
        dict(job_id=7, step_id=0, local_id=5, node_id="n2", signature_id=100,
             power_signature_id=10),
        dict(job_id=7, step_id=0, local_id=5, node_id="n4", signature_id=101,
             power_signature_id=11),
        dict(job_id=7, step_id=0, local_id=6, node_id="n2", signature_id=None,
             power_signature_id=12),
        dict(job_id=8, step_id=4294967291, local_id=0, node_id="n3", signature_id=102,
             power_signature_id=13),
    ]
    power = [
        dict(id=10, dc_power=310.523, dram_power=20.5, pck_power=180.25, edp=1.5e7,
             max_dc_power=320.0, min_dc_power=300.1, time=190.0, avg_f=2300000, def_f=2400000),
        dict(id=11, dc_power=305.7, dram_power=19.25, pck_power=170.0, edp=1.4e7,
             max_dc_power=315.0, min_dc_power=299.9, time=190.0, avg_f=2290000, def_f=2400000),
        dict(id=12, dc_power=120.1, dram_power=5.5, pck_power=60.0, edp=1.2e6,
             max_dc_power=130.0, min_dc_power=110.0, time=99.0, avg_f=2400000, def_f=2400000),
        dict(id=13, dc_power=900.25, dram_power=30.0, pck_power=200.0, edp=1.4e10,
             max_dc_power=1000.0, min_dc_power=800.0, time=4000.0, avg_f=2000000, def_f=2000000),
    ]
    sig = dict(dc_power=310.523, dram_power=20.5, pck_power=180.25, edp=1.5e7, gbs=12.3,
               io_mbs=0.7, tpi=0.02, cpi=0.61, gflops=85.1, time=190.0, perc_mpi=12.5,
               l1_misses=1000, l2_misses=500, l3_misses=100, instructions=10 ** 12,
               cycles=6 * 10 ** 11, avg_f=2300000, avg_imc_f=2000000, def_f=2400000, cpu_util=95,
               **{"flops_%d" % i: i * 1000 for i in range(1, 9)})
    sigs = [
        dict(sig, id=100, min_gpu_sig_id=None, max_gpu_sig_id=None),
        dict(sig, id=101, cpi=0.65, gbs=11.9, min_gpu_sig_id=None, max_gpu_sig_id=None),
        dict(sig, id=102, cpi=1.1, gflops=1200.5, min_gpu_sig_id=50, max_gpu_sig_id=56),
    ]
    gpus = [
        dict(id=50, gpu_power=200.5, gpu_freq=1410000, gpu_mem_freq=1215000, gpu_util=97,
             gpu_mem_util=40, gpu_gflops=1.1),
        dict(id=54, gpu_power=220.25, gpu_freq=1410000, gpu_mem_freq=1215000, gpu_util=95,
             gpu_mem_util=42, gpu_gflops=1.3),
        dict(id=56, gpu_power=180.0, gpu_freq=1400000, gpu_mem_freq=1215000, gpu_util=80,
             gpu_mem_util=30, gpu_gflops=0.9),
    ]  # ids 50, 52, 54, 56 with step 2 (Galera): GPU 1 (id 52) has no row
    return {
        "periodic_metrics": _table("periodic_metrics", periodic),
        "jobs": _table("jobs", jobs),
        "applications": _table("applications", apps),
        "power_signatures": _table("power_signatures", power),
        "signatures": _table("signatures", sigs),
        "gpu_signatures": _table("gpu_signatures", gpus),
    }


ID_STEP = 2


def gpu_rows(t):
    """The gpu_signatures records of signature 102, as a reader links them (GPU_LINK first)."""
    cols = [c for c in TABLES["gpu_signatures"] if c != "id"]
    pos = {50: 0, 54: 2, 56: 3}
    g = t["gpu_signatures"]
    rows = [dict(signature_id=102, gpu_index=pos[r["id"]], **{c: r[c] for c in cols})
            for r in g.to_pylist()]
    schema = pa.schema([(c, pa.int64()) for c in GPU_LINK] + list(arrow_schema("gpu_signatures", cols)))
    return pa.Table.from_pylist(rows, schema=schema)
