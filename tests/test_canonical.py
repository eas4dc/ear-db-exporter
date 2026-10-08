"""Step 2: canonical datasets built from in-memory records (no database)."""
from types import SimpleNamespace

import pyarrow as pa

from ear_db_export import canonical
from ear_db_export.read import GPU_LINK, TABLES, arrow_schema

KEY = b"salt"
KEYS = ["job_id", "step_id", "local_id"]


def records(**tables):
    """Fake Records: the given record tables, empty ones for the rest."""
    empty = {t: arrow_schema(t).empty_table() for t in TABLES}
    empty["gpu_signatures"] = gpu_rows([], [], [])
    return SimpleNamespace(join_keys=KEYS, **dict(empty, **tables))


def gpu_rows(sig_ids, index, power):
    """GPU records as a reader returns them: GPU_LINK, then the gpu_signatures columns."""
    cols = [c for c in TABLES["gpu_signatures"] if c != "id"]
    schema = pa.schema([(c, pa.int64()) for c in GPU_LINK] + list(arrow_schema("gpu_signatures", cols)))
    rows = [dict(zip(GPU_LINK + ("gpu_power",), r)) for r in zip(sig_ids, index, power)]
    return pa.Table.from_pylist(rows, schema=schema)


def wide():
    return pa.table({
        "id": [1, 2, 3], "start_time": [40, 0, 100], "end_time": [100, 60, 160],
        "node_id": ["n2", "n1", "n2"], "job_id": [7, 0, 7], "step_id": [0, 0, 0],
        "dc_energy": [1000, 900, 1100], "avg_f": [2000000, 0, 2100000], "temp": [50, 51, None],
        "dram_energy": [10, 11, 12], "pck_energy": [None, None, None], "gpu_energy": [0, 0, 5],
    })


def jobs_apps():
    jobs = pa.table({
        "job_id": [1, 1], "step_id": [0, 0], "local_id": [5, 6], "user_id": ["u", "v"],
        "user_acc": ["a", "a"], "user_group": ["g", "g"], "app_name": ["x", "y"],
        "start_time": [10, 20], "end_time": [30, 40], "start_mpi_time": [0, 0],
        "end_mpi_time": [0, 0], "policy": ["p", "p"], "threshold": [0.0, 0.0],
        "procs": [4, 4], "job_type": [0, 0], "def_f": [1, 1],
        "e_tag": pa.array([None, None], pa.string())})
    apps = pa.table({
        "job_id": [1, 1, 1, 2], "step_id": [0, 0, 0, 0], "local_id": [5, 5, 6, 0],
        "node_id": ["nb", "na", "nc", "nz"], "signature_id": [100, None, 101, 102],
        "power_signature_id": [10, 11, None, 12]})
    return dict(jobs=jobs, applications=apps)


def test_node_metrics():
    out = canonical.node_metrics(records(periodic_metrics=wide()), {"n1": "t1"}, None)
    rows = out.to_pylist()
    assert [(r["node"], r["node_tag"]) for r in rows] == [("n1", "t1"), ("n2", None), ("n2", None)]
    assert rows[0]["job_id"] is None and rows[0]["step_id"] is None  # job_id 0 -> idle
    assert rows[1]["job_id"] == 7
    assert out["start"].type == canonical.TS
    assert out["avg_cpu_freq_khz"].to_pylist() == [None, 2000000, 2100000]  # zero -> null
    assert out["gpu_energy_j"].to_pylist() == [None, None, 5]
    assert out["dc_energy_j"].to_pylist() == [900, 1000, 1100]


def test_pseudonymize():
    t = pa.table({"user_id": ["alice", None, "alice"], "other": [1, 2, 3]})
    out = canonical.pseudonymize(t, KEY)
    assert out["user_id"].to_pylist() == [canonical.pseudonym(KEY, "alice"), None,
                                          canonical.pseudonym(KEY, "alice")]
    assert len(out["user_id"][0].as_py()) == 16
    assert canonical.pseudonymize(t, None) is t


def test_job_nodes_join_on_local_id():
    r = records(**jobs_apps())
    out = canonical.job_nodes(r, {"na": "tagA"}, KEY)
    assert [(x["local_id"], x["node"], x["node_tag"]) for x in out.to_pylist()] == \
        [(5, "na", "tagA"), (5, "nb", None), (6, "nc", None)]
    assert out.column_names == r.jobs.column_names + ["node", "node_tag"]
    assert out["start_time"].type == canonical.TS
    assert out["user_id"][0].as_py() == canonical.pseudonym(KEY, "u")


def test_power_signatures_keeps_missing():
    ps = pa.table({"id": [10, 11], "dc_power": [300.0, 310.0]})
    out = canonical.power_signatures(records(power_signatures=ps, **jobs_apps()), {}, None)
    assert out["ps_dc_power"].to_pylist() == [310.0, 300.0, None]  # na, nb, nc
    assert "ps_id" not in out.column_names and out["job_start_mpi_time"].null_count == 3


def test_app_and_gpu_signatures():
    sigs = pa.table({"id": [100, 101], "cpi": [1.0, 2.0],
                     "min_gpu_sig_id": [50, None], "max_gpu_sig_id": [52, None]})
    # GPU 1 of signature 100 has no row: index 1 is missing, index 2 keeps its place
    r = records(signatures=sigs, gpu_signatures=gpu_rows([100, 100], [2, 0], [220.0, 200.0]),
                **jobs_apps())
    out = canonical.app_signatures(r, {}, None)
    assert out["node"].to_pylist() == ["nb", "nc"] and out["sig_cpi"].to_pylist() == [1.0, 2.0]
    assert "sig_min_gpu_sig_id" not in out.column_names
    g = canonical.gpu_signatures(r, {}, None)
    assert g.select(["job_id", "step_id", "local_id", "node", "gpu_index", "gpu_power"]).to_pylist() == [
        {"job_id": 1, "step_id": 0, "local_id": 5, "node": "nb", "gpu_index": 0, "gpu_power": 200.0},
        {"job_id": 1, "step_id": 0, "local_id": 5, "node": "nb", "gpu_index": 2, "gpu_power": 220.0}]
    assert "signature_id" not in g.column_names


def test_empty_records():
    r = records()
    for name, dataset in canonical.DATASETS.items():
        assert dataset.build(r, {}, KEY).num_rows == 0, name
