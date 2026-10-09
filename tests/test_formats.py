"""Step 3: format functions, resolve and apply."""
import pyarrow as pa
import pytest

from ear_db_export import canonical, cli, formats
from ear_db_export.config import ConfigError
from test_canonical import KEY, jobs_apps, records, wide


def node_metrics():
    return canonical.node_metrics(records(periodic_metrics=wide()), {}, None)


def test_long():
    out = pa.concat_tables(formats.long(node_metrics(), {"service": "s1"}))
    rows = out.to_pylist()
    names = [r["metric_name"] for r in rows]
    # 3 dc + 3 dram + 1 gpu (zeros dropped) + 2 avg_f (zero dropped) + 2 temp (null dropped)
    assert len(rows) == 11
    assert names == sorted(names, key=[m[1] for m in formats.LONG_METRICS].index)
    dc = [r for r in rows if r["metric_name"] == "ear_node_dc_energy_joules"]
    assert [dict(r["labels"])["node"] for r in dc] == ["n1", "n2", "n2"]  # sorted by node, time
    assert dict(dc[1]["labels"]) == {"node": "n2", "job_id": "7", "step_id": "0"}
    assert dc[0]["labels"] == [("node", "n1")]  # idle interval -> no job labels
    assert dc[1]["duration_s"] == 60 and dc[1]["timestamp"].isoformat() == "1970-01-01T00:01:40+00:00"
    assert {r["service"] for r in rows} == {"s1"} and {r["source"] for r in rows} == {"ear"}
    assert out.schema == formats.LONG_SCHEMA


def test_long_needs_service_and_yields_schema_when_empty():
    with pytest.raises(ConfigError):
        list(formats.long(node_metrics(), {}))
    empty = list(formats.long(node_metrics().slice(0, 0), {"service": "s"}))
    assert len(empty) == 1 and empty[0].num_rows == 0 and empty[0].schema == formats.LONG_SCHEMA


def test_allocations():
    t = canonical.job_nodes(records(**jobs_apps()), {"na": "tagA"}, KEY)
    out = formats.allocations(t, {})
    assert out.column_names == ["job_id", "step_id", "local_id", "node", "start", "end",
                                "user_id", "user_acc", "user_group", "app_name", "policy",
                                "job_type", "procs", "node_tag"]
    assert out["start"].to_pylist() == t["start_time"].to_pylist()


def test_resolve_and_apply():
    assert formats.resolve("node_metrics", "wide") is formats.wide
    with pytest.raises(ConfigError):
        formats.resolve("job_nodes", "long")  # long is only for node_metrics
    with pytest.raises(ConfigError):
        formats.resolve("job_nodes", "ear_db_export.formats:allocations")  # no imports
    tables = list(formats.apply("node_metrics", "wide", node_metrics(), {}))
    assert len(tables) == 1 and tables[0].num_rows == 3


def test_format_settings():
    view = ["view", "node_metrics", "job_nodes"]
    args = cli.parser().parse_args(view + ["--format", "node_metrics=long", "--format-opt",
                                           "service=s"])
    assert cli.format_settings(args) == ({"node_metrics": "long"}, {"service": "s"})
    for bad in (["--format", "power_signatures=wide"], ["--format", "node_metrics"],
                ["--format", "job_nodes=long"]):
        with pytest.raises(ConfigError):
            cli.format_settings(cli.parser().parse_args(view + bad))
