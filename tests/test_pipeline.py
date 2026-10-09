"""Running commands end to end with an in-memory reader: schedule, pipeline and write."""
import datetime as dt
import os
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from ear_db_export import pipeline, schedule, write
from ear_db_export.config import ConfigError
from ear_db_export.read import TABLES, Reader
from test_canonical import KEYS, gpu_rows, jobs_apps, wide

DAY = dt.date(2026, 7, 4)
T0 = schedule.day_epoch(DAY)


def at_day(table):
    """Move a table's start_time/end_time from epoch 0 to DAY."""
    for c in ("start_time", "end_time"):
        table = table.set_column(table.schema.get_field_index(c), c, pc.add(table[c], T0))
    return table


class FakeReader(Reader):
    """A store in memory: the record tables of test_canonical, moved to DAY."""

    provides = frozenset(TABLES)
    join_keys = KEYS

    def __init__(self):
        t = jobs_apps()
        self.tables = {
            "periodic_metrics": at_day(wide()),
            "jobs": at_day(t["jobs"]),
            "applications": t["applications"],
            "power_signatures": pa.table({"id": [10, 11], "dc_power": [300.0, 310.0]}),
            "signatures": pa.table({"id": [100, 101], "cpi": [1.0, 2.0],
                                    "min_gpu_sig_id": [50, None], "max_gpu_sig_id": [52, None]}),
        }
        self.gpus = gpu_rows([100, 100], [0, 2], [200.0, 220.0])

    def _window(self, name, start, end):
        t = self.tables[name]
        return t.filter(pc.and_(pc.greater_equal(t["end_time"], start), pc.less(t["end_time"], end)))

    def _by(self, name, col, keys):
        t = self.tables[name]
        return t.filter(pc.is_in(t[col], value_set=pa.array([k for k in keys if k is not None],
                                                            t[col].type)))

    def oldest_day(self):
        return DAY

    def periodic_metrics(self, start, end):
        return self._window("periodic_metrics", start, end)

    def jobs(self, start, end):
        return self._window("jobs", start, end)

    def applications(self, job_ids):
        return self._by("applications", "job_id", job_ids)

    def power_signatures(self, ids):
        return self._by("power_signatures", "id", ids)

    def signatures(self, ids):
        return self._by("signatures", "id", ids)

    def gpu_signatures(self, signatures):
        return self.gpus.filter(pc.is_in(self.gpus["signature_id"], value_set=signatures["id"]))


def periodic_args(**kw):
    return SimpleNamespace(**dict(dict(state=None, from_=DAY, to=DAY, lag_days=2,
                                       update_state=True), **kw))


def month_args(**kw):
    return SimpleNamespace(**dict(dict(from_=DAY.replace(day=1), to=dt.date(2026, 7, 31),
                                       overwrite=False), **kw))


def test_periodic_end_to_end(tmp_path):
    s = pipeline.Settings(str(tmp_path), formats={"node_metrics": "long", "job_nodes": "allocations"},
                          format_opts={"service": "s1"})
    pipeline.run("periodic", FakeReader(), s, periodic_args())
    metrics = pq.ParquetFile(tmp_path / "node_metrics" / "date=2026-07-04" / "part-0.parquet").read()
    alloc = pq.ParquetFile(tmp_path / "job_nodes" / "date=2026-07-04" / "part-0.parquet").read()
    assert metrics.num_rows == 11 and set(metrics["service"].to_pylist()) == {"s1"}
    assert alloc.num_rows == 3 and "start" in alloc.column_names
    assert schedule.load_watermark(str(tmp_path / schedule.STATE_FILE)) == DAY
    assert [os.path.basename(os.path.dirname(os.path.dirname(p))) for p, _, _ in s.written] == \
        ["node_metrics", "job_nodes"]


def test_dry_run_writes_nothing(tmp_path):
    s = pipeline.Settings(str(tmp_path), dry_run=True)
    pipeline.run("periodic", FakeReader(), s, periodic_args())
    assert os.listdir(tmp_path) == [] and s.written == []


def test_monthly_skips_existing(tmp_path):
    s = pipeline.Settings(str(tmp_path))
    pipeline.run("app-signatures", FakeReader(), s, month_args())
    gpu = pq.ParquetFile(tmp_path / "gpu_signatures" / "month=2026-07" / "part-0.parquet").read()
    assert gpu["gpu_index"].to_pylist() == [0, 2]
    s2 = pipeline.Settings(str(tmp_path))
    pipeline.run("app-signatures", FakeReader(), s2, month_args())
    assert s2.written == []  # month exists: skipped
    pipeline.run("app-signatures", FakeReader(), s2, month_args(overwrite=True))
    assert len(s2.written) == 2


def test_monthly_redoes_month_with_a_missing_dataset(tmp_path):
    pipeline.run("app-signatures", FakeReader(), pipeline.Settings(str(tmp_path)), month_args())
    os.remove(tmp_path / "gpu_signatures" / "month=2026-07" / "part-0.parquet")  # crash after 1st
    s = pipeline.Settings(str(tmp_path))
    pipeline.run("app-signatures", FakeReader(), s, month_args())
    assert len(s.written) == 2


def test_monthly_exports_whole_finished_months_only(tmp_path):
    s = pipeline.Settings(str(tmp_path))
    today = dt.datetime.now(dt.timezone.utc).date()
    pipeline.run("power-signatures", FakeReader(), s,
                 month_args(from_=dt.date(2026, 7, 15), to=today))  # mid-month to today
    months = sorted(os.listdir(tmp_path / "power_signatures"))
    assert months[0] == "month=2026-07" and "month=%s" % today.strftime("%Y-%m") not in months


def test_second_run_on_same_out_is_refused(tmp_path):
    with write.lock(str(tmp_path)):
        with pytest.raises(ConfigError, match="another"):
            pipeline.run("periodic", FakeReader(), pipeline.Settings(str(tmp_path)),
                         periodic_args())


def test_reader_without_needed_records_is_refused(tmp_path):
    class MetricsOnly(FakeReader):  # like a Prometheus reader
        provides = frozenset({"periodic_metrics"})
    with pytest.raises(ConfigError, match="jobs"):
        pipeline.run("periodic", MetricsOnly(), pipeline.Settings(str(tmp_path)), periodic_args())


def test_partition_path():
    assert write.partition_path("o", "node_metrics", {"date": dt.date(2026, 7, 4)}) == \
        "o/node_metrics/date=2026-07-04/part-0.parquet"
    assert write.partition_path("o", "app_signatures", {"month": dt.date(2026, 6, 15)}) == \
        "o/app_signatures/month=2026-06/part-0.parquet"


def test_parquet_atomic_overwrite(tmp_path):
    path = str(tmp_path / "a" / "x.parquet")
    t = pa.table({"v": [1, 2]})
    assert write.parquet(path, [t])[0] == 2
    write.parquet(path, [t.slice(0, 1)])
    assert pq.read_table(path).num_rows == 1 and not (tmp_path / "a" / "x.parquet.tmp").exists()

    def boom():
        yield t
        raise RuntimeError
    with pytest.raises(RuntimeError):
        write.parquet(path, boom())
    assert pq.read_table(path).num_rows == 1  # previous file intact, no .tmp
    assert not (tmp_path / "a" / "x.parquet.tmp").exists()


def test_watermark_roundtrip(tmp_path):
    p = str(tmp_path / "s.json")
    assert schedule.load_watermark(p) is None
    schedule.save_watermark(p, dt.date(2026, 1, 2))
    assert schedule.load_watermark(p) == dt.date(2026, 1, 2)


def test_months_and_defaults():
    assert schedule.default_range(dt.date(2026, 10, 1)) == (dt.date(2025, 10, 1), dt.date(2026, 9, 30))
    assert schedule.months(dt.date(2025, 12, 15), dt.date(2026, 2, 3)) == [
        dt.date(2025, 12, 1), dt.date(2026, 1, 1), dt.date(2026, 2, 1)]


def test_pending_days():
    args = periodic_args(from_=None, to=None)
    today = dt.date(2026, 10, 10)
    assert schedule.pending_days(args, dt.date(2026, 10, 6), None, today) == \
        [dt.date(2026, 10, 7), dt.date(2026, 10, 8)]
    assert schedule.pending_days(args, dt.date(2026, 10, 8), None, today) == []
    assert schedule.pending_days(args, None, dt.date(2026, 10, 7), today)[0] == dt.date(2026, 10, 7)
