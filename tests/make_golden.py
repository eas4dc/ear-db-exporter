"""Write tests/golden/ with the 0.3.0 pipeline: the view files 0.4.0 must reproduce.

Run once with ``uv run python tests/make_golden.py`` on 0.3.0; the files are
committed and this script is removed with the 0.3.0 pipeline.
"""
import os
import shutil
import sys
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.compute as pc

sys.path.insert(0, os.path.dirname(__file__))
from ear_db_export import pipeline  # noqa: E402
from ear_db_export.read import TABLES, Reader  # noqa: E402
import fakedata  # noqa: E402

GOLDEN = os.path.join(os.path.dirname(__file__), "golden")
TAGS = {"n2": "cpu", "n3": "gpu"}
KEY = b"golden-salt"


class FakeReader(Reader):
    provides = frozenset(TABLES)
    join_keys = ["job_id", "step_id", "local_id"]

    def __init__(self):
        self.t = fakedata.tables()

    def _window(self, name, start, end):
        t = self.t[name]
        return t.filter(pc.and_(pc.greater_equal(t["end_time"], start), pc.less(t["end_time"], end)))

    def _by(self, name, col, keys):
        t = self.t[name]
        return t.filter(pc.is_in(t[col], value_set=pa.array([k for k in keys if k is not None],
                                                            t[col].type)))

    def oldest_day(self):
        return fakedata.DAY

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
        g = fakedata.gpu_rows(self.t)
        return g.filter(pc.is_in(g["signature_id"], value_set=signatures["id"]))


def main():
    shutil.rmtree(GOLDEN, ignore_errors=True)
    day = SimpleNamespace(state=None, from_=fakedata.DAY, to=fakedata.DAY, lag_days=2,
                          update_state=False)
    month = SimpleNamespace(from_=fakedata.DAY.replace(day=1), to=fakedata.DAY.replace(day=31),
                            overwrite=True)
    wide = os.path.join(GOLDEN, "wide")
    s = pipeline.Settings(wide, TAGS, KEY)
    pipeline.run("periodic", FakeReader(), s, day)
    pipeline.run("power-signatures", FakeReader(), s, month)
    pipeline.run("app-signatures", FakeReader(), s, month)
    s = pipeline.Settings(os.path.join(GOLDEN, "long"), TAGS, KEY,
                          {"node_metrics": "long", "job_nodes": "allocations"},
                          {"service": "snellius"})
    pipeline.run("periodic", FakeReader(), s, day)
    for d, _, files in os.walk(GOLDEN):
        for f in files:
            if not f.endswith(".parquet"):
                os.remove(os.path.join(d, f))


if __name__ == "__main__":
    main()
