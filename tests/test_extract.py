"""extract and Records: the rows a Selection picks, from a fake database or a bundle."""
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ear_db_export import canonical, extract, formats, write
from ear_db_export.config import ConfigError
from ear_db_export.read.bundle import BundleSource
from ear_db_export.read.sql import SqlSource
from ear_db_export.selection import BATCH_STEPS, Selection, parse_jobs

import fakedata
import make_golden
from fakedb import DBCONF, make_engine
from test_sources import make_bundle

T0, DAY_S = fakedata.T0, 86400
MONTH = (T0 - 3 * DAY_S, T0 + 28 * DAY_S)  # July 2026
GOLDEN = make_golden.GOLDEN


def sql_source(profile="ear6"):
    return SqlSource(DBCONF, engine=make_engine(profile))


def read_all(source, sel):
    return {t: pa.concat_tables(list(parts)) for t, parts in extract.extract(source, sel)}


def golden(variant, dataset, part):
    return pq.ParquetFile(os.path.join(GOLDEN, variant, dataset, part, "part-0.parquet")).read()


@pytest.mark.parametrize("make", [lambda tmp: BundleSource(make_bundle(str(tmp), "ear6")),
                                  lambda tmp: sql_source("ear7")], ids=["bundle-ear6", "sql-ear7"])
def test_records_views_equal_golden(tmp_path, make):
    src = make(tmp_path)
    checks = [("wide", "node_metrics", "wide", (T0, T0 + DAY_S), "date=2026-07-04"),
              ("wide", "job_nodes", "wide", (T0, T0 + DAY_S), "date=2026-07-04"),
              ("long", "node_metrics", "long", (T0, T0 + DAY_S), "date=2026-07-04"),
              ("long", "job_nodes", "allocations", (T0, T0 + DAY_S), "date=2026-07-04"),
              ("wide", "power_signatures", "wide", MONTH, "month=2026-07"),
              ("wide", "app_signatures", "wide", MONTH, "month=2026-07"),
              ("wide", "gpu_signatures", "wide", MONTH, "month=2026-07")]
    for variant, name, fmt, (start, end), part in checks:
        records = extract.Records(src, Selection(start=start, end=end))
        table = canonical.DATASETS[name].build(records, make_golden.TAGS, make_golden.KEY)
        path = str(tmp_path / ("%s-%s.parquet" % (name, fmt)))
        write.parquet(path, formats.apply(name, fmt, table, {"service": "snellius"}))
        out, want = pq.ParquetFile(path).read(), golden(variant, name, part)
        if src.profile.name == "ear7":  # EAR 7 dropped Jobs.job_type, Signatures.EDP: NULL
            for c in ("job_type", "job_job_type", "sig_edp"):
                if c in want.column_names:
                    i = want.schema.get_field_index(c)
                    want = want.set_column(i, c, pa.nulls(want.num_rows, want.schema.field(c).type))
        assert out.schema.equals(want.schema), (name, fmt)
        assert out.equals(want), (name, fmt)


def test_unfiltered_reads_every_table_whole():
    got = read_all(sql_source(), Selection(tables=("all",)))
    assert list(got)[:6] == ["Jobs", "Applications", "Loops", "Power_signatures", "Signatures",
                             "GPU_signatures"]
    assert got["Periodic_metrics"].num_rows == 4 and got["GPU_signatures"].num_rows == 3


def test_job_filters_follow_the_links():
    got = read_all(sql_source(), Selection(users=("bob",), tables=("jobs", "periodic")))
    assert got["Jobs"]["job_id"].to_pylist() == [8]
    assert got["Applications"]["node_id"].to_pylist() == ["n3"]
    assert got["Signatures"]["id"].to_pylist() == [102]
    assert got["Power_signatures"]["id"].to_pylist() == [13]
    assert got["GPU_signatures"]["id"].to_pylist() == [50, 54, 56]  # range 50..56 step 2
    assert got["Periodic_metrics"]["id"].to_pylist() == [4]  # only job 8's rows


def test_jobs_steps_and_batch_steps():
    got = read_all(sql_source(), Selection(jobs=parse_jobs("8.sb,7.0"), tables=("jobs",)))
    assert sorted(set(got["Jobs"]["job_id"].to_pylist())) == [7, 8]
    got = read_all(sql_source(), Selection(batch_steps=True, tables=("jobs", "periodic")))
    assert got["Jobs"]["step_id"].to_pylist() == [BATCH_STEPS[0]]
    assert got["Periodic_metrics"]["job_id"].to_pylist() == [8]


def test_batch_steps_keeps_all_periodic_rows_of_the_job():
    eng = make_engine()
    eng.db.execute("UPDATE `Periodic_metrics` SET `step_id` = 0 WHERE `id` = 4")  # an srun step
    got = read_all(SqlSource(DBCONF, engine=eng), Selection(batch_steps=True, tables=("periodic",)))
    assert got["Periodic_metrics"]["id"].to_pylist() == [4]
    got = read_all(SqlSource(DBCONF, engine=make_engine()),
                   Selection(jobs=parse_jobs("8.0"), tables=("periodic",)))
    assert got["Periodic_metrics"].num_rows == 0  # explicit step: rows of that step only


def test_nodes_keep_their_rows_and_whole_job_rows():
    got = read_all(sql_source(), Selection(nodes=("n4",), tables=("jobs", "periodic")))
    assert got["Applications"]["node_id"].to_pylist() == ["n4"]
    assert got["Jobs"]["local_id"].to_pylist() == [5]  # only the step that ran on n4
    assert got["Periodic_metrics"].num_rows == 0  # n4 has none


def test_time_only_keeps_idle_rows():
    got = read_all(sql_source(), Selection(start=T0, end=T0 + 101, tables=("periodic",)))
    assert got["Periodic_metrics"]["id"].to_pylist() == [1, 2]  # 2 is idle (job 0)


def test_sample_is_deterministic_and_limit_takes_newest():
    pick = lambda **kw: read_all(sql_source(), Selection(tables=("jobs",), **kw))["Jobs"]
    assert pick(sample=1, seed=3)["job_id"].to_pylist() == pick(sample=1, seed=3)["job_id"].to_pylist()
    assert set(pick(limit=1)["job_id"].to_pylist()) == {8}
    assert len(set(pick(sample=5)["job_id"].to_pylist())) == 2


def test_loops_and_their_signatures():
    eng = make_engine()
    eng.db.execute("INSERT INTO `Loops` (`event`, `size`, `level`, `job_id`, `step_id`, `local_id`, "
                   "`node_id`, `total_iterations`, `signature_id`) VALUES "
                   "(1, 2, 0, 7, 0, 6, 'n2', 10, 101), (1, 2, 0, 9, 0, 0, 'n9', 10, 102)")
    got = read_all(SqlSource(DBCONF, engine=eng), Selection(users=("alice",), tables=("loops",)))
    assert got["Loops"]["job_id"].to_pylist() == [7]
    assert got["Signatures"]["id"].to_pylist() == [100, 101]  # apps' and loops', not job 9's


def test_gpu_ids_use_the_step():
    assert extract.gpu_ids([(7, 11, 17), (8, None, None)], 2) == (
        [7, 7, 7, 7], [0, 1, 2, 3], [11, 13, 15, 17])


def test_filters_need_a_profile(tmp_path):
    src = BundleSource(make_bundle(str(tmp_path)))
    src.profile = None
    with pytest.raises(ConfigError, match="profile"):
        read_all(src, Selection(users=("x",)))
    with pytest.raises(ConfigError, match="--tables"):
        read_all(src, Selection(tables=("nope",)))
