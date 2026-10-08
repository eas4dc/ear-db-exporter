"""dump: exact types, primary-key paging and the files written, with a fake engine (no database)."""
import json

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from ear_db_export import dump
from ear_db_export.canonical import pseudonym
from ear_db_export.config import ConfigError
from ear_db_export.read.sql import mariadb


@pytest.mark.parametrize("column_type, expected", [
    ("int(10) unsigned", pa.uint32()), ("bigint(20) unsigned", pa.uint64()), ("int(11)", pa.int32()),
    ("smallint(5) unsigned", pa.uint16()), ("float", pa.float32()), ("double", pa.float64()),
    ("varchar(64)", pa.string()), ("uuid", pa.string()), ("timestamp", pa.timestamp("us", "UTC")),
])
def test_arrow_type(column_type, expected):
    assert mariadb.arrow_type(column_type) == expected


class FakeEngine:
    """Answers the two query shapes of dump from in-memory tables: ``ORDER BY k LIMIT n`` and
    ``WHERE k > %s ORDER BY k LIMIT n``, or a whole-table SELECT."""

    quote = staticmethod(lambda name: name)
    arrow_type = staticmethod(mariadb.arrow_type)

    def __init__(self, tables, cols):
        self.tables, self.cols, self.queries = tables, cols, []

    def scalar(self, conn, sql, params=()):
        return "fake-1.0"

    def columns(self, conn, database, table):
        return self.cols.get(table, [])

    def show_create(self, conn, table):
        return "CREATE TABLE %s (...)" % table

    def explain(self, conn, sql, params):
        return ["%s: type=index" % sql.split(" FROM ")[1].split()[0]]

    def fetch(self, conn, sql, params, schema):
        self.queries.append((sql, params))
        t = self.tables[sql.split(" FROM ")[1].split()[0]]
        if "LIMIT" not in sql:
            return t
        key = sql.split("ORDER BY ")[1].split()[0]
        if params:
            t = t.filter(pc.greater(t[key], params[0]))
        return t.sort_by(key).slice(0, int(sql.rsplit("LIMIT", 1)[1]))

    def stream(self, conn, sql, params, schema):
        yield from self.fetch(conn, sql, params, schema).to_batches()


def make_engine():
    sigs = pa.table({"id": pa.array([1, 2, 3, 5, 8], pa.uint64()),
                     "DC_power": pa.array([1.5] * 5, pa.float32())})
    jobs = pa.table({"job_id": pa.array([7, 7], pa.uint32()), "step_id": pa.array([0, 1], pa.uint32()),
                     "user_id": ["alice", None]})
    cols = {"Signatures": [("id", "bigint(20) unsigned", True), ("DC_power", "float", False)],
            "Jobs": [("job_id", "int(10) unsigned", True), ("step_id", "int(10) unsigned", True),
                     ("user_id", "varchar(128)", False)]}
    return FakeEngine({"Signatures": sigs, "Jobs": jobs}, cols)


@pytest.mark.parametrize("page_rows, pages", [(2, 3), (5, 2), (100, 1)])
def test_paging_returns_every_row_once(monkeypatch, page_rows, pages):
    monkeypatch.setattr(dump, "PAGE_ROWS", page_rows)
    eng = make_engine()
    cols = eng.cols["Signatures"]
    schema = dump._schema(eng, "Signatures", cols)
    first, nxt, key = dump._queries(eng, "Signatures", cols, schema)
    got = pa.concat_tables(dump._pages(None, eng, schema, first, nxt, key))
    assert got["id"].to_pylist() == [1, 2, 3, 5, 8] and len(eng.queries) == pages


def test_dump_writes_tables_schema_and_manifest(tmp_path):
    eng = make_engine()
    m = dump.dump(None, eng, "EAR", ["Signatures", "Jobs"], str(tmp_path), b"salt")
    sigs = pq.read_table(tmp_path / "Signatures.parquet")
    assert sigs.schema.field("id").type == pa.uint64() and sigs.num_rows == 5
    assert sigs.schema.field("DC_power").type == pa.float32()
    jobs = pq.read_table(tmp_path / "Jobs.parquet")  # composite key: one streamed SELECT
    assert jobs["user_id"].to_pylist() == [pseudonym(b"salt", "alice"), None]
    assert "CREATE TABLE Jobs" in (tmp_path / "schema.sql").read_text()
    on_disk = json.loads((tmp_path / "manifest.json").read_text())
    assert on_disk == m and on_disk["pseudonymized"] is True
    assert {t: v["rows"] for t, v in on_disk["tables"].items()} == {"Signatures": 5, "Jobs": 2}


def test_dump_dry_run_and_errors(tmp_path):
    eng = make_engine()
    dump.dump(None, eng, "EAR", ["Signatures"], str(tmp_path), None, dry_run=True)
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(ConfigError, match="no table"):
        dump.dump(None, eng, "EAR", ["Nope"], str(tmp_path), None)
    eng.cols["Bad"] = [("x", "enum('a')", False)]
    with pytest.raises(ConfigError, match="Bad.x"):
        dump.dump(None, eng, "EAR", ["Bad"], str(tmp_path), None)
