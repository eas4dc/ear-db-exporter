"""Cleaning helpers: logical types, timestamps, NULLs, node tags and pseudonyms."""
import pyarrow as pa

from ear_db_export.clean import TS, node_tags, pseudonym, pseudonymize, to_ts, typed

KEY = b"salt"


def test_typed_keeps_float32_decimals_and_fills_missing_columns():
    """A stored FLOAT 0.1 reads 0.1; a column the table lacks is NULL; types are logical."""
    stored = pa.table({"id": pa.array([1], pa.uint32()), "dc_power": pa.array([310.523], pa.float32()),
                       "time": pa.array([0.1], pa.float32())})
    out = typed("power_signatures", stored, ["id", "dc_power", "time", "edp"])
    assert out.to_pylist() == [{"id": 1, "dc_power": 310.523, "time": 0.1, "edp": None}]
    assert out.schema.field("id").type == pa.int64() and out.schema.field("edp").type == pa.float64()
    assert typed("power_signatures", stored.slice(0, 0)).num_rows == 0


def test_to_ts_makes_zero_null():
    """Epoch seconds become UTC timestamps; EAR's 0 ("unset") becomes null."""
    out = to_ts(pa.array([0, 1783123200], pa.uint32()))
    assert out.type == TS and out[0].as_py() is None
    assert out[1].as_py().isoformat() == "2026-07-04T00:00:00+00:00"


def test_node_tags():
    """Each node's ear.conf tag; nodes without one are null."""
    assert node_tags(pa.array(["n1", "n2"]), {"n1": "gpu"}).to_pylist() == ["gpu", None]


def test_pseudonymize():
    """Each user value becomes its 16-hex-digit HMAC; nulls stay; no key changes nothing."""
    t = pa.table({"user_id": ["alice", None, "alice"], "other": [1, 2, 3]})
    out = pseudonymize(t, KEY)
    assert out["user_id"].to_pylist() == [pseudonym(KEY, "alice"), None, pseudonym(KEY, "alice")]
    assert pseudonym(KEY, "alice") == "dc663a1de92b83cd"
    assert pseudonymize(t, None) is t
