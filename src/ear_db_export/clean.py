"""Helpers the layouts share to turn EAR's stored values into clean ones.

EAR stores its data in the form that is quickest for EAR to write, which is not
the easiest to analyse. Times are Unix seconds, a missing value is often stored
as ``0``, many measurements are 32-bit floats, and user names are in clear. The
layouts use these helpers to give each column a proper type (:func:`typed`),
turn times into UTC timestamps (:func:`to_ts`), turn EAR's ``0`` into NULL
where it means "no value" (:func:`zero_to_null`), add each node's tag
(:func:`node_tags`) and replace user names by pseudonyms (:func:`pseudonymize`).
"""
import hashlib
import hmac

import pyarrow as pa
import pyarrow.compute as pc

from .read import ARROW, TABLES, arrow_schema, col_type

#: Timestamp type of every exported time column: seconds, UTC.
TS = pa.timestamp("s", tz="UTC")
#: Logical columns that identify a user, pseudonymised in every logical table that has them.
USER_FIELDS = ("user_id", "user_acc", "user_group")
#: Physical columns that hold a user in some EAR version, pseudonymised in tables as stored.
USER_COLUMNS = ("user_id", "user_name", "user_acc", "user_group", "pod_user_id")
#: ``jobs`` columns holding epoch seconds.
JOB_TIMES = ("start_time", "end_time", "start_mpi_time", "end_mpi_time")


def typed(table, t, columns=None):
    """Give the rows of a logical table their logical types (int64, float64, string, timestamp).

    Sources yield the types as stored (``uint32``, ``float32``, ...); every
    layout starts from this. ``FLOAT`` columns (float32) go through their
    shortest decimal text, so a stored 0.1 reads as 0.1, as the MySQL protocol
    returns it, and not as 0.10000000149011612. A column the table lacks (an
    optional column this EAR version does not have) is NULL.

    :param table: logical table name.
    :param t: ``pyarrow.Table`` with logical column names (extra columns are ignored).
    :param columns: logical columns to return, in order; default all of :data:`~ear_db_export.read.TABLES`.
    :returns: a table with exactly those columns and types.

    Example::

        typed("power_signatures", pa.table({"id": pa.array([1], pa.uint32()),
                                            "time": pa.array([0.1], pa.float32())}),
              ["id", "time", "edp"])
        # id: int64 = [1], time: double = [0.1], edp: double = [null]
    """
    names = list(columns or TABLES[table])
    if t.num_rows == 0:  # zero-chunk columns crash some kernels: build it empty
        return arrow_schema(table, names).empty_table()
    arrays = []
    for c in names:
        typ = ARROW[col_type(table, c)]
        if c not in t.column_names:
            arrays.append(pa.nulls(t.num_rows, typ))
            continue
        values = t[c]
        if pa.types.is_float32(values.type):
            values = pc.cast(values, pa.string())
        arrays.append(pc.cast(values, typ))
    return pa.Table.from_arrays(arrays, names=names)


def to_ts(arr):
    """Convert Unix epoch seconds to UTC timestamps; EAR's ``0`` ("unset") becomes null.

    :param arr: integer array or chunked array of epoch seconds.
    :returns: the same values as ``timestamp[s, UTC]``.
    """
    return pc.cast(zero_to_null(pc.cast(arr, pa.int64())), TS)


def with_timestamps(t, columns):
    """Return ``t`` with each of ``columns`` (epoch seconds) converted by :func:`to_ts`, in place of the original."""
    for name in columns:
        t = t.set_column(t.schema.get_field_index(name), name, to_ts(t[name]))
    return t


def zero_to_null(arr):
    """Return the array with every 0 replaced by null (EAR writes 0 when a value is missing)."""
    return pc.if_else(pc.equal(arr, 0), pa.scalar(None, arr.type), arr)


def node_tags(nodes, tags):
    """Look up the ear.conf tag of each node.

    :param nodes: array of node names.
    :param tags: ``{node: tag}`` from ear.conf.
    :returns: a string array of tags, null for nodes that ear.conf does not tag.
    """
    pos = pc.index_in(nodes, value_set=pa.array(list(tags), pa.string()))
    return pc.take(pa.array(list(tags.values()), pa.string()), pos)


def prefixed(table, prefix):
    """Return the table with every column renamed to ``prefix + name``."""
    return table.rename_columns([prefix + c for c in table.column_names])


def pseudonym(key, value):
    """Return the pseudonym of one value: ``HMAC-SHA256(key, value)``, first 16 hex digits.

    The same key and value always give the same pseudonym, so pseudonymised
    exports stay joinable across runs and sites that share the key.

    :param key: the secret key (contents of ``--salt-file``), bytes.
    :param value: the value to hide, str.
    :returns: 16 lowercase hex characters.
    """
    return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:16]


def pseudonymize(table, key, columns=USER_FIELDS):
    """Replace user-identifying columns by their pseudonyms.

    Each distinct value is hashed once, then mapped back onto the rows. Nulls
    stay null. Columns missing from the table are skipped.

    :param table: a ``pyarrow.Table``.
    :param key: the secret key, bytes; ``None`` returns the table unchanged.
    :param columns: names of the columns to replace.
    :returns: the table with those columns as pseudonym strings.

    Example::

        pseudonymize(pa.table({"user_id": ["alice", None]}), b"salt")
        # user_id: ["dc663a1de92b83cd", null]   (the first 16 hex digits of HMAC-SHA256)
    """
    if key is None:
        return table
    for name in columns:
        if name not in table.column_names:
            continue
        col = table[name]
        uniq = pc.unique(col).to_pylist()
        mapping = pa.array([None if v is None else pseudonym(key, v) for v in uniq], pa.string())
        idx = pc.index_in(col, value_set=pa.array(uniq, col.type))
        table = table.set_column(table.schema.get_field_index(name), name, mapping.take(idx))
    return table
