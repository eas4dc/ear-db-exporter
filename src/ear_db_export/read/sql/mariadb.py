"""MariaDB/MySQL engine of the SQL source: connection, quoting, and SELECTs streamed into pyarrow.

This is the only module that knows the database engine: the driver,
identifier quoting, ``EXPLAIN`` output, ``information_schema`` and type names,
grants, and the auto-increment step. :class:`~ear_db_export.read.sql.SqlSource`
and its queries use only these functions, with ``%s``
placeholders, so another engine EAR supports (PostgreSQL) is another module
with the same functions.

The connection uses an unbuffered cursor (``SSCursor``), so large results are
read in batches (:func:`stream`) instead of being loaded into memory by the driver.
"""
import logging

import pyarrow as pa
import pymysql
import pymysql.cursors

log = logging.getLogger(__name__)

#: Rows fetched from the server per round trip in :func:`fetch`.
BATCH_ROWS = 100_000
#: ``information_schema`` ``DATA_TYPE`` → logical type, for ``check``.
TYPE_FAMILY = {"tinyint": "int", "smallint": "int", "mediumint": "int", "int": "int",
               "bigint": "int", "float": "float", "double": "float", "decimal": "float",
               "char": "str", "varchar": "str", "text": "str", "uuid": "str",
               "timestamp": "ts"}
#: Integer ``DATA_TYPE`` → bits of the Arrow integer that holds it, for :func:`arrow_type`.
INT_BITS = {"tinyint": 8, "smallint": 16, "mediumint": 32, "int": 32, "bigint": 64}
#: Other ``DATA_TYPE`` → Arrow type, for :func:`arrow_type`. ``TIMESTAMP`` is read in
#: UTC (see :func:`connect`).
ARROW_TYPES = {"float": pa.float32(), "double": pa.float64(), "char": pa.string(),
               "varchar": pa.string(), "text": pa.string(), "uuid": pa.string(),
               "timestamp": pa.timestamp("us", tz="UTC")}


def quote(name):
    """Quote an identifier with backticks (any backtick inside is removed).

    :param name: physical table or column name.
    :returns: the quoted name.
    """
    return "`%s`" % name.replace("`", "")


def connect(db):
    """Open a read-only connection.

    The session is set to ``READ ONLY``, so a write fails even if the account
    has more privileges than expected, and its time zone to UTC, so
    ``TIMESTAMP`` columns read the same on every server. ``net_write_timeout``
    is raised to 10 minutes: a long streamed scan (``Loops`` has no index) must
    not be cut when the client pauses to write a file.

    :param db: a :class:`~ear_db_export.config.DbConf`, normally with the
        ``DBCommandsUser`` credentials.
    :returns: an open ``pymysql`` connection with an unbuffered cursor class.
    :raises pymysql.err.OperationalError: if the server cannot be reached or
        rejects the credentials.
    """
    log.info("connecting to %s:%d/%s as %s", db.host, db.port, db.database, db.user)
    conn = pymysql.connect(host=db.host, port=db.port, user=db.user, password=db.password,
                           database=db.database, connect_timeout=30, read_timeout=3600,
                           charset="utf8mb4", cursorclass=pymysql.cursors.SSCursor)
    with conn.cursor() as cur:
        cur.execute("SET time_zone = '+00:00'")
        cur.execute("SET SESSION net_write_timeout = 600")
        cur.execute("SET SESSION TRANSACTION READ ONLY")
    return conn


def read_only(conn):
    """Tell whether the session is read-only, as the server reports it.

    :param conn: connection from :func:`connect`.
    :returns: ``True`` if writes are refused in this session.
    """
    # MariaDB >= 11.1 and MySQL >= 8 name it transaction_read_only; older MariaDB tx_read_only
    for var in ("transaction_read_only", "tx_read_only"):
        try:
            return bool(scalar(conn, "SELECT @@session.%s" % var))
        except pymysql.err.MySQLError:
            continue
    return False


def grants(conn):
    """Return the privileges of the connected account (``SHOW GRANTS``).

    :param conn: connection from :func:`connect`.
    :returns: one ``GRANT …`` statement per item, cut before any ``IDENTIFIED``
        clause so no password hash is printed.
    """
    return [r[0].split(" IDENTIFIED ")[0] for r in rows(conn, "SHOW GRANTS FOR CURRENT_USER()")]


def rows(conn, sql, params=()):
    """Run a query and return all its rows. Use it only for small results.

    :param conn: connection from :func:`connect`.
    :param sql: SQL with ``%s`` placeholders.
    :param params: values for the placeholders.
    :returns: a tuple of row tuples.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def scalar(conn, sql, params=()):
    """Run a query and return the first column of its first row.

    :param conn: connection from :func:`connect`.
    :param sql: SQL with ``%s`` placeholders.
    :param params: values for the placeholders.
    :returns: the value, or ``None`` if the query returns no rows.
    """
    result = rows(conn, sql, params)
    return result[0][0] if result else None


def stream(conn, sql, params, schema):
    """Run a SELECT and yield its result as pyarrow record batches with a fixed schema.

    Rows are read :data:`BATCH_ROWS` at a time and converted batch by batch, so
    neither the driver nor this function holds the whole result. Consume the
    generator before running another query on the same connection.

    :param conn: connection from :func:`connect`.
    :param sql: the SELECT, with ``%s`` placeholders.
    :param params: values for the placeholders.
    :param schema: ``pyarrow.Schema`` whose fields are the SELECT columns, in order.
    :returns: an iterator of ``pyarrow.RecordBatch`` (none if there are no rows).
    """
    log.debug("SQL %s %s", sql, params)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        while True:
            chunk = cur.fetchmany(BATCH_ROWS)
            if not chunk:
                return
            # Rows -> columns, then one typed Arrow array per column
            cols = list(zip(*chunk))
            yield pa.RecordBatch.from_arrays(
                [pa.array(c, type=f.type) for c, f in zip(cols, schema)], schema=schema)


def fetch(conn, sql, params, schema):
    """Read the whole result of a SELECT into a pyarrow table with a fixed schema (:func:`stream`).

    :param conn: connection from :func:`connect`.
    :param sql: the SELECT, with ``%s`` placeholders.
    :param params: values for the placeholders.
    :param schema: ``pyarrow.Schema`` whose fields are the SELECT columns, in order.
    :returns: a ``pyarrow.Table`` with that schema (empty if there are no rows).
    """
    batches = list(stream(conn, sql, params, schema))
    if not batches:  # zero-chunk columns segfault some pyarrow kernels (seen in 21.0)
        return schema.empty_table()
    return pa.Table.from_batches(batches, schema=schema)


def mogrify(conn, sql, params):
    """Return a query with its placeholders filled in, as the server would see it (``--dry-run``).

    :param conn: connection from :func:`connect`.
    :param sql: SQL with ``%s`` placeholders.
    :param params: values for the placeholders.
    :returns: the SQL text.
    """
    with conn.cursor() as cur:
        return cur.mogrify(sql, params)


def id_step(conn):
    """Return the step between consecutive auto-increment ids.

    EAR computes the ids of rows it inserts together as ``first + i * step``.
    On a Galera cluster the step is greater than 1.

    :param conn: connection from :func:`connect`.
    :returns: the server's ``@@auto_increment_increment``.
    """
    return int(scalar(conn, "SELECT @@auto_increment_increment"))


def explain(conn, sql, params):
    """Summarise the ``EXPLAIN`` of a query, to check index use (``--dry-run``).

    :param conn: connection from :func:`connect`.
    :param sql: the query to explain, with ``%s`` placeholders.
    :param params: values for the placeholders.
    :returns: one line per table: ``"<table>: type=<access> key=<index> rows~<estimate>"``.
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("EXPLAIN " + sql, params)
        return ["%s: type=%s key=%s rows~%s" % (r["table"], r["type"], r["key"], r["rows"])
                for r in cur.fetchall()]


def live_columns(conn, database):
    """Read the tables and columns that exist in a database.

    :param conn: connection from :func:`connect`.
    :param database: database (schema) name.
    :returns: ``{table: {column: DATA_TYPE}}`` from ``information_schema.COLUMNS``,
        with physical names, for example ``{"Jobs": {"job_id": "int", ...}}``.
    """
    out = {}
    for table, col, dtype in rows(conn, "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                                        "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s",
                                  (database,)):
        out.setdefault(table, {})[col] = dtype
    return out


def columns(conn, database, table):
    """Read the columns of one table, in table order.

    :param conn: connection from :func:`connect`.
    :param database: database (schema) name.
    :param table: physical table name.
    :returns: ``[(column, COLUMN_TYPE, in primary key)]``, for example
        ``[("id", "bigint(20) unsigned", True), ...]``; empty if there is no such table.
    """
    return [(c, t, k == "PRI") for c, t, k in rows(
        conn, "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY FROM information_schema.COLUMNS "
              "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
        (database, table))]


def arrow_type(column_type):
    """Return the Arrow type that holds every value of a column exactly.

    :param column_type: the column's full type (``COLUMN_TYPE``), for example
        ``int(10) unsigned`` (→ ``uint32``) or ``float`` (→ ``float32``).
    :returns: a ``pyarrow.DataType``.
    :raises ValueError: for a type EAR's schema does not use.
    """
    base = column_type.split("(")[0].split()[0].lower()
    if base in INT_BITS:
        return getattr(pa, "%sint%d" % ("u" if "unsigned" in column_type else "",
                                        INT_BITS[base]))()
    if base in ARROW_TYPES:
        return ARROW_TYPES[base]
    raise ValueError("unsupported column type %r" % column_type)


def show_create(conn, table):
    """Return the ``CREATE TABLE`` statement of a table, as the server prints it.

    :param conn: connection from :func:`connect`.
    :param table: physical table name.
    :returns: the statement, without a trailing ``;``.
    """
    return rows(conn, "SHOW CREATE TABLE %s" % quote(table))[0][1]
