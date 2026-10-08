"""Dump EAR's database as it is stored: every table, every column, physical names and exact types.

Unlike the export commands, a dump needs no schema profile, so it works on any
EAR version. It writes into the output directory:

- ``<Table>.parquet``: the rows of each table, in primary-key order.
- ``schema.sql``: the ``CREATE TABLE`` statement of each table.
- ``manifest.json``: what was dumped (database, server version, start and end
  time, rows per table, whether user fields were pseudonymised). It is written
  last, so its presence marks a complete dump.

A table with a single integer primary key is read in pages of
:data:`PAGE_ROWS` rows (``WHERE id > last ORDER BY id LIMIT n``), so no query
keeps a long read open on a production server; other tables are read with one
streamed SELECT. Tables are read one after the other while EAR keeps writing,
so the dump is not a transactional snapshot: rows inserted during the dump may
or may not be in it.
"""
import datetime as dt
import json
import logging
import os
from importlib.metadata import PackageNotFoundError, version

import pyarrow as pa

from . import write
from .canonical import pseudonymize
from .config import ConfigError

log = logging.getLogger(__name__)

#: Rows per primary-key page.
PAGE_ROWS = 1_000_000
#: Columns that hold a user name in some EAR version, pseudonymised in every table that has them.
USER_COLUMNS = ("user_id", "user_name", "user_acc", "user_group", "pod_user_id")


def dump(conn, engine, database, tables, out, key, dry_run=False):
    """Write the given tables, their ``CREATE TABLE`` statements and a manifest to ``out``.

    :param conn: an open connection (read-only is enough).
    :param engine: the engine module, :mod:`~ear_db_export.read.sql.mariadb`.
    :param database: database name.
    :param tables: physical table names.
    :param out: output directory.
    :param key: pseudonymisation key for :data:`USER_COLUMNS`, or ``None`` to keep them in clear.
    :param dry_run: log each table's first query and its ``EXPLAIN``; write nothing.
    :returns: the manifest, a dict.
    :raises ConfigError: if a table does not exist or has a column type
        :func:`~ear_db_export.read.sql.mariadb.arrow_type` does not support.
    """
    manifest = {"tool": "ear-db-export " + _version(), "database": database,
                "server": engine.scalar(conn, "SELECT VERSION()"), "started": _now(),
                "pseudonymized": key is not None, "tables": {}}
    ddl = []
    for table in tables:
        cols = engine.columns(conn, database, table)
        if not cols:
            raise ConfigError("no table %s in %s" % (table, database))
        schema = _schema(engine, table, cols)
        first, nxt, pk = _queries(engine, table, cols, schema)
        if dry_run:
            log.info("[dry-run] %s SQL: %s%s", table, first,
                     "" if nxt is None else " (then pages of %d by %s)" % (PAGE_ROWS, pk))
            for line in engine.explain(conn, first, ()):
                log.info("[dry-run] %s EXPLAIN %s", table, line)
            continue
        ddl.append(engine.show_create(conn, table) + ";\n")
        pages = (pseudonymize(t, key, USER_COLUMNS)
                 for t in _pages(conn, engine, schema, first, nxt, pk))
        rows, nbytes = write.parquet(os.path.join(out, table + ".parquet"), pages)
        log.info("wrote %s: %d rows, %d bytes", table, rows, nbytes)
        manifest["tables"][table] = {"file": table + ".parquet", "rows": rows, "bytes": nbytes}
    if not dry_run:
        manifest["finished"] = _now()
        _write_text(os.path.join(out, "schema.sql"), "\n".join(ddl))
        _write_text(os.path.join(out, "manifest.json"), json.dumps(manifest, indent=2) + "\n")
    return manifest


def _schema(engine, table, cols):
    """Return the Arrow schema of a table from its ``(column, COLUMN_TYPE, in pk)`` list."""
    fields = []
    for name, ctype, _ in cols:
        try:
            fields.append((name, engine.arrow_type(ctype)))
        except ValueError as e:
            raise ConfigError("%s.%s: %s" % (table, name, e))
    return pa.schema(fields)


def _queries(engine, table, cols, schema):
    """Return ``(first, next, key)``: the SQL of the first page, of the following
    pages (``None`` when the table is read with one SELECT), and the paging column."""
    base = "SELECT %s FROM %s" % (", ".join(engine.quote(c) for c, _, _ in cols),
                                  engine.quote(table))
    pk = [c for c, _, in_pk in cols if in_pk]
    if len(pk) != 1 or not pa.types.is_integer(schema.field(pk[0]).type):
        return base, None, None
    k = engine.quote(pk[0])
    return ("%s ORDER BY %s LIMIT %d" % (base, k, PAGE_ROWS),
            "%s WHERE %s > %%s ORDER BY %s LIMIT %d" % (base, k, k, PAGE_ROWS), pk[0])


def _pages(conn, engine, schema, first, nxt, key):
    """Yield a table's rows as pyarrow tables (at least one, maybe empty): page by page, or streamed."""
    if nxt is None:
        empty = True
        for batch in engine.stream(conn, first, (), schema):
            empty = False
            yield pa.Table.from_batches([batch])
        if empty:
            yield schema.empty_table()
        return
    page = engine.fetch(conn, first, (), schema)
    while True:
        yield page
        if page.num_rows < PAGE_ROWS:
            return
        page = engine.fetch(conn, nxt, (page[key][-1].as_py(),), schema)


def _write_text(path, text):
    """Write a text file atomically: ``.tmp`` + rename."""
    with open(path + ".tmp", "w") as f:
        f.write(text)
    os.replace(path + ".tmp", path)


def _now():
    """Return the current UTC time as ISO 8601, to the second."""
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _version():
    """Return the installed version of this package."""
    try:
        return version("ear-db-export")
    except PackageNotFoundError:
        return "unknown"
