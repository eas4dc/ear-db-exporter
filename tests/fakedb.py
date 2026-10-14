"""A fake EAR database on SQLite (stdlib): EAR's tables from a profile's DDL, filled with fakedata.

``FakeEngine`` has the functions of :mod:`ear_db_export.read.sql.mariadb` that
the sources use, and runs their SQL for real (SQLite accepts the backtick
quoting, ``LIMIT``, ``BETWEEN`` and ``IN`` they generate), so paging, id
windows and filters are tested end to end without a server.
"""
import itertools
import sqlite3

import pyarrow as pa

from ear_db_export.config import DbConf
from ear_db_export.read.sql import mariadb
from ear_db_export.read.sql.profile import ddl_schema, load_profile

import fakedata

DBCONF = DbConf("fake", 3306, "EAR", "reader", "pw")
_DATABASES = itertools.count()  # a fresh in-memory database name per engine


class FakeEngine:
    """An engine "module" over an in-memory SQLite database with one EAR version's tables."""

    TYPE_FAMILY = mariadb.TYPE_FAMILY
    quote = staticmethod(mariadb.quote)
    arrow_type = staticmethod(mariadb.arrow_type)

    def __init__(self, profile="ear6", step=1):
        """Create the tables of one EAR version (``profile``) in a fresh in-memory database."""
        self.__name__ = "fake"
        self.profile, self.step, self.queries = profile, step, []
        #: Connections with a streamed result not read to the end (see :meth:`_run`).
        self.streaming = set()
        self.ddl = ddl_schema(profile)
        # One shared in-memory database: each connect() is a new connection to it (sources
        # close theirs), and self.db keeps it alive
        self.uri = "file:ear%d?mode=memory&cache=shared" % next(_DATABASES)
        self.db = sqlite3.connect(self.uri, uri=True)
        for table, cols in self.ddl.items():
            self.db.execute("CREATE TABLE `%s` (%s)" % (table, ", ".join("`%s`" % c[0] for c in cols)))

    def schema(self, table):
        """The exact Arrow schema of a physical table."""
        return pa.schema([(c, mariadb.arrow_type(t)) for c, t, _, _ in self.ddl[table]])

    def insert(self, table, t):
        """Insert a pyarrow table (physical names) into a physical table."""
        cols = t.column_names
        self.db.executemany("INSERT INTO `%s` (%s) VALUES (%s)" % (
            table, ", ".join("`%s`" % c for c in cols), ", ".join("?" * len(cols))),
            [tuple(r[c] for c in cols) for r in t.to_pylist()])
        self.db.commit()

    # -- the mariadb functions -------------------------------------------------

    def connect(self, db):
        """Open a new connection to the shared in-memory database (as mariadb.connect)."""
        return sqlite3.connect(self.uri, uri=True)

    def live_columns(self, conn, database):
        """Return ``{table: {column: DATA_TYPE}}`` from the profile's DDL (as mariadb.live_columns)."""
        return {t: {c: typ.split("(")[0].split()[0] for c, typ, _, _ in cols}
                for t, cols in self.ddl.items()}

    def columns(self, conn, database, table):
        """Return ``[(column, COLUMN_TYPE, in pk)]`` of a table from the DDL (as mariadb.columns)."""
        return [(c, typ, pk) for c, typ, _, pk in self.ddl.get(table, [])]

    def _run(self, conn, sql, params=()):
        """Run a query (``%s`` placeholders turned into SQLite's ``?``) and record it in ``self.queries``.

        Like MariaDB's unbuffered results (pymysql drops the unread rows of a
        streamed result when its connection runs another query), a query on a
        connection whose :meth:`stream` is still open fails.
        """
        if conn in self.streaming:
            raise AssertionError("query on a connection with an unread streamed result: "
                                 "MariaDB would drop its rows")
        self.queries.append((sql, list(params)))
        return conn.execute(sql.replace("%s", "?"), list(params))

    def rows(self, conn, sql, params=()):
        """Run a query and return all its rows (as mariadb.rows)."""
        return tuple(self._run(conn, sql, params).fetchall())

    def scalar(self, conn, sql, params=()):
        """Return the first column of the first row; ``SELECT VERSION()`` answers ``fake-1.0``."""
        if sql == "SELECT VERSION()":
            return "fake-1.0"
        r = self.rows(conn, sql, params)
        return r[0][0] if r else None

    def stream(self, conn, sql, params, schema):
        """Yield the result as record batches of 2 rows, to exercise multi-batch reads (as mariadb.stream).

        The connection takes no other query until the result is read to the end.
        """
        cur = self._run(conn, sql, params)
        self.streaming.add(conn)
        try:
            while True:
                chunk = cur.fetchmany(2)  # small batches: exercise multi-batch results
                if not chunk:
                    return
                cols = list(zip(*chunk))
                yield pa.RecordBatch.from_arrays(
                    [pa.array(c, type=f.type) for c, f in zip(cols, schema)], schema=schema)
        finally:
            self.streaming.discard(conn)

    def fetch(self, conn, sql, params, schema):
        """Return the whole result as one table (as mariadb.fetch)."""
        batches = list(self.stream(conn, sql, params, schema))
        return pa.Table.from_batches(batches, schema=schema) if batches else schema.empty_table()

    def mogrify(self, conn, sql, params):
        """Fill the placeholders with the values' repr, for dry-run logs (as mariadb.mogrify)."""
        return sql.replace("%s", "{}").format(*[repr(p) for p in params])

    def explain(self, conn, sql, params):
        """Return a fixed EXPLAIN line (SQLite's EXPLAIN differs)."""
        return ["fake: type=ALL key=None rows~0"]

    def id_step(self, conn):
        """Return the auto-increment step the engine was made with."""
        return self.step

    def show_create(self, conn, table):
        """Return a stand-in CREATE TABLE statement."""
        return "CREATE TABLE `%s` (...)" % table

    def read_only(self, conn):
        """Report the session as read-only."""
        return True

    def grants(self, conn):
        """Return the grants of a read-only account."""
        return ["GRANT SELECT ON `EAR`.* TO `reader`@`%`"]


def physical_tables(profile="ear6"):
    """The fakedata records as one EAR version stores them: ``{physical table: pyarrow.Table}``."""
    p = load_profile(profile)
    ddl = ddl_schema(profile)
    out = {}
    for logical, t in fakedata.tables().items():
        mapped = p.to_physical(logical, t.select([c for c in t.column_names if p.col(logical, c)]))
        schema = pa.schema([(c, mariadb.arrow_type(typ)) for c, typ, _, _ in ddl[p.tables[logical]]
                            if c in mapped.column_names])
        out[p.tables[logical]] = mapped.select(schema.names).cast(schema)
    return out


def make_engine(profile="ear6", step=fakedata.ID_STEP):
    """A FakeEngine holding the fakedata records."""
    eng = FakeEngine(profile, step)
    for table, t in physical_tables(profile).items():
        eng.insert(table, t)
    return eng
