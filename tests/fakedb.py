"""A fake EAR database on SQLite (stdlib): EAR's tables from a profile's DDL, filled with fakedata.

``FakeEngine`` has the functions of :mod:`ear_db_export.read.sql.mariadb` that
the sources use, and runs their SQL for real (SQLite accepts the backtick
quoting, ``LIMIT``, ``BETWEEN`` and ``IN`` they generate), so paging, id
windows and filters are tested end to end without a server.
"""
import sqlite3

import pyarrow as pa

from ear_db_export.config import DbConf
from ear_db_export.read.sql import mariadb
from ear_db_export.read.sql.profile import ddl_schema, load_profile

import fakedata

DBCONF = DbConf("fake", 3306, "EAR", "reader", "pw")


class FakeEngine:
    """An engine "module" over an in-memory SQLite database with one EAR version's tables."""

    TYPE_FAMILY = mariadb.TYPE_FAMILY
    quote = staticmethod(mariadb.quote)
    arrow_type = staticmethod(mariadb.arrow_type)

    def __init__(self, profile="ear6", step=1):
        self.__name__ = "fake"
        self.profile, self.step, self.queries = profile, step, []
        self.ddl = ddl_schema(profile)
        self.db = sqlite3.connect(":memory:")
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

    # -- the mariadb functions -------------------------------------------------

    def connect(self, db):
        return self.db

    def live_columns(self, conn, database):
        return {t: {c: typ.split("(")[0].split()[0] for c, typ, _, _ in cols}
                for t, cols in self.ddl.items()}

    def columns(self, conn, database, table):
        return [(c, typ, pk) for c, typ, _, pk in self.ddl.get(table, [])]

    def _run(self, sql, params=()):
        self.queries.append((sql, list(params)))
        return self.db.execute(sql.replace("%s", "?"), list(params))

    def rows(self, conn, sql, params=()):
        return tuple(self._run(sql, params).fetchall())

    def scalar(self, conn, sql, params=()):
        if sql == "SELECT VERSION()":
            return "fake-1.0"
        r = self.rows(conn, sql, params)
        return r[0][0] if r else None

    def stream(self, conn, sql, params, schema):
        cur = self._run(sql, params)
        while True:
            chunk = cur.fetchmany(2)  # small batches: exercise multi-batch results
            if not chunk:
                return
            cols = list(zip(*chunk))
            yield pa.RecordBatch.from_arrays([pa.array(c, type=f.type) for c, f in zip(cols, schema)],
                                             schema=schema)

    def fetch(self, conn, sql, params, schema):
        batches = list(self.stream(conn, sql, params, schema))
        return pa.Table.from_batches(batches, schema=schema) if batches else schema.empty_table()

    def mogrify(self, conn, sql, params):
        return sql.replace("%s", "{}").format(*[repr(p) for p in params])

    def explain(self, conn, sql, params):
        return ["fake: type=ALL key=None rows~0"]

    def id_step(self, conn):
        return self.step

    def show_create(self, conn, table):
        return "CREATE TABLE `%s` (...)" % table

    def read_only(self, conn):
        return True

    def grants(self, conn):
        return ["GRANT SELECT ON `EAR`.* TO `reader`@`%`"]


def physical_tables(profile="ear6"):
    """The fakedata records as one EAR version stores them: ``{physical table: pyarrow.Table}``."""
    p = load_profile(profile)
    ddl = ddl_schema(profile)
    out = {}
    for logical, t in fakedata.tables().items():
        phys = p.physical(logical, t)
        schema = pa.schema([(c, mariadb.arrow_type(typ)) for c, typ, _, _ in ddl[p.tables[logical]]
                            if c in phys.column_names])
        out[p.tables[logical]] = phys.select(schema.names).cast(schema)
    return out


def make_engine(profile="ear6", step=fakedata.ID_STEP):
    """A FakeEngine holding the fakedata records."""
    eng = FakeEngine(profile, step)
    for table, t in physical_tables(profile).items():
        eng.insert(table, t)
    return eng
