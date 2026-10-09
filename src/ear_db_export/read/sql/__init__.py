"""Source of EAR's SQL database (MariaDB/MySQL now; EAR's PostgreSQL schema has the same names).

:class:`SqlSource` reads it, read-only. It is made of three parts, so that
only one of them knows the engine:

- :mod:`~ear_db_export.read.sql.profile`: which physical names this EAR
  version uses (``Jobs.app_id`` for the logical ``jobs.app_name``).
- :mod:`~ear_db_export.read.sql.queries`: the SQL text of filters and SELECTs.
- :mod:`~ear_db_export.read.sql.mariadb`: the engine: connection, quoting,
  ``EXPLAIN``, ``information_schema``, the auto-increment step.

``Periodic_metrics`` (and ``Events``, ``Periodic_aggregations``) may have no
time index: a time window is found by binary search on the primary key
(:func:`~ear_db_export.read.sql.queries.bisect_id`).
"""
import datetime as dt
import logging
from functools import cached_property

import pyarrow as pa
import pyarrow.compute as pc

from ...config import ConfigError
from .. import TIME_COLUMNS
from . import mariadb, queries
from .profile import detect, diff, load_profile
from .queries import IN_BATCH, bisect_id

log = logging.getLogger(__name__)

# Periodic_metrics.end_time may have no index, so each window is read through a
# PK id window. bisect_id() returns rows that really end before/after the margins;
# a row is only missed if EARDBD inserted it more than SLACK after its
# end_time. Measured on a test cluster: worst out-of-order lag 210 s.
#: Margin, in seconds, around a window when searching its id range.
SLACK = 86400  # ponytail: fixed 1-day margin; make it a flag if a site shows longer insert delays


#: Rows per primary-key page of a scan.
PAGE_ROWS = 1_000_000


class SqlSource:
    """Read EAR's SQL database, read-only: tables as stored (physical names, exact types).

    Opening it connects, reads which tables and columns exist, and chooses the
    schema profile (``profile``, or autodetected) that maps them to logical
    names. Without a matching profile it can still read every table as it is
    (``need_profile=False``), but not filter.

    Every read is one of three shapes, so no query is an expensive join:

    - a table with a single integer primary key is read in pages of
      :data:`PAGE_ROWS` rows in key order (``WHERE id > last ORDER BY id LIMIT n``);
    - the tables of :data:`~ear_db_export.read.TIME_COLUMNS` (no time index in
      EAR's schema), when filtered by time, first map the time window (plus
      :data:`SLACK`) to an id window by binary search on the primary key;
    - other tables (``Jobs``, ``Applications``, ``Loops``, ``Global_energy``)
      are read with one streamed SELECT.

    :param dbconf: a :class:`~ear_db_export.config.DbConf`.
    :param profile: profile name or ``.toml`` path; ``None`` autodetects.
    :param engine: the engine module (:mod:`~ear_db_export.read.sql.mariadb`).
    :param dry_run: log each query with its ``EXPLAIN`` (and row count when
        filtered) instead of reading; reads return empty tables.
    :param need_profile: refuse a database no profile matches.
    :raises ConfigError: if no profile matches (and one is needed), or the profile is invalid.
    """

    def __init__(self, dbconf, profile=None, engine=mariadb, dry_run=False, need_profile=True):
        self.engine, self.dry_run, self.database = engine, dry_run, dbconf.database
        self.conn = engine.connect(dbconf)
        try:
            live = engine.live_columns(self.conn, dbconf.database)
            try:
                chosen = load_profile(profile) if profile else detect(live)
            except ConfigError:
                if need_profile or profile:
                    raise
                chosen = None
            self._check = diff(chosen, live, engine.TYPE_FAMILY) if chosen else \
                (["no schema profile matches %s" % dbconf.database], False)
            self.profile = chosen.bind(live) if chosen else None
        except BaseException:
            self.conn.close()
            raise
        self.provides = frozenset(live)
        self._schemas = {}

    def check(self):
        """Compare the profile with the live database (``check``).

        :returns: ``(lines, ok)`` from :func:`~ear_db_export.read.sql.profile.diff`.
        """
        return self._check

    def close(self):
        """Close the connection."""
        self.conn.close()

    @cached_property
    def id_step(self):
        """The server's auto-increment step (2 or more on Galera), for GPU id ranges."""
        return self.engine.id_step(self.conn)

    def info(self):
        """Describe the source for a bundle manifest.

        :returns: ``{engine, server, database, profile, id_step}``.
        """
        return {"engine": self.engine.__name__.rsplit(".", 1)[-1],
                "server": self.engine.scalar(self.conn, "SELECT VERSION()"),
                "database": self.database,
                "profile": self.profile.name if self.profile else None,
                "id_step": self.id_step}

    def ddl(self, tables):
        """Return the ``CREATE TABLE`` statements of physical tables, as the server prints them."""
        return "".join(self.engine.show_create(self.conn, t) + ";\n" for t in tables)

    def oldest_day(self):
        """Return the day the lowest-id ``Periodic_metrics`` row ends: the prune horizon.

        :returns: a ``datetime.date``, or ``None`` if the table is empty.
        """
        table = self.profile.tables["periodic_metrics"]
        key, tcol = (self.profile.col("periodic_metrics", c) for c in ("id", "end_time"))
        lo = self.engine.scalar(self.conn, "SELECT MIN(%s) FROM %s"
                                % (self.engine.quote(key), self.engine.quote(table)))
        if lo is None:
            return None
        row = self._probe(table, key, tcol)(lo)
        return dt.datetime.fromtimestamp(row[1], dt.timezone.utc).date() if row else None

    def scan(self, table, where=()):
        """Read the rows of a table that match a filter.

        :param table: physical table name.
        :param where: DNF filter on *logical* column names (see
            :mod:`ear_db_export.selection`); empty reads every row.
        :returns: an iterator of ``pyarrow.Table`` with the physical columns and
            exact types; at least one (maybe empty) table, which gives the schema.
        :raises ConfigError: if the table does not exist, has an unsupported
            column type, or a filter needs a profile the source lacks.
        """
        cols, schema = self._columns(table)
        q = self.engine.quote
        conds, params = [], []
        logical = self.profile.table_of(table) if self.profile else None
        if where:
            if logical is None:
                raise ConfigError("cannot filter %s without a schema profile (give --profile)" % table)
            sql, params = queries.where(q, self.profile.where(logical, where))
            conds.append("(%s)" % sql)
        pk = [c for c, _, in_pk in cols if in_pk]
        key = pk[0] if len(pk) == 1 and pa.types.is_integer(schema.field(pk[0]).type) else None
        # Tables without a time index: bound the time window by an id window first
        if key and where and logical in TIME_COLUMNS:
            tcol = self.profile.col(logical, TIME_COLUMNS[logical])
            start, end = _time_bounds(where, TIME_COLUMNS[logical])
            if start is not None or end is not None:
                window = self._id_window(table, key, tcol, start, end)
                if window is None:  # empty table
                    yield schema.empty_table()
                    return
                conds = ["%s BETWEEN %%s AND %%s" % q(key)] + conds
                params = list(window) + params
        names = [c for c, _, _ in cols]
        if key is None:  # one streamed SELECT
            sql = queries.select(q, table, names, conds)
            if self.dry_run:
                yield self._preview(table, sql, params, schema, count=bool(where))
                return
            yield from _at_least_one(self.engine.stream(self.conn, sql, params, schema), schema)
            return
        # Primary-key pages
        first = queries.select(q, table, names, conds, key, PAGE_ROWS)
        if self.dry_run:
            yield self._preview(table, first, params, schema, count=bool(where),
                                note=" (then pages of %d by %s)" % (PAGE_ROWS, key))
            return
        page = self.engine.fetch(self.conn, first, params, schema)
        nxt = queries.select(q, table, names, conds + ["%s > %%s" % q(key)], key, PAGE_ROWS)
        while True:
            yield page
            if page.num_rows < PAGE_ROWS:
                return
            page = self.engine.fetch(self.conn, nxt, params + [page[key][-1].as_py()], schema)

    def lookup(self, table, col, keys):
        """Read the rows whose column is one of ``keys``, in ``IN`` batches of ``IN_BATCH``.

        Only for primary-key or primary-key-prefix columns, so each batch is an
        index lookup. Duplicates and ``None`` are ignored.

        :param table: physical table name.
        :param col: logical column name.
        :param keys: values to look up.
        :returns: an iterator of ``pyarrow.Table`` (physical columns), at least one.
        """
        cols, schema = self._columns(table)
        keys = sorted(set(k for k in keys if k is not None))
        if self.dry_run:
            log.info("[dry-run] %s: primary-key lookups by %s for the rows above, IN batches of %d",
                     table, col, IN_BATCH)
            yield schema.empty_table()
            return
        logical = self.profile.table_of(table)
        q, phys = self.engine.quote, self.profile.col(logical, col)
        names = [c for c, _, _ in cols]
        yielded = False
        for i in range(0, len(keys), IN_BATCH):
            chunk = keys[i:i + IN_BATCH]
            sql = queries.select(q, table, names, ["%s IN (%s)" % (q(phys), ", ".join(["%s"] * len(chunk)))])
            yielded = True
            yield self.engine.fetch(self.conn, sql, chunk, schema)
        if not yielded:
            yield schema.empty_table()

    # -- Helpers ---------------------------------------------------------------

    def _columns(self, table):
        """``([(column, COLUMN_TYPE, in pk)], arrow schema)`` of a physical table (read once)."""
        if table in self._schemas:
            return self._schemas[table]
        cols = self.engine.columns(self.conn, self.database, table)
        if not cols:
            raise ConfigError("no table %s in %s" % (table, self.database))
        fields = []
        for name, ctype, _ in cols:
            try:
                fields.append((name, self.engine.arrow_type(ctype)))
            except ValueError as e:
                raise ConfigError("%s.%s: %s" % (table, name, e))
        self._schemas[table] = cols, pa.schema(fields)
        return self._schemas[table]

    def _probe(self, table, key, tcol):
        """Return ``probe(id)``: ``(id, time)`` of the first row with an id ``>= id`` and a time."""
        q = self.engine.quote
        sql = "SELECT %s, %s FROM %s WHERE %s >= %%s AND %s IS NOT NULL ORDER BY %s LIMIT 1" % (
            q(key), q(tcol), q(table), q(key), q(tcol), q(key))

        def probe(id_):
            r = self.engine.rows(self.conn, sql, (id_,))
            return r[0] if r else None
        return probe

    def _id_window(self, table, key, tcol, start, end):
        """Map a time window to the ids of the rows around it (plus :data:`SLACK`), by binary search.

        :returns: ``(first id, last id)``, or ``None`` if the table is empty.
        """
        q = self.engine.quote
        lo, hi = self.engine.rows(self.conn, "SELECT MIN(%s), MAX(%s) FROM %s"
                                  % (q(key), q(key), q(table)))[0]
        if lo is None:
            return None
        probe = self._probe(table, key, tcol)
        if probe(lo) is None:  # no row has a time: read them all
            return lo, hi
        a = bisect_id(probe, start - SLACK, lo, hi)[0] if start is not None else None
        b = bisect_id(probe, end + SLACK, lo, hi)[1] if end is not None else None
        return (lo if a is None else a), (hi if b is None else b)

    def _preview(self, table, sql, params, schema, count, note=""):
        """Log a query, its EXPLAIN and (if filtered) its row count instead of running it."""
        log.info("[dry-run] %s SQL: %s%s", table, self.engine.mogrify(self.conn, sql, params), note)
        for line in self.engine.explain(self.conn, sql, params):
            log.info("[dry-run] %s EXPLAIN %s", table, line)
        if count:
            base = sql.split(" ORDER BY ")[0]
            n = self.engine.scalar(self.conn, "SELECT COUNT(*) FROM (%s) t" % base, params)
            log.info("[dry-run] %s rows: %d", table, n)
        return schema.empty_table()


def _time_bounds(dnf, col):
    """Return ``(start, end)``: the widest ``col >= start`` / ``col < end`` bounds of every
    alternative of a DNF filter; a side is ``None`` if some alternative leaves it open."""
    if dnf and isinstance(dnf[0], tuple):
        dnf = [dnf]
    starts, ends = [], []
    for conj in dnf:
        starts.append(next((v for c, op, v in conj if c == col and op == ">="), None))
        ends.append(next((v for c, op, v in conj if c == col and op == "<"), None))
    return (None if None in starts else min(starts)), (None if None in ends else max(ends))


def _at_least_one(tables, schema):
    """Yield the record batches of ``tables`` as tables, or one empty table if there is none."""
    empty = True
    for batch in tables:
        empty = False
        yield pa.Table.from_batches([batch])
    if empty:
        yield schema.empty_table()
