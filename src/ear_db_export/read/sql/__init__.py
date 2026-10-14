"""Read EAR's SQL database (MariaDB or MySQL), read-only.

:class:`SqlSource` reads it, read-only. Three modules make it, so that only one
of them knows the engine:

- :mod:`~ear_db_export.read.sql.profile`: which physical names this EAR
  version uses (``Jobs.app_id`` for the logical ``jobs.app_name``).
- :mod:`~ear_db_export.read.sql.queries`: the SQL text of filters and SELECTs.
- :mod:`~ear_db_export.read.sql.mariadb`: the engine: connection, quoting,
  ``EXPLAIN``, ``information_schema``, the auto-increment step.

How each read method queries the database (every query reads one table):

- ``scan``: a table with a single integer primary key is read in pages of
  :data:`PAGE_ROWS` rows in key order (``WHERE id > last ORDER BY id LIMIT n``).
  Other tables (``Jobs``, ``Applications``, ``Loops``, ``Global_energy``), and any
  read with ``order`` or ``limit``, are read with one streamed ``SELECT``.
- ``window``: ``Periodic_metrics``, ``Events`` and ``Periodic_aggregations`` may have
  no time index, so the window (widened by :data:`SLACK` on each side) is first
  mapped to an id range by binary search on the primary key
  (:func:`~ear_db_export.read.sql.queries.bisect_id`); the range is then read in
  pages, with the exact time condition in the ``WHERE``. ``Global_energy``'s time
  is its primary key, so its window is a plain condition.
- ``lookup``: ``WHERE column IN (...)``, :data:`~ear_db_export.read.sql.queries.IN_BATCH`
  keys per query.
- ``dump``: like ``scan`` without a filter, with physical names; it needs no profile.

A streamed ``SELECT`` runs on a second read-only connection: MariaDB sends its
rows as they are read, and its connection can run nothing else until the last
one (pymysql would drop the unread rows). Everything else runs on the first
connection, so a job selection can look up each chunk's rows while the
``Jobs`` (or ``Loops``) stream is open.
"""
import logging
from functools import cached_property

import pyarrow as pa

from ...config import ConfigError
from .. import TIME_COLUMNS, time_conds
from . import mariadb, queries
from .profile import detect, diff, load_profile
from .queries import IN_BATCH, bisect_id

log = logging.getLogger(__name__)

# Periodic_metrics.end_time may have no index, so a time window is read through
# a primary-key id range. bisect_id() returns rows that really end before/after
# the margins; a row is only missed if EARDBD inserted it more than SLACK after
# its end_time. Measured on a test cluster: worst out-of-order lag 210 s. The
# margin also covers an EARDBD outage (EARD keeps the data and sends it later).
#: Margin, in seconds, around a window when searching its id range.
SLACK = 86400  # ponytail: fixed 1-day margin; make it a flag if a site shows longer insert delays

#: Rows per primary-key page of a scan.
PAGE_ROWS = 1_000_000


class SqlSource:
    """Read EAR's SQL database, read-only.

    Opening it connects, reads which tables and columns exist, and chooses the
    schema profile (``profile``, or autodetected) that maps them to logical
    names. Without a matching profile it can still :meth:`dump` every table as
    stored, but the logical reads (:meth:`scan`, :meth:`window`, :meth:`lookup`)
    refuse to run.

    :param dbconf: a :class:`~ear_db_export.config.DbConf`.
    :param profile: profile name or ``.toml`` path; ``None`` autodetects.
    :param engine: the engine module (:mod:`~ear_db_export.read.sql.mariadb`).
    :param dry_run: log each query with its ``EXPLAIN`` (and row count when
        filtered) instead of reading; reads yield empty tables.
    :param tags: ``{node: tag}`` from ear.conf, for ``--tags`` and ``node_tag``.
    :raises ConfigError: if ``profile`` is given and invalid, or names columns
        the database has under other names (see :meth:`Profile.bind`).

    Example::

        src = SqlSource(config.load("/etc/ear/ear.conf")[0])
        for jobs in src.scan("jobs", [Cond("user_id", "in", ["alice"])]):
            print(jobs.num_rows)      # logical names: jobs["user_id"], even on EAR 7 (user_name)
        src.close()
    """

    #: The database holds user names in clear (a bundle may hold pseudonyms).
    salt_fingerprint = None

    def __init__(self, dbconf, profile=None, engine=mariadb, dry_run=False, tags=None):
        """Connect read-only, read the live columns, and choose and check the schema profile."""
        self.engine, self.dry_run, self.database = engine, dry_run, dbconf.database
        self.tags = dict(tags or {})
        self._dbconf, self._stream_conn = dbconf, None
        self.conn = engine.connect(dbconf)
        try:
            live = engine.live_columns(self.conn, dbconf.database)
            chosen, why = None, None
            if profile:
                chosen = load_profile(profile)
            else:
                try:
                    chosen = detect(live)
                except ConfigError as e:
                    why = str(e)
            self._check = diff(chosen, live, engine.TYPE_FAMILY) if chosen else ([why], False)
            self.profile = chosen.bind(live) if chosen and self._check[1] else chosen
        except BaseException:
            self.conn.close()
            raise
        self.provides = frozenset(live)
        self._schemas = {}

    def check(self):
        """Compare the profile with the live database (``check``).

        :returns: ``(lines, ok)`` from :func:`~ear_db_export.read.sql.profile.diff`,
            or a one-line explanation and ``False`` when no profile matched.
        """
        return self._check

    def has(self, table):
        """Tell whether the database has a logical table (through the profile)."""
        return self.profile is not None and table in self.profile.tables

    def close(self):
        """Close the connections."""
        if self._stream_conn is not None:
            self._stream_conn.close()
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

    # -- The four access paths -------------------------------------------------

    def dump(self, table):
        """Read a physical table whole, as stored: physical names, exact types. Needs no profile.

        :param table: physical table name, one of :attr:`provides`.
        :returns: an iterator of ``pyarrow.Table``, at least one.
        :raises ConfigError: if the database has no such table.
        """
        if table not in self.provides:
            raise ConfigError("no table %s in %s" % (table, self.database))
        yield from self._read(table)

    def scan(self, table, where=(), order=None, limit=None):
        """Read the rows of a table that match a filter.

        :param table: logical table name.
        :param where: a list of :class:`~ear_db_export.read.Cond` on logical columns.
        :param order: ``[(logical column, "ascending" | "descending")]``, or ``None``.
        :param limit: at most this many rows (after ``order``), or ``None``.
        :returns: an iterator of ``pyarrow.Table`` with logical column names and
            the types as stored; at least one (maybe empty) table.
        :raises ConfigError: without a matching profile, or for a column the
            database lacks.

        Example, the 10 newest job steps of alice::

            src.scan("jobs", [Cond("user_id", "in", ["alice"])],
                     order=[("end_time", "descending")], limit=10)
        """
        physical = self._physical(table)
        sql, params = self._where(table, where)
        order = [(self.profile.col(table, c), d) for c, d in order or ()]
        for t in self._read(physical, sql, params, order=order, limit=limit, count=bool(where)):
            yield self.profile.to_logical(table, t)

    def window(self, table, start, end, where=()):
        """Read the rows of a time-series table whose time is in ``[start, end)``.

        The time column is :data:`~ear_db_export.read.TIME_COLUMNS` ``[table]``.
        On a table with a single integer primary key the window is first mapped
        to an id range by binary search (:meth:`_id_window`), so the read costs
        the window's rows, not the table's, with or without a time index.

        :param table: logical name of a time-series table (a key of ``TIME_COLUMNS``).
        :param start: Unix seconds (included), or ``None``.
        :param end: Unix seconds (excluded), or ``None``.
        :param where: more conditions (logical columns), for example on ``node_id``.
        :returns: an iterator of ``pyarrow.Table`` (logical names, stored types), at least one.

        Example, the node metrics of n1 on 2026-07-04::

            src.window("periodic_metrics", 1783123200, 1783209600,
                       [Cond("node_id", "in", ["n1"])])
            # SELECT ... FROM Periodic_metrics WHERE id BETWEEN <a> AND <b>
            #   AND (end_time >= 1783123200 AND end_time < 1783209600 AND node_id IN ('n1'))
            #   ORDER BY id LIMIT 1000000, then the next pages
        """
        physical = self._physical(table)
        sql, params = self._where(table, time_conds(table, start, end) + list(where))
        ids = self._id_range(physical, table, start, end)
        for t in self._read(physical, sql, params, ids=ids, count=True):
            yield self.profile.to_logical(table, t)

    def lookup(self, table, column, keys):
        """Read the rows whose column is one of ``keys``, in ``IN`` batches of ``IN_BATCH``.

        Only for primary-key or primary-key-prefix columns, so each batch is an
        index lookup. Duplicates and ``None`` are ignored.

        :param table: logical table name.
        :param column: logical column name.
        :param keys: values to look up.
        :returns: an iterator of ``pyarrow.Table`` (logical names, stored types), at least one.

        Example::

            src.lookup("signatures", "id", [100, 101])
            # SELECT ... FROM Signatures WHERE id IN (100, 101)
        """
        physical = self._physical(table)
        cols, schema = self._columns(physical)
        keys = sorted(set(k for k in keys if k is not None))
        if self.dry_run:
            log.info("[dry-run] %s: primary-key lookups by %s for the rows above, IN batches of %d",
                     physical, column, IN_BATCH)
            yield self.profile.to_logical(table, schema.empty_table())
            return
        q, key = self.engine.quote, self.profile.col(table, column)
        names = [c for c, _, _ in cols]
        if not keys:
            yield self.profile.to_logical(table, schema.empty_table())
        for i in range(0, len(keys), IN_BATCH):
            chunk = keys[i:i + IN_BATCH]
            sql = queries.select(q, physical, names,
                                 ["%s IN (%s)" % (q(key), ", ".join(["%s"] * len(chunk)))])
            yield self.profile.to_logical(table, self.engine.fetch(self.conn, sql, chunk, schema))

    # -- Helpers ---------------------------------------------------------------

    def _physical(self, table):
        """Return the physical name of a logical table, refusing a missing or mismatched profile.

        :raises ConfigError: if no profile matched the database, if the profile
            does not match it (see ``check``), or if the database lacks the table.
        """
        lines, ok = self._check
        if self.profile is None:
            raise ConfigError("%s (reading by logical names needs a schema profile)" % lines[0])
        if not ok:
            raise ConfigError("schema does not match profile %s:\n%s"
                              % (self.profile.name, "\n".join(lines)))
        if table not in self.profile.tables:
            raise ConfigError("no %s table in %s (profile %s)"
                              % (table, self.database, self.profile.name))
        return self.profile.tables[table]

    def _where(self, table, conds):
        """Render conditions on logical columns of a table as SQL on physical ones: ``(sql, params)``."""
        return queries.where(self.engine.quote, self.profile.where(table, conds))

    def _columns(self, table):
        """Return ``([(column, COLUMN_TYPE, in pk)], arrow schema)`` of a physical table (read once)."""
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

    def _int_key(self, table):
        """Return the name of a physical table's primary key if it is a single integer column, else ``None``."""
        cols, schema = self._columns(table)
        pk = [c for c, _, in_pk in cols if in_pk]
        return pk[0] if len(pk) == 1 and pa.types.is_integer(schema.field(pk[0]).type) else None

    def _read(self, table, sql="", params=(), order=None, limit=None, ids=None, count=False):
        """Run the SELECT of a physical table and yield its rows as tables, at least one.

        :param table: physical table name.
        :param sql: a condition on physical columns (from :func:`queries.where`), or ``""``.
        :param params: its placeholder values.
        :param order: ``[(physical column, direction)]``, or ``None``.
        :param limit: row limit, or ``None``.
        :param ids: ``(first id, last id)`` to read only that primary-key range, or ``None``.
        :param count: in a dry run, also log the number of matching rows.

        Without ``order`` and ``limit``, a table with a single integer primary key
        is read in pages of :data:`PAGE_ROWS` rows in key order; anything else
        is one streamed ``SELECT``.
        """
        cols, schema = self._columns(table)
        q, key = self.engine.quote, self._int_key(table)
        conds, params = (["(%s)" % sql] if sql else []), list(params)
        if ids is not None:
            conds.insert(0, "%s BETWEEN %%s AND %%s" % q(key))
            params = list(ids) + params
        names = [c for c, _, _ in cols]
        if key is None or order or limit:  # one streamed SELECT
            sql = queries.select(q, table, names, conds, order, limit)
            if self.dry_run:
                yield self._preview(table, sql, params, schema, count)
                return
            if self._stream_conn is None:  # opened on the first stream; see the module docstring
                self._stream_conn = self.engine.connect(self._dbconf)
            yield from _at_least_one(self.engine.stream(self._stream_conn, sql, params, schema),
                                     schema)
            return
        # Primary-key pages: the first, then each one after the last key read
        first = queries.select(q, table, names, conds, [(key, "ascending")], PAGE_ROWS)
        if self.dry_run:
            yield self._preview(table, first, params, schema, count,
                                note=" (then pages of %d by %s)" % (PAGE_ROWS, key))
            return
        page = self.engine.fetch(self.conn, first, params, schema)
        nxt = queries.select(q, table, names, conds + ["%s > %%s" % q(key)],
                             [(key, "ascending")], PAGE_ROWS)
        while True:
            yield page
            if page.num_rows < PAGE_ROWS:
                return
            page = self.engine.fetch(self.conn, nxt, params + [page[key][-1].as_py()], schema)

    def _id_range(self, table, logical, start, end):
        """Return the primary-key range to read for a time window, or ``None`` to read without one.

        ``None`` when the window has no bound, or when the table has no single
        integer primary key (``Global_energy``, whose time is its key).
        """
        key = self._int_key(table)
        if key is None or (start is None and end is None):
            return None
        return self._id_window(table, key, self.profile.col(logical, TIME_COLUMNS[logical]),
                               start, end)

    def _probe(self, table, key, tcol):
        """Return ``probe(id)``: ``(id, time)`` of the first row with an id ``>= id`` and a time.

        One primary-key lookup per call, the step of :func:`queries.bisect_id`.
        """
        q = self.engine.quote
        sql = "SELECT %s, %s FROM %s WHERE %s >= %%s AND %s IS NOT NULL ORDER BY %s LIMIT 1" % (
            q(key), q(tcol), q(table), q(key), q(tcol), q(key))

        def probe(id_):
            """Return ``(id, time)`` of the first row from ``id_`` on, or ``None`` past the last row."""
            r = self.engine.rows(self.conn, sql, (id_,))
            return r[0] if r else None
        return probe

    def _id_window(self, table, key, tcol, start, end):
        """Map a time window to the ids of the rows around it (plus :data:`SLACK`), by binary search.

        The ids are the last row that ends before ``start - SLACK`` and the first
        that ends at or after ``end + SLACK``, so the range holds every row whose
        time is in the window, as long as EARDBD inserted it less than ``SLACK``
        after its time. The exact time condition is applied by the query itself.

        :returns: ``(first id, last id)``, or ``None`` if the table is empty.

        Example: rows 1..1000 end on day D-1, rows 1001..2000 on day D, rows
        2001..3000 on day D+1; the window of day D gives ``(1000, 3000)``
        (one day of margin on each side), and the query keeps rows 1001..2000.
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
        """Log a query, its ``EXPLAIN`` and (if ``count``) its row count instead of running it (dry run).

        :returns: an empty table of the query's schema.
        """
        log.info("[dry-run] %s SQL: %s%s", table, self.engine.mogrify(self.conn, sql, params), note)
        for line in self.engine.explain(self.conn, sql, params):
            log.info("[dry-run] %s EXPLAIN %s", table, line)
        if count:
            base = sql.split(" ORDER BY ")[0]
            n = self.engine.scalar(self.conn, "SELECT COUNT(*) FROM (%s) t" % base, params)
            log.info("[dry-run] %s rows: %d", table, n)
        return schema.empty_table()


def _at_least_one(batches, schema):
    """Yield record batches as tables, or one empty table of ``schema`` if there are none."""
    empty = True
    for batch in batches:
        empty = False
        yield pa.Table.from_batches([batch])
    if empty:
        yield schema.empty_table()
