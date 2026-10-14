"""Read slurmdbd's database (``slurm_acct_db``), read-only, through ear-db-export's MariaDB engine.

:class:`SlurmSource` connects with :func:`ear_db_export.read.sql.mariadb.connect`
(a ``READ ONLY`` session in UTC), picks the schema profile, and reads one table
at a time, with no joins. How each read queries the database:

- ``window``: the rows whose time column is in ``[start, end)``. On a table with
  a single integer primary key (``job_table``, whose ``time_submit`` leads no
  index), the window is first mapped to a ``job_db_inx`` range by binary search
  (:func:`~ear_db_export.read.sql.queries.bisect_id`, widened by :data:`SLACK`),
  then read in pages of ``page_rows`` in key order, with the exact time condition
  in the ``WHERE``. Any other table is read with one streamed ``SELECT``
  (``event_table`` has an index on ``time_start``; ``resv_table`` is small).
- ``lookup``: ``WHERE column IN (...)``, :data:`~ear_db_export.read.sql.queries.IN_BATCH`
  keys per query, on a primary key or its prefix (``step_table`` by ``job_db_inx``).
- ``dump``: the whole table: pages by primary key, or one streamed ``SELECT``.

Every read yields ``pyarrow.Table`` objects with physical names and the types as
stored, at least one (maybe empty). Columns the profile wants and the database
lacks are left out, with one warning each; :attr:`SlurmSource.report` keeps them
for the manifest.
"""
import logging

import pyarrow as pa

from ear_db_export.config import ConfigError
from ear_db_export.read.sql import mariadb, queries
from ear_db_export.read.sql.queries import IN_BATCH, bisect_id

from .profile import TABLES, detect, load_profile, physical_table, release

log = logging.getLogger(__name__)

# job_db_inx is given when slurmdbd inserts a job, normally at submission. A job
# is only missed if slurmdbd stored it more than SLACK after its time_submit (a
# slurmdbd outage: slurmctld keeps the records and sends them later).
#: Margin, in seconds, around a window when searching its id range.
SLACK = 7 * 86400  # ponytail: fixed margin; make it a flag if a site has longer slurmdbd outages
#: Rows per primary-key page (``--page-rows``).
PAGE_ROWS = 100_000
#: Tables that may be missing from a database (``job_script_table``: Slurm 22.05+).
OPTIONAL_TABLES = frozenset({"job_script"})


class SlurmSource:
    """slurmdbd's database, read-only.

    :param dbconf: a :class:`~ear_db_export.config.DbConf`.
    :param profile: profile name or ``.toml`` path; ``None`` detects it from
        ``convert_version_table`` (:func:`~slurm_db_export.profile.detect`).
    :param engine: the engine module (:mod:`ear_db_export.read.sql.mariadb`; tests replace it).
    :param dry_run: log each query, its ``EXPLAIN`` and row count instead of reading.
    :param page_rows: rows per primary-key page.
    :ivar clusters: the clusters of ``cluster_table`` not marked deleted.
    :ivar version: ``convert_version_table.version``, or ``None``.
    :ivar report: ``{physical table: {"columns": [...], "missing": [...]}}`` of the tables read.

    Example::

        src = SlurmSource(DbConf("slurmmaster", 3306, "slurm_acct_db", "ro", pw))
        for jobs in src.window("job", "lenox.hu", "time_submit", 1788220800, 1788307200):
            print(jobs.num_rows)
        src.close()
    """

    def __init__(self, dbconf, profile=None, engine=mariadb, dry_run=False, page_rows=PAGE_ROWS):
        """Connect read-only, read the live tables, the schema version and the clusters, and pick the profile."""
        self.engine, self.dry_run, self.database = engine, dry_run, dbconf.database
        self.page_rows = page_rows
        self.conn = engine.connect(dbconf)
        try:
            self.live = engine.live_columns(self.conn, dbconf.database)
            self.version = None
            if "convert_version_table" in self.live:
                self.version = engine.scalar(self.conn, "SELECT MAX(version) FROM convert_version_table")
            self.clusters = []
            if "cluster_table" in self.live:
                self.clusters = [r[0] for r in engine.rows(
                    self.conn, "SELECT name FROM cluster_table WHERE deleted = 0 ORDER BY name")]
            job = self.live.get(physical_table("job", self.clusters[0]), {}) if self.clusters else {}
            self.profile = load_profile(profile) if profile else detect(self.version, job)
            self.release = release(self.version, job)
        except BaseException:
            self.conn.close()
            raise
        self.report, self._tables = {}, {}

    def close(self):
        """Close the connection."""
        self.conn.close()

    def has(self, table, cluster):
        """Tell whether the database has a logical table (of a cluster)."""
        return physical_table(table, cluster) in self.live

    def info(self):
        """Describe the source for a bundle manifest: engine, server, database, profile, schema version, Slurm release, columns."""
        return {"engine": self.engine.__name__.rsplit(".", 1)[-1],
                "server": self.engine.scalar(self.conn, "SELECT VERSION()"),
                "database": self.database, "profile": self.profile.name,
                "schema_version": self.version, "slurm_release": self.release,
                "columns": self.report}

    def check(self, clusters):
        """Compare the profile with the database, for ``check``.

        :param clusters: the cluster names to check.
        :returns: ``(lines, ok)``: one line per table, with its missing and
            unexported columns; ``ok`` is ``False`` if a required table or column is missing.
        """
        lines, ok = ["schema version %s (Slurm %s), profile %s" % (
            self.version, self.release or "unknown", self.profile.name)], True
        seen = set()
        for cluster in clusters:
            for table in TABLES:
                physical = physical_table(table, cluster)
                if physical in seen:
                    continue  # a global table, already checked for another cluster
                seen.add(physical)
                if physical not in self.live:
                    optional = table in OPTIONAL_TABLES
                    lines.append("%s: TABLE MISSING%s" % (physical, " (optional)" if optional else ""))
                    ok = ok and optional
                    continue
                have = [c for c, _, _ in self.engine.columns(self.conn, self.database, physical)]
                try:
                    names, missing = self.profile.select(table, have)
                except ConfigError as e:
                    lines.append("%s: MISMATCH: %s" % (physical, e))
                    ok = False
                    continue
                lines.append("%s: OK, %d columns" % (physical, len(names)))
                if missing:
                    lines.append("  missing optional (not exported): " + ", ".join(missing))
                extra = [c for c in have if c not in names]
                if extra:
                    lines.append("  not exported: " + ", ".join(extra))
        return lines, ok

    # -- Reads -----------------------------------------------------------------

    def window(self, table, cluster, column, start, end):
        """Read the rows of a table whose ``column`` (epoch seconds) is in ``[start, end)``.

        :param table: logical table name.
        :param cluster: cluster name.
        :param column: logical time column (``time_submit``, ``time_start``).
        :param start: Unix seconds (included), or ``None``.
        :param end: Unix seconds (excluded), or ``None``.
        :returns: an iterator of ``pyarrow.Table``, at least one.

        Example, the jobs submitted on 2026-09-01::

            src.window("job", "lenox.hu", "time_submit", 1788220800, 1788307200)
            # SELECT ... FROM `lenox.hu_job_table` WHERE `job_db_inx` BETWEEN <a> AND <b>
            #   AND (`time_submit` >= 1788220800 AND `time_submit` < 1788307200)
            #   ORDER BY `job_db_inx` LIMIT 100000, then the next pages
        """
        physical, schema, key = self.table(table, cluster)
        tcol = self.profile.col(table, column)
        conds = ([("%s >= %%s" % self.engine.quote(tcol), start)] if start is not None else []) + \
                ([("%s < %%s" % self.engine.quote(tcol), end)] if end is not None else [])
        sql, params = [c for c, _ in conds], [v for _, v in conds]
        ids = self._id_range(physical, key, tcol, start, end) if key else None
        yield from self._read(physical, schema, key, sql, params, ids, count=True)

    def lookup(self, table, cluster, column, keys):
        """Read the rows whose ``column`` is one of ``keys`` (duplicates and ``None`` ignored), in ``IN`` batches.

        Only for a primary key or its prefix, so each batch is an index lookup.

        Example: ``src.lookup("step", "lenox.hu", "job_db_inx", [10, 11])``.
        """
        physical, schema, _ = self.table(table, cluster)
        keys = sorted(set(k for k in keys if k is not None))
        if self.dry_run:
            log.info("[dry-run] %s: lookups by %s for the rows above, IN batches of %d",
                     physical, column, IN_BATCH)
            keys = []
        if not keys:
            yield schema.empty_table()
        q = self.engine.quote
        for i in range(0, len(keys), IN_BATCH):
            chunk = keys[i:i + IN_BATCH]
            sql = queries.select(q, physical, schema.names, ["%s IN (%s)" % (
                q(self.profile.col(table, column)), ", ".join(["%s"] * len(chunk)))])
            yield self.engine.fetch(self.conn, sql, chunk, schema)

    def dump(self, table, cluster):
        """Read a whole table: an iterator of ``pyarrow.Table``, at least one."""
        physical, schema, key = self.table(table, cluster)
        yield from self._read(physical, schema, key, [], [])

    # -- Helpers ---------------------------------------------------------------

    def table(self, table, cluster):
        """Return ``(physical name, arrow schema of the columns to read, integer primary key or None)``.

        Worked out once per table: the profile chooses the columns
        (:meth:`~slurm_db_export.profile.Profile.select`); each one the database
        lacks, or whose type no Arrow type holds, is logged as a warning and left out.

        :raises ConfigError: if the database has no such table, or lacks a required column.
        """
        physical = physical_table(table, cluster)
        if physical in self._tables:
            return self._tables[physical]
        if physical not in self.live:
            raise ConfigError("no table %s in %s" % (physical, self.database))
        cols = self.engine.columns(self.conn, self.database, physical)
        names, missing = self.profile.select(table, [c for c, _, _ in cols])
        for c in missing:
            log.warning("%s has no %s column (Slurm %s): not exported", physical,
                        self.profile.col(table, c), self.release or "unknown")
        types = {c: t for c, t, _ in cols}
        fields = []
        for name in names:
            try:
                fields.append((name, self.engine.arrow_type(types[name])))
            except ValueError as e:
                log.warning("%s.%s: %s: not exported", physical, name, e)
                missing.append(name)
        schema = pa.schema(fields)
        pk = [c for c, _, in_pk in cols if in_pk]
        key = pk[0] if len(pk) == 1 and pk[0] in names and \
            pa.types.is_integer(schema.field(pk[0]).type) else None
        self.report[physical] = {"columns": schema.names, "missing": missing}
        self._tables[physical] = physical, schema, key
        return self._tables[physical]

    def _read(self, table, schema, key, conds, params, ids=None, count=False):
        """Run the SELECT of a physical table and yield its rows as tables, at least one.

        :param key: the table's integer primary key: read in pages in key order;
            ``None``: one streamed ``SELECT``.
        :param conds: SQL conditions (``%s`` placeholders), joined with AND.
        :param params: their values.
        :param ids: ``(first, last)`` primary-key range to read, or ``None``.
        :param count: in a dry run, also log the number of matching rows.
        """
        q, conds, params = self.engine.quote, list(conds), list(params)
        if ids is not None:
            conds.insert(0, "%s BETWEEN %%s AND %%s" % q(key))
            params = list(ids) + params
        if key is None:
            sql = queries.select(q, table, schema.names, conds)
            if self.dry_run:
                yield self._preview(table, sql, params, schema, count)
                return
            empty = True
            for batch in self.engine.stream(self.conn, sql, params, schema):
                empty = False
                yield pa.Table.from_batches([batch])
            if empty:
                yield schema.empty_table()
            return
        order = [(key, "ascending")]
        first = queries.select(q, table, schema.names, conds, order, self.page_rows)
        if self.dry_run:
            yield self._preview(table, first, params, schema, count,
                                " (then pages of %d by %s)" % (self.page_rows, key))
            return
        nxt = queries.select(q, table, schema.names, conds + ["%s > %%s" % q(key)], order,
                             self.page_rows)
        page = self.engine.fetch(self.conn, first, params, schema)
        while True:
            yield page
            if page.num_rows < self.page_rows:
                return
            page = self.engine.fetch(self.conn, nxt, params + [page[key][-1].as_py()], schema)

    def _id_range(self, table, key, tcol, start, end):
        """Map a time window to the primary-key range of the rows around it (plus :data:`SLACK`), by binary search.

        :returns: ``(first id, last id)``; ``None`` to read without a range (no
            bound given, or an empty table: then the query finds nothing either).
        """
        if start is None and end is None:
            return None
        q = self.engine.quote
        lo, hi = self.engine.rows(self.conn, "SELECT MIN(%s), MAX(%s) FROM %s"
                                  % (q(key), q(key), q(table)))[0]
        if lo is None:
            return None
        sql = "SELECT %s, %s FROM %s WHERE %s >= %%s ORDER BY %s LIMIT 1" % (
            q(key), q(tcol), q(table), q(key), q(key))

        def probe(id_):
            """Return ``(id, time)`` of the first row from ``id_`` on, or ``None`` past the last row."""
            r = self.engine.rows(self.conn, sql, (id_,))
            return r[0] if r else None
        a = bisect_id(probe, start - SLACK, lo, hi)[0] if start is not None else None
        b = bisect_id(probe, end + SLACK, lo, hi)[1] if end is not None else None
        return (lo if a is None else a), (hi if b is None else b)

    def _preview(self, table, sql, params, schema, count, note=""):
        """Log a query, its ``EXPLAIN`` and (if ``count``) its row count instead of running it; return an empty table."""
        log.info("[dry-run] %s SQL: %s%s", table, self.engine.mogrify(self.conn, sql, params), note)
        for line in self.engine.explain(self.conn, sql, params):
            log.info("[dry-run] %s EXPLAIN %s", table, line)
        if count:
            base = sql.split(" ORDER BY ")[0]
            n = self.engine.scalar(self.conn, "SELECT COUNT(*) FROM (%s) t" % base, params)
            log.info("[dry-run] %s rows: %d", table, n)
        return schema.empty_table()
