"""Reader of EAR's SQL database (MariaDB/MySQL now; EAR's PostgreSQL schema has the same names).

:class:`SqlReader` implements :class:`ear_db_export.read.Reader`. It is made
of three parts, so that only one of them knows the engine:

- :mod:`~ear_db_export.read.sql.profile`: which physical names this EAR
  version uses (``Jobs.app_id`` for the logical ``jobs.app_name``).
- :mod:`~ear_db_export.read.sql.queries`: the SQL text, built from the profile.
- :mod:`~ear_db_export.read.sql.mariadb`: the engine: connection, quoting,
  ``EXPLAIN``, ``information_schema``, the auto-increment step.

Two EAR storage details are resolved here, so later steps never see them:
``Periodic_metrics`` may have no time index (a day is found by binary search
on the primary key), and a signature points to its GPUs with an id range whose
step is the server's auto-increment step (Galera uses steps > 1).
"""
import datetime as dt
import logging
from functools import cached_property

import pyarrow as pa
import pyarrow.compute as pc

from .. import GPU_LINK, TABLES, Reader, arrow_schema
from . import mariadb
from .profile import detect, diff, load_profile
from .queries import IN_BATCH, Queries, bisect_id

log = logging.getLogger(__name__)

# Periodic_metrics.end_time may have no index, so each window is read through a
# PK id window. bisect_id() returns rows that really end before/after the margins;
# a row is only missed if EARDBD inserted it more than SLACK after its
# end_time. Measured on a test cluster: worst out-of-order lag 210 s.
#: Margin, in seconds, around a window when searching its id range.
SLACK = 86400  # ponytail: fixed 1-day margin; make it a flag if a site shows longer insert delays


class SqlReader(Reader):
    """Read EAR records from its SQL database, read-only.

    Opening it connects, chooses the schema profile (``profile`` or
    autodetected) and compares it with the live database (:meth:`check`).

    :param dbconf: a :class:`~ear_db_export.config.DbConf`.
    :param profile: profile name or ``.toml`` path; ``None`` autodetects.
    :param engine: the engine module (:mod:`~ear_db_export.read.sql.mariadb`).
    :param dry_run: log each query with its ``EXPLAIN`` and row count, and
        return empty tables instead of reading.
    :raises ConfigError: if no profile matches, or the profile is invalid.
    """

    provides = frozenset(TABLES)

    def __init__(self, dbconf, profile=None, engine=mariadb, dry_run=False):
        self.engine, self.dry_run, self.database = engine, dry_run, dbconf.database
        self.conn = engine.connect(dbconf)
        try:
            live = engine.live_columns(self.conn, dbconf.database)
            chosen = load_profile(profile) if profile else detect(live)
            self._check = diff(chosen, live, engine.TYPE_FAMILY)
            self.profile = chosen.bind(live)
        except BaseException:
            self.conn.close()
            raise
        self.q = Queries(self.profile, engine.quote)
        has_local_id = "local_id" in self.profile.columns["jobs"]  # EAR >= 5.0
        self.join_keys = ["job_id", "step_id"] + (["local_id"] if has_local_id else [])

    def check(self):
        """Compare the profile with the live database (``check-schema``).

        :returns: ``(lines, ok)`` from :func:`~ear_db_export.read.sql.profile.diff`.
        """
        return self._check

    def close(self):
        """Close the connection."""
        self.conn.close()

    # -- Reader interface ------------------------------------------------------

    def oldest_day(self):
        """Return the day the lowest-id ``Periodic_metrics`` row ends, or ``None`` if empty."""
        if self._bounds is None:
            return None
        return dt.datetime.fromtimestamp(self._bounds[2], dt.timezone.utc).date()

    def periodic_metrics(self, start, end):
        """Read ``Periodic_metrics`` rows ending in ``[start, end)`` through a primary-key id window."""
        if self._bounds is None:
            return arrow_schema("periodic_metrics").empty_table()
        # Map the window (plus a margin) to the ids of existing rows
        min_id, max_id, _ = self._bounds
        a, _ = bisect_id(self._probe, start - SLACK, min_id, max_id)
        _, b = bisect_id(self._probe, end + SLACK, min_id, max_id)
        id_lo = min_id if a is None else a
        id_hi = max_id if b is None else b
        return self._fetch("Periodic_metrics", *self.q.periodic_window(start, end, id_lo, id_hi))

    def jobs(self, start, end):
        """Read the ``Jobs`` rows ending in ``[start, end)`` (full scan: ``end_time`` has no index)."""
        return self._fetch("Jobs", *self.q.by_end_time("jobs", start, end))

    def applications(self, job_ids):
        """Read the ``Applications`` rows of these jobs (primary-key prefix lookups)."""
        return self._fetch_in("applications", "job_id", job_ids)

    def power_signatures(self, ids):
        """Read ``Power_signatures`` rows by id."""
        return self._fetch_in("power_signatures", "id", ids)

    def signatures(self, ids):
        """Read ``Signatures`` rows by id."""
        return self._fetch_in("signatures", "id", ids)

    def gpu_signatures(self, signatures):
        """Read the GPU rows of these signatures: one row per GPU, with :data:`~ear_db_export.read.GPU_LINK`.

        EAR stores a signature's GPUs as ids ``min_gpu_sig_id, +step, …,
        max_gpu_sig_id``, where ``step`` is the server's auto-increment step
        (``mysql_io_functions.c``). On Galera the ids in between belong to
        nobody, or to rows another DB node inserted at the same time.
        """
        gpu_cols = [c for c in TABLES["gpu_signatures"] if c != "id"]
        schema = pa.schema([(c, pa.int64()) for c in GPU_LINK] +
                           list(arrow_schema("gpu_signatures", gpu_cols)))
        if signatures.num_rows == 0:
            return schema.empty_table()
        # 1. Expand each id range into (signature_id, gpu_index, gpu id)
        step = self.engine.id_step(self.conn)
        sig_ids, index, gpu_ids = [], [], []
        for s in signatures.select(["id", "min_gpu_sig_id", "max_gpu_sig_id"]).to_pylist():
            if s["min_gpu_sig_id"] is None or s["max_gpu_sig_id"] is None:
                continue
            for i, gid in enumerate(range(s["min_gpu_sig_id"], s["max_gpu_sig_id"] + 1, step)):
                sig_ids.append(s["id"])
                index.append(i)
                gpu_ids.append(gid)
        # 2. Look the ids up and keep the GPUs that have a row
        gpus = self._fetch_in("gpu_signatures", "id", gpu_ids)
        pos = pc.index_in(pa.array(gpu_ids, pa.int64()), value_set=gpus["id"])
        cols = [pa.array(sig_ids, pa.int64()), pa.array(index, pa.int64())]
        cols += [pc.take(gpus[c], pos) for c in gpu_cols]
        out = pa.Table.from_arrays(cols, schema=schema).filter(pc.is_valid(pos))
        if pos.null_count:
            log.warning("%d GPU ids in min/max_GPU_sig_id ranges (step %d) have no "
                        "GPU_signatures row (skipped; gpu_index keeps its position)",
                        pos.null_count, step)
        return out

    # -- Helpers ---------------------------------------------------------------

    @cached_property
    def _bounds(self):
        """``(min id, max id, end_time of the min-id row)`` of ``Periodic_metrics``, or ``None``."""
        lo, hi = self.engine.rows(self.conn, self.q.id_bounds())[0]
        if lo is None:
            return None
        return lo, hi, self._probe(lo)[1]

    def _probe(self, id_):
        """Return ``(id, end_time)`` of the first ``Periodic_metrics`` row with id ``>= id_`` (one PK lookup)."""
        r = self.engine.rows(self.conn, self.q.first_at_or_after(), (id_,))
        return r[0] if r else None

    def _fetch(self, label, sql, params, schema):
        """Run a SELECT into a table, or preview it on a dry-run."""
        if not self.dry_run:
            return self.engine.fetch(self.conn, sql, params, schema)
        log.info("[dry-run] %s SQL: %s", label, self.engine.mogrify(self.conn, sql, params))
        for line in self.engine.explain(self.conn, sql, params):
            log.info("[dry-run] %s EXPLAIN %s", label, line)
        n = self.engine.scalar(self.conn, "SELECT COUNT(*) FROM (%s) t" % sql, params)
        log.info("[dry-run] %s rows: %d", label, n)
        return schema.empty_table()

    def _fetch_in(self, table, col, keys):
        """Fetch the rows whose column is in a list of keys, in batches of ``IN_BATCH``.

        Only for primary-key or primary-key-prefix columns. Duplicates and
        ``None`` are ignored.
        """
        if self.dry_run:
            log.info("[dry-run] %s: primary-key lookups by %s for the rows above, "
                     "IN batches of %d", self.profile.tables[table], col, IN_BATCH)
            return arrow_schema(table).empty_table()
        keys = sorted(set(k for k in keys if k is not None))
        parts = []
        for i in range(0, len(keys), IN_BATCH):
            chunk = keys[i:i + IN_BATCH]
            sql, schema = self.q.in_list(table, col, len(chunk))
            parts.append(self.engine.fetch(self.conn, sql, chunk, schema))
        return pa.concat_tables(parts) if parts else arrow_schema(table).empty_table()
