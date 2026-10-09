"""SQL text of every read, built from a schema profile. No connection here.

These queries know EAR's SQL schema (through the profile) but not the engine:
identifiers are quoted by the engine's ``quote`` and values are ``%s``
placeholders, which MariaDB and PostgreSQL drivers both accept. Each SELECT
aliases physical to logical names, so its result has the columns of
:data:`ear_db_export.read.TABLES`.

Only single-table scans and primary-key lookups are generated; joins happen
later in pyarrow (step 2), so no query can turn into an expensive server-side
join. ``Periodic_metrics`` may have no index on ``end_time`` (EAR's
``edb_create`` adds one unless it only prints the SQL), so it is read through
a primary-key id window found by :func:`bisect_id`, which works either way.
"""
from .. import TABLES, arrow_schema

#: Keys per ``IN (...)`` list.
IN_BATCH = 5000
#: SQL of the DNF comparison operators (``in``/``not in`` are rendered apart).
OPS = {"=": "=", "==": "=", "!=": "<>", "<": "<", "<=": "<=", ">": ">", ">=": ">=",
       "like": "LIKE"}


def where(quote, dnf):
    """Render a DNF filter (an OR of ANDs) as a SQL condition with ``%s`` placeholders.

    :param quote: the engine's identifier quoting function.
    :param dnf: ``[[(column, op, value), ...], ...]`` on physical column names;
        ``op`` is one of :data:`OPS`, ``in`` or ``not in``.
    :returns: ``(sql, params)``; ``("", [])`` for an empty filter.
    """
    ors, params = [], []
    for conj in dnf:
        ands = []
        for col, op, value in conj:
            if op in ("in", "not in"):
                values = list(value)
                if not values:  # IN () is invalid SQL
                    ands.append("1=0" if op == "in" else "1=1")
                    continue
                ands.append("%s %s (%s)" % (quote(col), op.upper(), ", ".join(["%s"] * len(values))))
                params += values
            else:
                ands.append("%s %s %%s" % (quote(col), OPS[op]))
                params.append(value)
        ors.append("(%s)" % " AND ".join(ands) if ands else "1=1")
    return " OR ".join(ors), params


def select(quote, table, columns, conds=(), order=None, limit=None):
    """Build a SELECT of physical columns.

    :param quote: the engine's identifier quoting function.
    :param table: physical table name.
    :param columns: physical column names, in order.
    :param conds: SQL conditions, joined with AND (each already parenthesised if needed).
    :param order: column to order by, or ``None``.
    :param limit: row limit, or ``None``.
    :returns: the SQL text.
    """
    sql = "SELECT %s FROM %s" % (", ".join(quote(c) for c in columns), quote(table))
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    if order:
        sql += " ORDER BY " + quote(order)
    if limit:
        sql += " LIMIT %d" % limit
    return sql


class Queries:
    """The SQL of one EAR schema version, for one engine.

    :param profile: a bound :class:`~ear_db_export.read.sql.profile.Profile`.
    :param quote: the engine's identifier quoting function.
    """

    def __init__(self, profile, quote):
        self.profile, self.quote = profile, quote

    def col(self, table, col):
        """Return the quoted physical name of a logical column, for WHERE clauses."""
        return self.quote(self.profile.columns[table][col])

    def table(self, table):
        """Return the quoted physical name of a logical table."""
        return self.quote(self.profile.tables[table])

    def select(self, table, where=""):
        """Build a SELECT of every logical column of a table.

        Optional columns that the profile does not map (missing from this
        deployment) are selected as ``NULL``.

        :param table: logical table name.
        :param where: SQL appended after ``FROM``, with ``%s`` placeholders.
        :returns: ``(sql, schema)``: the query, and the pyarrow schema of its result.
        """
        mapped = self.profile.columns[table]
        # `physical` AS `logical`, or NULL for unmapped optional columns
        exprs = ["%s AS %s" % (self.quote(mapped[c]) if c in mapped else "NULL", self.quote(c))
                 for c in TABLES[table]]
        sql = "SELECT %s FROM %s %s" % (", ".join(exprs), self.table(table), where)
        return sql.strip(), arrow_schema(table)

    def by_end_time(self, table, start, end):
        """Query the rows whose ``end_time`` is in ``[start, end)``.

        ``end_time`` is not indexed in EAR, so this is a full table scan. It is
        used for ``Jobs``, which is small enough.

        :returns: ``(sql, params, schema)``.
        """
        sql, schema = self.select(table, "WHERE %s >= %%s AND %s < %%s" % (
            self.col(table, "end_time"), self.col(table, "end_time")))
        return sql, (start, end), schema

    def periodic_window(self, start, end, id_lo, id_hi):
        """Query the ``Periodic_metrics`` rows ending in ``[start, end)`` within an id window.

        The ``id BETWEEN`` condition lets the database use the primary key; the
        ``end_time`` condition then keeps only the rows of the requested window.

        :returns: ``(sql, params, schema)``.
        """
        c = lambda name: self.col("periodic_metrics", name)
        sql, schema = self.select("periodic_metrics", "WHERE %s BETWEEN %%s AND %%s AND "
                                  "%s >= %%s AND %s < %%s" % (c("id"), c("end_time"), c("end_time")))
        return sql, (id_lo, id_hi, start, end), schema

    def in_list(self, table, col, n):
        """Query the rows whose column is one of ``n`` values (``IN (%s, …)``).

        Use it only on primary-key or primary-key-prefix columns, so it is an
        index lookup.

        :returns: ``(sql, schema)``.
        """
        return self.select(table, "WHERE %s IN (%s)" % (self.col(table, col),
                                                        ", ".join(["%s"] * n)))

    def first_at_or_after(self):
        """Query the ``(id, end_time)`` of the first ``Periodic_metrics`` row with ``id >= %s``."""
        c = lambda name: self.col("periodic_metrics", name)
        return "SELECT %s, %s FROM %s WHERE %s >= %%s ORDER BY %s LIMIT 1" % (
            c("id"), c("end_time"), self.table("periodic_metrics"), c("id"), c("id"))

    def id_bounds(self):
        """Query ``MIN(id), MAX(id)`` of ``Periodic_metrics`` (primary key only)."""
        c = self.col("periodic_metrics", "id")
        return "SELECT MIN(%s), MAX(%s) FROM %s" % (c, c, self.table("periodic_metrics"))


def bisect_id(probe, target, lo, hi):
    """Binary-search the ids around a point in time, with primary-key lookups only.

    Finds two ids ``a`` and ``b`` of existing rows with
    ``end_time(a) < target <= end_time(b)`` and no row between them. It does not
    assume ``end_time`` grows with the id: it only relies on the rows it probes,
    so callers add a time margin around the window they want.

    :param probe: ``probe(id)`` returns ``(id, end_time)`` of the first row
        with an id ``>= id``, or ``None`` (one primary-key lookup).
    :param target: point in time, Unix seconds.
    :param lo: lowest id of the table.
    :param hi: highest id of the table.
    :returns: ``(a, b)``. ``a`` is ``None`` if the first row already ends at or
        after ``target``; ``b`` is ``None`` if no row does.
    """
    first = probe(lo)
    if first[1] >= target:
        return None, first[0]
    # Invariants: row a exists with end_time(a) < target; no rows exist in
    # [b_bound, b); b (if set) is a row with end_time(b) >= target.
    a, b_bound, b = first[0], hi + 1, None
    while True:
        mid = (a + b_bound) // 2
        if mid <= a:  # interval closed: a and b are adjacent rows
            break
        row = probe(mid)
        if row is None or row[0] >= b_bound:
            b_bound = mid  # no row in [mid, b_bound): shrink from above
        elif row[1] < target:
            a = row[0]  # still before target: move a up
        else:
            b, b_bound = row[0], row[0]  # at or after target: new b
    return a, b
