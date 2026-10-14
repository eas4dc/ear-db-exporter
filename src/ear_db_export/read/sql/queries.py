"""Build the SQL text of the reads, and the binary search that finds a time window's ids; never connects.

Identifiers are quoted by the engine's ``quote`` and values are ``%s``
placeholders, which MariaDB and PostgreSQL drivers both accept. Only
single-table scans and primary-key lookups are generated; rows are combined
later in pyarrow (:mod:`ear_db_export.selection`), so no query can turn into an
expensive server-side join. ``Periodic_metrics`` may have no index on
``end_time`` (EAR's ``edb_create`` adds one unless it only prints the SQL), so
a time window is read through a primary-key id window found by
:func:`bisect_id`, which works either way.
"""

#: Keys per ``IN (...)`` list.
IN_BATCH = 5000
#: SQL of the comparison operators of :class:`~ear_db_export.read.Cond`
#: (``in``, ``ranges`` and ``like_any`` are rendered apart).
OPS = {"=": "=", ">=": ">=", "<": "<"}


def where(quote, conds):
    """Render a filter, a list of conditions that must all hold, as SQL with ``%s`` placeholders.

    :param quote: the engine's identifier quoting function.
    :param conds: a list of :class:`~ear_db_export.read.Cond` on physical column names.
    :returns: ``(sql, params)``; ``("", [])`` for an empty filter.

    Example::

        where(quote, [Cond("user_name", "in", ["alice", "bob"]),
                      Cond("job_id", "ranges", [(7, 7), (100, 200)]),
                      Cond("app_name", "like_any", ["lmp%"]),
                      Cond("end_time", ">=", 1783123200)])
        # ("`user_name` IN (%s, %s) AND (`job_id` IN (%s) OR `job_id` BETWEEN %s AND %s)"
        #  " AND (`app_name` LIKE %s) AND `end_time` >= %s",
        #  ["alice", "bob", 7, 100, 200, "lmp%", 1783123200])
    """
    parts, params = [], []
    for col, op, value in conds:
        if op == "in":
            sql, values = _in(quote(col), list(value))
        elif op == "ranges":
            sql, values = _ranges(quote(col), list(value))
        elif op == "like_any":
            sql, values = _any(["%s LIKE %%s" % quote(col)] * len(value)), list(value)
        else:
            sql, values = "%s %s %%s" % (quote(col), OPS[op]), [value]
        parts.append(sql)
        params += values
    return " AND ".join(parts), params


def _in(column, values):
    """Render ``column IN (values)``; ``1=0`` (always false) for no values, since ``IN ()`` is invalid SQL."""
    if not values:
        return "1=0", []
    return "%s IN (%s)" % (column, ", ".join(["%s"] * len(values))), values


def _ranges(column, ranges):
    """Render "column in one of the inclusive ``(lo, hi)`` ranges": one ``IN`` for single values, a ``BETWEEN`` per range."""
    single = [lo for lo, hi in ranges if lo == hi]
    alternatives, params = [], []
    if single:
        sql, values = _in(column, single)
        alternatives.append(sql)
        params += values
    for lo, hi in ranges:
        if lo != hi:
            alternatives.append("%s BETWEEN %%s AND %%s" % column)
            params += [lo, hi]
    return _any(alternatives), params


def _any(alternatives):
    """Join SQL conditions with ``OR`` in parentheses; ``1=0`` (always false) if there are none."""
    return "(%s)" % " OR ".join(alternatives) if alternatives else "1=0"


def select(quote, table, columns, conds=(), order=None, limit=None):
    """Build a SELECT of physical columns.

    :param quote: the engine's identifier quoting function.
    :param table: physical table name.
    :param columns: physical column names, in order.
    :param conds: SQL conditions, joined with AND (each already parenthesised if needed).
    :param order: ``[(column, "ascending" | "descending")]`` to sort by, or ``None``.
    :param limit: row limit, or ``None``.
    :returns: the SQL text.

    Example::

        select(quote, "Jobs", ["job_id", "end_time"], ["`end_time` >= %s"],
               [("end_time", "descending")], 10)
        # "SELECT `job_id`, `end_time` FROM `Jobs` WHERE `end_time` >= %s
        #  ORDER BY `end_time` DESC LIMIT 10"
    """
    sql = "SELECT %s FROM %s" % (", ".join(quote(c) for c in columns), quote(table))
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    if order:
        sql += " ORDER BY " + ", ".join(quote(c) + (" DESC" if d == "descending" else "")
                                        for c, d in order)
    if limit:
        sql += " LIMIT %d" % limit
    return sql


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
