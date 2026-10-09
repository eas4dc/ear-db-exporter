"""SQL text of the reads: filters, SELECTs, and the id window search. No connection here.

Identifiers are quoted by the engine's ``quote`` and values are ``%s``
placeholders, which MariaDB and PostgreSQL drivers both accept. Only
single-table scans and primary-key lookups are generated; rows are combined
later in pyarrow (:mod:`ear_db_export.extract`), so no query can turn into an
expensive server-side join. ``Periodic_metrics`` may have no index on
``end_time`` (EAR's ``edb_create`` adds one unless it only prints the SQL), so
a time window is read through a primary-key id window found by
:func:`bisect_id`, which works either way.
"""

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
