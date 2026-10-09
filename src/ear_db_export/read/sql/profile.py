"""Schema profiles: the names one EAR version uses in its SQL database, and ``check``.

The exporter uses the *logical* table and column names of
:data:`ear_db_export.read.TABLES` (``app_name``), the same for every EAR
version. A profile (a TOML file in ``read/sql/profiles/``) maps them to the
*physical* names of one EAR schema version (``Jobs.app_id``). Sources return
tables with their physical names (as stored); :meth:`Profile.logical` turns
them into logical ones for the datasets, :meth:`Profile.where` turns filters
on logical names into physical ones, so nothing after it depends on the EAR
version.

A profile file looks like this:

.. code-block:: toml

   [tables.jobs]
   name = "Jobs"                 # physical table
   [tables.jobs.columns]
   app_name = "app_id"           # logical = physical
   job_id = "job_id"

   [tables.learning_jobs]
   name = "Learning_jobs"
   like = "jobs"                 # same columns as [tables.jobs]

Next to each profile, ``<name>.sql`` holds EAR's own DDL for that version
(:func:`ddl`); a test checks that the profile maps every column of it.

EAR's PostgreSQL schema uses the same names, unquoted and so in lower case:
the same profiles serve both engines if the PostgreSQL engine's ``quote``
lower-cases names. To support a new EAR version, copy a profile and its DDL,
fix the names and run ``ear-db-export check``.
"""
import logging
import os
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

try:
    import tomllib
except ImportError:  # Python < 3.11
    import tomli as tomllib

from ...config import ConfigError
from .. import ARROW, OPTIONAL_TABLES, TABLES, col_type, is_optional

log = logging.getLogger(__name__)

#: Directory of the bundled profile files (``<name>.toml``) and their DDL (``<name>.sql``).
PROFILE_DIR = os.path.join(os.path.dirname(__file__), "profiles")


@dataclass
class Profile:
    """Mapping from logical to physical names for one EAR schema version.

    :ivar name: profile name, the file name without ``.toml`` (``ear6``).
    :ivar tables: ``{logical table: physical table}``.
    :ivar columns: ``{logical table: {logical col: physical col}}``.
    """

    name: str
    tables: dict   # logical table -> physical table
    columns: dict  # logical table -> {logical col: physical col}

    def bind(self, live):
        """Drop what the live database lacks and may lack: optional columns and tables.

        Changes the profile in place, so later reads return NULL for those
        columns instead of failing. Each dropped column is logged as a warning;
        a missing optional table is logged at info level.

        :param live: ``{table: {column: type}}`` from the engine's ``live_columns``.
        :returns: the profile itself, for chaining.
        """
        for t in [t for t in self.tables if t in OPTIONAL_TABLES and self.tables[t] not in live]:
            log.info("no %s table in the DB (optional)", self.tables[t])
            del self.tables[t], self.columns[t]
        for t, cols in self.columns.items():
            have = live.get(self.tables[t], {})
            for c in [c for c, p in cols.items() if p not in have and is_optional(t, c)]:
                log.warning("%s.%s (%s) not in DB: exported as NULL", self.tables[t], cols[c], c)
                del cols[c]
        return self

    def table_of(self, physical):
        """Return the logical name of a physical table, or ``None`` if the profile has no such table."""
        return next((t for t, p in self.tables.items() if p == physical), None)

    def col(self, table, col):
        """Return the physical name of a logical column, or ``None`` if it is not mapped (or absent)."""
        return self.columns[table].get(col)

    def where(self, table, dnf):
        """Map a filter on logical columns to physical ones.

        :param table: logical table name.
        :param dnf: ``[[(column, op, value), ...], ...]`` (OR of ANDs), or a flat
            ``[(column, op, value), ...]`` (one AND).
        :returns: the same filter on physical column names, as a list of lists.
        :raises ConfigError: if a column is not in this database.
        """
        if dnf and isinstance(dnf[0], tuple):
            dnf = [dnf]
        out = []
        for conj in dnf:
            mapped = []
            for c, op, value in conj:
                p = self.col(table, c)
                if p is None:
                    raise ConfigError("cannot filter on %s.%s: not in this database (profile %s)"
                                      % (self.tables[table], c, self.name))
                mapped.append((p, op, value))
            out.append(mapped)
        return out

    def logical(self, table, t, columns=None):
        """Turn a table with physical names into logical columns and types.

        ``FLOAT`` columns (float32) go through their shortest decimal text, so a
        stored 0.1 reads as 0.1, as the MySQL protocol returns it, and not as
        0.10000000149011612.

        :param table: logical table name.
        :param t: ``pyarrow.Table`` with physical column names (extra columns are ignored).
        :param columns: logical columns to return, in order; default all of :data:`TABLES`.
        :returns: a table with those columns, typed as :data:`~ear_db_export.read.ARROW`;
            a column the profile or the table lacks is NULL.
        """
        names = list(columns or TABLES[table])
        if t.num_rows == 0:  # zero-chunk columns crash some kernels: build it empty
            return pa.schema([(c, ARROW[col_type(table, c)]) for c in names]).empty_table()
        arrays = []
        for c in names:
            typ, p = ARROW[col_type(table, c)], self.col(table, c)
            if p is None or p not in t.column_names:
                arrays.append(pa.nulls(t.num_rows, typ))
                continue
            values = t[p]
            if pa.types.is_float32(values.type):
                values = pc.cast(values, pa.string())
            arrays.append(pc.cast(values, typ))
        return pa.Table.from_arrays(arrays, names=names)

    def physical(self, table, t):
        """Rename the logical columns of a table to this version's physical names.

        :param table: logical table name.
        :param t: ``pyarrow.Table`` with logical column names.
        :returns: the mapped columns, renamed; unmapped ones are left out. Types are unchanged.
        """
        keep = [c for c in t.column_names if self.col(table, c) is not None]
        return t.select(keep).rename_columns([self.col(table, c) for c in keep])


def load_profile(name_or_path):
    """Load and validate a profile.

    Every table of :data:`TABLES` must be mapped, with all its required
    columns. Unknown tables or columns are errors too, so a typo in a profile
    does not go unnoticed.

    :param name_or_path: a bundled profile name (``ear6``) or a path ending in ``.toml``.
    :returns: a :class:`Profile`.
    :raises ConfigError: if the file cannot be read or parsed, or does not match
        :data:`TABLES`. The message lists every problem found.
    """
    path = name_or_path if name_or_path.endswith(".toml") else \
        os.path.join(PROFILE_DIR, name_or_path + ".toml")
    try:
        with open(path, "rb") as f:
            doc = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise ConfigError("cannot load profile %s: %s" % (path, e))
    name = os.path.basename(path)[:-len(".toml")]
    tables, columns, errors = {}, {}, []
    specs = doc.get("tables", {})
    # Everything in the file must be a known logical table/column
    for t, spec in specs.items():
        if t not in TABLES:
            errors.append("unknown table [tables.%s]" % t)
            continue
        tables[t] = spec["name"]
        like = spec.get("like")
        if like is not None and like not in specs:
            errors.append("[tables.%s] like = %r: no such table in the profile" % (t, like))
        columns[t] = dict(specs.get(like, {}).get("columns", {}) if like else {},
                          **spec.get("columns", {}))
        errors += ["unknown column %s.%s" % (t, c) for c in columns[t] if c not in TABLES[t]]
    # Every logical table and required column must be in the file
    for t, cols in TABLES.items():
        if t not in tables:
            errors.append("missing table [tables.%s]" % t)
            continue
        errors += ["missing required column %s.%s" % (t, c)
                   for c in cols if not is_optional(t, c) and c not in columns[t]]
    if errors:
        raise ConfigError("profile %s is invalid:\n  " % path + "\n  ".join(errors))
    return Profile(name, tables, columns)


def available_profiles():
    """List the bundled profiles.

    :returns: sorted profile names, for example ``["ear4", "ear6"]``.
    """
    return sorted(f[:-5] for f in os.listdir(PROFILE_DIR) if f.endswith(".toml"))


def _required_present(profile, live):
    """Tell whether all required physical columns of a profile exist in the live DB.

    An optional table that is not in the DB at all is ignored.

    :param profile: a :class:`Profile`.
    :param live: ``{table: {column: type}}`` from the engine's ``live_columns``.
    :returns: ``True`` if none is missing.
    """
    return all(
        p in live.get(profile.tables[t], {})
        for t, cols in profile.columns.items()
        if not (t in OPTIONAL_TABLES and profile.tables[t] not in live)
        for c, p in cols.items() if not is_optional(t, c))


def detect(live):
    """Pick the bundled profile that matches a live database.

    :param live: ``{table: {column: type}}`` from the engine's ``live_columns``.
    :returns: the only :class:`Profile` whose required physical columns all exist.
    :raises ConfigError: if no profile or more than one matches. Then pass
        ``--profile`` explicitly, or run ``check`` to see the differences.
    """
    matches = [n for n in available_profiles() if _required_present(load_profile(n), live)]
    if len(matches) != 1:
        raise ConfigError("schema autodetect matched %s (of %s); pass --profile or run check"
                          % (matches or "no profile", available_profiles()))
    log.info("autodetected profile %s", matches[0])
    return load_profile(matches[0])


def diff(profile, live, type_family):
    """Compare a profile with a live database (the ``check`` report).

    For each table it reports missing required columns, missing optional
    columns (exported as NULL), type mismatches and extra columns that are not
    exported. A missing optional table is reported but is not a mismatch.

    :param profile: a :class:`Profile`.
    :param live: ``{table: {column: type}}`` from the engine's ``live_columns``.
    :param type_family: the engine's ``{DATA_TYPE: logical type}`` (``TYPE_FAMILY``).
    :returns: ``(lines, ok)``: report lines, and ``False`` if a table or a
        required column is missing or a type does not match.
    """
    lines, ok = [], True
    for t, cols in profile.columns.items():
        phys = profile.tables[t]
        have = live.get(phys)
        if have is None:
            optional = t in OPTIONAL_TABLES
            lines.append("%s (%s): TABLE MISSING%s" % (phys, t, " (optional)" if optional else ""))
            ok = ok and optional
            continue
        # Classify the differences for this table
        missing_req = [p for c, p in cols.items() if p not in have and not is_optional(t, c)]
        missing_opt = [p for c, p in cols.items() if p not in have and is_optional(t, c)]
        extra = sorted(set(have) - set(cols.values()))
        types = ["%s is %s, expected %s" % (p, have[p], col_type(t, c)) for c, p in cols.items()
                 if p in have and type_family.get(have[p]) != col_type(t, c)]
        ok = ok and not missing_req and not types
        status = "OK" if not (missing_req or types) else "MISMATCH"
        lines.append("%s (%s): %s" % (phys, t, status))
        for label, items in [("missing", missing_req), ("missing optional (NULL)", missing_opt),
                             ("type", types), ("extra, not exported", extra)]:
            if items:
                lines.append("  %s: %s" % (label, ", ".join(items)))
    return lines, ok


def ddl(name):
    """Return EAR's ``CREATE TABLE`` / ``CREATE INDEX`` statements of a bundled profile.

    :param name: bundled profile name (``ear7``).
    :returns: the text of ``profiles/<name>.sql``: MySQL DDL, one column per line.
    """
    with open(os.path.join(PROFILE_DIR, name + ".sql")) as f:
        return f.read()


def ddl_schema(name):
    """Parse the tables of :func:`ddl` into columns.

    :param name: bundled profile name.
    :returns: ``{physical table: [(column, column type, nullable, in primary key)]}``
        in DDL order, for example ``("job_id", "int unsigned", False, True)``. The
        column type is lower-case, as ``information_schema.COLUMN_TYPE`` gives it
        (:func:`ear_db_export.read.sql.mariadb.arrow_type` reads it).
    """
    tables, table = {}, None
    for line in ddl(name).splitlines():
        line = line.strip().rstrip(",")
        if line.startswith("CREATE TABLE"):
            table = line.split()[5]
            tables[table] = []
        elif table and line.startswith("PRIMARY KEY"):
            pk = {c.strip() for c in line[line.index("(") + 1:line.rindex(")")].split(",")}
            tables[table] = [(c, typ, nullable and c not in pk, c in pk)
                             for c, typ, nullable, _ in tables[table]]
        elif table and line and not line.startswith(")"):
            col, rest = line.split(None, 1)
            upper = rest.upper()
            cut = min([i for i in (upper.find(" NOT NULL"), upper.find(" DEFAULT"),
                                   upper.find(" AUTO_INCREMENT")) if i >= 0] or [len(rest)])
            tables[table].append((col, rest[:cut].lower(), "NOT NULL" not in upper, False))
        elif line.startswith(")"):
            table = None
    return tables
