"""Schema profiles: the names one EAR version uses in its SQL database, and ``check-schema``.

The exporter uses the *logical* table and column names of
:data:`ear_db_export.read.TABLES` (``app_name``), the same for every EAR
version. A profile (a TOML file in ``read/sql/profiles/``) maps them to the
*physical* names of one EAR schema version (``Jobs.app_id``). Every SELECT
aliases physical to logical names, so nothing after the SQL reader depends on
the EAR version.

A profile file looks like this:

.. code-block:: toml

   [tables.jobs]
   name = "Jobs"                 # physical table
   [tables.jobs.columns]
   app_name = "app_id"           # logical = physical
   job_id = "job_id"

EAR's PostgreSQL schema uses the same names, unquoted and so in lower case:
the same profiles serve both engines if the PostgreSQL engine's ``quote``
lower-cases names. To support a new EAR version, copy a profile, fix the names
and run ``ear-db-export check-schema``.
"""
import logging
import os
from dataclasses import dataclass

try:
    import tomllib
except ImportError:  # Python < 3.11
    import tomli as tomllib

from ...config import ConfigError
from .. import TABLES, col_type, is_optional

log = logging.getLogger(__name__)

#: Directory of the bundled profile files (``<name>.toml``).
PROFILE_DIR = os.path.join(os.path.dirname(__file__), "profiles")


@dataclass
class Profile:
    """Mapping from logical to physical names for one EAR schema version.

    :ivar name: profile name, the file name without ``.toml`` (``ear6``).
    :ivar tables: ``{logical table: physical table}``.
    :ivar columns: ``{logical table: {logical column: physical column}}``.
    """

    name: str
    tables: dict   # logical table -> physical table
    columns: dict  # logical table -> {logical col: physical col}

    def bind(self, live):
        """Drop the optional columns that the live database does not have.

        Changes the profile in place, so later SELECTs return NULL for those
        columns instead of failing. Each dropped column is logged as a warning.

        :param live: ``{table: {column: type}}`` from the engine's ``live_columns``.
        :returns: the profile itself, for chaining.
        """
        for t, cols in self.columns.items():
            have = live.get(self.tables[t], {})
            for c in [c for c, p in cols.items() if p not in have and is_optional(t, c)]:
                log.warning("%s.%s (%s) not in DB: exported as NULL", self.tables[t], cols[c], c)
                del cols[c]
        return self


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
    # Everything in the file must be a known logical table/column
    for t, spec in doc.get("tables", {}).items():
        if t not in TABLES:
            errors.append("unknown table [tables.%s]" % t)
            continue
        tables[t] = spec["name"]
        columns[t] = dict(spec.get("columns", {}))
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

    :param profile: a :class:`Profile`.
    :param live: ``{table: {column: type}}`` from the engine's ``live_columns``.
    :returns: ``True`` if none is missing.
    """
    return all(
        p in live.get(profile.tables[t], {})
        for t, cols in profile.columns.items() for c, p in cols.items() if not is_optional(t, c))


def detect(live):
    """Pick the bundled profile that matches a live database.

    :param live: ``{table: {column: type}}`` from the engine's ``live_columns``.
    :returns: the only :class:`Profile` whose required physical columns all exist.
    :raises ConfigError: if no profile or more than one matches. Then pass
        ``--profile`` explicitly, or run ``check-schema`` to see the differences.
    """
    matches = [n for n in available_profiles() if _required_present(load_profile(n), live)]
    if len(matches) != 1:
        raise ConfigError("schema autodetect matched %s (of %s); pass --profile or run check-schema"
                          % (matches or "no profile", available_profiles()))
    log.info("autodetected profile %s", matches[0])
    return load_profile(matches[0])


def diff(profile, live, type_family):
    """Compare a profile with a live database (the ``check-schema`` report).

    For each table it reports missing required columns, missing optional
    columns (exported as NULL), type mismatches and extra columns that are not
    exported.

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
            lines.append("%s (%s): TABLE MISSING" % (phys, t))
            ok = False
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
