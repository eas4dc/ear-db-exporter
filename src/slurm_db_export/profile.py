"""Schema profiles: which slurmdbd tables and columns are exported, and how each Slurm version names them.

The exporter uses *logical* table and column names (:data:`TABLES`), the same
for every Slurm version. A profile (``profiles/<name>.toml``) says how one range
of slurmdbd schema versions differs from them:

.. code-block:: toml

   convert = [5, 7]                   # convert_version_table.version range
   [renames.job]
   het_job_id = "pack_job_id"         # logical = physical
   [optional]
   job = ["submit_line"]              # may be missing: exported as NULL, with a warning
   [extra]
   step = ["max_rss"]                 # more columns, this profile only (optional)

A column that is not renamed keeps its name. Slurm renamed only two things since
17.11 (``pack_job_*`` → ``het_job_*`` in 20.02; the step usage columns →
``tres_usage_*`` in 18.08), so two profiles cover 17.11 to 25.05: ``slurm17``
and ``slurm20``. Slurm adds columns in most releases; a column added after the
first release of a profile's range is optional in it.

Tables :data:`TABLES` maps to ``None`` (events, reservations, the dimension
tables) are exported with every column the database has, so they need no profile.

To support a new Slurm version: compare its DDL (``check`` prints the
differences), then add the new columns to ``optional``, or copy a profile.
"""
import logging
import os
from dataclasses import dataclass, field

try:
    import tomllib
except ImportError:  # Python < 3.11
    import tomli as tomllib

from ear_db_export.config import ConfigError

log = logging.getLogger(__name__)

#: Directory of the bundled profiles (``<name>.toml``).
PROFILE_DIR = os.path.join(os.path.dirname(__file__), "profiles")

#: Logical tables and the logical columns exported from each; ``None``: every column.
TABLES = {
    "job": ("job_db_inx", "id_job", "id_array_job", "id_array_task", "het_job_id",
            "het_job_offset", "id_user", "id_group", "account", "id_assoc", "id_qos", "id_wckey",
            "id_resv", "partition", "job_name", "constraints", "flags", "submit_line",
            "time_submit", "time_eligible", "time_start", "time_end", "time_suspended",
            "timelimit", "tres_req", "tres_alloc", "cpus_req", "mem_req", "nodes_alloc",
            "nodelist", "node_inx", "state", "exit_code", "priority", "kill_requid",
            "state_reason_prev", "deleted", "mod_time", "script_hash_inx"),
    "step": ("job_db_inx", "id_step", "step_het_comp", "time_start", "time_end", "state",
             "exit_code", "nodes_alloc", "tres_alloc", "tres_usage_in_ave", "tres_usage_in_max",
             "tres_usage_in_tot", "tres_usage_out_ave", "tres_usage_out_max",
             "tres_usage_out_tot", "user_sec", "user_usec", "sys_sec", "sys_usec"),
    "event": None,
    "resv": None,
    "assoc": None,
    "assoc_usage_hour": None,
    "job_script": None,
    "qos": None,
    "tres": None,
}
#: Tables of one cluster, named ``<cluster>_<table>_table``; the others are global (``<table>_table``).
PER_CLUSTER = frozenset({"job", "step", "event", "resv", "assoc", "assoc_usage_hour",
                         "job_script"})


def physical_table(table, cluster):
    """Return the physical name of a logical table.

    Example: ``physical_table("job", "lenox.hu")`` is ``"lenox.hu_job_table"``;
    ``physical_table("tres", "lenox.hu")`` is ``"tres_table"``.
    """
    return "%s_%s_table" % (cluster, table) if table in PER_CLUSTER else "%s_table" % table


@dataclass
class Profile:
    """How one range of slurmdbd schema versions names the logical columns.

    :ivar name: the file name without ``.toml`` (``slurm20``).
    :ivar convert: ``(lowest, highest)`` ``convert_version_table.version`` of the range.
    :ivar renames: ``{table: {logical column: physical column}}`` for the columns
        whose name differs.
    :ivar optional: ``{table: set of logical columns}`` that may be missing.
    :ivar extra: ``{table: [logical columns]}`` this profile exports besides :data:`TABLES`.
    """

    name: str
    convert: tuple
    renames: dict = field(default_factory=dict)
    optional: dict = field(default_factory=dict)
    extra: dict = field(default_factory=dict)

    def columns(self, table):
        """Return the logical columns exported from a table: :data:`TABLES` plus :attr:`extra`.

        :returns: a list, or ``None`` for a table exported with every column.
        """
        base = TABLES[table]
        return None if base is None else list(base) + list(self.extra.get(table, ()))

    def col(self, table, column):
        """Return the physical name of a logical column."""
        return self.renames.get(table, {}).get(column, column)

    def is_optional(self, table, column):
        """Tell whether a logical column may be missing from the database."""
        return column in self.optional.get(table, ()) or column in self.extra.get(table, ())

    def select(self, table, have):
        """Choose the physical columns to read from a table, given the columns it has.

        :param table: logical table name.
        :param have: the physical columns of the live table, in table order.
        :returns: ``(physical columns, missing)``: what to read (in :meth:`columns`
            order, or the table's own for a table exported whole), and the
            logical columns the profile wants but the table lacks.
        :raises ConfigError: if a missing column is not optional.

        Example, ``slurm17`` on a 17.11 ``job_table``::

            p.select("job", live_columns)
            # (["job_db_inx", ..., "pack_job_id", ...], ["constraints", "flags", ...])
        """
        wanted = self.columns(table)
        if wanted is None:
            return list(have), []
        have = set(have)
        missing = [c for c in wanted if self.col(table, c) not in have]
        required = [c for c in missing if not self.is_optional(table, c)]
        if required:
            raise ConfigError("%s table lacks required column(s) %s of profile %s: run check, "
                              "or give --profile" % (table, ", ".join(required), self.name))
        return [self.col(table, c) for c in wanted if c not in missing], missing

    def to_logical(self, table, t):
        """Rename the physical columns of a ``pyarrow.Table`` to their logical names (others are kept)."""
        names = {p: c for c, p in self.renames.get(table, {}).items()}
        return t.rename_columns([names.get(c, c) for c in t.column_names])


def load_profile(name_or_path):
    """Load a profile: a bundled name (``slurm20``) or a path ending in ``.toml``.

    :raises ConfigError: if the file cannot be read, or names a table or column
        :data:`TABLES` does not have.
    """
    path = name_or_path if name_or_path.endswith(".toml") else \
        os.path.join(PROFILE_DIR, name_or_path + ".toml")
    try:
        with open(path, "rb") as f:
            doc = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise ConfigError("cannot load profile %s: %s" % (path, e))
    p = Profile(os.path.basename(path)[:-len(".toml")], tuple(doc.get("convert", (0, 0))),
                doc.get("renames", {}), {t: set(c) for t, c in doc.get("optional", {}).items()},
                doc.get("extra", {}))
    errors = []
    for section in ("renames", "optional", "extra"):
        for t, cols in getattr(p, section).items():
            if TABLES.get(t) is None:
                errors.append("[%s.%s]: no such table with a column list" % (section, t))
            elif section != "extra":
                errors += ["[%s.%s] %s: unknown column" % (section, t, c)
                           for c in cols if c not in p.columns(t)]
    if errors:
        raise ConfigError("profile %s is invalid:\n  %s" % (path, "\n  ".join(errors)))
    return p


def available_profiles():
    """Return the bundled profile names, sorted (``["slurm17", "slurm20"]``)."""
    return sorted(f[:-5] for f in os.listdir(PROFILE_DIR) if f.endswith(".toml"))


def detect(version, job_columns):
    """Pick the bundled profile of a database.

    :param version: ``convert_version_table.version``, or ``None`` if unreadable.
    :param job_columns: the physical columns of one cluster's ``job_table``, for
        a database without a readable version.
    :returns: the :class:`Profile` whose ``convert`` range holds ``version``; the
        newest one, with a warning, for a version newer than every range.
    :raises ConfigError: if no profile fits.

    Example: ``detect(16, ...)`` is ``slurm20`` (Slurm 24.11 or 25.05).
    """
    profiles = [load_profile(n) for n in available_profiles()]
    if version is not None:
        for p in profiles:
            if p.convert[0] <= version <= p.convert[1]:
                log.info("schema version %d: profile %s", version, p.name)
                return p
        newest = max(profiles, key=lambda p: p.convert[1])
        if version > newest.convert[1]:
            log.warning("schema version %d is newer than every profile: using %s; run check",
                        version, newest.name)
            return newest
        raise ConfigError("schema version %d is older than Slurm 17.11: no profile" % version)
    for p in profiles:  # no version: the job table's het/pack column tells them apart
        if p.col("job", "het_job_id") in job_columns:
            log.info("no schema version: profile %s by its columns", p.name)
            return p
    raise ConfigError("cannot tell the Slurm version: give --profile (%s)"
                      % ", ".join(available_profiles()))


#: ``convert_version_table.version`` → the Slurm releases that write it (from as_mysql_convert.c).
RELEASES = {5: "17.11", 6: "18.08", 7: "19.05", 8: "20.02", 9: "20.11", 10: "21.08",
            11: "21.08 or 22.05", 12: "22.05", 13: "23.02", 14: "23.11", 15: "23.11 or 24.05",
            16: "24.11 or 25.05"}


def release(version, job_columns):
    """Estimate the Slurm release of a database, for the manifest.

    Two pairs of releases share a schema version; a column added by the later one
    tells them apart (``std_err`` in 24.05, ``resv_req`` in 25.05).

    :param version: ``convert_version_table.version``, or ``None``.
    :param job_columns: the physical columns of one cluster's ``job_table``.
    :returns: for example ``"25.05 or later"``, or ``None`` if unknown.
    """
    if version == 15:
        return "24.05" if "std_err" in job_columns else "23.11"
    if version == 16:
        return "25.05 or later" if "resv_req" in job_columns else "24.11"
    return RELEASES.get(version)
