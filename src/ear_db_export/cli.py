"""Command line: ``ear-db-export check`` and ``ear-db-export export LAYOUT [flags]``.

:func:`main` is the entry point of the ``ear-db-export`` command. Another
program can mount it as one of its own subcommands (the ``prog`` argument).

``export LAYOUT`` runs these steps (:func:`run`), the same for every layout:

1. Parse the flags. Each layout is a subcommand with its own flags
   (:data:`ear_db_export.layouts.LAYOUTS`), so it accepts only the filters it uses.
2. Check the output paths (``--path``/``--hive``) and open the source: a
   bundle (``--bundle``), eacct files (``--eacct``), or EAR's database (ear.conf
   or the ``--db-*`` flags), with the node tags of ear.conf.
3. Wrap the source in :class:`~ear_db_export.read.pseudonymized.Pseudonymized`
   with the ``--salt-file`` key, unless ``--no-pseudonymize`` or its user fields
   are already pseudonyms.
4. Work out the time window: ``--from/--to``, or ``--resume`` from the
   bundle's checkpoint (nothing to do when it is up to date).
5. Run the layout into an :class:`~ear_db_export.output.Output`, then finish the
   bundle: checkpoint, checksums, and the manifest last.

``check`` prints the account's grants and compares the schema profile with the
database.

Flags can be read from a file, one per line (``--out=/data/ear``), with ``@``:
``ear-db-export export node_metrics @/etc/ear-db-export/daily.args --resume --to 2026-10-12``.

Exit codes: ``0`` success, ``1`` runtime error (database, I/O), ``2``
configuration or schema error (:class:`~ear_db_export.config.ConfigError`).
"""
import os

# Arrow's default allocator (mimalloc) keeps freed memory: a run's RSS grew to 2-4
# times the data it held. Arrow reads this once, when pyarrow is imported, so it
# is set before any import that loads pyarrow; a value already set is kept.
os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")

import argparse  # noqa: E402
import logging  # noqa: E402
import re  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402

from . import __version__, config  # noqa: E402
from .clean import pseudonym  # noqa: E402
from .config import ConfigError, DbConf, DbError  # noqa: E402
from .layouts import LAYOUTS  # noqa: E402
from .output import Output, read_checkpoint, template_for  # noqa: E402
from .read.bundle import BundleSource  # noqa: E402
from .read.eacct import EacctSource  # noqa: E402
from .read.pseudonymized import Pseudonymized, fingerprint  # noqa: E402
from .read.sql import SqlSource, mariadb  # noqa: E402
from .read.sql.profile import available_profiles  # noqa: E402
from .selection import split, window_from_args  # noqa: E402

log = logging.getLogger("ear_db_export")
#: The ``--db-*`` flags (argparse names). Either all are given or none.
DB_FLAGS = ("db_host", "db_port", "db_name", "db_user", "db_password_file")
#: The flags recorded in a bundle's manifest, as given (user names pseudonymised).
FILTER_FLAGS = ("tables", "jobs", "users", "accounts", "groups", "apps", "etags", "policies",
                "nodes", "tags", "batch_steps", "limit")
#: The filter flags whose values are user fields, pseudonyms in a pseudonymised source.
USER_FLAGS = ("users", "accounts", "groups")
DESCRIPTION = "Read-only export of EAR data: its tables as stored, or layouts for analysis."
#: The database engine module (tests replace it).
ENGINE = mariadb


def parser(prog="ear-db-export"):
    """Build the argument parser: ``check``, and ``export`` with one subcommand per layout.

    :param prog: program name shown in usage and help, so the parser can be
        mounted as a subcommand of another program.
    :returns: an ``argparse.ArgumentParser``.

    Example: ``parser().parse_args(["export", "node_metrics", "--service", "x",
    "--out", "o", "--resume", "--to", "2026-10-12"])`` gives ``command="export"``,
    ``layout="node_metrics"``, ``resume=True``, …
    """
    p = argparse.ArgumentParser(prog=prog, description=DESCRIPTION, fromfile_prefix_chars="@")
    p.add_argument("--version", action="version", version="%(prog)s " + __version__)
    sub = p.add_subparsers(dest="command", required=True, metavar="COMMAND")
    check = sub.add_parser("check", help="show the account's grants and compare the schema "
                                         "profile with the database")
    _db_arguments(check.add_argument_group("database (default: ear.conf's read-only account)"))
    _log_arguments(check)
    export = sub.add_parser("export", help="write a bundle of one layout",
                            description="Write a bundle of one layout: its files, SHA256SUMS, "
                                        "checkpoint.json and manifest.json.")
    layouts = export.add_subparsers(dest="layout", required=True, metavar="LAYOUT")
    for layout in LAYOUTS.values():
        lp = layouts.add_parser(layout.NAME, help=layout.HELP,
                                description="%s.\nReads: %s." % (layout.HELP, layout.READS),
                                formatter_class=argparse.RawDescriptionHelpFormatter)
        layout.add_arguments(lp)
        _source_arguments(lp)
        _output_arguments(lp)
        _log_arguments(lp)
    return p


def _db_arguments(group):
    """Add the flags that choose EAR's database (``--conf``, ``--db-*``, ``--profile``) to an argument group."""
    group.add_argument("--conf", help="ear.conf (default: $EAR_ETC/ear/ear.conf or /etc/ear/ear.conf)")
    group.add_argument("--db-host")
    group.add_argument("--db-port", type=int)
    group.add_argument("--db-name")
    group.add_argument("--db-user")
    group.add_argument("--db-password-file", help="file whose first line is the password")
    group.add_argument("--profile", help="schema profile name (%s) or .toml path; default: "
                                         "autodetect" % ", ".join(available_profiles()))


def _source_arguments(parser):
    """Add the source flags of ``export``: the database's, or ``--bundle`` / ``--eacct`` instead."""
    g = parser.add_argument_group("source (default: EAR's database, with ear.conf's read-only account)")
    _db_arguments(g)
    other = g.add_mutually_exclusive_group()
    other.add_argument("--bundle", metavar="DIR", help="read a bundle of `export raw` instead")
    other.add_argument("--eacct", metavar="FILE", action="append",
                       help="read `eacct -l -c` CSV files instead (repeat for several)")


def _output_arguments(parser):
    """Add the output flags of ``export``: ``--out``, ``--as``, paths, pseudonyms and ``--dry-run``."""
    g = parser.add_argument_group("output")
    g.add_argument("--out", required=True, metavar="DIR", help="the bundle's directory")
    g.add_argument("--as", dest="as_", choices=["parquet", "csv"], default="parquet",
                   help="file format (default: parquet)")
    paths = g.add_mutually_exclusive_group()
    paths.add_argument("--path", metavar="TEMPLATE",
                       help="file path inside --out, with fields {layout} {table} {ext} {from} {to}, "
                            "and per row {date} {year} {month} {day} (default {layout}.{ext}; "
                            "{table}.{ext} for raw)")
    paths.add_argument("--hive", action="store_true",
                       help="one file per day: --path {layout}/date={date}/part-0.{ext}")
    salt = g.add_mutually_exclusive_group()  # a salt, or an explicit opt-out; never both
    salt.add_argument("--salt-file", help="secret key for the HMAC pseudonyms of user fields")
    salt.add_argument("--no-pseudonymize", action="store_true", help="write user fields in clear")
    g.add_argument("--dry-run", action="store_true",
                   help="print SQL, EXPLAIN, row counts and target paths; write nothing")


def _log_arguments(parser):
    """Add ``--log-level``."""
    parser.add_argument("--log-level", default="INFO", type=str.upper,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])


def db_settings(args):
    """Work out the database connection and the node tags from the flags.

    Either all ``--db-*`` flags are given, or none and ear.conf is used; a mix
    is refused. With the ``--db-*`` flags, ear.conf is still read if ``--conf``
    is given, but only for the node tags.

    :param args: parsed flags (``conf`` and the :data:`DB_FLAGS`).
    :returns: ``(DbConf, node_tags)``.
    :raises ConfigError: on a partial set of ``--db-*`` flags, or if ear.conf or
        the password file cannot be read.
    """
    given = [f for f in DB_FLAGS if getattr(args, f) is not None]
    if not given:
        return config.load(args.conf or config.default_conf_path())
    if len(given) != len(DB_FLAGS):
        raise ConfigError("manual DB connection needs all of --db-host --db-port --db-name "
                          "--db-user --db-password-file")
    try:  # read the password from a file, so it never appears in `ps`
        with open(args.db_password_file) as f:
            password = f.readline().rstrip("\n")
    except OSError as e:
        raise ConfigError("cannot read --db-password-file: %s" % e)
    dbconf = DbConf(args.db_host, args.db_port, args.db_name, args.db_user, password)
    if not args.conf:
        log.warning("no --conf: node_tag will be NULL and --tags is unavailable")
    return dbconf, (config.load(args.conf)[1] if args.conf else {})


def open_source(args):
    """Open the source of ``export``: the bundle of ``--bundle``, the files of ``--eacct``, or EAR's database.

    The node tags come from ``--conf``; a bundle keeps its own when ``--conf`` is not given.
    """
    if args.bundle or args.eacct:
        tags = config.load(args.conf)[1] if args.conf else None
        if args.bundle:
            return BundleSource(args.bundle, args.profile, tags=tags)
        return EacctSource(args.eacct, tags=tags)
    dbconf, tags = db_settings(args)
    return SqlSource(dbconf, args.profile, engine=ENGINE, dry_run=args.dry_run, tags=tags)


def pseudonym_key(args, salt_fingerprint=None):
    """Read the pseudonymisation key from ``--salt-file``.

    A dry run, ``--no-pseudonymize`` and a source whose user fields are already
    pseudonyms need none.

    :param args: parsed flags (``dry_run``, ``salt_file``, ``no_pseudonymize``).
    :param salt_fingerprint: the source's, not ``None`` when it holds pseudonyms.
    :returns: the key as bytes, or ``None`` when user fields are written as they are.
    :raises ConfigError: if neither ``--salt-file`` nor ``--no-pseudonymize`` is
        given, or the salt file is unreadable or empty.
    """
    if args.dry_run or args.no_pseudonymize or salt_fingerprint is not None:
        return None
    if not args.salt_file:
        raise ConfigError("give --salt-file (or --no-pseudonymize to write user names in clear)")
    return read_salt(args.salt_file)


def read_salt(path):
    """Read the pseudonymisation key from a salt file, as bytes.

    :raises ConfigError: if the file is unreadable or empty.
    """
    try:
        with open(path, "rb") as f:
            key = f.read().strip()
    except OSError as e:
        raise ConfigError("cannot read --salt-file: %s" % e)
    if not key:
        raise ConfigError("--salt-file %s is empty" % path)
    return key


def filters_as_pseudonyms(args, salt_fingerprint):
    """On a source that holds pseudonyms (a pseudonymised bundle), make ``--users/--accounts/--groups`` pseudonyms too.

    With ``--salt-file`` the names given are pseudonymised with it, which must be
    the salt the source was made with. Without it, each value must already be a
    pseudonym (16 hex digits): a name would silently match nothing.

    :param args: parsed flags; their user flags are replaced in place.
    :param salt_fingerprint: the source's, ``None`` when it holds names in clear
        (then nothing changes).
    :raises ConfigError: on a salt that is not the source's, or a name without ``--salt-file``.

    Example: on a bundle made with salt S, ``--users alice --salt-file S`` filters
    by ``pseudonym(S, "alice")``; ``--users alice`` alone is refused.
    """
    given = [name for name in USER_FLAGS if getattr(args, name, None)]
    if salt_fingerprint is None or not given:
        return
    if args.salt_file:
        key = read_salt(args.salt_file)
        if fingerprint(key) != salt_fingerprint:
            raise ConfigError("--salt-file is not the salt the source was pseudonymised with")
        for name in given:
            setattr(args, name, ",".join(pseudonym(key, v) for v in split(getattr(args, name))))
        return
    names = [v for name in given for v in split(getattr(args, name))
             if not re.fullmatch("[0-9a-f]{16}", v)]
    if names:
        raise ConfigError("the source holds pseudonyms, so %s would match nothing: give --salt-file "
                          "to filter by name, or the pseudonyms" % ", ".join(names))


def describe_filters(args, key, window):
    """Return the run's filters for the manifest: the window, and each filter flag given.

    :param args: parsed flags.
    :param key: the pseudonymisation key: user names, accounts and groups are
        recorded as pseudonyms; ``None`` records them as given.
    :param window: the run's :class:`~ear_db_export.selection.Window`.
    :returns: ``{"window": {"from", "to"}, flag: value, ...}``.
    """
    out = {"window": window.describe()}
    for name in FILTER_FLAGS:
        value = getattr(args, name, None)
        if value:
            out[name] = value
    for name in ("users", "accounts", "groups"):
        if key is not None and name in out:
            out[name] = ",".join(pseudonym(key, v) for v in split(out[name]))
    return out


def run(args):
    """Run one command with already-parsed flags.

    :param args: parsed flags from :func:`parser`.
    :returns: the exit code: ``0``, or ``2`` when ``check`` finds a mismatch.
    :raises ConfigError: on bad configuration, or a database that does not match
        its profile when the layout reads by logical names.
    """
    if args.command == "check":
        return _check(args)
    layout = LAYOUTS[args.layout]
    template = template_for(layout.NAME, args.path, args.hive)
    source = open_source(args)
    try:
        filters_as_pseudonyms(args, source.salt_fingerprint)
        key = pseudonym_key(args, source.salt_fingerprint)
        if key is not None:
            source = Pseudonymized(source, key)
        window = window_from_args(args, read_checkpoint(args.out) if args.resume else None)
        if window.empty:  # --resume: the bundle is up to date
            return 0
        with Output(args.out, layout.NAME, template, args.as_, window, source.salt_fingerprint,
                    args.dry_run, filters=describe_filters(args, key, window)) as out:
            layout.run(source, args, out)
            out.finish({"source": source.info(), "node_tags": source.tags})
        return 0
    finally:
        source.close()


def _check(args):
    """Print the account, its grants and the profile comparison (``check``); return 0, or 2 on a mismatch."""
    dbconf, _ = db_settings(args)
    source = SqlSource(dbconf, args.profile, engine=ENGINE)
    try:
        eng, conn = source.engine, source.conn
        print("database %s, session read-only: %s\ngrants:\n  %s" % (
            source.database, "yes" if eng.read_only(conn) else "NO", "\n  ".join(eng.grants(conn))))
        lines, ok = source.check()
        print("profile %s vs %s:\n%s" % (source.profile.name if source.profile else "(none)",
                                          source.database, "\n".join(lines)))
        return 0 if ok else 2
    finally:
        source.close()


def main(argv=None, prog="ear-db-export"):
    """Entry point: parse the flags, set up logging, run, and map errors to exit codes.

    Logs go to stdout with UTC timestamps. A
    :class:`~ear_db_export.config.ConfigError` is logged as one line (exit
    ``2``), a :class:`~ear_db_export.config.DbError` too (exit ``1``); any other
    exception is logged with its traceback (exit ``1``).

    :param argv: arguments without the program name; ``None`` uses ``sys.argv[1:]``.
    :param prog: program name for usage and help, when mounted as a subcommand
        of another program.
    :returns: the exit code (``0``, ``1`` or ``2``).
    :raises SystemExit: from argparse, for ``--help`` or invalid flags.
    """
    args = parser(prog).parse_args(argv)
    logging.basicConfig(stream=sys.stdout, level=args.log_level,
                        format="%(asctime)s %(levelname)s %(message)s")
    logging.Formatter.converter = time.gmtime  # UTC timestamps in logs
    t0 = time.time()
    try:
        code = run(args)
    except ConfigError as e:
        log.error("%s", e)
        code = 2
    except DbError as e:
        log.error("%s", e)
        code = 1
    except Exception:
        log.exception("%s failed", args.command)
        code = 1
    log.info("finished in %.0f s, exit code %d", time.time() - t0, code)
    return code


if __name__ == "__main__":
    sys.exit(main())
