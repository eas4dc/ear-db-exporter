"""Command line: ``slurm-db-export check`` and ``slurm-db-export export {raw,jobs} [flags]``.

``export raw`` runs these steps (:func:`run`):

1. Check the output path template and the window (``--from`` must start the
   period of the template's per-row fields: the first of a month by default).
2. Connect read-only to slurmdbd's database (the ``--db-*`` flags) and pick the
   schema profile (:class:`~slurm_db_export.read.SlurmSource`).
3. Read the ``--salt-file`` key, unless ``--no-pseudonymize``.
4. Run the layout into an :class:`~ear_db_export.output.Output` (the bundle of
   ear-db-export: files, ``SHA256SUMS``, ``checkpoint.json``, ``manifest.json`` last).

``export jobs`` reads a raw bundle instead of the database. ``check`` prints the
account's grants and compares the profile with each cluster's tables.

Flags can be read from a file, one per line, with ``@``:
``slurm-db-export export raw @/etc/slurm-db-export/prod.args --from 2026-09-01 --to 2026-10-13``.

Exit codes: ``0`` success, ``1`` runtime error (database, I/O), ``2``
configuration or schema error (:class:`~ear_db_export.config.ConfigError`).
"""
import argparse
import datetime as dt
import logging
import os
import sys
import time

from ear_db_export.clean import pseudonym
from ear_db_export.config import ConfigError, DbConf
from ear_db_export.output import RUN_FIELDS, ROW_FIELDS, Output, template_fields
from ear_db_export.read.sql import mariadb
from ear_db_export.selection import Window, when, window_from_args

from .layouts import jobs, raw
from .profile import available_profiles
from .read import SlurmSource

log = logging.getLogger("slurm_db_export")
TOOL = "slurm-db-export"
DESCRIPTION = "Read-only export of slurmdbd's accounting database, for scheduling datasets."
#: Default ``--path``: one file per cluster, table and month.
DEFAULT_PATH = "{table}/year={year}/month={month}/part-0.{ext}"
#: The database engine module (tests replace it).
ENGINE = mariadb


def parser(prog=TOOL):
    """Build the argument parser: ``check``, ``export raw`` and ``export jobs``.

    Example: ``parser().parse_args(["export", "raw", "--db-host", "h", "--db-user", "u",
    "--db-password-file", "pw", "--from", "2026-09-01", "--to", "2026-10-13",
    "--out", "o", "--no-pseudonymize"])`` gives ``command="export"``, ``layout="raw"``, …
    """
    p = argparse.ArgumentParser(prog=prog, description=DESCRIPTION, fromfile_prefix_chars="@")
    sub = p.add_subparsers(dest="command", required=True, metavar="COMMAND")
    check = sub.add_parser("check", help="show the account's grants and compare the schema "
                                         "profile with the database")
    _db_arguments(check)
    check.add_argument("--cluster", action="append", metavar="NAME",
                       help="cluster to check (default: every cluster of cluster_table)")
    _log_arguments(check)
    export = sub.add_parser("export", help="write a bundle of one layout")
    layouts = export.add_subparsers(dest="layout", required=True, metavar="LAYOUT")
    r = layouts.add_parser(raw.NAME, help=raw.HELP, description=raw.HELP)
    raw.add_arguments(r)
    _db_arguments(r)
    w = r.add_argument_group("time window: the jobs submitted in it, and the events, "
                             "reservations and usage that started in it")
    w.add_argument("--from", dest="from_", type=when, required=True, metavar="DATE",
                   help="first day YYYY-MM-DD (with the default --path, the first of a month)")
    w.add_argument("--to", type=when, required=True, metavar="DATE",
                   help="last day (included), or a UTC time (excluded); not in the future")
    _output_arguments(r)
    s = r.add_argument_group("pseudonyms").add_mutually_exclusive_group()
    s.add_argument("--salt-file", help="secret key for the HMAC pseudonyms of user, group and "
                                       "account ids")
    s.add_argument("--no-pseudonymize", action="store_true", help="write them as stored")
    r.add_argument("--dry-run", action="store_true",
                   help="print SQL, EXPLAIN, row counts and target paths; write nothing")
    _log_arguments(r)
    j = layouts.add_parser(jobs.NAME, help=jobs.HELP, description=jobs.HELP)
    jobs.add_arguments(j)
    _output_arguments(j)
    _log_arguments(j)
    return p


def _db_arguments(parser):
    """Add the flags that choose slurmdbd's database and the schema profile."""
    g = parser.add_argument_group("database (a read-only account; never slurmdbd's StorageUser)")
    g.add_argument("--db-host", required=True)
    g.add_argument("--db-port", type=int, default=3306)
    g.add_argument("--db-name", default="slurm_acct_db")
    g.add_argument("--db-user", required=True)
    g.add_argument("--db-password-file", required=True, help="file whose first line is the password")
    g.add_argument("--profile", help="schema profile name (%s) or .toml path; default: from "
                                     "convert_version_table" % ", ".join(available_profiles()))


def _output_arguments(parser):
    """Add ``--out`` and ``--path``."""
    g = parser.add_argument_group("output")
    g.add_argument("--out", required=True, metavar="DIR", help="the bundle's directory")
    g.add_argument("--path", default=DEFAULT_PATH, metavar="TEMPLATE",
                   help="file path inside --out, with {table} (cluster/table) {ext} {from} {to}, "
                        "and per row {date} {year} {month} {day} (default %s)"
                        % DEFAULT_PATH.replace("%", "%%"))


def _log_arguments(parser):
    """Add ``--log-level``."""
    parser.add_argument("--log-level", default="INFO", type=str.upper,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])


def dbconf(args):
    """Build the connection settings from the ``--db-*`` flags; the password comes from its file.

    :raises ConfigError: if the password file cannot be read.
    """
    try:  # from a file, so the password never appears in `ps`
        with open(args.db_password_file) as f:
            password = f.readline().rstrip("\n")
    except OSError as e:
        raise ConfigError("cannot read --db-password-file: %s" % e)
    return DbConf(args.db_host, args.db_port, args.db_name, args.db_user, password)


def check_template(template):
    """Check a ``--path`` template: known fields, relative, inside ``--out``, with ``{table}``.

    :raises ConfigError: otherwise.
    """
    fields = template_fields(template)
    unknown = sorted(fields - set(RUN_FIELDS) - set(ROW_FIELDS) - {"layout"})
    if unknown:
        raise ConfigError("--path %s: unknown field(s) %s" % (template, ", ".join(unknown)))
    if os.path.isabs(template) or ".." in template.split("/"):
        raise ConfigError("--path %s: must be a relative path inside --out" % template)
    if "table" not in fields:
        raise ConfigError("--path %s: needs {table}: one file per cluster and table" % template)


def check_window_start(template, window):
    """Refuse a window that would overwrite a period file with part of its period.

    A run rewrites each file it writes whole, so with ``{month}`` in the path a
    window from the 15th would replace a full month file with half a month.

    :raises ConfigError: if ``--from`` is not the first second of the template's
        smallest per-row period (a day, a month or a year).
    """
    fields = template_fields(template)
    start = dt.datetime.fromtimestamp(window.start, dt.timezone.utc)
    if fields & {"date", "day"}:
        ok, period = start.time() == dt.time(0), "a day"
    elif "month" in fields:
        ok, period = start.time() == dt.time(0) and start.day == 1, "a month"
    elif "year" in fields:
        ok, period = start.time() == dt.time(0) and (start.month, start.day) == (1, 1), "a year"
    else:
        return
    if not ok:
        raise ConfigError("--from %s is not the start of %s: the run would overwrite that "
                          "period's file with part of it" % (start.isoformat(), period))


def salt(args):
    """Read the pseudonymisation key from ``--salt-file``; ``None`` with ``--no-pseudonymize`` or a dry run.

    :raises ConfigError: if neither flag is given, or the file is unreadable or empty.
    """
    if args.dry_run or args.no_pseudonymize:
        return None
    if not args.salt_file:
        raise ConfigError("give --salt-file (or --no-pseudonymize to write user ids as stored)")
    try:
        with open(args.salt_file, "rb") as f:
            key = f.read().strip()
    except OSError as e:
        raise ConfigError("cannot read --salt-file: %s" % e)
    if not key:
        raise ConfigError("--salt-file %s is empty" % args.salt_file)
    return key


def run(args):
    """Run one command with already-parsed flags; return the exit code (``2`` when ``check`` finds a mismatch)."""
    if args.command == "check":
        return _check(args)
    check_template(args.path)
    if args.layout == jobs.NAME:
        manifest = jobs.bundle_manifest(args.bundle)
        with Output(args.out, jobs.NAME, args.path, "parquet", Window(),
                    manifest.get("salt_fingerprint"), tool=TOOL) as out:
            jobs.run(args, out, manifest)
            out.finish({"source": {"bundle": os.path.abspath(args.bundle),
                                   "profile": manifest["source"]["profile"],
                                   "window": manifest.get("filters", {}).get("window")}})
        return 0
    window = window_from_args(argparse.Namespace(from_=args.from_, to=args.to, resume=False))
    check_window_start(args.path, window)
    key = salt(args)
    source = SlurmSource(dbconf(args), args.profile, engine=ENGINE, dry_run=args.dry_run,
                         page_rows=args.page_rows)
    try:
        fingerprint = None if key is None else pseudonym(key, TOOL)
        with Output(args.out, raw.NAME, args.path, "parquet", window, fingerprint, args.dry_run,
                    tool=TOOL) as out:
            raw.run(source, args, out, key)
            out.finish({"source": source.info(),
                        "filters": {"window": window.describe(),
                                    "clusters": args.cluster or source.clusters,
                                    "with_usage_hour": args.with_usage_hour,
                                    "with_scripts": args.with_scripts}})
        return 0
    finally:
        source.close()


def _check(args):
    """Print the account, its grants and the profile comparison (``check``); return 0, or 2 on a mismatch."""
    source = SlurmSource(dbconf(args), args.profile, engine=ENGINE)
    try:
        eng, conn = source.engine, source.conn
        print("database %s, session read-only: %s\ngrants:\n  %s" % (
            source.database, "yes" if eng.read_only(conn) else "NO",
            "\n  ".join(eng.grants(conn))))
        clusters = args.cluster or source.clusters
        print("clusters: %s" % ", ".join(clusters or ["none"]))
        lines, ok = source.check(clusters)
        print("\n".join(lines))
        return 0 if ok and clusters else 2
    finally:
        source.close()


def main(argv=None, prog=TOOL):
    """Entry point: parse the flags, set up logging, run, and map errors to exit codes.

    Logs go to stdout with UTC timestamps. A :class:`~ear_db_export.config.ConfigError`
    is logged as one line (exit ``2``); any other exception with its traceback (exit ``1``).

    :param argv: arguments without the program name; ``None`` uses ``sys.argv[1:]``.
    :returns: the exit code.
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
    except Exception:
        log.exception("export failed")
        code = 1
    log.info("finished in %.0f s, exit code %d", time.time() - t0, code)
    return code


if __name__ == "__main__":
    sys.exit(main())
