"""Command line: ``ear-db-export [global flags] <command> [command flags]``.

:func:`main` is the entry point of the ``ear-db-export`` command. Another
program can mount it as one of its own subcommands (see the ``prog`` argument).
A run goes through these steps (:func:`run`):

1. Parse the flags (:func:`parser`).
2. Check every setting before connecting: the connection and node tags from
   ear.conf or the ``--db-*`` flags (:func:`db_settings`), the
   pseudonymisation key (:func:`pseudonym_key`) and the output formats
   (:func:`format_settings`).
3. ``dump`` connects read-only and writes the tables as stored
   (:func:`ear_db_export.dump.dump`); it needs no schema profile.
4. The other commands open the reader (:class:`~ear_db_export.read.sql.SqlReader`):
   connect read-only, choose the schema profile and compare it with the database.
5. ``check-schema`` prints the account's grants and that comparison. Export
   commands refuse a mismatch, then run the pipeline
   (:func:`ear_db_export.pipeline.run`) and log a summary of the files written.

Exit codes: ``0`` success, ``1`` runtime error (database, I/O), ``2``
configuration or schema error (:class:`~ear_db_export.config.ConfigError`).
"""
import argparse
import datetime as dt
import logging
import sys
import time

from . import config, dump, formats, pipeline, write
from .config import ConfigError, DbConf
from .read.sql import SqlReader, mariadb
from .read.sql.profile import available_profiles

log = logging.getLogger("ear_db_export")
#: The ``--db-*`` flags (argparse names). Either all are given or none.
DB_FLAGS = ("db_host", "db_port", "db_name", "db_user", "db_password_file")
#: Commands that write data and therefore need a pseudonymisation decision.
EXPORTS = tuple(pipeline.COMMANDS) + ("dump",)
DESCRIPTION = "Read-only export of EAR data to Parquet datasets."


def _date(s):
    """Parse a ``YYYY-MM-DD`` flag value.

    :param s: the flag value.
    :returns: a ``datetime.date``.
    :raises ValueError: if the value is not an ISO date (argparse reports it).
    """
    return dt.date.fromisoformat(s)


def parser(prog="ear-db-export"):
    """Build the argument parser with the global flags and one subcommand per command.

    :param prog: program name shown in usage and help, so the parser can be
        mounted as a subcommand of another program.
    :returns: an ``argparse.ArgumentParser``.
    """
    p = argparse.ArgumentParser(prog=prog, description=DESCRIPTION)
    # Where the DB is: ear.conf, or the five --db-* flags
    p.add_argument("--conf", help="ear.conf (default: $EAR_ETC/ear/ear.conf or /etc/ear/ear.conf)")
    g = p.add_argument_group("manual DB connection (instead of ear.conf; give all five)")
    g.add_argument("--db-host")
    g.add_argument("--db-port", type=int)
    g.add_argument("--db-name")
    g.add_argument("--db-user")
    g.add_argument("--db-password-file", help="file whose first line is the password")
    # How to read it and where to write
    p.add_argument("--profile", help="schema profile name (%s) or .toml path; default: autodetect"
                   % ", ".join(available_profiles()))
    p.add_argument("--out", default=".", help="output directory (default: .)")
    p.add_argument("--format", action="append", default=[], metavar="DATASET=FORMAT",
                   help="output format of a dataset (default: wide). Repeatable")
    p.add_argument("--format-opt", action="append", default=[], metavar="KEY=VALUE",
                   help="option passed to the formats, e.g. service=NAME for long. Repeatable")
    p.add_argument("--log-level", default="INFO", type=str.upper,
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--dry-run", action="store_true",
                   help="print SQL, EXPLAIN, row counts, formats and target paths; write nothing")
    # Pseudonymisation: a salt, or an explicit opt-out; never both
    s = p.add_mutually_exclusive_group()
    s.add_argument("--salt-file", help="secret key for HMAC pseudonyms of user fields")
    s.add_argument("--no-pseudonymize", action="store_true", help="export user fields in clear")

    # One subparser per command
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("check-schema", help="show the account's grants and diff the profile "
                                        "against the live DB")
    pp = sub.add_parser("periodic", help="daily node metrics + job allocations (cron)")
    pp.add_argument("--lag-days", type=int, default=2)
    pp.add_argument("--state", help="watermark file (default: OUT/.ear_export_state.json)")
    pp.add_argument("--from", dest="from_", type=_date, metavar="YYYY-MM-DD")
    pp.add_argument("--to", type=_date, metavar="YYYY-MM-DD")
    pp.add_argument("--update-state", action="store_true",
                    help="with --from/--to: also advance the watermark")
    for name in ("power-signatures", "app-signatures"):
        sp = sub.add_parser(name, help="one-off monthly dataset (default: last 12 full months)")
        sp.add_argument("--from", dest="from_", type=_date, metavar="YYYY-MM-DD")
        sp.add_argument("--to", type=_date, metavar="YYYY-MM-DD")
        sp.add_argument("--overwrite", action="store_true", help="redo months that exist")
    dp = sub.add_parser("dump", help="tables as stored: OUT/<Table>.parquet, schema.sql, "
                                     "manifest.json")
    dp.add_argument("--tables", help="comma-separated table names (default: every table)")
    return p


def db_settings(args):
    """Work out the database connection and the node tags from the flags.

    Either all ``--db-*`` flags are given, or none and ear.conf is used; a mix
    is refused. With the ``--db-*`` flags, ear.conf is still read if ``--conf``
    is given, but only for the node tags.

    :param args: parsed flags (uses ``conf`` and the :data:`DB_FLAGS`).
    :returns: ``(DbConf, node_tags)``.
    :raises ConfigError: on a partial set of ``--db-*`` flags, or if ear.conf or
        the password file cannot be read.
    """
    given = [f for f in DB_FLAGS if getattr(args, f) is not None]
    conf_path = args.conf or config.default_conf_path()
    # No --db-* flags: everything comes from ear.conf
    if not given:
        return config.load(conf_path)
    if len(given) != len(DB_FLAGS):
        raise ConfigError("manual DB connection needs all of --db-host --db-port --db-name "
                          "--db-user --db-password-file")
    # Read the password from a file so it never appears in `ps`
    try:
        with open(args.db_password_file) as f:
            password = f.readline().rstrip("\n")
    except OSError as e:
        raise ConfigError("cannot read --db-password-file: %s" % e)
    dbconf = DbConf(args.db_host, args.db_port, args.db_name, args.db_user, password)
    tags = {}
    if args.conf:  # optional: only for node_tag
        tags = config.load(args.conf)[1]
    else:
        log.warning("no --conf: node_tag will be NULL")
    return dbconf, tags


def pseudonym_key(args):
    """Read the pseudonymisation key from ``--salt-file``.

    The key is only needed by commands that write data. ``check-schema``,
    ``--dry-run`` and ``--no-pseudonymize`` need none.

    :param args: parsed flags (uses ``command``, ``dry_run``, ``salt_file`` and
        ``no_pseudonymize``).
    :returns: the key as bytes, or ``None`` when user fields are not pseudonymised.
    :raises ConfigError: if an export has neither ``--salt-file`` nor
        ``--no-pseudonymize``, or the salt file is unreadable or empty.
    """
    if args.command not in EXPORTS or args.dry_run or args.no_pseudonymize:
        return None
    if not args.salt_file:
        raise ConfigError("give --salt-file (or --no-pseudonymize to export user names in clear)")
    try:
        with open(args.salt_file, "rb") as f:
            key = f.read().strip()
    except OSError as e:
        raise ConfigError("cannot read --salt-file: %s" % e)
    if not key:
        raise ConfigError("--salt-file %s is empty" % args.salt_file)
    return key


def _pairs(values, flag):
    """Split ``KEY=VALUE`` flag values into a dict.

    :param values: the flag values.
    :param flag: flag name, for the error message.
    :returns: ``{key: value}``.
    :raises ConfigError: if a value has no ``=``.
    """
    bad = [v for v in values if "=" not in v]
    if bad:
        raise ConfigError("%s takes KEY=VALUE, got %s" % (flag, ", ".join(bad)))
    return dict(v.split("=", 1) for v in values)


def format_settings(args):
    """Read and check ``--format`` and ``--format-opt``.

    :param args: parsed flags (uses ``command``, ``format`` and ``format_opt``).
    :returns: ``(formats, opts)``: ``{dataset: format name}`` and ``{key: value}``.
    :raises ConfigError: if a dataset is not written by the command, or a
        format is unknown for it.
    """
    chosen = _pairs(args.format, "--format")
    opts = _pairs(args.format_opt, "--format-opt")
    cmd = pipeline.COMMANDS.get(args.command)
    datasets = cmd.datasets if cmd else ()
    for dataset, name in chosen.items():
        if dataset not in datasets:
            raise ConfigError("--format %s=%s: %s writes %s" % (
                dataset, name, args.command, ", ".join(datasets) or "no datasets"))
        formats.resolve(dataset, name)
    return chosen, opts


def run(args):
    """Run one command with already-parsed flags.

    :param args: parsed flags from :func:`parser`.
    :returns: the exit code: ``0``, or ``2`` when ``check-schema`` finds a mismatch.
    :raises ConfigError: on bad configuration, or if an export finds the
        database does not match the profile.
    """
    # 1. Settings first: fail before connecting if they are wrong
    dbconf, tags = db_settings(args)
    key = pseudonym_key(args)
    chosen, opts = format_settings(args)
    if args.command == "dump":
        return run_dump(args, dbconf, key)
    # 2. Open the reader: connect, choose the profile, compare it with the DB
    reader = SqlReader(dbconf, args.profile, dry_run=args.dry_run)
    try:
        lines, ok = reader.check()
        if args.command == "check-schema":
            print("account %s on %s:%d/%s, session read-only: %s\ngrants:\n  %s" % (
                dbconf.user, dbconf.host, dbconf.port, dbconf.database,
                "yes" if reader.engine.read_only(reader.conn) else "NO",
                "\n  ".join(reader.engine.grants(reader.conn))))
            print("profile %s vs %s:\n%s" % (reader.profile.name, dbconf.database,
                                              "\n".join(lines)))
            return 0 if ok else 2
        if not ok:
            raise ConfigError("schema does not match profile %s:\n%s"
                              % (reader.profile.name, "\n".join(lines)))
        # 3. Read, build, format and write every period of the command
        settings = pipeline.Settings(args.out, tags, key, chosen, opts, args.dry_run)
        pipeline.run(args.command, reader, settings, args)
        _summary(settings.written)
        return 0
    finally:
        reader.close()


def run_dump(args, dbconf, key):
    """Run ``dump``: connect read-only and write the tables as stored (no schema profile).

    :param args: parsed flags (uses ``tables``, ``out`` and ``dry_run``).
    :param dbconf: the :class:`~ear_db_export.config.DbConf`.
    :param key: pseudonymisation key, or ``None``.
    :returns: the exit code, ``0``.
    """
    conn = mariadb.connect(dbconf)
    try:
        tables = args.tables.split(",") if args.tables else \
            sorted(mariadb.live_columns(conn, dbconf.database))
        with write.lock(args.out, args.dry_run):
            manifest = dump.dump(conn, mariadb, dbconf.database, tables, args.out, key,
                                 args.dry_run)
    finally:
        conn.close()
    _summary([(t["file"], t["rows"], t["bytes"]) for t in manifest["tables"].values()])
    return 0


def _summary(written):
    """Log the totals and one line per file written in this run.

    :param written: ``(path, rows, bytes)`` per file, from :class:`~ear_db_export.pipeline.Settings`.
    """
    total_rows = sum(r for _, r, _ in written)
    total_bytes = sum(b for _, _, b in written)
    log.info("summary: %d files, %d rows, %.1f MB", len(written), total_rows,
             total_bytes / 1e6)
    for path, rows, nbytes in written:
        log.info("  %s  %d rows  %.1f MB", path, rows, nbytes / 1e6)


def main(argv=None, prog="ear-db-export"):
    """Entry point: parse the flags, set up logging, run, and map errors to exit codes.

    Logs go to stdout with UTC timestamps. A
    :class:`~ear_db_export.config.ConfigError` is logged as one line (exit
    ``2``); any other exception is logged with its traceback (exit ``1``).

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
    except Exception:
        log.exception("export failed")
        code = 1
    log.info("finished in %.0f s, exit code %d", time.time() - t0, code)
    return code


if __name__ == "__main__":
    sys.exit(main())
