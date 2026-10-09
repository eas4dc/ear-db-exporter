"""Command line: ``ear-db-export <check|export|view> [flags]``.

:func:`main` is the entry point of the ``ear-db-export`` command. Another
program can mount it as one of its own subcommands (see the ``prog`` argument).
A run goes through these steps (:func:`run`):

1. Parse the flags (:func:`parser`).
2. Check every setting before reading anything: the source (a bundle with
   ``--bundle``, else the database from ear.conf or the ``--db-*`` flags,
   :func:`db_settings`), the pseudonymisation key (:func:`pseudonym_key`) and
   the output formats (:func:`format_settings`).
3. Open the source. For the database: connect read-only, choose the schema
   profile and compare it with the database.
4. ``check`` prints the account's grants and that comparison. ``view`` and a
   filtered ``export`` refuse a mismatch, build the
   :class:`~ear_db_export.selection.Selection` and run the pipeline
   (:mod:`ear_db_export.pipeline`), then log a summary of the files written.

Exit codes: ``0`` success, ``1`` runtime error (database, I/O), ``2``
configuration or schema error (:class:`~ear_db_export.config.ConfigError`).
"""
import argparse
import logging
import sys
import time

from . import canonical, config, extract, formats, pipeline, selection
from .config import ConfigError, DbConf
from .read.bundle import BundleSource
from .read.sql import SqlSource, mariadb
from .read.sql.profile import available_profiles

log = logging.getLogger("ear_db_export")
#: The ``--db-*`` flags (argparse names). Either all are given or none.
DB_FLAGS = ("db_host", "db_port", "db_name", "db_user", "db_password_file")
DESCRIPTION = "Read-only export of EAR data: bundles of its tables, and analysis datasets."
#: The database engine module (tests replace it).
ENGINE = mariadb


def _common():
    """Flags of every command: where the data is, logging, dry-run."""
    p = argparse.ArgumentParser(add_help=False)
    g = p.add_argument_group("source")
    g.add_argument("--conf", help="ear.conf (default: $EAR_ETC/ear/ear.conf or /etc/ear/ear.conf)")
    g.add_argument("--db-host")
    g.add_argument("--db-port", type=int)
    g.add_argument("--db-name")
    g.add_argument("--db-user")
    g.add_argument("--db-password-file", help="file whose first line is the password")
    g.add_argument("--profile", help="schema profile name (%s) or .toml path; default: autodetect"
                   % ", ".join(available_profiles()))
    p.add_argument("--log-level", default="INFO", type=str.upper,
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--dry-run", action="store_true",
                   help="print SQL, EXPLAIN, row counts and target paths; write nothing")
    return p


def _data():
    """Flags of the commands that read data: bundle, filters, periods, output."""
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--bundle", metavar="DIR", help="read this bundle instead of the database")
    f = p.add_argument_group("filters (AND across flags, OR within a list)")
    f.add_argument("--from", dest="from_", type=selection.when, metavar="DATE",
                   help="YYYY-MM-DD (whole day) or YYYY-MM-DDTHH:MM[:SS] (UTC)")
    f.add_argument("--to", type=selection.when, metavar="DATE",
                   help="last day (included), or an exact UTC time (excluded)")
    f.add_argument("--jobs", help="job ids: 123,456.0 (step),1000-2000,789.sb (batch step)")
    for name in ("users", "accounts", "groups", "etags", "policies"):
        f.add_argument("--" + name, help="comma-separated list")
    f.add_argument("--apps", help="application names; * and ? wildcards")
    f.add_argument("--nodes", help="nodes, as a Slurm hostlist: n[01-04],gpu1")
    f.add_argument("--tags", help="nodes of these ear.conf Island tags")
    f.add_argument("--batch-steps", action="store_true",
                   help="only the batch step of each job (job granularity)")
    f.add_argument("--sample", type=int, metavar="N", help="N jobs picked at random (see --seed)")
    f.add_argument("--seed", type=int, default=0)
    f.add_argument("--limit", type=int, metavar="N", help="the newest N jobs")
    o = p.add_argument_group("output")
    o.add_argument("--out", default=".", help="output directory (default: .)")
    o.add_argument("--as", dest="as_", choices=["parquet", "csv"], default="parquet",
                   help="file format (default: parquet)")
    s = o.add_mutually_exclusive_group()  # a salt, or an explicit opt-out; never both
    s.add_argument("--salt-file", help="secret key for HMAC pseudonyms of user fields")
    s.add_argument("--no-pseudonymize", action="store_true", help="write user fields in clear")
    t = p.add_argument_group("periods")
    t.add_argument("--every", choices=["day", "month"],
                   help="one partition per day or month (default: one output)")
    t.add_argument("--incremental", action="store_true",
                   help="continue after the last exported day (watermark); for timers")
    t.add_argument("--lag-days", type=int, default=2,
                   help="with --incremental: stop this many days before today (default: 2)")
    t.add_argument("--state", help="watermark file (default: OUT/.ear_export_state.json)")
    t.add_argument("--update-state", action="store_true",
                   help="with --from/--to: also advance the watermark")
    t.add_argument("--overwrite", action="store_true", help="redo partitions that exist")
    return p


def parser(prog="ear-db-export"):
    """Build the argument parser: one subcommand per command, flags after the command.

    :param prog: program name shown in usage and help, so the parser can be
        mounted as a subcommand of another program.
    :returns: an ``argparse.ArgumentParser``.
    """
    p = argparse.ArgumentParser(prog=prog, description=DESCRIPTION)
    sub = p.add_subparsers(dest="command", required=True)
    common, data = _common(), _data()
    sub.add_parser("check", parents=[common], help="show the account's grants and compare the "
                                                   "schema profile with the database")
    e = sub.add_parser("export", parents=[common, data],
                       help="a bundle: the selected tables as stored, schema.sql, SHA256SUMS, "
                            "manifest.json")
    e.add_argument("--tables", help="table groups (%s, all) or table names; default jobs,periodic"
                   % ", ".join(extract.GROUPS))
    v = sub.add_parser("view", parents=[common, data], help="analysis datasets")
    v.add_argument("names", nargs="+", choices=sorted(canonical.DATASETS), metavar="DATASET",
                   help="one or more of: %s" % ", ".join(sorted(canonical.DATASETS)))
    v.add_argument("--format", action="append", default=[], metavar="DATASET=FORMAT",
                   help="output format of a dataset (default: wide). Repeatable")
    v.add_argument("--format-opt", action="append", default=[], metavar="KEY=VALUE",
                   help="option passed to the formats, e.g. service=NAME for long. Repeatable")
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
    if not given:
        return config.load(args.conf or config.default_conf_path())
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
    tags = config.load(args.conf)[1] if args.conf else {}
    if not args.conf:
        log.warning("no --conf: node_tag will be NULL")
    return dbconf, tags


def pseudonym_key(args, source_pseudonymized=False):
    """Read the pseudonymisation key from ``--salt-file``.

    A dry-run, ``--no-pseudonymize`` and a bundle whose user fields are already
    pseudonyms need none.

    :param args: parsed flags (uses ``dry_run``, ``salt_file`` and ``no_pseudonymize``).
    :param source_pseudonymized: the source is a pseudonymised bundle.
    :returns: the key as bytes, or ``None`` when user fields are written as they are.
    :raises ConfigError: if neither ``--salt-file`` nor ``--no-pseudonymize`` is
        given, or the salt file is unreadable or empty.
    """
    if args.dry_run or args.no_pseudonymize or source_pseudonymized:
        return None
    if not args.salt_file:
        raise ConfigError("give --salt-file (or --no-pseudonymize to write user names in clear)")
    try:
        with open(args.salt_file, "rb") as f:
            key = f.read().strip()
    except OSError as e:
        raise ConfigError("cannot read --salt-file: %s" % e)
    if not key:
        raise ConfigError("--salt-file %s is empty" % args.salt_file)
    return key


def _pairs(values, flag):
    """Split ``KEY=VALUE`` flag values into a dict; ConfigError if a value has no ``=``."""
    bad = [v for v in values if "=" not in v]
    if bad:
        raise ConfigError("%s takes KEY=VALUE, got %s" % (flag, ", ".join(bad)))
    return dict(v.split("=", 1) for v in values)


def format_settings(args):
    """Read and check ``--format`` and ``--format-opt`` of ``view``.

    :returns: ``(formats, opts)``: ``{dataset: format name}`` and ``{key: value}``.
    :raises ConfigError: if a dataset is not written by this run, or a format is unknown for it.
    """
    if args.command != "view":
        return {}, {}
    chosen = _pairs(args.format, "--format")
    opts = _pairs(args.format_opt, "--format-opt")
    for dataset, name in chosen.items():
        if dataset not in args.names:
            raise ConfigError("--format %s=%s: this run writes %s" % (dataset, name,
                                                                      ", ".join(args.names)))
        formats.resolve(dataset, name)
    return chosen, opts


def open_source(args):
    """Open the source: the bundle of ``--bundle``, or the database.

    :returns: ``(source, node_tags)``.
    """
    if getattr(args, "bundle", None):
        source = BundleSource(args.bundle, args.profile)
        tags = config.load(args.conf)[1] if args.conf else source.tags
        return source, tags
    dbconf, tags = db_settings(args)
    return SqlSource(dbconf, args.profile, engine=ENGINE, dry_run=args.dry_run,
                     need_profile=False), tags


def run(args):
    """Run one command with already-parsed flags.

    :param args: parsed flags from :func:`parser`.
    :returns: the exit code: ``0``, or ``2`` when ``check`` finds a mismatch.
    :raises ConfigError: on bad configuration, or a database that does not match
        its profile when the command needs one.
    """
    chosen, opts = format_settings(args)
    source, tags = open_source(args)
    try:
        if args.command == "check":
            return _check(source)
        key = pseudonym_key(args, getattr(source, "pseudonymized", False))
        sel = selection.from_args(args, tags)
        _refuse_mismatch(source, args.command == "view" or sel.filters() or bool(args.every))
        fingerprint = canonical.pseudonym(key, "ear-db-export") if key else \
            (source.manifest.get("salt_fingerprint") if isinstance(source, BundleSource) else None)
        settings = pipeline.Settings(args.out, tags, key, fingerprint, chosen, opts, args.dry_run,
                                     args.as_)
        if args.command == "export":
            pipeline.run_export(source, sel, settings, args)
        else:
            pipeline.run_view(args.names, source, sel, settings, args)
        _summary(settings.written)
        return 0
    finally:
        source.close()


def _check(source):
    """Print the account, its grants and the profile comparison (``check``)."""
    if isinstance(source, BundleSource):
        raise ConfigError("check reads the database, not a bundle")
    eng, conn = source.engine, source.conn
    print("database %s, session read-only: %s\ngrants:\n  %s" % (
        source.database, "yes" if eng.read_only(conn) else "NO", "\n  ".join(eng.grants(conn))))
    lines, ok = source.check()
    print("profile %s vs %s:\n%s" % (source.profile.name if source.profile else "(none)",
                                      source.database, "\n".join(lines)))
    return 0 if ok else 2


def _refuse_mismatch(source, needed):
    """Refuse to read through a schema profile that does not match the database."""
    if not needed or isinstance(source, BundleSource):
        return
    lines, ok = source.check()
    if source.profile is None:
        raise ConfigError("%s; give --profile (filters and datasets need one)" % lines[0])
    if not ok:
        raise ConfigError("schema does not match profile %s:\n%s" % (source.profile.name,
                                                                     "\n".join(lines)))


def _summary(written):
    """Log the totals and one line per file written in this run."""
    total_rows = sum(r for _, r, _ in written)
    total_bytes = sum(b for _, _, b in written)
    log.info("summary: %d files, %d rows, %.1f MB", len(written), total_rows, total_bytes / 1e6)
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
