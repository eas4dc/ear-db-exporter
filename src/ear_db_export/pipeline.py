"""Run ``view`` and ``export``: for each period, read → (dataset → format) → write.

- :func:`run_view`: for each period of :func:`ear_db_export.schedule.periods`,
  build each dataset (:mod:`ear_db_export.canonical`) from the period's
  :class:`~ear_db_export.extract.Records`, shape it (:mod:`ear_db_export.formats`)
  and write one file.
- :func:`run_export`: for each period, write every table
  :func:`ear_db_export.extract.extract` yields as one file of a *bundle*, then
  update the bundle's ``manifest.json`` and ``SHA256SUMS``
  (:func:`ear_db_export.write.finish_bundle`), so a bundle is consistent after
  every period.

User fields are pseudonymised before anything is written: by the datasets for
``view``, here for the physical columns of ``export``.
"""
import datetime as dt
import logging
import os
from dataclasses import dataclass, field, replace
from importlib.metadata import PackageNotFoundError, version

import pyarrow as pa

from . import canonical, extract, formats, schedule, write
from .canonical import USER_COLUMNS, pseudonymize
from .config import ConfigError

log = logging.getLogger(__name__)


@dataclass
class Settings:
    """The choices of one run, from the command line, and the record of what it wrote.

    :ivar out: output directory (``--out``).
    :ivar tags: ``{node: tag}`` from ear.conf (or the bundle), for ``node_tag``.
    :ivar key: pseudonymisation key (bytes), or ``None`` to write user fields as they are.
    :ivar fingerprint: the salt fingerprint recorded in a bundle (``None`` in clear).
    :ivar formats: ``{dataset: format name}`` (``--format``); missing datasets use ``wide``.
    :ivar format_opts: ``{key: value}`` passed to every format (``--format-opt``).
    :ivar dry_run: write nothing (``--dry-run``); the source previews its queries.
    :ivar ext: output file format, ``parquet`` or ``csv`` (``--as``).
    :ivar written: ``(path, rows, bytes)`` of each file written, for the summary.
    """

    out: str
    tags: dict = field(default_factory=dict)
    key: object = None
    fingerprint: object = None
    formats: dict = field(default_factory=dict)
    format_opts: dict = field(default_factory=dict)
    dry_run: bool = False
    ext: str = "parquet"
    written: list = field(default_factory=list)


def run_view(names, source, sel, settings, args):
    """Write datasets, one file per dataset and period.

    :param names: canonical dataset names.
    :param source: the source (with a schema profile).
    :param sel: the :class:`~ear_db_export.selection.Selection`.
    :param settings: the run's :class:`Settings`; files written are appended to ``settings.written``.
    :param args: parsed flags, for the schedule.
    :raises ConfigError: on bad period flags or format options, or if another run
        is writing to the same output directory.
    """
    with write.lock(settings.out, settings.dry_run):  # one run per output directory
        for period in schedule.periods(args, source, settings.out, names, settings.ext):
            records = extract.Records(source, _in(sel, period))
            for name in names:
                _dataset(name, records, period, settings)


def _dataset(name, records, period, settings):
    """Build, format and write one dataset for one period."""
    dataset = canonical.DATASETS[name]
    table = dataset.build(records, settings.tags, settings.key)
    fmt = settings.formats.get(name, "wide")
    tables = formats.apply(name, fmt, table, settings.format_opts)
    path = write.partition_path(settings.out, name, period.partition, settings.ext)
    if settings.dry_run:
        log.info("[dry-run] would write %s as %s to %s", name, fmt, path)
        return
    rows, nbytes = _write(path, tables, settings.ext, dataset.delimiter)
    log.info("wrote %s (%s): %d rows, %d bytes", path, fmt, rows, nbytes)
    settings.written.append((path, rows, nbytes))
    if rows == 0 and dataset.empty_warning:
        log.warning("%s %s: %s", name, _label(period), dataset.empty_warning)


def run_export(source, sel, settings, args):
    """Write a bundle: the selected tables as stored, one file per table and period.

    :param source: the source.
    :param sel: the :class:`~ear_db_export.selection.Selection`.
    :param settings: the run's :class:`Settings`.
    :param args: parsed flags, for the schedule.
    :raises ConfigError: on bad flags, a bundle in ``--out`` with another salt,
        filters without a schema profile, or another run on the same directory.
    """
    names = extract.tables(sel, source.provides)
    timed = [t for t in names if t not in extract.WHOLE]
    if not settings.dry_run:
        write.check_bundle(settings.out, settings.fingerprint)
    manifest = {"format": 1, "tool": "ear-db-export " + _version(), "created": _now(),
                "source": source.info(), "selection": sel.to_json(settings.key),
                "every": args.every, "files": settings.ext,
                "pseudonymized": settings.fingerprint is not None,
                "salt_fingerprint": settings.fingerprint, "node_tags": settings.tags}
    ddl = source.ddl(names)
    with write.lock(settings.out, settings.dry_run):
        for period in schedule.periods(args, source, settings.out, timed, settings.ext):
            psel = _in(sel, period)
            if period.partition:  # whole tables are written once, after the periods
                psel = replace(psel, tables=tuple(timed))
            files = {}
            for table, parts in extract.extract(source, psel):
                files[table] = _table(table, parts, period.partition, settings)
            if not settings.dry_run:
                write.finish_bundle(settings.out, dict(manifest, finished=_now()), ddl, files)
                ddl = ""  # schema.sql is written once
        whole = [t for t in names if t in extract.WHOLE]
        if whole and args.every:
            files = {t: _table(t, source.scan(t), {}, settings) for t in whole}
            if not settings.dry_run:
                write.finish_bundle(settings.out, dict(manifest, finished=_now()), ddl, files)


def _table(table, parts, partition, settings):
    """Write one table of a bundle (pseudonymised); return its ``{relative path: file info}``."""
    path = write.partition_path(settings.out, table, partition, settings.ext)
    if settings.dry_run:
        for _ in parts:  # runs the source's previews
            pass
        log.info("[dry-run] would write %s", path)
        return {}
    tables = (pseudonymize(t, settings.key, USER_COLUMNS) for t in parts)
    rows, nbytes = _write(path, tables, settings.ext)
    log.info("wrote %s: %d rows, %d bytes", path, rows, nbytes)
    settings.written.append((path, rows, nbytes))
    rel = os.path.relpath(path, settings.out)
    return {rel: {"rows": rows, "bytes": nbytes, "sha256": write.sha256(path)}}


def _write(path, tables, ext, delimiter=","):
    """Write tables to a Parquet or CSV file; ``(rows, bytes)``."""
    if ext == "parquet":
        return write.parquet(path, tables)
    try:
        return write.csv(path, tables, delimiter)
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid) as e:
        raise ConfigError("%s cannot be written as CSV (%s): use Parquet" % (path, e))


def _in(sel, period):
    """The Selection limited to a period (or as it is, for a run without --every)."""
    return sel if period.start is None else sel.between(period.start, period.end)


def _label(period):
    """``date=2026-07-04`` / ``month=2026-07`` / ``""`` for log lines."""
    if not period.partition:
        return ""
    (key, value), = period.partition.items()
    return "%s=%s" % (key, value.strftime("%Y-%m") if key == "month" else value.isoformat())


def _now():
    """The current UTC time, ISO 8601, to the second."""
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _version():
    """The installed version of this package."""
    try:
        return version("ear-db-export")
    except PackageNotFoundError:
        return "unknown"
