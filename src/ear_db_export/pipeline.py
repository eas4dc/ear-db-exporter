"""Run an export: for each period of a command, read → canonical dataset → format → write.

A command is only two choices, listed in :data:`COMMANDS`: which canonical
datasets it writes, and which schedule picks its periods
(:mod:`ear_db_export.schedule`). :func:`run` then does the same for every
command and period:

1. **Read**: a :class:`~ear_db_export.read.Records` for the period, which
   reads each record table from the reader the first time a dataset needs it.
2. **Canonical dataset**: ``canonical.DATASETS[name].build(records, tags, key)``.
3. **Format**: ``formats.apply(name, --format, table, --format-opt)``.
4. **Write**: ``write.parquet`` to ``write.partition_path(--out, name, period)``,
   or a "would write" line on a dry-run.

No step knows how the others work: the pipeline only passes their outputs on.
"""
import logging
from dataclasses import dataclass, field

from . import canonical, formats, schedule, write
from .config import ConfigError
from .read import Records

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Command:
    """What an export command does.

    :ivar datasets: canonical datasets it writes, in order.
    :ivar schedule: ``schedule(reader, settings, args, datasets)``, a generator
        of :class:`~ear_db_export.schedule.Period`.
    """

    datasets: tuple
    schedule: object


#: Export commands of the command line.
COMMANDS = {
    "periodic": Command(("node_metrics", "job_nodes"), schedule.daily),
    "power-signatures": Command(("power_signatures",), schedule.monthly),
    "app-signatures": Command(("app_signatures", "gpu_signatures"), schedule.monthly),
}


@dataclass
class Settings:
    """The choices of one run, from the command line, and the record of what it wrote.

    :ivar out: output directory (``--out``).
    :ivar tags: ``{node: tag}`` from ear.conf, for ``node_tag``.
    :ivar key: pseudonymisation key (bytes), or ``None`` to export user fields in clear.
    :ivar formats: ``{dataset: format name}`` (``--format``); missing datasets use ``wide``.
    :ivar format_opts: ``{key: value}`` passed to every format (``--format-opt``).
    :ivar dry_run: write nothing (``--dry-run``); the reader previews its queries.
    :ivar written: ``(path, rows, bytes)`` of each file written, for the summary.
    """

    out: str
    tags: dict = field(default_factory=dict)
    key: object = None
    formats: dict = field(default_factory=dict)
    format_opts: dict = field(default_factory=dict)
    dry_run: bool = False
    written: list = field(default_factory=list)


def run(command, reader, settings, args):
    """Run one export command.

    :param command: a key of :data:`COMMANDS`.
    :param reader: a :class:`~ear_db_export.read.Reader`.
    :param settings: the run's :class:`Settings`; files written are appended to ``settings.written``.
    :param args: parsed flags, for the schedule (date range, watermark, overwrite).
    :raises ConfigError: if the reader cannot provide a record table the
        datasets need, on a bad date range or format option, or if another run
        is writing to the same output directory.
    """
    cmd = COMMANDS[command]
    # Refuse before reading anything if the reader lacks a needed record table
    needed = {t for name in cmd.datasets for t in canonical.DATASETS[name].inputs}
    if needed - reader.provides:
        raise ConfigError("%s needs %s, which this reader cannot read"
                          % (command, ", ".join(sorted(needed - reader.provides))))
    with write.lock(settings.out, settings.dry_run):  # one run per output directory
        for period in cmd.schedule(reader, settings, args, cmd.datasets):
            records = Records(reader, period.start, period.end)           # 1. read
            for name in cmd.datasets:
                export(name, records, period, settings)


def export(name, records, period, settings):
    """Build, format and write one dataset for one period (steps 2-4).

    :param name: canonical dataset name.
    :param records: the period's :class:`~ear_db_export.read.Records`.
    :param period: the :class:`~ear_db_export.schedule.Period`.
    :param settings: the run's :class:`Settings`.
    """
    dataset = canonical.DATASETS[name]
    table = dataset.build(records, settings.tags, settings.key)         # 2. canonical
    fmt = settings.formats.get(name, "wide")
    tables = formats.apply(name, fmt, table, settings.format_opts)      # 3. format
    path = write.partition_path(settings.out, name, **period.partition)
    if settings.dry_run:
        log.info("[dry-run] would write %s as %s to %s", name, fmt, path)
        return
    rows, nbytes = write.parquet(path, tables)                          # 4. write
    log.info("wrote %s (%s): %d rows, %d bytes", path, fmt, rows, nbytes)
    settings.written.append((path, rows, nbytes))
    if rows == 0 and dataset.empty_warning:
        (key, value), = period.partition.items()
        log.warning("%s %s=%s: %s", name, key, value, dataset.empty_warning)
