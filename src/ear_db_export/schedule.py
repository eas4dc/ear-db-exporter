"""Which periods a command exports: days for ``periodic``, months for the signature commands.

A schedule is a generator of :class:`Period`. The pipeline exports every
dataset of a period, then asks for the next one, so code after a ``yield``
runs only once the period is fully written. :func:`daily` uses that to move
the *watermark* (the last day fully exported, saved in
``OUT/.ear_export_state.json``) after each day: a failed run stops before the
save and resumes from the failed day.
"""
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass

from .config import ConfigError
from .write import partition_path

log = logging.getLogger(__name__)

#: Watermark file name, inside ``--out`` unless ``--state`` is given.
STATE_FILE = ".ear_export_state.json"
#: Warn when the oldest pending day is this close to the prune horizon.
PRUNE_WARN_DAYS = 7


@dataclass(frozen=True)
class Period:
    """One period to export.

    :ivar start: start, Unix seconds (included).
    :ivar end: end, Unix seconds (excluded).
    :ivar partition: ``{"date": day}`` or ``{"month": first day}``, for
        :func:`ear_db_export.write.partition_path`.
    """

    start: int
    end: int
    partition: dict


# == Schedules: (reader, settings, args, datasets) -> iterator of Period ============

def daily(reader, settings, args, datasets):
    """Yield the days ``periodic`` exports, and move the watermark after each one.

    Without ``--from/--to``: every day after the watermark up to ``today -
    --lag-days``; on the first run, from the oldest day still in the database
    (the *prune horizon*: EAR deletes older ``Periodic_metrics`` rows). With
    ``--from/--to``: exactly those days, and the watermark moves only with
    ``--update-state``. The watermark never moves backwards, nor on a dry-run.

    :param reader: the :class:`~ear_db_export.read.Reader`, for the prune horizon.
    :param settings: the run's :class:`~ear_db_export.pipeline.Settings`.
    :param args: parsed flags (``state``, ``from_``, ``to``, ``lag_days``, ``update_state``).
    :param datasets: unused (same signature as :func:`monthly`).
    :raises ConfigError: on an invalid ``--from/--to`` range.
    """
    horizon = reader.oldest_day()
    if horizon is None:
        log.warning("Periodic_metrics is empty: nothing to export")
        return
    log.info("oldest Periodic_metrics row ends on %s (prune horizon)", horizon)
    state = args.state or os.path.join(settings.out, STATE_FILE)
    watermark = load_watermark(state)
    days = pending_days(args, watermark, horizon, dt.datetime.now(dt.timezone.utc).date())
    if not days:
        log.info("up to date (watermark %s, lag %d days)", watermark, args.lag_days)
        return
    if days[0] < horizon + dt.timedelta(PRUNE_WARN_DAYS):
        log.warning("earliest pending day %s is within %d days of the prune horizon %s: "
                    "export soon or data will be lost", days[0], PRUNE_WARN_DAYS, horizon)
    manual = bool(args.from_)
    for day in days:
        log.info("exporting %s", day)
        yield Period(day_epoch(day), day_epoch(day + dt.timedelta(1)), {"date": day})
        # The day is written: advance the watermark, unless this is a manual re-export
        if settings.dry_run or (manual and not args.update_state):
            continue
        if watermark is None or day > watermark:  # never move the watermark backwards
            save_watermark(state, day)
            watermark = day


def monthly(reader, settings, args, datasets):
    """Yield the calendar months a signature command exports.

    Default range: the last 12 complete months. Only whole, finished months
    are exported, so every partition holds a complete month: ``--from`` and
    ``--to`` are widened to the months they fall in, and a month that has not
    ended yet is skipped. A month whose partitions all exist is skipped unless
    ``--overwrite`` is given.

    :param reader: unused (same signature as :func:`daily`).
    :param settings: the run's :class:`~ear_db_export.pipeline.Settings`.
    :param args: parsed flags (``from_``, ``to``, ``overwrite``).
    :param datasets: the command's datasets; a month is done when all of them are written.
    :raises ConfigError: if ``--from`` is after ``--to``.
    """
    today = dt.datetime.now(dt.timezone.utc).date()
    default_from, default_to = default_range(today)
    frm, to = args.from_ or default_from, args.to or default_to
    if frm > to:
        raise ConfigError("--from %s is after --to %s" % (frm, to))
    ms = months(frm, to)
    if (frm, to) != (ms[0], _next_month(ms[-1]) - dt.timedelta(1)):
        log.info("exporting whole months: %s .. %s", ms[0].strftime("%Y-%m"),
                 ms[-1].strftime("%Y-%m"))
    for month in ms:
        name = month.strftime("%Y-%m")
        if _next_month(month) > today:
            log.warning("%s has not ended yet: skipped", name)
            continue
        # Skip finished months unless asked to redo them
        paths = [partition_path(settings.out, d, {"month": month}) for d in datasets]
        if all(os.path.exists(p) for p in paths) and not args.overwrite:
            log.info("%s exists, skipping (use --overwrite to redo)", name)
            continue
        log.info("exporting %s for %s", ", ".join(datasets), name)
        yield Period(day_epoch(month), day_epoch(_next_month(month)), {"month": month})


# == Days and months ===============================================================

def day_epoch(day):
    """Return the Unix time of 00:00 UTC of a day.

    :param day: ``datetime.date``.
    :returns: seconds since the epoch, as int.
    """
    return int(dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc).timestamp())


def _days(first, last):
    """List every day from ``first`` to ``last``, both included (empty if ``last < first``)."""
    return [first + dt.timedelta(n) for n in range((last - first).days + 1)]


def pending_days(args, watermark, horizon_day, today):
    """Decide which days ``periodic`` exports.

    :param args: parsed flags (uses ``from_``, ``to`` and ``lag_days``).
    :param watermark: last exported day, or ``None`` on the first run.
    :param horizon_day: day of the oldest row in ``Periodic_metrics``.
    :param today: today's UTC date.
    :returns: the days of ``--from .. --to`` if given; otherwise from the day
        after the watermark (or the horizon on a first run) to
        ``today - lag_days``.
    :raises ConfigError: if only one of ``--from``/``--to`` is given, or
        ``--from`` is after ``--to``.
    """
    # Explicit range: exactly what was asked for
    if args.from_ or args.to:
        if not (args.from_ and args.to) or args.from_ > args.to:
            raise ConfigError("periodic needs both --from and --to, with --from <= --to")
        return _days(args.from_, args.to)
    # Incremental: continue after the watermark, or start at the oldest data
    first = watermark + dt.timedelta(1) if watermark else horizon_day
    if watermark is None:
        log.warning("no state file: starting at the oldest day in the DB (%s), which is "
                    "partial because older rows were pruned", first)
    return _days(first, today - dt.timedelta(args.lag_days))


def _month_start(d):
    """Return the first day of the month of ``d``."""
    return d.replace(day=1)


def _next_month(d):
    """Return the first day of the month after the month of ``d``."""
    return (d.replace(day=28) + dt.timedelta(4)).replace(day=1)  # day 28 + 4 is always next month


def default_range(today):
    """Return the default range of the signature commands: the last 12 complete calendar months.

    :param today: today's date.
    :returns: ``(first day, last day)`` of the range, both ``datetime.date``.
    """
    to = _month_start(today) - dt.timedelta(1)
    frm = _month_start(to)
    for _ in range(11):
        frm = _month_start(frm - dt.timedelta(1))
    return frm, to


def months(frm, to):
    """List the calendar months a date range touches.

    :param frm: first day, ``datetime.date``.
    :param to: last day (inclusive), ``datetime.date``.
    :returns: the first day of each month, from the month of ``frm`` to the month of ``to``.
    """
    m, out = _month_start(frm), []
    while m <= to:
        out.append(m)
        m = _next_month(m)
    return out


# == Watermark =====================================================================

def load_watermark(path):
    """Read the watermark: the last fully exported day, or ``None`` if the file does not exist."""
    try:
        with open(path) as f:
            return dt.date.fromisoformat(json.load(f)["last_exported_day"])
    except FileNotFoundError:
        return None


def save_watermark(path, day):
    """Write the watermark (``{"last_exported_day": "YYYY-MM-DD"}``) atomically: ``.tmp`` + rename."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_exported_day": day.isoformat()}, f)
    os.replace(tmp, path)
