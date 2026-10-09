"""Which periods a run writes: one output, or one partition per day or month (``--every``).

A schedule is a generator of :class:`Period`. The pipeline writes every table
or dataset of a period, then asks for the next one, so code after a ``yield``
runs only once the period is fully written. :func:`periods` uses that to move
the *watermark* (the last day fully exported, saved in
``OUT/.ear_export_state.json``) after each period: a failed run stops before
the save and resumes from the failed period.

- ``--every day --incremental``: every day after the watermark up to
  ``today - --lag-days``; the first run starts at the oldest day still in the
  database (the *prune horizon*: EAR deletes old ``Periodic_metrics`` rows).
  This is the daily timer.
- ``--every day|month --from A --to B``: exactly those days, or the whole
  months they touch; partitions that exist are skipped unless ``--overwrite``,
  and the watermark moves only with ``--update-state``.
- ``--every month`` alone: the last 12 complete months.
- no ``--every``: one output for the whole selection.

Only finished months are written, so every month partition is complete.
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
    """One period to write.

    :ivar start: start, Unix seconds (included), or ``None`` (no time bound).
    :ivar end: end, Unix seconds (excluded), or ``None``.
    :ivar partition: ``{"date": day}``, ``{"month": first day}``, or ``{}`` (one output),
        for :func:`ear_db_export.write.partition_path`.
    """

    start: object
    end: object
    partition: dict


def periods(args, source, out, names, ext="parquet", today=None):
    """Yield the periods of a run, and move the watermark after each one when asked.

    :param args: parsed flags (``every``, ``incremental``, ``from_``, ``to``,
        ``lag_days``, ``state``, ``update_state``, ``overwrite``, ``dry_run``).
    :param source: the source, for the prune horizon (``oldest_day()``).
    :param out: output directory: partitions that exist, and the default watermark file.
    :param names: tables or datasets of the run; a period is done when all are written.
    :param ext: output file extension.
    :param today: today's UTC date (default: now).
    :raises ConfigError: on an invalid combination of flags or date range.
    """
    _check_flags(args)
    if not args.every:
        yield Period(None, None, {})
        return
    today = today or dt.datetime.now(dt.timezone.utc).date()
    state = args.state or os.path.join(out, STATE_FILE)
    watermark = load_watermark(state)
    if args.incremental:
        horizon = source.oldest_day()
        if horizon is None:
            log.warning("Periodic_metrics is empty: nothing to export")
            return
        log.info("oldest Periodic_metrics row ends on %s (prune horizon)", horizon)
        if watermark is None:
            log.warning("no state file: starting at the oldest day in the DB (%s), which is "
                        "partial because older rows were pruned", horizon)
        first = watermark + dt.timedelta(1) if watermark else horizon
        last = today - dt.timedelta(args.lag_days)
        if first <= last and first < horizon + dt.timedelta(PRUNE_WARN_DAYS):
            log.warning("earliest pending day %s is within %d days of the prune horizon %s: "
                        "export soon or data will be lost", first, PRUNE_WARN_DAYS, horizon)
    elif args.every == "month":
        default_from, default_to = default_range(today)
        first, last = args.from_ or default_from, args.to or default_to
    elif args.from_ and args.to:
        first, last = args.from_, args.to
    else:
        raise ConfigError("--every day needs --from and --to, or --incremental")
    if first > last:
        if args.incremental:
            log.info("up to date (watermark %s, lag %d days)", watermark, args.lag_days)
            return
        raise ConfigError("--from %s is after --to %s" % (first, last))
    for p0, p1 in _split(args.every, first, last):
        partition = {"date": p0} if args.every == "day" else {"month": p0}
        label = p0.isoformat() if args.every == "day" else p0.strftime("%Y-%m")
        if p1 > today:
            log.warning("%s has not ended yet: skipped", label)
            continue
        if not args.incremental and not args.overwrite and \
                all(os.path.exists(partition_path(out, n, partition, ext)) for n in names):
            log.info("%s exists, skipping (use --overwrite to redo)", label)
            continue
        log.info("exporting %s", label)
        yield Period(day_epoch(p0), day_epoch(p1), partition)
        # The period is written: advance the watermark, never backwards, never on a dry-run
        done = p1 - dt.timedelta(1)
        if not args.dry_run and (args.incremental or args.update_state) and \
                (watermark is None or done > watermark):
            save_watermark(state, done)
            watermark = done


def _check_flags(args):
    """Refuse the combinations of period flags that have no meaning."""
    if not args.every and (args.incremental or args.update_state):
        raise ConfigError("--incremental and --update-state need --every")
    if args.incremental and (args.from_ or args.to):
        raise ConfigError("--incremental continues after the watermark: no --from/--to")
    if args.every and any(isinstance(d, dt.datetime) for d in (args.from_, args.to)):
        raise ConfigError("--every takes whole days: give --from/--to as YYYY-MM-DD")


def _split(every, first, last):
    """Yield ``(start, end)`` days of each day, or month, from ``first`` to ``last`` (included)."""
    if every == "day":
        for d in _days(first, last):
            yield d, d + dt.timedelta(1)
        return
    ms = months(first, last)
    if (first, last) != (ms[0], _next_month(ms[-1]) - dt.timedelta(1)):
        log.info("exporting whole months: %s .. %s", ms[0].strftime("%Y-%m"),
                 ms[-1].strftime("%Y-%m"))
    for m in ms:
        yield m, _next_month(m)


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
