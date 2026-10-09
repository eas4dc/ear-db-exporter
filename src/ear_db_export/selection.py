"""Which rows a run reads: the filters of the command line, as one :class:`Selection`.

Filters on job steps (``--jobs``, ``--users``, ``--apps``, …) select rows of
``Jobs``; :mod:`ear_db_export.extract` then follows EAR's links to the rows of
the other tables. ``--nodes``/``--tags`` keep the rows of those nodes, and
``--from``/``--to`` bound jobs by their end time and the other rows by their
own time. Different flags combine with AND, the values of one flag with OR.

Filters are written as pyarrow's DNF filter tuples on *logical* column names,
``[[("user_id", "in", ["alice"]), ("end_time", ">=", 1783136226)], ...]``: an
OR of ANDs, which a source translates to SQL or to a pyarrow expression.
``like`` (SQL ``LIKE`` syntax) is the one operator pyarrow's DNF lacks.
"""
import dataclasses
import datetime as dt
import re

from .canonical import pseudonym
from .config import ConfigError, expand_hostlist

#: The batch step of a job, which EAR stores per job: ``SLURM_BATCH_SCRIPT``
#: (Slurm >= 20.11) and ``NULL_STEPID`` (older Slurm, PBS, no scheduler).
BATCH_STEPS = (0xFFFFFFFB, 0xFFFFFFFE)
_JOB = re.compile(r"(\d+)(?:-(\d+))?(?:\.(\d+|sb|sbatch))?")


@dataclasses.dataclass(frozen=True)
class Selection:
    """The filters of one run. Every field is optional; an empty Selection is the whole database.

    :ivar start: first second, Unix time (included), or ``None``.
    :ivar end: last second, Unix time (excluded), or ``None``.
    :ivar jobs: ``((first job id, last job id, step ids), ...)`` from :func:`parse_jobs`;
        empty step ids mean every step.
    :ivar users, accounts, groups, etags, policies: values of the ``Jobs`` columns
        ``user_id``, ``user_acc``, ``user_group``, ``e_tag``, ``policy``.
    :ivar apps: ``app_name`` patterns (``*`` and ``?`` wildcards).
    :ivar nodes: node names.
    :ivar batch_steps: keep only the batch step of each job (job granularity).
    :ivar sample: keep this many jobs, picked at random with ``seed`` (0: all).
    :ivar seed: seed of ``sample``.
    :ivar limit: keep the newest this many jobs, by end time (0: all), like ``eacct -n``.
    :ivar tables: table groups or physical table names to read (``jobs``, ``periodic``, …).
    """

    start: object = None
    end: object = None
    jobs: tuple = ()
    users: tuple = ()
    accounts: tuple = ()
    groups: tuple = ()
    apps: tuple = ()
    etags: tuple = ()
    policies: tuple = ()
    nodes: tuple = ()
    batch_steps: bool = False
    sample: int = 0
    seed: int = 0
    limit: int = 0
    tables: tuple = ("jobs", "periodic")

    def job_filters(self):
        """Tell whether any filter selects job steps (all filters but time and nodes)."""
        return bool(self.jobs or self.users or self.accounts or self.groups or self.apps or
                    self.etags or self.policies or self.batch_steps or self.sample or self.limit)

    def filters(self):
        """Tell whether the Selection filters anything at all."""
        return self.job_filters() or self.start is not None or self.end is not None or \
            bool(self.nodes)

    def steps_named(self):
        """Tell whether rows must match job steps, not only jobs (``--jobs 123.0``, ``.sb``)."""
        return any(steps for _, _, steps in self.jobs)

    def job_where(self):
        """Return the filter on ``jobs`` records, as DNF on logical column names.

        :returns: ``[[(column, op, value), ...], ...]``; ``[[]]`` selects every job.
        """
        base = time_where("end_time", self.start, self.end)
        for col, values in (("user_id", self.users), ("user_acc", self.accounts),
                            ("user_group", self.groups), ("e_tag", self.etags),
                            ("policy", self.policies)):
            if values:
                base.append((col, "in", list(values)))
        if self.batch_steps:
            base.append(("step_id", "in", list(BATCH_STEPS)))
        ors = [base]
        for alternatives in (self._job_alternatives(), self._app_alternatives()):
            if alternatives:
                ors = [conj + alt for conj in ors for alt in alternatives]
        return ors

    def _job_alternatives(self):
        out = []
        for lo, hi, steps in self.jobs:
            alt = [("job_id", "=", lo)] if lo == hi else [("job_id", ">=", lo), ("job_id", "<=", hi)]
            out.append(alt + ([("step_id", "in", list(steps))] if steps else []))
        return out

    def _app_alternatives(self):
        return [[("app_name", "like", glob_to_like(a))] if "*" in a or "?" in a
                else [("app_name", "=", a)] for a in self.apps]

    def between(self, start, end):
        """Return the Selection limited to ``[start, end)`` (a period of ``--every``).

        :param start: Unix time of the period start.
        :param end: Unix time of the period end.
        :returns: a new Selection whose time bounds are the intersection.
        """
        lo = start if self.start is None else max(start, self.start)
        hi = end if self.end is None else min(end, self.end)
        return dataclasses.replace(self, start=lo, end=hi)

    def to_json(self, key=None):
        """Return the Selection for a manifest, with users, accounts and groups pseudonymised.

        :param key: pseudonymisation key, or ``None`` to keep them in clear.
        :returns: a JSON-serialisable dict.
        """
        out = dataclasses.asdict(self)
        if key is not None:
            for f in ("users", "accounts", "groups"):
                out[f] = [pseudonym(key, v) for v in out[f]]
        return out


def time_where(col, start, end):
    """Return the AND conditions ``start <= col < end`` (either bound may be ``None``)."""
    out = []
    if start is not None:
        out.append((col, ">=", start))
    if end is not None:
        out.append((col, "<", end))
    return out


def parse_jobs(text):
    """Parse a ``--jobs`` value, in ``eacct -j`` syntax plus ranges.

    :param text: comma-separated ``JOB``, ``JOB.STEP``, ``FIRST-LAST`` or
        ``JOB.sb`` (the batch step, also ``.sbatch``), for example
        ``"123,456.0,1000-2000,789.sb"``.
    :returns: ``((first, last, steps), ...)``: ``steps`` is ``()`` for every
        step, ``(step,)``, or :data:`BATCH_STEPS`.
    :raises ConfigError: on a token that is not one of these forms.
    """
    out = []
    for token in (t.strip() for t in text.split(",")):
        m = _JOB.fullmatch(token)
        if not m:
            raise ConfigError("--jobs: %r is not JOB, JOB.STEP, FIRST-LAST or JOB.sb" % token)
        lo, hi, step = m.groups()
        lo, hi = int(lo), int(hi or lo)
        if hi < lo:
            raise ConfigError("--jobs: descending range %r" % token)
        steps = () if step is None else BATCH_STEPS if step in ("sb", "sbatch") else (int(step),)
        out.append((lo, hi, steps))
    return tuple(out)


def glob_to_like(glob):
    """Turn a shell-style pattern (``*``, ``?``) into a SQL ``LIKE`` pattern.

    ``%``, ``_`` and ``\\`` in the name are escaped with ``\\``, the escape
    character of MariaDB's ``LIKE`` and of ``pyarrow.compute.match_like``.

    :param glob: for example ``"lmp_*"``.
    :returns: for example ``"lmp\\_%"``.
    """
    escaped = glob.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped.replace("*", "%").replace("?", "_")


def when(text):
    """Parse a ``--from``/``--to`` value: a day, or a date and time in UTC.

    :param text: ``YYYY-MM-DD``, or ``YYYY-MM-DDTHH:MM[:SS]`` (a space instead
        of ``T`` and a trailing ``Z`` are accepted).
    :returns: a ``datetime.date``, or a timezone-aware ``datetime.datetime`` in UTC.
    :raises ValueError: if it is neither (argparse reports it).
    """
    text = text.strip()
    if len(text) == 10:
        return dt.date.fromisoformat(text)
    t = dt.datetime.fromisoformat(text[:-1] if text.endswith("Z") else text)
    return t.replace(tzinfo=dt.timezone.utc) if t.tzinfo is None else t.astimezone(dt.timezone.utc)


def epoch(value, end=False):
    """Return the Unix time of a :func:`when` value.

    :param value: a date or an aware datetime, or ``None``.
    :param end: for a date, return the end of that day (the next midnight), so
        ``--to`` includes the whole day.
    :returns: seconds, int, or ``None``.
    """
    if value is None:
        return None
    if not isinstance(value, dt.datetime):
        value = dt.datetime(value.year, value.month, value.day, tzinfo=dt.timezone.utc) + \
            dt.timedelta(days=1 if end else 0)
    return int(value.timestamp())


def nodes_of(hostlist, tags, node_tags):
    """Resolve ``--nodes`` and ``--tags`` into node names.

    :param hostlist: ``--nodes`` value (a Slurm-style hostlist) or ``None``.
    :param tags: ``--tags`` values (comma-separated string) or ``None``.
    :param node_tags: ``{node: tag}`` from ear.conf.
    :returns: sorted node names (empty: no node filter).
    :raises ConfigError: on a bad hostlist, or ``--tags`` without any tag in ear.conf.
    """
    nodes = set()
    if hostlist:
        try:
            nodes.update(expand_hostlist(hostlist))
        except ValueError as e:
            raise ConfigError("--nodes: %s" % e)
    if tags:
        if not node_tags:
            raise ConfigError("--tags needs the Island tags of ear.conf (give --conf)")
        wanted = set(split(tags))
        tagged = {n for n, t in node_tags.items() if t in wanted}
        if not tagged:
            raise ConfigError("--tags %s: no node has these tags in ear.conf" % tags)
        nodes.update(tagged)
    return tuple(sorted(nodes))


def split(value):
    """Split a comma-separated flag value into its non-empty items."""
    return tuple(v.strip() for v in (value or "").split(",") if v.strip())


def from_args(args, node_tags):
    """Build the Selection of the parsed command-line flags.

    :param args: parsed flags (``from_``, ``to``, ``jobs``, ``users``, ``accounts``,
        ``groups``, ``apps``, ``etags``, ``policies``, ``nodes``, ``tags``,
        ``batch_steps``, ``sample``, ``seed``, ``limit``, ``tables``).
    :param node_tags: ``{node: tag}`` from ear.conf, for ``--tags``.
    :returns: a :class:`Selection`.
    :raises ConfigError: on an invalid value, or ``--from`` not before ``--to``.
    """
    start, end = epoch(args.from_), epoch(args.to, end=True)
    if start is not None and end is not None and start >= end:
        raise ConfigError("--from %s is not before --to %s" % (args.from_, args.to))
    return Selection(
        start=start, end=end, jobs=parse_jobs(args.jobs) if args.jobs else (),
        users=split(args.users), accounts=split(args.accounts), groups=split(args.groups),
        apps=split(args.apps), etags=split(args.etags), policies=split(args.policies),
        nodes=nodes_of(args.nodes, args.tags, node_tags), batch_steps=args.batch_steps,
        sample=args.sample or 0, seed=args.seed or 0, limit=args.limit or 0,
        tables=split(args.tables) or Selection.tables)
