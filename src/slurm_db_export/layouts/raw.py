"""``raw``: slurmdbd's tables as stored, for a time window, one file per cluster, table and month.

Rows keep their physical names and stored types; nothing is filtered out:
``deleted`` rows and jobs that never started (``time_start = 0``, censored
observations) are written like the others. Which rows each table gives:

==========================================  ===============================================
Table                                       Rows
==========================================  ===============================================
``<cluster>_job_table``                     ``time_submit`` in ``[from, to)``; the columns of
                                            the profile (:data:`~slurm_db_export.profile.TABLES`)
``<cluster>_step_table``                    the steps of those jobs (by ``job_db_inx``)
``<cluster>_event_table``                   ``time_start`` in ``[from, to)``, every column
``<cluster>_resv_table``                    ``time_start`` in ``[from, to)``, every column
``<cluster>_assoc_table``, ``qos_table``,   whole: a snapshot at the end of the window
``tres_table``
``<cluster>_assoc_usage_hour_table``        ``time_start`` in ``[from, to)`` (``--with-usage-hour``)
``<cluster>_job_script_table``              the scripts of the jobs (``--with-scripts``, 22.05+)
==========================================  ===============================================

Each row goes to the month of its own time (steps and scripts: their job's
``time_submit``; snapshots: the window's last second), so consecutive windows
split the data without gaps or repeats. A run rewrites every month file it
touches whole; that is why ``--from`` must be the first of a month: re-running
the last two months daily picks up the jobs that were still pending or running.

Flow, as a UML activity diagram:

.. mermaid::

    flowchart TD
        A["raw --cluster C --from --to"] --> D["tres_table, qos_table:<br/>source.dump, snapshot"]
        A --> E["per cluster C"]
        E --> AS["C_assoc_table: dump,<br/>users hidden, lineage dropped"]
        E --> J["C_job_table: source.window(time_submit),<br/>job_db_inx range, pages"]
        J --> S["C_step_table: source.lookup(job_db_inx)<br/>for each page"]
        J --> SC["C_job_script_table: lookup(hash_inx)<br/>--with-scripts"]
        E --> EV["C_event_table, C_resv_table:<br/>source.window(time_start)"]
        E --> U["C_assoc_usage_hour_table:<br/>window(time_start), --with-usage-hour"]
        D --> O["Output.write(C/table, rows, times)<br/>split by month"]
        AS --> O
        J --> O
        S --> O
        SC --> O
        EV --> O
        U --> O
"""
import logging

import pyarrow as pa
import pyarrow.compute as pc

from ear_db_export.config import ConfigError

from ..clean import hide

log = logging.getLogger(__name__)

NAME = "raw"
HELP = "slurmdbd's tables as stored, for a time window: jobs, steps, events, reservations, dimensions"


def add_arguments(parser):
    """Add the flags of ``raw``: the clusters, the optional tables and the page size."""
    g = parser.add_argument_group("raw")
    g.add_argument("--cluster", action="append", metavar="NAME",
                   help="cluster to export (repeat for several; default: every cluster of "
                        "cluster_table)")
    g.add_argument("--with-usage-hour", action="store_true",
                   help="also <cluster>_assoc_usage_hour_table (no time index: a full scan; "
                        "check with --dry-run first)")
    g.add_argument("--with-scripts", action="store_true",
                   help="also the batch scripts of the jobs (<cluster>_job_script_table, Slurm 22.05+)")
    g.add_argument("--page-rows", type=int, default=100_000, metavar="N",
                   help="rows per primary-key page of job_table (default 100000)")


def run(source, args, out, key):
    """Write the tables of every cluster for the run's window (see the module's table).

    :param source: a :class:`~slurm_db_export.read.SlurmSource`.
    :param args: parsed flags (``cluster``, ``with_usage_hour``, ``with_scripts``).
    :param out: the :class:`~ear_db_export.output.Output`; its window is the run's.
    :param key: the pseudonymisation salt, or ``None`` to write user ids as stored.
    """
    start, end = out.window.start, out.window.end
    clusters = args.cluster or source.clusters
    if not clusters:
        raise ConfigError("cluster_table lists no cluster: give --cluster")
    snapshot = end - 1  # the last second of the window: its month holds the snapshots

    def at(t, seconds):
        """One time per row, all ``seconds``: the partition of a snapshot."""
        return pa.array([seconds] * t.num_rows, pa.int64())

    for table in ("tres", "qos"):
        for t in source.dump(table, clusters[0]):
            out.write("%s_table" % table, t, times=at(t, snapshot))
    for c in clusters:
        log.info("cluster %s", c)
        for t in source.dump("assoc", c):
            out.write("%s/assoc_table" % c, hide("assoc", t, key), times=at(t, snapshot))
        scripts = args.with_scripts and source.has("job_script", c)
        if args.with_scripts and not scripts:
            log.warning("%s has no job_script_table (Slurm < 22.05): no scripts", c)
        seen = set()  # scripts already written: one script serves many jobs
        for jobs in source.window("job", c, "time_submit", start, end):
            submit = jobs["time_submit"]
            for steps in source.lookup("step", c, "job_db_inx", jobs["job_db_inx"].to_pylist()):
                pos = pc.index_in(steps["job_db_inx"], value_set=jobs["job_db_inx"])
                out.write("%s/step_table" % c, steps, times=pc.take(submit, pos))
            if scripts:
                _write_scripts(source, out, c, jobs, seen)
            out.write("%s/job_table" % c, hide("job", jobs, key), times=submit)
        windowed = ["event", "resv"] + (["assoc_usage_hour"] if args.with_usage_hour else [])
        for table in windowed:
            for t in source.window(table, c, "time_start", start, end):
                out.write("%s/%s_table" % (c, table), hide(table, t, key), times=t["time_start"])


def _write_scripts(source, out, cluster, jobs, seen):
    """Write the batch scripts of a page of jobs not written yet, each in the month of its first job.

    :param seen: ``hash_inx`` values already written in this run; updated.
    """
    col = source.profile.col("job", "script_hash_inx")
    if col not in jobs.column_names:
        return
    first = {}  # hash_inx -> earliest time_submit of the page's jobs using it
    for h, s in zip(jobs[col].to_pylist(), jobs["time_submit"].to_pylist()):
        if h and h not in seen:
            first[h] = min(s, first.get(h, s))
    seen.update(first)
    for t in source.lookup("job_script", cluster, "hash_inx", list(first)):
        times = pa.array([first.get(h) for h in t["hash_inx"].to_pylist()], pa.int64())
        out.write("%s/job_script_table" % cluster, t, times=times)
