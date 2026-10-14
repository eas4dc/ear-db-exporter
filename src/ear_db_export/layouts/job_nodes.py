"""``job_nodes``: one row per job step and node it ran on, over a time window.

The companion of ``node_metrics``: ``Periodic_metrics`` keeps one job per node
and interval, so on a shared node the other steps are missing. This layout
lists every step on every node, with its start and end, so a consumer can find
all the steps on node N at time t (``node = N and start_time <= t < end_time``)::

    job_id  step_id  local_id  user_id  app_name  start_time           end_time             …  node  node_tag
    42      0        5         alice    lmp_mpi   2026-07-04 03:37:06  2026-07-04 04:37:06  …  n2    gpu
    42      0        5         alice    lmp_mpi   2026-07-04 03:37:06  2026-07-04 04:37:06  …  n3    gpu

- A step is in the window it **ended** in, so consecutive windows never repeat
  or miss a step. A step still running on day D is in the file of the day it
  ends (EAR writes ``Jobs`` rows when steps end).
- Columns: every ``jobs`` column (times as UTC timestamps, EAR's 0 as null),
  then ``node`` and ``node_tag`` (from ear.conf). User fields are pseudonyms
  unless ``--no-pseudonymize``.
- Steps without applications are left out.

Flags: the time window and ``--nodes``/``--tags`` only.

Flow, as a UML sequence diagram:

.. mermaid::

    sequenceDiagram
        participant CLI as cli.run
        participant L as job_nodes layout
        participant J as JobSelection
        participant C as JobChunk
        participant S as Source
        participant O as Output
        CLI->>L: run(source, args, out)
        L->>J: from_args(args, out.window, source.tags)
        L->>J: chunks(source)
        J->>S: scan("jobs", end_time in window)
        loop each chunk of 5000 job steps
            J->>S: lookup("applications", "job_id", ids)
            J-->>L: JobChunk (--nodes applied)
            L->>C: per_node
            L->>L: steps_on_nodes(): timestamps, node, node_tag
            L->>O: write("job_nodes", rows, times=end_time)
        end
"""
from ..clean import JOB_TIMES, node_tags, with_timestamps
from ..read import TABLES
from ..selection.jobs import JobSelection

NAME = "job_nodes"
HELP = "one row per job step and node it ran on: who ran where, and when"
READS = "Jobs, Applications"


def add_arguments(parser):
    """Add the flags of ``job_nodes``: the time window and the nodes (no job filters)."""
    JobSelection.add_arguments(parser, "job steps that ended in [--from, --to)", job_filters=False)


def run(source, args, out):
    """Write the job steps of the window, one row per step and node, a chunk of steps at a time."""
    sel = JobSelection.from_args(args, out.window, source.tags)
    for chunk in sel.chunks(source):
        rows = steps_on_nodes(chunk.per_node, source.tags)
        out.write(NAME, rows, times=rows["end_time"])


def steps_on_nodes(per_node, tags):
    """Shape :attr:`JobChunk.per_node` rows: the ``jobs`` columns with timestamps, then ``node`` and ``node_tag``.

    :param per_node: one row per step and node, from :attr:`ear_db_export.selection.jobs.JobChunk.per_node`.
    :param tags: ``{node: tag}`` from ear.conf.
    :returns: the rows to write.
    """
    out = with_timestamps(per_node.select(list(TABLES["jobs"])), JOB_TIMES)
    out = out.append_column("node", per_node["node_id"])
    return out.append_column("node_tag", node_tags(per_node["node_id"], tags))
