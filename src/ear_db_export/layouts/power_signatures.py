"""``power_signatures``: one row per job step and node, with the power signature EARD measured for it.

EARD writes a power signature for every application on every node, with or
without EARL: average DC, DRAM and package power, min/max DC power, time and
frequencies. Each row has the job step (``job_*``), the node, and the signature
(``ps_*``)::

    job_job_id  job_step_id  …  job_end_time         node  node_tag  ps_dc_power  ps_pck_power  ps_time  …
    42          0            …  2026-07-04 04:37:06  n2    gpu       310.5        180.2         3600
    42          0            …  2026-07-04 04:37:06  n3    gpu       null         null          null

- Every ``jobs`` column, prefixed ``job_`` (times as UTC timestamps), then
  ``node``, ``node_tag``, and every ``power_signatures`` column but ``id``,
  prefixed ``ps_``.
- A row whose power signature is missing is kept with null ``ps_*`` (and
  counted in a warning).
- A step is in the window it ended in.

Flags: every job filter (``--jobs``, ``--users``, …, ``--batch-steps`` for one
row per job and node, ``--limit``), the time window and the nodes.

Flow, as a UML sequence diagram:

.. mermaid::

    sequenceDiagram
        participant CLI as cli.run
        participant L as power_signatures layout
        participant J as JobSelection
        participant C as JobChunk
        participant S as Source
        participant O as Output
        CLI->>L: run(source, args, out)
        L->>J: from_args(args, out.window, source.tags)
        L->>J: chunks(source)
        J->>S: scan("jobs", where(): window, job filters, --limit)
        loop each chunk of 5000 job steps
            J->>S: lookup("applications", "job_id", ids)
            J-->>L: JobChunk (--nodes applied)
            L->>C: per_node, power_signatures
            C->>S: lookup("power_signatures", "id", ids)
            L->>L: with_power_signature(): job_*, node, ps_*
            L->>O: write("power_signatures", rows, times=job_end_time)
        end
"""
import logging

import pyarrow.compute as pc

from ..clean import JOB_TIMES, node_tags, prefixed, typed, with_timestamps
from ..read import TABLES
from ..selection.jobs import JobSelection

log = logging.getLogger(__name__)

NAME = "power_signatures"
HELP = "one row per job step and node, with EARD's power signature"
READS = "Jobs, Applications, Power_signatures"


def add_arguments(parser):
    """Add the flags of ``power_signatures``: every job filter, the time window and the nodes."""
    JobSelection.add_arguments(parser, "job steps that ended in [--from, --to)")


def run(source, args, out):
    """Write the selected job steps with their power signatures, a chunk of steps at a time."""
    sel = JobSelection.from_args(args, out.window, source.tags)
    missing = 0
    for chunk in sel.chunks(source):
        rows, n = with_power_signature(chunk.per_node, typed("power_signatures", chunk.power_signatures),
                                       source.tags)
        missing += n
        out.write(NAME, rows, times=rows["job_end_time"])
    if missing:
        log.warning("%d (job step, node) rows have no power signature (kept, ps_* NULL)", missing)


def with_power_signature(per_node, signatures, tags):
    """Build the rows: ``job_*`` columns, ``node``, ``node_tag``, then the matching power signature as ``ps_*``.

    :param per_node: one row per step and node, from :attr:`ear_db_export.selection.jobs.JobChunk.per_node`.
    :param signatures: ``power_signatures`` rows with logical types.
    :param tags: ``{node: tag}`` from ear.conf.
    :returns: ``(rows, number of rows without a power signature)``.

    Example: a row with ``power_signature_id`` 10 gets ``ps_dc_power`` etc. of
    signature 10; one with an id that has no row gets nulls.
    """
    out = prefixed(with_timestamps(per_node.select(list(TABLES["jobs"])), JOB_TIMES), "job_")
    out = out.append_column("node", per_node["node_id"])
    out = out.append_column("node_tag", node_tags(per_node["node_id"], tags))
    pos = pc.index_in(per_node["power_signature_id"], value_set=signatures["id"])
    for name in signatures.column_names:
        if name != "id":
            out = out.append_column("ps_" + name, pc.take(signatures[name], pos))
    return out, pos.null_count
