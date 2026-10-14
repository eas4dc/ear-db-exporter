"""``eacct``: one row per job step and node, in the layout of ``eacct -l -c`` (153 columns).

A reimplementation of ``eacct -l -c`` that streams: it holds one chunk of
job steps at a time, whatever the number of jobs. The columns are those of
EAR 6/7's eacct (:data:`ear_db_export.read.eacct.COLUMNS`), so tools that read eacct
files (ear_analytics, :class:`~ear_db_export.read.eacct.EacctSource`) read it:
``JOBID … NODENAME``, the EARL signature (0 when the step had none, and
``EARL_ENABLED`` 0), ``GPU0_*`` … ``GPU7_*`` and EARD's power signature
(``NODEMGR_*``). With ``--as csv`` it uses ``;``, as eacct does.

The file differs from eacct's own in a few details, such as UTC times and
pseudonymised users; :doc:`/data` lists them. Like ``eacct -n``,
``--limit`` takes the newest steps, but it counts steps, not jobs.

Flags: the time window (steps that ended in it), every job filter, the nodes.

Flow, as a UML sequence diagram:

.. mermaid::

    sequenceDiagram
        participant CLI as cli.run
        participant L as eacct layout
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
            L->>C: per_node, signatures, power_signatures, gpus
            C->>S: lookup("signatures" / "power_signatures" / "gpu_signatures", "id", ids)
            L->>L: eacct_rows(): the 153 columns
            L->>O: write("eacct", rows, times=JOB_END_TIME, delimiter=DELIMITER)
        end
"""
import pyarrow as pa
import pyarrow.compute as pc

from ..clean import to_ts, typed
from ..read.eacct import COLUMNS
from ..selection.jobs import JobSelection

NAME = "eacct"
HELP = "one row per job step and node, as `eacct -l -c` writes it"
READS = "Jobs, Applications, Signatures, Power_signatures, GPU_signatures"
#: eacct's field separator.
DELIMITER = ";"


def add_arguments(parser):
    """Add the flags of ``eacct``: every job filter, the time window and the nodes."""
    JobSelection.add_arguments(parser, "job steps that ended in [--from, --to)")


def run(source, args, out):
    """Write the selected job steps in eacct's layout, a chunk of steps at a time."""
    sel = JobSelection.from_args(args, out.window, source.tags)
    for chunk in sel.chunks(source):
        rows = eacct_rows(chunk)
        out.write(NAME, rows, times=rows["JOB_END_TIME"], delimiter=DELIMITER)


def eacct_rows(chunk):
    """Build the eacct rows of a chunk: one per job step and node, the 153 columns of :data:`COLUMNS`.

    Each column comes from where :data:`COLUMNS` says: the ``jobs`` and
    ``applications`` columns of :attr:`JobChunk.per_node`, the step's EARL
    signature and EARD power signature (matched by id), the GPU of each
    ``GPU<i>_*`` column (matched by signature and position), the two dates
    (formatted from the epoch times), ``EARL_ENABLED``, or 0 for values the
    database does not store. Nulls become 0 (numbers) or ``""`` (text).

    :param chunk: a :class:`~ear_db_export.selection.jobs.JobChunk`.
    :returns: a table with the eacct headers as column names.

    Example: a step that ran without EARL on node n2 gives ``EARL_ENABLED`` 0,
    ``CPI`` 0 and ``GPU0_POWER_W`` 0, but its ``NODEMGR_DC_NODE_POWER_W``.
    """
    t = chunk.per_node
    sigs = typed("signatures", chunk.signatures)
    power = typed("power_signatures", chunk.power_signatures)
    gpus = chunk.gpus
    sig_pos = pc.index_in(t["signature_id"], value_set=sigs["id"])
    power_pos = pc.index_in(t["power_signature_id"], value_set=power["id"])
    gpu_pos = {}
    cols = []
    for header, table, col, gpu in COLUMNS:
        if table in ("jobs", "applications"):
            values = t[col]
        elif table == "signatures":
            values = pc.take(sigs[col], sig_pos)
        elif table == "power_signatures":
            values = pc.take(power[col], power_pos)
        elif table == "gpu_signatures":
            if gpu not in gpu_pos:  # the GPUs at this position, and where each row's is
                g = gpus.filter(pc.equal(gpus["gpu_index"], gpu))
                gpu_pos[gpu] = (g, pc.index_in(t["signature_id"], value_set=g["signature_id"]))
            g, pos = gpu_pos[gpu]
            values = pc.take(g[col], pos)
        elif header == "EARL_ENABLED":
            values = pc.cast(pc.is_valid(sig_pos), pa.int64())
        elif table == "derived":  # JOB_START_DATE / JOB_END_DATE
            values = pc.strftime(to_ts(t[col]), format="%Y-%m-%d %H:%M:%S")
        else:  # not stored in the database: eacct prints 0
            values = pa.array([0] * t.num_rows, pa.int64())
        cols.append(_fill(values))
    return pa.table(cols, names=[h for h, _, _, _ in COLUMNS])


def _fill(values):
    """Replace the nulls of a column as eacct prints them: ``""`` for text, 0 for numbers."""
    if pa.types.is_string(values.type):
        return pc.fill_null(values, "")
    return pc.fill_null(values, pa.scalar(0, values.type))
