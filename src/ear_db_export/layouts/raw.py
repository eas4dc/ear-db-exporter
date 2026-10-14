"""``raw``: EAR's tables as stored, whole, or the rows the filters pick and every row they point to.

For back-ups, moving or archiving a database, and sending a subset of it. The
files keep the physical names and exact types of the database, with
``schema.sql`` (its ``CREATE TABLE`` statements), so a bundle can be loaded
back, and read again with ``--bundle``.

- **Without filters**, every table named (default: all) is copied whole
  (``dump``). No schema profile is needed, so any EAR version can be exported.
- **With filters** (a *linked dump*), each table gets these rows:

=============================================  ================================================
Table                                          Rows
=============================================  ================================================
``Jobs``                                       steps that ended in ``[from, to)`` and match
                                               the job filters
``Applications``                               the applications of those steps (on ``--nodes``)
``Power_signatures``, ``Signatures``,          the rows those applications (and loops) point to
``GPU_signatures``
``Loops``                                      the loops of those steps
``Periodic_metrics``, ``Events``               their own time in ``[from, to)`` and ``--nodes``;
                                               with job filters, the rows of the selected jobs
                                               over the jobs' time span
``Periodic_aggregations``, ``Global_energy``   their own time in ``[from, to)``, or the jobs' span
``Learning_*``, any other table                whole, and only when named
=============================================  ================================================

Consecutive windows split the database without gaps or repeats: a row with its
own time goes to the window of that time, a linked row to its job's.

Files: ``OUT/<Table>.parquet`` (or ``--path`` with ``{table}``), Parquet only.

Flow, as a UML activity diagram:

.. mermaid::

    flowchart TD
        A["raw TABLE..., filters"] --> B{"any filter?"}
        B -- no --> D["source.dump(table), each table:<br/>as stored, no profile needed"]
        B -- yes --> J["JobSelection.chunks: Jobs, Applications,<br/>Power_signatures, Signatures, GPU_signatures"]
        J --> K["JobKeys: every selected step"]
        K --> L["JobSelection.loops: one Loops scan,<br/>then the loops' signatures and GPUs"]
        K --> T["TimeSelection per time-series table:<br/>the window, or the jobs' span with job filters"]
        B -- yes --> W["Learning_*, other tables named:<br/>source.dump, whole"]
        D --> O["Output.write(Table, rows renamed to physical names)"]
        J --> O
        L --> O
        T --> O
        W --> O
"""
from ..config import ConfigError
from ..read import TIME_COLUMNS, concat
from ..selection.jobs import JobKeys, JobSelection, distinct, gpu_signatures_of
from ..selection.timeseries import TimeSelection

NAME = "raw"
HELP = "EAR's tables as stored: whole, or the rows the filters pick and every row they point to"
READS = "the TABLEs named (default: every table)"

#: EAR's tables in the order they are written, each after the tables it points to.
ORDER = ("Jobs", "Applications", "Loops", "Power_signatures", "Signatures", "GPU_signatures",
         "Periodic_metrics", "Events", "Periodic_aggregations", "Global_energy",
         "Learning_applications", "Learning_jobs", "Learning_signatures")
#: The tables a filtered ``all`` means: those the filters apply to (not ``Learning_*``).
FILTERED = ORDER[:10]
#: Logical tables written from the job steps.
JOB_TABLES = ("jobs", "applications", "loops", "power_signatures", "signatures", "gpu_signatures")
#: Logical time-series tables.
TIME_TABLES = tuple(TIME_COLUMNS)


def add_arguments(parser):
    """Add the flags of ``raw``: the TABLE names, every job filter, the time window and the nodes."""
    parser.add_argument("tables", nargs="*", metavar="TABLE",
                        help="tables as the database names them (Jobs, Periodic_metrics, …), "
                             "or all (the default)")
    JobSelection.add_arguments(parser, "job steps that ended in it, and the time-series rows in it")


def run(source, args, out):
    """Write the tables: copied whole without filters, or a linked dump with them; and ``schema.sql``."""
    if out.ext != "parquet":
        raise ConfigError("raw writes Parquet only (a bundle is read back with --bundle)")
    if not source.provides:
        raise ConfigError("this source holds no tables as stored: use another layout")
    sel = JobSelection.from_args(args, out.window, source.tags)
    tables = resolve_tables(args.tables, source.provides, sel.filters())
    out.write_text("schema.sql", source.ddl(tables))
    if not sel.filters():
        for table in tables:
            for t in source.dump(table):
                out.write(table, t)
        return
    if source.profile is None:
        raise ConfigError("filters need a schema profile: give --profile")
    linked_dump(source, sel, tables, out)


def resolve_tables(names, provides, filtered):
    """Turn the TABLE arguments into physical tables, in :data:`ORDER` (then others alphabetically).

    :param names: the TABLE arguments; none means ``all``.
    :param provides: the physical tables of the source.
    :param filtered: whether the run has filters: then ``all`` means the
        tables filters apply to (:data:`FILTERED`); ``Learning_*`` and other
        tables are only included when named.
    :raises ConfigError: on a name the source does not have.

    Example: ``resolve_tables(["Signatures", "Jobs"], provides, False)`` is
    ``["Jobs", "Signatures"]``.
    """
    wanted = set()
    for name in names or ["all"]:
        if name == "all":
            wanted |= {t for t in provides if t in FILTERED or not filtered}
        elif name in provides:
            wanted.add(name)
        else:
            raise ConfigError("%r is not a table of the source (it has %s)"
                              % (name, ", ".join(sorted(provides))))
    order = {t: i for i, t in enumerate(ORDER)}
    return sorted(wanted, key=lambda t: (order.get(t, len(ORDER)), t))


def linked_dump(source, sel, tables, out):
    """Write the rows the selection picks from each table, and every row they point to (see the module's table).

    :param source: a source with a schema profile.
    :param sel: the :class:`~ear_db_export.selection.jobs.JobSelection` of the run.
    :param tables: physical tables to write, from :func:`resolve_tables`.
    :param out: the :class:`~ear_db_export.output.Output`.
    """
    profile = source.profile
    logical = {t: profile.table_of(t) for t in tables}
    wanted = set(logical.values())

    def write(table, rows):
        """Write rows of a logical table under its physical name, with physical column names."""
        out.write(profile.tables[table], profile.to_physical(table, rows))

    keys = JobKeys(by_step=sel.steps_named())
    time_by_jobs = sel.filters_jobs() and bool(wanted & set(TIME_TABLES))
    if wanted & set(JOB_TABLES) or time_by_jobs:
        for chunk in sel.chunks(source):
            if "jobs" in wanted:
                write("jobs", chunk.jobs)
            if "applications" in wanted:
                write("applications", chunk.applications)
            if "power_signatures" in wanted:
                write("power_signatures", chunk.power_signatures)
            if "signatures" in wanted:
                write("signatures", chunk.signatures)
            if "gpu_signatures" in wanted:
                write("gpu_signatures", chunk.gpu_signatures)
            keys.add(chunk.jobs)
        if "loops" in wanted:
            for loops in sel.loops(source, keys):
                write("loops", loops)
                signatures = concat(source.lookup("signatures", "id", distinct(loops["signature_id"])))
                if "signatures" in wanted:
                    write("signatures", signatures)
                if "gpu_signatures" in wanted:
                    write("gpu_signatures", gpu_signatures_of(source, signatures))
    for table in TIME_TABLES:
        if table in wanted:
            rows = TimeSelection(table, sel.window, sel.nodes, keys if sel.filters_jobs() else None)
            for t in rows.batches(source):
                write(table, t)
    for physical, table in logical.items():
        if table not in JOB_TABLES + TIME_TABLES:  # Learning_*, or a table EAR does not create
            for t in source.dump(physical):
                out.write(physical, t)
