Architecture
============

Data flow
---------

Every ``export`` reads with a *selection* from a *source*, shapes the rows with
a *layout*, and writes them to a *bundle*:

.. mermaid::

   flowchart LR
       F[flags] --> SEL["selection<br/>TimeSelection<br/>JobSelection"]
       SEL --> SRC["source<br/>database, bundle<br/>or eacct files"]
       SRC --> L["layout<br/>raw, eacct, …"]
       L --> O["Output<br/>the bundle"]

.. list-table::
   :header-rows: 1
   :widths: 18 30 52

   * - Step
     - Module
     - Does
   * - Select
     - :mod:`ear_db_export.selection`
     - one class per family of EAR's tables, owning its flags, what they mean
       and how its tables are read: :class:`~ear_db_export.selection.timeseries.TimeSelection`
       and :class:`~ear_db_export.selection.jobs.JobSelection`
   * - Read
     - :mod:`ear_db_export.read`
     - the sources: one method per access path (``scan``, ``window``,
       ``lookup``, ``dump``); logical names in and out, types as stored
   * - Shape
     - :mod:`ear_db_export.layouts`, :mod:`ear_db_export.clean`
     - one module per layout, its pipeline written top to bottom, with its
       flow as a UML diagram in its docstring
   * - Write
     - :mod:`ear_db_export.output`
     - atomic files at ``--path``, the checkpoint, checksums and the manifest
       (its guarantees: :mod:`ear_db_export.output`)

:mod:`ear_db_export.cli` runs the steps, the same for every layout: it opens
the source (wrapped in :class:`~ear_db_export.read.pseudonymized.Pseudonymized`
unless names stay in clear), resolves the time window (``--resume`` reads the
checkpoint), opens the :class:`~ear_db_export.output.Output` and calls the
layout's ``run(source, args, out)``. The tool schedules nothing: a cron job
calls it with ``--resume``.

The two families of tables
--------------------------

EAR's tables are selected in two different ways, so each family has its own
selection class, and a filter means one thing, in one place.
:class:`~ear_db_export.selection.timeseries.TimeSelection` reads a time-series
table by its own time column and by node.
:class:`~ear_db_export.selection.jobs.JobSelection` reads ``Jobs`` by its
columns and job ids, then the rows the job steps point to, by id:

.. mermaid::

   flowchart LR
       subgraph TS[TimeSelection]
           PM[Periodic_metrics]
           EV[Events]
           PA[Periodic_aggregations]
           GE[Global_energy]
       end
       subgraph JS[JobSelection]
           J[Jobs] -- "job, step, local_id" --> A[Applications]
           A --> S[Signatures] --> G[GPU_signatures]
           A --> P[Power_signatures]
           J -- "job, step, local_id" --> L[Loops] --> S
       end

Which flag selects what, in which table, is in :mod:`ear_db_export.selection`.
A layout adds the flags of the selection it uses (``add_arguments``), so
``export job_nodes --users x`` is refused by argparse rather than ignored.
``raw`` uses both: with job filters, the time-series tables are read over the
selected jobs' time span and matched to them
(:class:`~ear_db_export.selection.jobs.JobKeys`).

Access paths: how each table is read
------------------------------------

Each read is one source method, an *access path*: ``scan``, ``window``,
``lookup`` or ``dump`` (:mod:`ear_db_export.read`). Which one a table gets is
fixed in code, by its keys in EAR's DDL and by the selection that reads it.
:doc:`audit` lists every query a run sends, table by table, and
:mod:`ear_db_export.read.sql` how each method builds them. A streamed
``SELECT`` (``Jobs``, ``Loops``, tables without an integer key) runs on a
connection of its own, so the lookups of each chunk can run while it is open.

Memory
------

Nothing holds a whole table. A time-series layout holds one page (up to a
million rows) at a time; a job layout one chunk of :data:`~ear_db_export.selection.jobs.CHUNK`
job steps with their applications and signatures. The output writer keeps one
open file per path and appends to it. Rows come in id order, which is roughly
time order; there is no global sort.

Names: logical and physical
---------------------------

EAR's tables keep the same names in every version, but their columns change
(EAR 7 renamed most). :data:`~ear_db_export.read.TABLES` lists every table and
column with one set of *logical* names; a schema profile
(:mod:`~ear_db_export.read.sql.profile`) maps them to the *physical* names of
one EAR version.

- Sources rename at the boundary, and only rename:
  :meth:`~ear_db_export.read.sql.profile.Profile.to_logical` on the way in,
  :meth:`~ear_db_export.read.sql.profile.Profile.to_physical` back for ``raw``.
  Types stay as stored until a layout calls :func:`~ear_db_export.clean.typed`.
- Filters are :class:`~ear_db_export.read.Cond` lists on logical names; the
  profile maps them (:meth:`~ear_db_export.read.sql.profile.Profile.where`),
  then ``queries.where`` renders SQL and ``bundle.expression`` a pyarrow filter.
- A ``raw`` dump without filters never renames anything (``dump``), and needs
  no profile.

What each module may know
-------------------------

- Only :mod:`~ear_db_export.read.sql.mariadb` imports the database driver, runs
  queries or writes engine-specific SQL.
- Physical column names live only in the profiles; physical table names (the
  same in every EAR version) also in :data:`ear_db_export.layouts.raw.ORDER`.
- A filter's meaning lives in its selection class; layouts choose a selection,
  they do not build filters.
- User fields are pseudonymised only by
  :class:`~ear_db_export.read.pseudonymized.Pseudonymized`, before any layout
  sees a row.
- :mod:`ear_db_export.output` knows paths and files, not tables or layouts.
- Nothing in the package opens a writable connection or contains a statement
  that changes data (``tests/test_api.py`` checks both).

Extending
---------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - To support
     - Add
   * - A new layout
     - A module in ``layouts/`` with ``NAME``, ``HELP``, ``READS``,
       ``add_arguments`` and ``run``, using a selection; add it to
       :data:`~ear_db_export.layouts.LAYOUTS`.
   * - A new filter
     - Its flag and condition in the selection class of its family
       (``add_arguments``, ``from_args``, ``where``).
   * - A new EAR version
     - A profile and its DDL in ``read/sql/profiles/`` (``earN.toml`` and
       ``earN.sql``). A column the new version removed becomes optional (``?``)
       in :data:`~ear_db_export.read.TABLES`; a new one is added there as optional.
   * - A new source
     - A class with the members of a source (:mod:`ear_db_export.read`); a
       source may offer only some tables (``has``), like
       :class:`~ear_db_export.read.eacct.EacctSource`.
   * - PostgreSQL
     - ``read/sql/postgres.py`` with the functions of
       :mod:`~ear_db_export.read.sql.mariadb`, and a flag to choose it. EAR's
       PostgreSQL schema has the same names, unquoted, so in lower case: its
       ``quote`` must lower-case names.

Package layout
--------------

.. code-block:: text

   src/ear_db_export/
     cli.py            command line (check, export LAYOUT), exit codes
     config.py         ear.conf: connection, node tags, hostlists
     layouts/          one module per layout: raw, node_metrics, job_nodes, power_signatures, eacct
     selection/        Window and the shared flags; timeseries.py: TimeSelection; jobs.py: JobSelection
     read/             TABLES, Cond; sources: bundle.py, eacct.py (also eacct's CSV layout), pseudonymized.py
       sql/            EAR's SQL database: SqlSource, queries, mariadb engine,
                       profile.py and profiles/ (TOML + EAR's DDL per version)
     clean.py          types, timestamps, NULLs, node tags, pseudonyms
     output.py         bundles: paths, files, checkpoint, checksums, manifest

The stable Python API that other programs import is listed in :doc:`api`.
