Architecture
============

Data flow
---------

Every run selects rows, reads them from a source following EAR's links, and
writes either the tables as stored or datasets built from them:

.. code-block:: text

   flags ─► Selection ─► source ─────────────► extract ─┬─► export: tables as stored ───────────► bundle: OUT/<Table>.parquet,
            (filters)    database (read-only)  EAR's    │                                         schema.sql, SHA256SUMS, manifest.json
                         or a bundle           links    └─► view: Records ─► dataset ─► format ─► OUT/<dataset>.parquet|csv

.. list-table::
   :header-rows: 1
   :widths: 18 30 52

   * - Step
     - Module
     - Does
   * - Select
     - :mod:`ear_db_export.selection`
     - turns the filter flags into a :class:`~ear_db_export.selection.Selection`,
       and its filters into DNF tuples on *logical* column names
   * - Source
     - :mod:`ear_db_export.read` (:class:`~ear_db_export.read.sql.SqlSource`,
       :class:`~ear_db_export.read.bundle.BundleSource`)
     - reads one table at a time, as stored (physical names, exact types):
       ``scan(table, where)`` and ``lookup(table, column, keys)``
   * - Extract
     - :mod:`ear_db_export.extract`
     - from the selected job steps, reads every table they point to, in order
       (:data:`~ear_db_export.extract.ORDER`); :class:`~ear_db_export.extract.Records`
       gives the same rows with logical names to the datasets
   * - Dataset
     - :mod:`ear_db_export.canonical`
     - one clean table per subject: joined, UTC timestamps, NULLs, units,
       pseudonymised users
   * - Format
     - :mod:`ear_db_export.formats`
     - the shape a consumer wants: ``wide``, ``long``, ``allocations``
   * - Write
     - :mod:`ear_db_export.write`
     - atomic Parquet or CSV files, Hive-style partitions, the bundle's
       ``schema.sql``, ``SHA256SUMS`` and ``manifest.json``

Two modules drive the steps: :mod:`ear_db_export.schedule` picks the periods
(one output, or one per day or month, with the watermark of
``--incremental``), and :mod:`ear_db_export.pipeline` runs ``view`` and
``export`` for each period. :mod:`ear_db_export.cli` comes first: it reads the
flags and ear.conf (:mod:`ear_db_export.config`), opens the source and checks
the schema profile.

Example run
-----------

``view node_metrics job_nodes --every day --from 2026-07-04 --to 2026-07-04
--format node_metrics=long`` on a test cluster:

1. **Selection**: no job filter; the period limits it to 2026-07-04 00:00 to
   2026-07-05 00:00 UTC.
2. **Read**: ``Jobs`` gives the 73 steps that ended that day (one scan),
   ``Applications`` the nodes they ran on (primary-key lookups). About 25
   primary-key lookups find the ids of that day in ``Periodic_metrics``, then
   one paged ``SELECT`` returns 14 071 rows.
3. **Datasets**: ``node_metrics`` has 14 071 rows (one per node and minute);
   ``job_nodes`` has 73 rows (one per step and node).
4. **Format**: ``long`` turns ``node_metrics`` into 77 391 rows (one per node,
   minute and metric that has a value); ``job_nodes`` stays ``wide``.
5. **Write**: ``OUT/node_metrics/date=2026-07-04/part-0.parquet`` and
   ``OUT/job_nodes/date=2026-07-04/part-0.parquet``.

Names: logical and physical
---------------------------

EAR's tables keep the same names in every version, but their columns change
(EAR 7 renamed most). :data:`~ear_db_export.read.TABLES` lists every table
and column with one set of *logical* names; a schema profile
(:mod:`~ear_db_export.read.sql.profile`) maps them to the *physical* names of
one EAR version, and holds that version's DDL (``profiles/ear7.sql``), which a
test checks the profile covers.

- Sources and bundles hold **physical** tables: nothing is renamed or
  converted on the way to a bundle.
- Filters are written on **logical** names; the profile maps them
  (:meth:`~ear_db_export.read.sql.profile.Profile.where`) when a source reads.
- The datasets read **logical** tables
  (:meth:`~ear_db_export.read.sql.profile.Profile.logical`), restricted to the
  columns of :data:`~ear_db_export.canonical.INPUTS`, so a column added to
  ``TABLES`` does not change a dataset's schema.

EAR's storage details are resolved before the datasets, so they never see
them:

- ``Periodic_metrics``, ``Events`` and ``Periodic_aggregations`` may have no
  time index, so a time window is found by binary search on the primary key,
  which works either way (:func:`~ear_db_export.read.sql.queries.bisect_id`).
- A signature points to its GPUs with an id range whose step is the server's
  auto-increment step (Galera uses steps > 1); bundles record the step.
  :attr:`Records.gpu_signatures <ear_db_export.extract.Records.gpu_signatures>`
  returns one row per GPU, linked to its signature.
- EAR 5.0 added ``local_id`` to the key that links ``Jobs`` and
  ``Applications``; joins use it when the profile maps it.

What each module may know
-------------------------

These rules keep the steps independent; tests and reviews check them.

- Only :mod:`~ear_db_export.read.sql.mariadb` imports the database driver, runs
  queries or writes engine-specific SQL.
- Physical *column* names live only in the profiles; physical *table* names
  (the same in every EAR version) also in :data:`~ear_db_export.extract.GROUPS`.
- Nothing in the package opens a writable connection or contains a statement
  that changes data (``tests/test_api.py`` checks both).
- User fields are pseudonymised only through
  :func:`~ear_db_export.canonical.pseudonymize`: by the datasets for ``view``,
  by the pipeline for ``export``, before anything is written.
- Formats see one dataset and the ``--format-opt`` values, nothing else.
- :mod:`ear_db_export.write` knows paths and files, not datasets or tables.

Extending
---------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - To support
     - Add
   * - A new EAR version
     - A profile and its DDL in ``read/sql/profiles/`` (``earN.toml`` and
       ``earN.sql``; see ``ear7.toml`` for a version that renamed most
       columns). A column the new version removed becomes optional (``?``) in
       :data:`~ear_db_export.read.TABLES`; a new one is added there as optional.
   * - PostgreSQL
     - ``read/sql/postgres.py`` with the functions of
       :mod:`~ear_db_export.read.sql.mariadb`, and a flag to choose it (EAR's
       backend is chosen when EAR is built and is not in ear.conf). EAR's
       PostgreSQL schema has the same names, but unquoted, so in lower case: its
       ``quote`` must lower-case names. ``columns()`` and ``show_create()``
       must be emulated from ``information_schema``. EAR's PostgreSQL writer
       takes the first GPU id from the wrong sequence
       (``postgresql_io_functions.c``), so GPU ranges there need checking.
   * - Prometheus (node metrics only)
     - A source whose ``provides`` is ``{"Periodic_metrics"}`` and whose
       ``scan`` queries a Prometheus server; a run can then take node metrics
       from it and jobs from the database. Its values are W, not J, and its
       metric names changed in EAR 7.
   * - A new output shape
     - A format function in :data:`~ear_db_export.formats.FORMATS` (:doc:`reference`).
   * - A new dataset
     - A builder and an entry in :data:`~ear_db_export.canonical.DATASETS`
       (and its formats in :data:`~ear_db_export.formats.FORMATS`).
   * - A new filter
     - A field of :class:`~ear_db_export.selection.Selection`, its flag in
       :mod:`ear_db_export.cli`, and its condition in ``job_where``.
   * - Loading a bundle into a database, synthetic data
     - Not here: this package only reads. EAS's internal ``ear-db-synth`` does
       both, on top of this package's profiles and bundles.

Not loading the production database
-----------------------------------

- **Read-only.** The connection uses ``DBCommandsUser`` and sets the session
  to ``READ ONLY`` (:func:`~ear_db_export.read.sql.mariadb.connect`).
- **No server-side joins.** Queries are single-table scans or primary-key
  lookups in batches of 5000 keys; joins happen in pyarrow.
- **No time index needed.** A time window of ``Periodic_metrics`` (and
  ``Events``, ``Periodic_aggregations``) is found by binary search over
  primary-key ids, about 25 lookups, then read in primary-key pages with
  ``id BETWEEN``, whether or not the site has an index.
- **Short queries.** Tables with an integer key are read in pages of a million
  rows, so the server never holds a long read open on them. ``Jobs`` (small)
  and ``Loops`` (no key at all) are read with one streamed scan.
- **Streaming.** Results are read in batches of 100 000 rows with an
  unbuffered cursor; with ``--every`` one period is in memory at a time.
- ``--dry-run`` makes the source print every query with its ``EXPLAIN`` and
  row count instead of reading, so the access plan can be checked on the
  production database before a real run. :doc:`audit` lists every query.

Output guarantees
-----------------

- Each file is written as ``.tmp`` and renamed: readers never see half a file.
- Re-running a partition produces the same content.
- A bundle's ``manifest.json`` is written last, after ``SHA256SUMS``, and after
  every period with ``--every``: a bundle with a manifest is complete up to
  its last period.
- ``--incremental`` moves its watermark after each finished day, so a failed
  run resumes from the failed day.
- The 0.3.0 dataset files are reproduced byte for byte in content by the
  0.4.0 commands (``tests/test_golden.py``).

Package layout
--------------

.. code-block:: text

   src/ear_db_export/
     cli.py          command line (check, export, view), exit codes
     config.py       ear.conf: connection, node tags, hostlists
     selection.py    the filters of a run
     schedule.py     which periods: one output, days with a watermark, whole months
     pipeline.py     view and export, period by period
     extract.py      follow EAR's links; Records for the datasets
     read/           TABLES (logical names); bundle.py: BundleSource
       sql/          EAR's SQL database: SqlSource, queries, mariadb engine,
                     profile.py and profiles/ (TOML + EAR's DDL per version)
     canonical.py    the datasets
     eacct.py        eacct's CSV layout, as data
     formats.py      output shapes
     write.py        Parquet/CSV files, partitions, lock, bundle files

The stable Python API that other programs import is listed in :doc:`api`.
