Architecture
============

Data flow
---------

An export moves data through four steps. Each step is one module or package,
and hands a pyarrow table to the next:

.. code-block:: text

   EAR database ─► 1. Read ─────► 2. Canonical dataset ─► 3. Format ──────► 4. Write ─► OUT/
                   EAR's records    one clean table per     the shape a        one Parquet file
                   of a period      subject                 consumer wants     per dataset and period

.. list-table::
   :header-rows: 1
   :widths: 16 26 26 32

   * - Step
     - Module
     - Input
     - Output
   * - 1. Read
     - :mod:`ear_db_export.read` (:mod:`~ear_db_export.read.sql` for EAR's SQL database)
     - a period: a day or a month
     - *records*: rows of EAR's own tables (``Periodic_metrics``, ``Jobs``, …)
       with the column names of :data:`~ear_db_export.read.TABLES`
   * - 2. Canonical dataset
     - :mod:`ear_db_export.canonical`
     - the records of the period
     - one table per dataset (``node_metrics``, ``job_nodes``, …): joined,
       with timestamps, NULLs and units
   * - 3. Format
     - :mod:`ear_db_export.formats`
     - one dataset, and the ``--format-opt`` values
     - the table(s) to write: ``wide``, ``long``, ``allocations``, or your own
   * - 4. Write
     - :mod:`ear_db_export.write`
     - those tables
     - ``OUT/<dataset>/date=…/part-0.parquet`` (or ``month=…``)

Two modules drive the steps:

- :mod:`ear_db_export.schedule` picks the periods: every day after the
  *watermark* (the last day fully exported) for ``periodic``, calendar months
  for the signature commands.
- :mod:`ear_db_export.pipeline` runs the four steps for each period. A
  command is only a list of datasets and a schedule
  (:data:`~ear_db_export.pipeline.COMMANDS`).

:mod:`ear_db_export.cli` comes first: it reads the flags and ear.conf
(:mod:`ear_db_export.config`), opens the reader and checks the schema profile.

``dump`` (:mod:`ear_db_export.dump`) skips the four steps: it reads each table
as stored, with the engine functions of step 1 but no profile, and writes it
with step 4's :func:`~ear_db_export.write.parquet`.

Example run
-----------

``periodic --format node_metrics=long --from 2026-07-04 --to 2026-07-04`` on a
test cluster:

1. **Schedule**: one period, 2026-07-04 00:00 to 2026-07-05 00:00 UTC.
2. **Read**: about 25 primary-key lookups find the ids of that day in
   ``Periodic_metrics``, then one ``SELECT`` returns 14 071 records. ``Jobs``
   gives the 73 steps that ended that day, and ``Applications`` the nodes they
   ran on.
3. **Canonical**: ``node_metrics`` has 14 071 rows (one per node and minute);
   ``job_nodes`` has 73 rows (one per step and node).
4. **Format**: ``long`` turns ``node_metrics`` into 77 391 rows (one per node,
   minute and metric that has a value); ``job_nodes`` stays ``wide``.
5. **Write**: ``OUT/node_metrics/date=2026-07-04/part-0.parquet`` and
   ``OUT/job_nodes/date=2026-07-04/part-0.parquet``; then the schedule moves
   the watermark to 2026-07-04.

The steps
---------

1. Read
~~~~~~~

A :class:`~ear_db_export.read.Reader` reads one record table per method, for
a time window or for a list of ids. Every reader returns the same columns
(:data:`~ear_db_export.read.TABLES`), so nothing after step 1 knows where the
data came from. :class:`~ear_db_export.read.Records` wraps a reader for one
period: each record table is read the first time a dataset asks for it, and
kept for the other datasets of the period.

The SQL reader (:class:`~ear_db_export.read.sql.SqlReader`) has three parts,
so that only one of them knows the database engine:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Module
     - Knows
   * - :mod:`~ear_db_export.read.sql.profile`
     - the names one EAR version uses (``Jobs.app_id`` for the logical ``app_name``)
   * - :mod:`~ear_db_export.read.sql.queries`
     - the SQL text: single-table scans and primary-key lookups, built from the profile
   * - :mod:`~ear_db_export.read.sql.mariadb`
     - the engine: driver, identifier quoting, ``EXPLAIN``, ``information_schema``,
       the auto-increment step

The reader also resolves EAR's storage details, so later steps never see them:

- ``Periodic_metrics`` may have no index on ``end_time`` (EAR's
  ``edb_create`` adds one, except when it only prints the SQL), so a day is
  found by binary search on the primary key, which works either way.
- A signature points to its GPUs with an id range whose step is the server's
  auto-increment step (Galera uses steps > 1). The reader returns one GPU row
  per GPU, already linked to its signature.
- EAR 5.0 added ``local_id`` to the key that links ``Jobs`` and
  ``Applications``; the reader says which keys apply (``join_keys``).

2. Canonical dataset
~~~~~~~~~~~~~~~~~~~~

:data:`~ear_db_export.canonical.DATASETS` lists every dataset with the record
tables it reads and the function that builds it. The functions are pure
pyarrow: they join, convert epoch seconds to UTC timestamps, turn EAR's
"no value" zeros into NULL, add ``node_tag`` from ear.conf and pseudonymise
user fields. :doc:`reference` shows the input and output of each one.

3. Format
~~~~~~~~~

A format function takes one canonical dataset and returns the table to write.
``--format DATASET=NAME`` chooses it per dataset; the default ``wide`` writes
the dataset as it is. See :doc:`reference`.

4. Write
~~~~~~~~

One Parquet file per dataset and period, in Hive-style partitions, written as
``.tmp`` and renamed so readers never see half a file. Copying the files
elsewhere is the caller's job (:doc:`guide`).

What each step may know
-----------------------

These rules keep the steps independent; tests and reviews check them.

- Only :mod:`~ear_db_export.read.sql.mariadb` imports the database driver, runs
  queries or writes engine-specific SQL.
- Only :mod:`ear_db_export.read` and below know physical table and column
  names, primary keys, id ranges or the auto-increment step.
  :mod:`ear_db_export.dump` reads tables as stored, but takes their names and
  keys from the database, never from code.
- Nothing in the package opens a writable connection
  (``connect(read_only=False)`` is for other programs; ``tests/test_api.py``
  checks it).
- Only :mod:`ear_db_export.canonical` pseudonymises user fields, so no format
  receives them in clear.
- Formats see one canonical dataset and the ``--format-opt`` values, nothing else.
- :mod:`ear_db_export.write` knows paths and Parquet, not datasets or formats.
- :mod:`ear_db_export.pipeline` only passes each step's output to the next.

Extending the exporter
----------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - To support
     - Add
   * - A new EAR version
     - A profile in ``read/sql/profiles/`` (see ``ear7.toml`` for a version
       that renamed most columns). A column the new version removed becomes
       optional (``?``) in :data:`~ear_db_export.read.TABLES`.
   * - PostgreSQL
     - ``read/sql/postgres.py`` with the functions of
       :mod:`~ear_db_export.read.sql.mariadb`, a ``DbConf.engine`` field and a
       flag to choose it (EAR's backend is chosen when EAR is built and is
       not in ear.conf). EAR's PostgreSQL schema has the same names, but
       unquoted, so in lower case: its ``quote`` must lower-case names for the
       profiles and queries to be shared. No unsigned types; ids step by 1.
   * - Prometheus (node metrics only)
     - ``read/prometheus.py``: a :class:`~ear_db_export.read.Reader` that
       implements ``periodic_metrics`` and ``oldest_day`` and sets ``provides``
       to ``{"periodic_metrics"}``, querying a Prometheus server (EAR's
       ``prometheus.so`` report plugin is only scraped and keeps 30 s of
       data). Its values are W, not J, its metric names changed in EAR 7, and
       its labels are malformed in EAR 5–7 (job ids inside the ``node`` label),
       which must be fixed in EAR first. ``periodic`` also writes
       ``job_nodes``, which needs ``Jobs``: :class:`~ear_db_export.read.Records`
       must then take one reader per record table.
   * - A new output shape
     - A format function in :data:`~ear_db_export.formats.FORMATS` (:doc:`reference`).
   * - CSV files (e.g. like ``eacct -c``)
     - A writer next to :func:`~ear_db_export.write.parquet`, a flag to choose
       it, and the file extension in :func:`~ear_db_export.write.partition_path`.
       An ``eacct``-like layout needs ``sig_*`` and ``ps_*`` in one dataset.
   * - A whole table, as stored
     - Nothing: ``dump --tables``.
   * - A new dataset
     - A builder and an entry in :data:`~ear_db_export.canonical.DATASETS`,
       then the dataset in a :data:`~ear_db_export.pipeline.COMMANDS` entry.
   * - Another destination
     - Nothing in the tool: copy ``OUT/`` with a script (:doc:`guide`).

Not loading the production database
-----------------------------------

- **Read-only.** The connection uses ``DBCommandsUser`` and sets the session
  to ``READ ONLY`` (:func:`~ear_db_export.read.sql.mariadb.connect`).
- **No server-side joins.** Queries are single-table scans or primary-key
  lookups in batches of 5000 keys; joins happen in pyarrow (step 2).
- **No time index needed.** A day of ``Periodic_metrics`` is found by binary
  search over primary-key ids (:func:`~ear_db_export.read.sql.queries.bisect_id`),
  about 25 lookups, and then read with ``id BETWEEN``, whether or not the
  site has an index on ``end_time``. ``Jobs`` is small enough for a full scan
  per period.
- **Short dump queries.** ``dump`` reads tables in primary-key pages of a
  million rows, so the server never holds a long read open.
- **Streaming.** Results are read in batches of 100 000 rows with an
  unbuffered cursor.
- ``--dry-run`` makes the reader print every query with its ``EXPLAIN`` and
  row count instead of reading, so the access plan can be checked on the
  production database before a real run.

.. list-table:: Access per query (:doc:`audit` lists every query)
   :header-rows: 1
   :widths: 25 75

   * - Query
     - Access
   * - ``Periodic_metrics`` per day
     - **PK range**: the day is mapped to an ``id`` window by about 25 PK
       lookups, then ``id BETWEEN … AND end_time …``. Avoids a full scan of the
       2-month table per day. Assumes EARDBD inserts rows less than 1 day after
       their ``end_time`` (measured worst case: 210 s).
   * - ``Jobs`` per day or month
     - **Full table scan** (``end_time`` is not indexed). Fine at ``Jobs``
       table sizes, but it is one scan per exported period.
   * - ``Applications``, ``*signatures``
     - PK or PK-prefix ``IN (…)`` lookups, 5000 keys per query.
   * - ``dump``, per table
     - **PK pages** (``WHERE id > last ORDER BY id LIMIT 1000000``) for tables
       with one integer key; one streamed scan for the others.

Output guarantees
-----------------

- Each file is written as ``.tmp`` and renamed (:func:`ear_db_export.write.parquet`):
  readers never see half a file.
- Re-running a partition produces the same content.
- ``periodic`` moves its watermark after each finished day, so a failed run
  resumes from the failed day.
- User fields are replaced by ``HMAC-SHA256(salt, value)`` (first 16 hex
  digits) in step 2, before any format sees them. The same salt gives the
  same pseudonyms in every run.

Package layout
--------------

.. code-block:: text

   src/ear_db_export/
     cli.py          command line, exit codes
     config.py       ear.conf: connection, node tags, hostlists
     schedule.py     which periods: days with a watermark, whole months
     pipeline.py     the commands, and the four steps for each period
     dump.py         tables as stored, with schema.sql and manifest.json
     read/           1. read: TABLES, Reader, Records
       sql/          EAR's SQL database: SqlReader, queries, profiles, mariadb engine
     canonical.py    2. canonical datasets
     formats.py      3. formats
     write.py        4. Parquet partitions, output-directory lock

The stable Python API that other programs import is listed in :doc:`api`.
