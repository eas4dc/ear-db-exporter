Python API
==========

These are the names other programs can import: to read EAR's configuration,
connect to EAR's database, map its schema, read tables from any source, select
rows the way the layouts do, write a bundle, or mount the command line as one
of their own subcommands. ``tests/test_api.py`` lists them. Removing or
renaming one is a breaking change: bump the minor version while the package is
at ``0.x``. The layouts' files (:doc:`data`) matter to code outside the package
too.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Module
     - Names
   * - :mod:`ear_db_export.config`
     - ``ConfigError``, ``DbConf``, ``DB_KEYS``, ``default_conf_path``,
       ``parse``, ``load``, ``expand_hostlist``
   * - :mod:`ear_db_export.read`
     - ``TABLES``, ``OPTIONAL_TABLES``, ``TIME_COLUMNS``, ``Cond``, ``arrow_schema``, ``concat``
   * - :mod:`ear_db_export.read.sql`
     - ``SqlSource``
   * - :mod:`ear_db_export.read.bundle`, :mod:`~ear_db_export.read.eacct`,
       :mod:`~ear_db_export.read.pseudonymized`
     - ``BundleSource``, ``EacctSource`` (and eacct's CSV layout: ``COLUMNS``, ``HEADER``),
       ``Pseudonymized``
   * - :mod:`ear_db_export.read.sql.mariadb`
     - ``connect``, ``read_only``, ``grants``, ``rows``, ``scalar``, ``stream``, ``fetch``,
       ``explain``, ``live_columns``, ``columns``, ``arrow_type``, ``show_create``,
       ``quote``, ``mogrify``, ``id_step``, ``TYPE_FAMILY``
   * - :mod:`ear_db_export.read.sql.profile`
     - ``Profile``, ``load_profile``, ``detect``, ``diff``, ``available_profiles``,
       ``ddl``, ``ddl_schema``
   * - :mod:`ear_db_export.selection`
     - ``Window``, ``window_from_args``
   * - :mod:`ear_db_export.selection.jobs`, :mod:`~ear_db_export.selection.timeseries`
     - ``JobSelection``, ``JobChunk``, ``JobKeys``, ``parse_jobs``, ``gpu_ids``, ``TimeSelection``
   * - :mod:`ear_db_export.layouts`
     - ``LAYOUTS``, ``ORDER``
   * - :mod:`ear_db_export.clean`
     - ``typed``, ``pseudonym``, ``pseudonymize``, ``USER_FIELDS``, ``USER_COLUMNS``
   * - :mod:`ear_db_export.output`
     - ``Output``, ``template_for``, ``read_checkpoint``, ``read_manifest``
   * - :mod:`ear_db_export.cli`
     - ``main``

All of them except ``config`` and ``cli`` are documented in :doc:`reference`.

Example: another program
------------------------

Open a bundle (or the database), select alice's job steps of July, and read
them with their power signatures, one chunk of steps at a time:

.. code-block:: python

   import datetime as dt

   from ear_db_export.clean import typed
   from ear_db_export.read.bundle import BundleSource
   from ear_db_export.selection import Window
   from ear_db_export.selection.jobs import JobSelection

   def utc(*day):
       """Unix seconds of a UTC midnight."""
       return int(dt.datetime(*day, tzinfo=dt.timezone.utc).timestamp())

   source = BundleSource("/data/site-x")                 # or SqlSource(config.load(path)[0])
   july = Window(utc(2026, 7, 1), utc(2026, 8, 1))       # [start, end)
   for chunk in JobSelection(window=july, users=("alice",)).chunks(source):
       steps = chunk.per_node                             # one row per step and node, logical types
       power = typed("power_signatures", chunk.power_signatures)
       print(steps.num_rows, power.num_rows)

A program that writes to a database opens its own connection with its own
account; this package never does.

EAR configuration file (``ear_db_export.config``)
-------------------------------------------------

.. automodule:: ear_db_export.config
   :members:

Command-line entry point (``ear_db_export.cli``)
------------------------------------------------

.. automodule:: ear_db_export.cli
   :members: main, parser, run, open_source, db_settings, pseudonym_key, describe_filters, DB_FLAGS, ENGINE
