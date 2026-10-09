Python API
==========

These are the names other programs can import: to read EAR's configuration,
connect to EAR's database, map its schema, read tables or bundles, follow
EAR's links, or mount the command line as one of their own subcommands.
``tests/test_api.py`` lists them. Removing or renaming one is a breaking
change: bump the minor version while the package is at ``0.x``. The other
modules (``pipeline``, ``schedule``, ``formats``, ``write``) can change in any
release; only the dataset columns (:doc:`data`) matter to code outside the
package.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Module
     - Names
   * - :mod:`ear_db_export.config`
     - ``ConfigError``, ``DbConf``, ``DB_KEYS``, ``default_conf_path``,
       ``parse``, ``load``, ``expand_hostlist``
   * - :mod:`ear_db_export.read`
     - ``TABLES``, ``OPTIONAL_TABLES``, ``TIME_COLUMNS``, ``arrow_schema``
   * - :mod:`ear_db_export.read.sql`
     - ``SqlSource``
   * - :mod:`ear_db_export.read.bundle`
     - ``BundleSource``
   * - :mod:`ear_db_export.read.sql.mariadb`
     - ``connect``, ``read_only``, ``grants``, ``rows``, ``scalar``, ``stream``, ``fetch``,
       ``explain``, ``live_columns``, ``columns``, ``arrow_type``, ``show_create``,
       ``quote``, ``mogrify``, ``id_step``, ``TYPE_FAMILY``
   * - :mod:`ear_db_export.read.sql.profile`
     - ``Profile``, ``load_profile``, ``detect``, ``diff``, ``available_profiles``,
       ``ddl``, ``ddl_schema``
   * - :mod:`ear_db_export.selection`
     - ``Selection``, ``parse_jobs``
   * - :mod:`ear_db_export.extract`
     - ``extract``, ``tables``, ``Records``, ``GROUPS``
   * - :mod:`ear_db_export.canonical`
     - ``pseudonym``, ``pseudonymize``, ``USER_FIELDS``, ``USER_COLUMNS``,
       ``DATASETS``, ``INPUTS``
   * - :mod:`ear_db_export.eacct`
     - ``COLUMNS``, ``HEADER``
   * - :mod:`ear_db_export.cli`
     - ``main``

All of them except ``config`` and ``cli`` are documented in :doc:`reference`.

Example: another program
------------------------

Open a bundle (or the database), select rows and read them with logical names:

.. code-block:: python

   import pyarrow as pa
   from ear_db_export.read.bundle import BundleSource
   from ear_db_export.read.sql import SqlSource
   from ear_db_export import config, extract
   from ear_db_export.selection import Selection, parse_jobs

   source = BundleSource("/data/site-x")                      # or:
   # source = SqlSource(config.load(config.default_conf_path())[0])   # read-only session
   sel = Selection(jobs=parse_jobs("123456"), tables=("jobs", "periodic"))
   for table, parts in extract.extract(source, sel):          # physical tables, as stored
       t = pa.concat_tables(list(parts))
       logical = source.profile.table_of(table)
       print(table, source.profile.logical(logical, t).num_rows)

A program that writes to a database (EAS's ``ear-db-synth``) opens its own
connection with its own account; this package never does.

EAR configuration file (``ear_db_export.config``)
-------------------------------------------------

.. automodule:: ear_db_export.config
   :members:

Command-line entry point (``ear_db_export.cli``)
------------------------------------------------

.. automodule:: ear_db_export.cli
   :members: main, parser, run, open_source, db_settings, pseudonym_key, format_settings, DB_FLAGS, ENGINE
