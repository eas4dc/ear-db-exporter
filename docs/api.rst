Python API
==========

These are the names other programs can import: to read EAR's configuration,
connect to EAR's database, map its schema, read tables, store a set of tables,
or mount the command line as one of their own subcommands.
``tests/test_api.py`` lists them. Removing or renaming one is a breaking
change: bump the minor version while the package is at ``0.x``. The other
modules (``pipeline``, ``schedule``, ``canonical``, ``formats``, ``write``)
can change in any release; only the dataset columns (:doc:`data`) matter to
code outside the package.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Module
     - Names
   * - :mod:`ear_db_export.config`
     - ``ConfigError``, ``DbConf``, ``DB_KEYS``, ``ACCOUNTS``, ``default_conf_path``,
       ``parse``, ``load``, ``expand_hostlist``
   * - :mod:`ear_db_export.read.sql.mariadb`
     - ``connect``, ``read_only``, ``grants``, ``rows``, ``scalar``, ``stream``, ``fetch``,
       ``explain``, ``live_columns``, ``columns``, ``arrow_type``, ``show_create``,
       ``quote``, ``mogrify``, ``id_step``, ``TYPE_FAMILY``
   * - :mod:`ear_db_export.dump`
     - ``dump``, ``USER_COLUMNS``
   * - :mod:`ear_db_export.read`
     - ``TABLES``, ``Reader``, ``Records``, ``arrow_schema``
   * - :mod:`ear_db_export.read.sql`
     - ``SqlReader``
   * - :mod:`ear_db_export.read.sql.profile`
     - ``Profile``, ``load_profile``, ``detect``, ``diff``, ``available_profiles``
   * - :mod:`ear_db_export.cli`
     - ``main``

All of them except ``config`` and ``cli`` are documented in :doc:`reference`.

Example: another program
------------------------

Read ear.conf, connect, read one table with its exact types, and store a set
of tables (Parquet + ``schema.sql`` + ``manifest.json``):

.. code-block:: python

   import pyarrow as pa
   from ear_db_export import config, dump
   from ear_db_export.read.sql import mariadb

   db, node_tags = config.load(config.default_conf_path())   # DBCommandsUser
   conn = mariadb.connect(db)                                 # read-only session
   cols = mariadb.columns(conn, db.database, "Jobs")          # [(name, COLUMN_TYPE, in pk)]
   schema = pa.schema([(c, mariadb.arrow_type(t)) for c, t, _ in cols])
   jobs = mariadb.fetch(conn, "SELECT * FROM `Jobs`", (), schema)
   dump.dump(conn, mariadb, db.database, ["Jobs", "Applications"], "/data/set", key=None)

A program that loads data uses EAR's daemon account and asks for a writable
session explicitly; this package's own commands never do:

.. code-block:: python

   db, _ = config.load(path, account="daemon")               # DBUser / DBPassw
   conn = mariadb.connect(db, read_only=False)

EAR configuration file (``ear_db_export.config``)
-------------------------------------------------

.. automodule:: ear_db_export.config
   :members:

Command-line entry point (``ear_db_export.cli``)
------------------------------------------------

.. automodule:: ear_db_export.cli
   :members: main, parser, run, run_dump, db_settings, pseudonym_key, format_settings, DB_FLAGS, EXPORTS
