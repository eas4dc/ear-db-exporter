Reference
=========

The modules in the order a run uses them. The text comes from the
docstrings, which have examples, and each layout's flow as a UML diagram.
What the files hold is in :doc:`data`; the overview is :doc:`architecture`.

.. contents:: On this page
   :local:
   :depth: 1

The package (``ear_db_export``)
-------------------------------

.. automodule:: ear_db_export

Layouts (``ear_db_export.layouts``)
-----------------------------------

.. automodule:: ear_db_export.layouts
   :members:
   :no-value:

raw
~~~

.. automodule:: ear_db_export.layouts.raw
   :members:

node_metrics
~~~~~~~~~~~~

.. automodule:: ear_db_export.layouts.node_metrics
   :members:

job_nodes
~~~~~~~~~

.. automodule:: ear_db_export.layouts.job_nodes
   :members:

power_signatures
~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.layouts.power_signatures
   :members:

eacct
~~~~~

.. automodule:: ear_db_export.layouts.eacct
   :members:

Selections (``ear_db_export.selection``)
----------------------------------------

.. automodule:: ear_db_export.selection
   :members:

Time series (``selection.timeseries``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.selection.timeseries
   :members:

Job steps (``selection.jobs``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.selection.jobs
   :members:

Sources (``ear_db_export.read``)
--------------------------------

.. automodule:: ear_db_export.read
   :members: Cond, time_conds, concat, col_type, is_optional, arrow_schema

.. autodata:: ear_db_export.read.TABLES
   :no-value:
.. autodata:: ear_db_export.read.OPTIONAL_TABLES
.. autodata:: ear_db_export.read.TIME_COLUMNS

EAR's database (``read.sql``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.read.sql
   :members:

SQL text (``read.sql.queries``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.read.sql.queries
   :members:

MariaDB engine (``read.sql.mariadb``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.read.sql.mariadb
   :members:

Schema profiles (``read.sql.profile``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

EAR versions name some tables and columns differently: the application name
is ``Jobs.app_id`` in EAR 5 and 6, and the job key is ``Jobs.id`` in EAR 4.3.
The exporter uses one set of *logical names* for every version
(``jobs.app_name``, ``jobs.job_id``), and a profile maps them to the
*physical names* of one EAR schema version. The profile is autodetected from
the live database, or forced with ``--profile``. ``ear-db-export
check`` compares it with the database and lists missing, optional and
extra columns.

.. list-table::
   :header-rows: 1

   * - Profile
     - EAR
     - Status
   * - ``ear7``
     - 7.x
     - verified against two live EAR 7 databases; ``job_type`` and ``edp``
       are NULL (EAR 7 removed them); builds with Kubernetes support are not covered
   * - ``ear6``
     - 5.0 – 6.x
     - verified against a live EAR 6.0.03.25 database; EAR 5.x lacks
       ``Signatures.cpu_util`` and ``GPU_signatures.GPU_gflops`` (exported as NULL)
   * - ``ear4``
     - 4.3
     - built from EAR 4.3.1's DDL; not yet run against a live 4.3 database

EAR 7 renamed most ``Jobs``, ``Applications`` and ``Signatures`` columns
(``user_id`` → ``user_name``, ``local_id`` → ``app_id``, ``DC_power`` →
``node_power``, ``FLOPS1..8`` → ``sp64_ops … dp512_ops``…); careful:
``Jobs.app_id`` is the job name (text) in EAR 5–6 and the former ``local_id``
(integer) in EAR 7. To support a new EAR version, copy a file from
``src/ear_db_export/read/sql/profiles/``, fix the physical names and run
``check`` until it exits ``0``. Next to
each profile, ``earN.sql`` holds EAR's own DDL for that version; a test checks
that the profile maps every column of it.

.. automodule:: ear_db_export.read.sql.profile
   :members:

Every table and its physical names, generated from
:data:`~ear_db_export.read.TABLES` and the bundled profiles. *Optional*
columns may be missing in a deployment (EAR drops some when
``DBReportNodeDetail`` or ``DBReportSigDetail`` is ``0``); they read as
``NULL``. A dash means the profile does not map the column (the version does
not have it). ``Loops``, ``Events``, ``Periodic_aggregations``,
``Global_energy`` and ``Learning_*`` may be missing altogether
(:data:`~ear_db_export.read.OPTIONAL_TABLES`).

.. logical-schema::

Bundles (``read.bundle``)
~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.read.bundle
   :members:

eacct files (``read.eacct``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.read.eacct
   :members: HEADER, EacctSource, EACCT_TABLES, V5_NAMES, read_csv

.. autodata:: ear_db_export.read.eacct.COLUMNS
   :no-value:

Pseudonyms (``read.pseudonymized``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.read.pseudonymized
   :members:

Cleaning (``ear_db_export.clean``)
----------------------------------

.. automodule:: ear_db_export.clean
   :members:

Output (``ear_db_export.output``)
---------------------------------

.. automodule:: ear_db_export.output
   :members:
