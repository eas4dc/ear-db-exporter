Reference
=========

The modules of each step, in the order an export runs them. The text comes
from the docstrings. Data users want :doc:`data` instead; the overview is
:doc:`architecture`.

.. contents:: On this page
   :local:
   :depth: 1

Step 1: read (``ear_db_export.read``)
-------------------------------------

A reader gets EAR's *records* for a period: rows of EAR's own tables, with
the column names of :data:`~ear_db_export.read.TABLES`. Those names are the
*logical names*: the same for every EAR version, mapped to each version's own
names by the SQL reader's profiles (`Schema profiles`_).

For example, one ``jobs`` record, whatever the store or the EAR version::

   job_id  step_id  local_id  user_id  app_name  start_time  end_time    policy     …
   42      0        5         alice    lammps    1783136226  1783139826  min_energy

.. automodule:: ear_db_export.read
   :no-members:

.. autoclass:: ear_db_export.read.Reader
   :members:

.. autoclass:: ear_db_export.read.Records
   :members:

.. autodata:: ear_db_export.read.TABLES
   :no-value:
.. autodata:: ear_db_export.read.GPU_LINK
.. autofunction:: ear_db_export.read.arrow_schema
.. autofunction:: ear_db_export.read.col_type
.. autofunction:: ear_db_export.read.is_optional

EAR's SQL database (``read.sql``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.read.sql
   :members:

.. automodule:: ear_db_export.read.sql.queries
   :members:

MariaDB/MySQL engine (``read.sql.mariadb``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ear_db_export.read.sql.mariadb
   :members:

Schema profiles
~~~~~~~~~~~~~~~

EAR versions name some tables and columns differently: the application name
is ``Jobs.app_id`` in EAR 5 and 6, and the job key is ``Jobs.id`` in EAR 4.3.
The exporter uses one set of *logical names* for every version
(``jobs.app_name``, ``jobs.job_id``), and a profile maps them to the
*physical names* of one EAR schema version. The profile is autodetected from
the live database, or forced with ``--profile``. ``ear-db-export
check-schema`` compares it with the database and lists missing, optional and
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
``check-schema`` until it exits ``0``.

.. automodule:: ear_db_export.read.sql.profile
   :members:

The record tables and their physical names, generated from
:data:`~ear_db_export.read.TABLES` and the bundled profiles. *Optional*
columns may be missing in a deployment (EAR drops some when
``DBReportNodeDetail`` or ``DBReportSigDetail`` is ``0``); they are exported
as ``NULL``. A dash means the profile does not map the column.

.. logical-schema::

Step 2: canonical datasets (``ear_db_export.canonical``)
--------------------------------------------------------

.. automodule:: ear_db_export.canonical
   :no-members:

Each function builds one dataset from the records of a period. The examples
show a few columns; the full lists are in :doc:`data`.

.. autofunction:: ear_db_export.canonical.node_metrics
.. autofunction:: ear_db_export.canonical.job_nodes
.. autofunction:: ear_db_export.canonical.power_signatures
.. autofunction:: ear_db_export.canonical.app_signatures
.. autofunction:: ear_db_export.canonical.gpu_signatures

.. autoclass:: ear_db_export.canonical.Dataset
.. autodata:: ear_db_export.canonical.DATASETS
   :no-value:

.. list-table::
   :header-rows: 1
   :widths: 25 50 25

   * - Dataset
     - Record tables read
     - Built by
   * - ``node_metrics``
     - ``periodic_metrics``
     - :func:`~ear_db_export.canonical.node_metrics`
   * - ``job_nodes``
     - ``jobs``, ``applications``
     - :func:`~ear_db_export.canonical.job_nodes`
   * - ``power_signatures``
     - ``jobs``, ``applications``, ``power_signatures``
     - :func:`~ear_db_export.canonical.power_signatures`
   * - ``app_signatures``
     - ``jobs``, ``applications``, ``signatures``
     - :func:`~ear_db_export.canonical.app_signatures`
   * - ``gpu_signatures``
     - ``jobs``, ``applications``, ``signatures``, ``gpu_signatures``
     - :func:`~ear_db_export.canonical.gpu_signatures`

Helpers the builders share:

.. autodata:: ear_db_export.canonical.NODE_METRICS
.. autodata:: ear_db_export.canonical.TS
.. autodata:: ear_db_export.canonical.USER_FIELDS
.. autofunction:: ear_db_export.canonical.to_ts
.. autofunction:: ear_db_export.canonical.pseudonym
.. autofunction:: ear_db_export.canonical.pseudonymize
.. autofunction:: ear_db_export.canonical.node_tags
.. autofunction:: ear_db_export.canonical.jobs_x_apps
.. autofunction:: ear_db_export.canonical.clean_jobs

Step 3: formats (``ear_db_export.formats``)
-------------------------------------------

A format takes one canonical dataset and the ``--format-opt`` values, and
returns the table to write. ``--format DATASET=NAME`` chooses it; datasets
without ``--format`` use ``wide``. A new format is a function added to
:data:`~ear_db_export.formats.FORMATS`:

.. code-block:: python

   def my_format(table, opts):     # table: e.g. node_metrics; opts: {"service": "x"}
       return out_table            # or yield several tables that share one schema

.. automodule:: ear_db_export.formats
   :no-members:

.. autofunction:: ear_db_export.formats.wide
.. autofunction:: ear_db_export.formats.long
.. autofunction:: ear_db_export.formats.allocations
.. autodata:: ear_db_export.formats.FORMATS
   :no-value:
.. autofunction:: ear_db_export.formats.resolve
.. autofunction:: ear_db_export.formats.apply
.. autodata:: ear_db_export.formats.LONG_METRICS
.. autodata:: ear_db_export.formats.LONG_SCHEMA
   :no-value:
.. autodata:: ear_db_export.formats.LABELS
   :no-value:

Step 4: write (``ear_db_export.write``)
---------------------------------------

.. automodule:: ear_db_export.write
   :members:

Running an export (``pipeline``, ``schedule``)
----------------------------------------------

A command is a list of canonical datasets and a schedule. For ``periodic``
that is ``node_metrics`` and ``job_nodes``, one period per day; for
``app-signatures``, ``app_signatures`` and ``gpu_signatures``, one period per
month. The pipeline then runs the four steps for every period.

.. automodule:: ear_db_export.pipeline
   :members:

.. automodule:: ear_db_export.schedule
   :members:

Dump (``ear_db_export.dump``)
-----------------------------

.. automodule:: ear_db_export.dump
   :members:
