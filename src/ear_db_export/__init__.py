"""Read-only export of EAR data to Parquet datasets, in four steps.

1. :mod:`~ear_db_export.read`: read EAR's records for a period
   (:mod:`~ear_db_export.read.sql` for EAR's SQL database).
2. :mod:`~ear_db_export.canonical`: build one canonical dataset per subject.
3. :mod:`~ear_db_export.formats`: shape each dataset for its consumer.
4. :mod:`~ear_db_export.write`: write one Parquet file per dataset and period.

:mod:`~ear_db_export.pipeline` runs the steps for each period that
:mod:`~ear_db_export.schedule` picks; :mod:`~ear_db_export.cli` is the command
line and :mod:`~ear_db_export.config` reads ear.conf. The stable Python API is
listed in ``docs/api.rst``.
"""
