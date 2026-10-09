"""Read-only export of EAR data: bundles of its tables as stored, and analysis datasets.

1. :mod:`~ear_db_export.selection`: which rows a run reads (the filters).
2. :mod:`~ear_db_export.read`: the sources, EAR's SQL database
   (:mod:`~ear_db_export.read.sql`) or a bundle (:mod:`~ear_db_export.read.bundle`).
3. :mod:`~ear_db_export.extract`: follow EAR's links from the selected job steps
   to every table; ``export`` writes them as stored, a *bundle*.
4. :mod:`~ear_db_export.canonical` and :mod:`~ear_db_export.formats`: ``view``
   builds one dataset per subject and shapes it for its consumer.
5. :mod:`~ear_db_export.write`: atomic Parquet/CSV files, bundle checksums and manifest.

:mod:`~ear_db_export.pipeline` runs the steps for each period that
:mod:`~ear_db_export.schedule` picks; :mod:`~ear_db_export.cli` is the command
line and :mod:`~ear_db_export.config` reads ear.conf. The stable Python API is
listed in ``docs/api.rst``.
"""
