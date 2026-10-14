"""Read-only export of EAR data: its tables as stored, or layouts for analysis.

One run reads with a *selection* from a *source*, shapes the rows with a
*layout*, and writes them to a *bundle*:

1. :mod:`~ear_db_export.read`: the sources, EAR's SQL database
   (:mod:`~ear_db_export.read.sql`), a bundle (:mod:`~ear_db_export.read.bundle`)
   or eacct CSV files (:mod:`~ear_db_export.read.eacct`), each with four access
   paths: ``scan``, ``window``, ``lookup`` and ``dump``.
2. :mod:`~ear_db_export.selection`: which rows, by family:
   :class:`~ear_db_export.selection.timeseries.TimeSelection` (time-series tables,
   by time and node) and :class:`~ear_db_export.selection.jobs.JobSelection`
   (job steps, by ``Jobs`` columns, then every row they point to). Each owns its
   flags and what they mean.
3. :mod:`~ear_db_export.layouts`: one module per layout (``raw``,
   ``node_metrics``, ``job_nodes``, ``power_signatures``, ``eacct``), each a
   pipeline you read top to bottom; :mod:`~ear_db_export.clean` has the
   cleaning helpers they share.
4. :mod:`~ear_db_export.output`: the bundle: files at ``--path`` (or ``--hive``),
   ``checkpoint.json``, ``SHA256SUMS`` and ``manifest.json``.

:mod:`~ear_db_export.cli` is the command line and runs these steps;
:mod:`~ear_db_export.config` reads ear.conf.
"""
from importlib.metadata import PackageNotFoundError, version

try:
    #: The installed version of this package (``"unknown"`` when run from a source tree not installed).
    __version__ = version("ear-db-export")
except PackageNotFoundError:
    __version__ = "unknown"
