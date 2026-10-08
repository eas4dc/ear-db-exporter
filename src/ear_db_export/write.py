"""Step 4, write: one Parquet file per dataset and period, in Hive-style partitions.

Every dataset is a directory of partitions under the output directory::

    OUT/node_metrics/date=2026-07-04/part-0.parquet
    OUT/app_signatures/month=2026-06/part-0.parquet

pyarrow, DuckDB, Spark and Polars read such a directory as one table with a
``date`` or ``month`` column. Where the files go afterwards (a data lake, S3,
another layout) is up to the caller.

Every file is written as ``<path>.tmp`` and renamed when complete, so a reader
never sees half a file, and re-running a partition replaces it. A run holds
:func:`lock` on the output directory, so two runs never write the same files.
"""
import contextlib
import fcntl
import itertools
import os

import pyarrow.parquet as pq

from .config import ConfigError

#: Rows per Parquet row group.
ROW_GROUP = 1_000_000
#: Lock file inside the output directory, held for the whole run.
LOCK_FILE = ".ear_export.lock"


@contextlib.contextmanager
def lock(out, dry_run=False):
    """Hold an exclusive lock on the output directory for the duration of a run.

    :param out: output directory; created if missing.
    :param dry_run: take no lock and create nothing.
    :raises ConfigError: if another run holds the lock.
    """
    if dry_run:
        yield
        return
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, LOCK_FILE), "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ConfigError("another ear-db-export run is writing to %s" % out)
        yield


def partition_path(out, dataset, **partition):
    """Return the file of one partition.

    :param out: output directory (``--out``).
    :param dataset: canonical dataset name, for example ``node_metrics``.
    :param partition: exactly one of ``date=`` (a day, ``datetime.date``) or
        ``month=`` (any date in the month).
    :returns: ``OUT/<dataset>/date=YYYY-MM-DD/part-0.parquet`` or
        ``OUT/<dataset>/month=YYYY-MM/part-0.parquet``.
    """
    (key, value), = partition.items()
    value = value.strftime("%Y-%m") if key == "month" else value.isoformat()
    return os.path.join(out, dataset, "%s=%s" % (key, value), "part-0.parquet")


def parquet(path, tables):
    """Write tables to one Parquet file (zstd), atomically.

    The tables are written in order through one writer, so a large output can
    be produced piece by piece. Re-running overwrites the file; a crash leaves
    the previous file, or no file, and at most a stale ``.tmp``.

    :param path: final file path; missing directories are created.
    :param tables: non-empty iterable of ``pyarrow.Table`` sharing one schema
        (what :func:`ear_db_export.formats.apply` returns).
    :returns: ``(rows written, file size in bytes)``.
    """
    tables = iter(tables)
    first = next(tables)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    rows = 0
    try:
        with pq.ParquetWriter(tmp, first.schema, compression="zstd") as writer:
            for t in itertools.chain([first], tables):
                writer.write_table(t, row_group_size=ROW_GROUP)
                rows += t.num_rows
        os.replace(tmp, path)  # atomic on POSIX: readers see the old or the new file
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return rows, os.path.getsize(path)
