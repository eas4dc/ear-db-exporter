"""Write: one file per table or dataset and period, in Hive-style partitions, and bundle files.

A dataset or table is one file, or with ``--every`` a directory of partitions
under the output directory::

    OUT/Jobs.parquet
    OUT/node_metrics/date=2026-07-04/part-0.parquet
    OUT/app_signatures/month=2026-06/part-0.parquet

pyarrow, DuckDB, Spark and Polars read such a directory as one table with a
``date`` or ``month`` column. Where the files go afterwards (a data lake, S3,
another layout) is up to the caller.

Every file is written as ``<path>.tmp`` and renamed when complete, so a reader
never sees half a file, and re-running a partition replaces it. A run holds
:func:`lock` on the output directory, so two runs never write the same files.

A *bundle* (``export``) also has ``schema.sql``, ``SHA256SUMS`` (check it with
``sha256sum -c SHA256SUMS``) and ``manifest.json``, written last by
:func:`finish_bundle`: a bundle without a manifest is incomplete.
"""
import contextlib
import datetime as dt
import fcntl
import hashlib
import itertools
import json
import os

import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .config import ConfigError

#: Rows per Parquet row group.
ROW_GROUP = 1_000_000
#: Lock file inside the output directory, held for the whole run.
LOCK_FILE = ".ear_export.lock"
#: Bundle files: manifest (written last), CREATE TABLE statements, checksums.
MANIFEST, SCHEMA, SUMS = "manifest.json", "schema.sql", "SHA256SUMS"


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


def partition_path(out, name, partition=None, ext="parquet"):
    """Return the file of a table or dataset, or of one of its partitions.

    :param out: output directory (``--out``).
    :param name: dataset or table name, for example ``node_metrics`` or ``Jobs``.
    :param partition: ``None`` or ``{}`` (one file), or exactly one of
        ``{"date": day}`` or ``{"month": any date in the month}``.
    :param ext: file extension, ``parquet`` or ``csv``.
    :returns: ``OUT/<name>.parquet``, ``OUT/<name>/date=YYYY-MM-DD/part-0.parquet``
        or ``OUT/<name>/month=YYYY-MM/part-0.parquet``.
    """
    if not partition:
        return os.path.join(out, "%s.%s" % (name, ext))
    (key, value), = partition.items()
    value = value.strftime("%Y-%m") if key == "month" else value.isoformat()
    return os.path.join(out, name, "%s=%s" % (key, value), "part-0." + ext)


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


def csv(path, tables, delimiter=","):
    """Write tables to one CSV file, atomically, with a plain header line.

    :param path: final file path; missing directories are created.
    :param tables: non-empty iterable of ``pyarrow.Table`` sharing one schema.
    :param delimiter: field separator (``;`` for the ``eacct`` dataset, as eacct writes).
    :returns: ``(rows written, file size in bytes)``.
    """
    tables = iter(tables)
    first = next(tables)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp, rows = path + ".tmp", 0
    try:
        with open(tmp, "wb") as f:
            f.write((delimiter.join(first.column_names) + "\n").encode())
            opts = pacsv.WriteOptions(include_header=False, delimiter=delimiter)
            with pacsv.CSVWriter(f, first.schema, write_options=opts) as writer:
                for t in itertools.chain([first], tables):
                    writer.write_table(t)
                    rows += t.num_rows
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return rows, os.path.getsize(path)


def text(path, content):
    """Write a text file atomically: ``.tmp`` + rename."""
    with open(path + ".tmp", "w") as f:
        f.write(content)
    os.replace(path + ".tmp", path)


def sha256(path):
    """Return the SHA-256 of a file, as hex."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_manifest(out):
    """Return the manifest of a bundle directory, or ``None`` if it has none."""
    try:
        with open(os.path.join(out, MANIFEST)) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def check_bundle(out, fingerprint):
    """Refuse to add files to a bundle pseudonymised with another key (or in clear).

    :param out: output directory.
    :param fingerprint: the run's salt fingerprint, or ``None`` in clear.
    :raises ConfigError: if ``out`` has a manifest with a different fingerprint.
    """
    old = read_manifest(out)
    if old is not None and old.get("salt_fingerprint") != fingerprint:
        raise ConfigError("%s already holds a bundle pseudonymised %s: use another --out"
                          % (out, "in clear" if old.get("salt_fingerprint") is None
                             else "with another salt"))


def finish_bundle(out, manifest, ddl, files):
    """Write ``schema.sql``, ``SHA256SUMS`` and, last, ``manifest.json`` of a bundle.

    A previous manifest in ``out`` (an earlier ``--every`` run) is merged: its
    files are kept, this run's are added or replaced, and ``created`` is kept.

    :param out: output directory.
    :param manifest: this run's manifest, without ``tables``.
    :param ddl: ``CREATE TABLE`` statements of the tables (``""`` to keep any existing schema.sql).
    :param files: ``{table: {relative path: {"rows", "bytes", "sha256"}}}`` written by this run.
    :returns: the manifest written.
    """
    old = read_manifest(out) or {}
    merged = {t: dict(v["files"]) for t, v in old.get("tables", {}).items()}
    for t, fs in files.items():
        merged.setdefault(t, {}).update(fs)
    manifest = dict(manifest, created=old.get("created", manifest.get("created")), tables={
        t: {"rows": sum(f["rows"] for f in fs.values()),
            "bytes": sum(f["bytes"] for f in fs.values()), "files": dict(sorted(fs.items()))}
        for t, fs in sorted(merged.items())})
    sums = {rel: f["sha256"] for fs in merged.values() for rel, f in fs.items()}
    if ddl:
        text(os.path.join(out, SCHEMA), ddl)
    if os.path.exists(os.path.join(out, SCHEMA)):
        sums[SCHEMA] = sha256(os.path.join(out, SCHEMA))
    text(os.path.join(out, SUMS), "".join("%s  %s\n" % (h, rel) for rel, h in sorted(sums.items())))
    text(os.path.join(out, MANIFEST), json.dumps(manifest, indent=2, default=str) + "\n")
    return manifest
