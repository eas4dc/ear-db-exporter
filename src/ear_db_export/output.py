"""Write a bundle: the data files of one layout, then ``checkpoint.json``, ``SHA256SUMS`` and, last, ``manifest.json``.

A *bundle* is the output directory (``--out``) of one layout. Every run into it:

1. writes each data file as ``<path>.tmp``, while the layout streams rows into it;
2. when the layout is done, renames every file into place (a reader never sees
   half a file; a rerun replaces the files with the same path);
3. writes ``checkpoint.json`` (the end of the run's time window; it never moves
   back), the text files (``schema.sql``), ``SHA256SUMS`` (check a bundle with
   ``sha256sum -c SHA256SUMS``) and, last, ``manifest.json``: a bundle without a
   manifest is incomplete.

A failed run removes its ``.tmp`` files and leaves the bundle as it was. A run
holds a lock on the directory, so two runs never write the same bundle. A run
into an existing bundle must be of the same layout and use the same salt; the
manifest keeps the files of earlier runs.

Where files go: ``--path TEMPLATE``, a path inside ``--out`` with these fields:

====================================  ===========================================================
Field                                 Value
====================================  ===========================================================
``{layout}``                          the layout: ``node_metrics``
``{table}``                           the table (``raw`` only): ``Jobs``, ``Periodic_metrics``
``{ext}``                             ``parquet`` or ``csv``
``{from}``                            the window's start: ``2026-07-04``, or ``2026-07-04T10:00:00``
                                      (needs ``--from``, or ``--resume``)
``{to}``                              the window's last day (``2026-07-31`` for ``--to 2026-07-31``),
                                      or its end time when it is not a midnight (needs ``--to``)
``{date}``                            per row: the UTC day of the row's time, ``2026-07-04``
``{year}`` ``{month}`` ``{day}``      per row: ``2026``, ``07``, ``04``
====================================  ===========================================================

Rows are split into files by the per-row fields; a row without a time goes to
``__HIVE_DEFAULT_PARTITION__``, which Hive-style readers read as null. The
default is ``{layout}.{ext}`` (``{table}.{ext}`` for ``raw``); ``--hive`` is
``{layout}/date={date}/part-0.{ext}``. Examples::

    --hive                                                  # OUT/node_metrics/date=2026-07-04/part-0.parquet
    --path '{layout}/year={year}/month={month}/part-0.{ext}'  # Hive by month
    --path '{layout}_{from}_{to}.{ext}'                     # one file per run: power_signatures_2026-07-01_2026-07-31.parquet
"""
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import logging
import os
import string
from importlib.metadata import PackageNotFoundError, version

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .clean import TS, to_ts
from .config import ConfigError

log = logging.getLogger(__name__)

#: Bundle files: manifest (written last), checksums, checkpoint, and the run lock.
MANIFEST, SUMS, CHECKPOINT, LOCK_FILE = "manifest.json", "SHA256SUMS", "checkpoint.json", \
    ".ear_export.lock"
#: Version of the manifest's format.
FORMAT = 2
#: Rows per Parquet row group (at most).
ROW_GROUP = 1_000_000
#: Template fields with one value per run.
RUN_FIELDS = ("layout", "table", "ext", "from", "to")
#: Template fields with one value per row, from the row's time: the ``strftime`` format of each.
ROW_FIELDS = {"date": "%Y-%m-%d", "year": "%Y", "month": "%m", "day": "%d"}
#: The template of ``--hive``.
HIVE = "{layout}/date={date}/part-0.{ext}"
#: The partition value of rows without a time (Hive's own name for null).
NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def template_for(layout, path=None, hive=False):
    """Return the path template of a run (``--path``, ``--hive`` or the default), after checking it.

    :param layout: the layout's name.
    :param path: ``--path``, or ``None``.
    :param hive: ``--hive``.
    :returns: the template.
    :raises ConfigError: on an unknown field, a path outside ``--out``, per-row
        fields or no ``{table}`` in ``raw``, or ``{table}`` in another layout.

    Example: ``template_for("node_metrics", hive=True)`` is
    ``"{layout}/date={date}/part-0.{ext}"``.
    """
    raw = layout == "raw"
    if hive and raw:
        raise ConfigError("--hive: raw writes one file per table; use --path with {table}")
    template = HIVE if hive else path or ("{table}.{ext}" if raw else "{layout}.{ext}")
    fields = template_fields(template)
    unknown = sorted(fields - set(RUN_FIELDS) - set(ROW_FIELDS))
    if unknown:
        raise ConfigError("--path %s: unknown field(s) %s (fields: %s)" % (
            template, ", ".join(unknown), ", ".join(RUN_FIELDS + tuple(ROW_FIELDS))))
    if os.path.isabs(template) or ".." in template.split("/"):
        raise ConfigError("--path %s: must be a relative path inside --out" % template)
    if raw and (fields & set(ROW_FIELDS) or "table" not in fields):
        raise ConfigError("--path %s: raw needs {table} and splits by table only (no %s)"
                          % (template, ", ".join("{%s}" % f for f in ROW_FIELDS)))
    if not raw and "table" in fields:
        raise ConfigError("--path %s: {table} is only for raw; use {layout}" % template)
    return template


def template_fields(template):
    """Return the names of the ``{fields}`` of a path template, as a set."""
    return {f for _, f, _, _ in string.Formatter().parse(template) if f}


def read_checkpoint(out):
    """Return the checkpoint of a bundle: the end of its last finished window, Unix seconds, or ``None``."""
    try:
        with open(os.path.join(out, CHECKPOINT)) as f:
            return json.load(f)["end"]
    except FileNotFoundError:
        return None


def read_manifest(out):
    """Return the manifest of a bundle directory, or ``None`` if it has none."""
    try:
        with open(os.path.join(out, MANIFEST)) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


class Output:
    """The bundle a run writes. Use it as a context manager, write rows, then :meth:`finish`.

    :param out: output directory (``--out``); created if missing.
    :param layout: the layout's name (``node_metrics``), the ``{layout}`` field.
    :param template: path template from :func:`template_for`.
    :param ext: ``"parquet"`` or ``"csv"``.
    :param window: the run's :class:`~ear_db_export.selection.Window`: it fills
        ``{from}``/``{to}``, and its end becomes the checkpoint.
    :param fingerprint: salt fingerprint of the run's pseudonyms, or ``None`` in clear.
    :param dry_run: write nothing and take no lock; log the paths instead.
    :param tool: the program named in the manifest (``slurm-db-export`` shares this class).
    :raises ConfigError: if another run holds the lock, ``--out`` holds a bundle of
        another layout or salt, or the template needs a window bound the run lacks.

    Example::

        with Output("/data/nm", "node_metrics", template_for("node_metrics", hive=True),
                    "parquet", window) as out:
            for rows in batches:
                out.write("node_metrics", rows, times=rows["timestamp"])   # split by day
            out.finish({"source": source.info()})
    """

    def __init__(self, out, layout, template, ext, window, fingerprint=None, dry_run=False,
                 tool="ear-db-export"):
        """Keep the run's settings and check that the template's {from}/{to} have a window bound."""
        self.out, self.layout, self.template, self.ext, self.tool = out, layout, template, ext, tool
        self.window, self.fingerprint, self.dry_run = window, fingerprint, dry_run
        fields = template_fields(template)
        #: The per-row fields of the template, in :data:`ROW_FIELDS` order.
        self.row_fields = [f for f in ROW_FIELDS if f in fields]
        for name, bound in (("from", window.start), ("to", window.end)):
            if name in fields and bound is None:
                raise ConfigError("--path %s uses {%s}: give --%s" % (template, name, name))
        self._files, self._texts, self._seen, self._lock = {}, {}, set(), None

    def __enter__(self):
        """Take the lock on ``--out`` and check that its bundle (if any) can take this run."""
        if self.dry_run:
            return self
        os.makedirs(self.out, exist_ok=True)
        self._lock = open(os.path.join(self.out, LOCK_FILE), "w")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock.close()
            raise ConfigError("another %s run is writing to %s" % (self.tool, self.out))
        old = read_manifest(self.out)
        if old is not None and old.get("layout") != self.layout:
            self.__exit__(None, None, None)
            raise ConfigError("%s holds a bundle of layout %s: use another --out"
                              % (self.out, old.get("layout")))
        if old is not None and old.get("salt_fingerprint") != self.fingerprint:
            self.__exit__(None, None, None)
            raise ConfigError("%s holds a bundle pseudonymised %s: use another --out" % (
                self.out, "in clear" if old.get("salt_fingerprint") is None else "with another salt"))
        return self

    def __exit__(self, exc_type, exc, tb):
        """On an error, remove this run's ``.tmp`` files; then release the lock."""
        if exc_type is not None:
            for f in self._files.values():
                f.discard()
        if self._lock is not None:
            self._lock.close()
            self._lock = None
        return False

    def write(self, name, table, times=None, delimiter=","):
        """Append rows to the file(s) of ``name``.

        :param name: the table (``raw``) or the layout's name: the ``{table}`` field.
        :param table: the rows; every call for one file must have the same schema.
        :param times: one time per row (epoch seconds, or timestamps), needed
            when the template has per-row fields; rows are split by them.
        :param delimiter: field separator, for CSV.
        :raises ConfigError: if two names would write the same file, or the rows
            cannot be written as CSV.

        With a flat template an empty ``table`` still creates its file, so the
        file has its schema; with per-row fields it creates nothing.
        """
        if not self.row_fields:
            self._append(self._path(name, {}), name, table, delimiter)
            return
        if times is None:
            raise ValueError("the template splits by row time but %s has none" % name)
        for fields, part in self._split(table, times):
            self._append(self._path(name, fields), name, part, delimiter)

    def write_text(self, rel, text):
        """Write a text file of the bundle (``schema.sql``) when the run finishes; it is checksummed too."""
        self._texts[rel] = text

    def finish(self, manifest):
        """Rename the run's files into place, then write the checkpoint, texts, ``SHA256SUMS`` and the manifest.

        :param manifest: what the caller records: ``source``, ``filters``,
            ``salt_fingerprint``, ``node_tags``, ...
        :returns: the manifest written, or ``None`` on a dry run.
        """
        if self.dry_run:
            log.info("[dry-run] nothing written (files would go to %s)",
                     os.path.join(self.out, self.template))
            return None
        files = {}
        for rel, f in sorted(self._files.items()):
            files.setdefault(f.name, {})[rel] = f.close()
            log.info("wrote %s: %d rows, %d bytes", f.path, f.rows, files[f.name][rel]["bytes"])
        for rel, text in self._texts.items():
            _write_text(os.path.join(self.out, rel), text)
        old = read_manifest(self.out) or {}
        outputs = _merge(old.get("outputs", {}), files)
        checkpoint = self._save_checkpoint()
        extra = sorted(set(old.get("texts", [])) | set(self._texts)) + ([CHECKPOINT] if checkpoint else [])
        sums = {rel: f["sha256"] for o in outputs.values() for rel, f in o["files"].items()}
        sums.update({rel: sha256(os.path.join(self.out, rel)) for rel in extra})
        _write_text(os.path.join(self.out, SUMS),
                    "".join("%s  %s\n" % (h, rel) for rel, h in sorted(sums.items())))
        now = _now()
        manifest = dict(manifest, format=FORMAT, tool=self.tool + " " + _version(),
                        layout=self.layout, created=old.get("created", now), finished=now,
                        salt_fingerprint=self.fingerprint, checkpoint=checkpoint,
                        texts=sorted(set(old.get("texts", [])) | set(self._texts)), outputs=outputs)
        _write_text(os.path.join(self.out, MANIFEST), json.dumps(manifest, indent=2, default=str) + "\n")
        rows = sum(f.rows for f in self._files.values())
        log.info("summary: %d files, %d rows, %.1f MB", len(self._files), rows,
                 sum(i["bytes"] for fs in files.values() for i in fs.values()) / 1e6)
        return manifest

    # -- Helpers ---------------------------------------------------------------

    def _path(self, name, row_fields):
        """Return the relative path of a file: the template filled with the run's and the row's fields."""
        return self.template.format(layout=self.layout, table=name, ext=self.ext,
                                    **{"from": _day_or_time(self.window.start),
                                       "to": _day_or_time(self.window.end, last=True)},
                                    **row_fields)

    def _split(self, table, times):
        """Yield ``(row fields, rows)`` for each distinct value of the template's per-row fields.

        Example, with ``{date}``: rows ending on 2026-07-04 and 2026-07-05 give
        ``({"date": "2026-07-04"}, rows of the 4th)`` and ``({"date": "2026-07-05"}, ...)``.
        """
        ts = to_ts(times) if pa.types.is_integer(times.type) else pc.cast(times, TS, safe=False)
        values = [pc.fill_null(pc.strftime(ts, format=ROW_FIELDS[f]), NULL_PARTITION)
                  for f in self.row_fields]
        key = values[0] if len(values) == 1 else pc.binary_join_element_wise(*values, "/")
        for k in pc.unique(key).to_pylist():
            yield dict(zip(self.row_fields, k.split("/"))), table.filter(pc.equal(key, k))

    def _append(self, rel, name, table, delimiter):
        """Append rows to the file at a relative path, opening it on first use (only logging it on a dry run)."""
        if self.dry_run:
            if rel not in self._seen:
                self._seen.add(rel)
                log.info("[dry-run] would write %s", os.path.join(self.out, rel))
            return
        f = self._files.get(rel)
        if f is None:
            f = self._files[rel] = _File(os.path.join(self.out, rel), name, table.schema,
                                         self.ext, delimiter)
        elif f.name != name:
            raise ConfigError("--path %s: %s and %s would both write %s"
                              % (self.template, f.name, name, rel))
        f.write(table)

    def _save_checkpoint(self):
        """Write ``checkpoint.json`` with the window's end, unless it has no end or would move back.

        :returns: :data:`CHECKPOINT` if the bundle has a checkpoint, else ``None``.
        """
        old = read_checkpoint(self.out)
        end = self.window.end
        if end is not None and (old is None or end > old):
            iso = dt.datetime.fromtimestamp(end, dt.timezone.utc).isoformat()
            _write_text(os.path.join(self.out, CHECKPOINT),
                        json.dumps({"layout": self.layout, "end": end, "end_utc": iso}) + "\n")
            old = end
        return CHECKPOINT if old is not None else None


class _File:
    """One data file being written: rows go to ``<path>.tmp`` until :meth:`close` renames it.

    :param path: the final path.
    :param name: the table or layout it holds.
    :param schema: the schema of its rows.
    :param ext: ``"parquet"`` or ``"csv"``.
    :param delimiter: field separator, for CSV.
    """

    def __init__(self, path, name, schema, ext, delimiter):
        """Open ``<path>.tmp`` with a Parquet writer, or a CSV writer after a plain header line."""
        self.path, self.name, self.tmp, self.rows = path, name, path + ".tmp", 0
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        if ext == "parquet":
            self._raw, self._writer = None, pq.ParquetWriter(self.tmp, schema, compression="zstd")
            return
        self._raw = open(self.tmp, "wb")
        self._raw.write((delimiter.join(schema.names) + "\n").encode())
        try:
            opts = pacsv.WriteOptions(include_header=False, delimiter=delimiter)
            self._writer = pacsv.CSVWriter(self._raw, schema, write_options=opts)
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid) as e:
            self._raw.close()
            os.remove(self.tmp)
            raise ConfigError("%s cannot be written as CSV (%s): use Parquet" % (path, e))

    def write(self, table):
        """Append rows."""
        if self._raw is None:
            self._writer.write_table(table, row_group_size=ROW_GROUP)
        else:
            self._writer.write_table(table)
        self.rows += table.num_rows

    def close(self):
        """Close the file, rename it into place, and return its ``{"rows", "bytes", "sha256"}``."""
        self._writer.close()
        if self._raw is not None:
            self._raw.close()
        os.replace(self.tmp, self.path)  # atomic on POSIX: readers see the old or the new file
        return {"rows": self.rows, "bytes": os.path.getsize(self.path), "sha256": sha256(self.path)}

    def discard(self):
        """Close the file and remove its ``.tmp`` (the run failed)."""
        with contextlib.suppress(Exception):
            self._writer.close()
        if self._raw is not None:
            self._raw.close()
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.tmp)


def sha256(path):
    """Return the SHA-256 of a file, as hex."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _merge(old, new):
    """Merge this run's files into the manifest's ``outputs``: same paths replaced, others kept, totals recomputed.

    :param old: ``{name: {"rows", "bytes", "files": {path: info}}}`` from the manifest.
    :param new: ``{name: {path: info}}`` written by this run.
    """
    merged = {name: dict(o["files"]) for name, o in old.items()}
    for name, files in new.items():
        merged.setdefault(name, {}).update(files)
    return {name: {"rows": sum(f["rows"] for f in fs.values()),
                   "bytes": sum(f["bytes"] for f in fs.values()), "files": dict(sorted(fs.items()))}
            for name, fs in sorted(merged.items())}


def _day_or_time(seconds, last=False):
    """Format a window bound for ``{from}``/``{to}``: a day at midnight, else ``YYYY-MM-DDTHH:MM:SS``.

    :param last: for an exclusive end at midnight, give the last day *in* the
        window (the day before), as ``--to`` was given.
    """
    if seconds is None:
        return None
    t = dt.datetime.fromtimestamp(seconds, dt.timezone.utc)
    if t.time() != dt.time(0):
        return t.strftime("%Y-%m-%dT%H:%M:%S")
    return (t.date() - dt.timedelta(days=1 if last else 0)).isoformat()


def _write_text(path, text):
    """Write a text file atomically: ``.tmp``, then rename."""
    with open(path + ".tmp", "w") as f:
        f.write(text)
    os.replace(path + ".tmp", path)


def _now():
    """Return the current UTC time, ISO 8601, to the second."""
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _version():
    """Return the installed version of this package, or ``"unknown"``."""
    try:
        return version("ear-db-export")
    except PackageNotFoundError:
        return "unknown"
