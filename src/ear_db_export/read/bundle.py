"""Read a bundle written by ``ear-db-export export raw``: the same reads as from the database.

:class:`BundleSource` offers the members of
:class:`~ear_db_export.read.sql.SqlSource` over a bundle directory, so every
layout works the same on a bundle a site sent as on a live database. The
manifest says which EAR schema profile the tables follow, the auto-increment
step of the database they came from (for GPU id ranges), its node tags, and
whether user fields are already pseudonyms.
"""
import json
import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from ..config import ConfigError
from ..output import MANIFEST
from . import Cond, concat, time_conds
from .sql.profile import load_profile


class BundleSource:
    """A ``raw`` bundle directory, read like a database.

    :param path: bundle directory.
    :param profile: profile name or ``.toml`` path, instead of the manifest's.
    :param tags: ``{node: tag}`` to use instead of the manifest's ``node_tags``.
    :ivar manifest: the parsed ``manifest.json``.
    :ivar salt_fingerprint: the fingerprint of the salt its user fields were
        pseudonymised with, or ``None`` if they are in clear.
    :raises ConfigError: if the directory has no manifest (incomplete bundle),
        or is not a bundle of ``export raw``.

    Example::

        src = BundleSource("/data/site-x")
        jobs = concat(src.scan("jobs", [Cond("user_id", "in", ["3f2a9c1e0b7d4e61"])]))
    """

    dry_run = False

    def __init__(self, path, profile=None, tags=None):
        """Read the manifest, refuse anything but a raw bundle, and bind the profile to its columns."""
        self.path = path
        try:
            with open(os.path.join(path, MANIFEST)) as f:
                self.manifest = json.load(f)
        except OSError as e:
            raise ConfigError("%s is not a complete bundle (no %s): %s" % (path, MANIFEST, e))
        if self.manifest.get("layout") != "raw":
            raise ConfigError("%s is not a bundle of `export raw` (layout %s): only EAR's tables "
                              "as stored can be read back" % (path, self.manifest.get("layout")))
        source = self.manifest.get("source", {})
        name = profile or source.get("profile")
        self.id_step = source.get("id_step") or 1
        self.salt_fingerprint = self.manifest.get("salt_fingerprint")
        self.tags = dict(self.manifest.get("node_tags") or {}) if tags is None else dict(tags)
        self.provides = frozenset(self.manifest.get("outputs", {}))
        self.profile = None
        if name:
            live = {t: {f.name: None for f in self._dataset(t).schema} for t in self.provides}
            self.profile = load_profile(name).bind(live)

    def has(self, table):
        """Tell whether the bundle holds a logical table."""
        return self.profile is not None and self.profile.tables.get(table) in self.provides

    def info(self):
        """Describe the original source (a re-export keeps where the data came from)."""
        return self.manifest.get("source", {})

    def ddl(self, tables):
        """Return the ``CREATE TABLE`` statements the bundle was written with (its ``schema.sql``)."""
        try:
            with open(os.path.join(self.path, "schema.sql")) as f:
                return f.read()
        except OSError:
            return ""

    def close(self):
        """Nothing to release."""

    # -- The four access paths -------------------------------------------------

    def dump(self, table):
        """Read a physical table whole, as stored. Needs no profile.

        :param table: physical table name, one of :attr:`provides`.
        :returns: an iterator of ``pyarrow.Table``, at least one.
        """
        if table not in self.provides:
            raise ConfigError("the bundle has no table %s" % table)
        yield from self._read(table, [])

    def scan(self, table, where=(), order=None, limit=None):
        """Read the rows of a table that match a filter (see :meth:`SqlSource.scan`).

        With ``order`` or ``limit`` the matching rows are sorted in memory.
        """
        physical = self._physical(table)
        tables = self._read(physical, self.profile.where(table, where))
        if order or limit:
            t = concat(tables)
            if order:
                t = t.sort_by([(self.profile.col(table, c), d) for c, d in order])
            tables = [t.slice(0, limit) if limit else t]
        for t in tables:
            yield self.profile.to_logical(table, t)

    def window(self, table, start, end, where=()):
        """Read the rows of a time-series table whose time is in ``[start, end)`` (see :meth:`SqlSource.window`)."""
        return self.scan(table, time_conds(table, start, end) + list(where))

    def lookup(self, table, column, keys):
        """Read the rows whose column is one of ``keys`` (see :meth:`SqlSource.lookup`)."""
        return self.scan(table, [Cond(column, "in", sorted(set(k for k in keys if k is not None)))])

    # -- Helpers ---------------------------------------------------------------

    def _physical(self, table):
        """Return the physical name of a logical table; ConfigError without a profile or the table."""
        if self.profile is None:
            raise ConfigError("the bundle's manifest names no schema profile: give --profile")
        if not self.has(table):
            raise ConfigError("the bundle has no %s table" % table)
        return self.profile.tables[table]

    def _dataset(self, table):
        """Return the Parquet files of one physical table as a dataset (no partition columns added)."""
        files = sorted(self.manifest["outputs"][table]["files"])
        return ds.dataset([os.path.join(self.path, f) for f in files], format="parquet")

    def _read(self, table, conds):
        """Yield the rows of a physical table that match conditions on physical columns, at least one table."""
        data = self._dataset(table)
        empty = True
        for batch in data.to_batches(filter=expression(conds, data.schema)):
            if batch.num_rows:
                empty = False
                yield pa.Table.from_batches([batch])
        if empty:
            yield data.schema.empty_table()


def expression(conds, schema):
    """Turn a filter (a list of :class:`~ear_db_export.read.Cond`, all of which must hold) into a pyarrow expression.

    Values are typed as their column (``uint32`` ids, UTC timestamps), so
    comparisons and ``isin`` match exactly.

    :param conds: the conditions, on column names of ``schema``.
    :param schema: the schema of the data to filter.
    :returns: a ``pyarrow.compute.Expression``, or ``None`` for an empty filter.

    Example::

        expression([Cond("user_name", "in", ["alice"]), Cond("end_time", ">=", 100)], schema)
        # (is_in(user_name, {value_set=["alice"]}) and (end_time >= 100))
    """
    out = None
    for col, op, value in conds:
        field, typ = pc.field(col), schema.field(col).type
        if op == "in":
            e = field.isin(pa.array(list(value), typ))
        elif op == "ranges":
            e = _any([(field >= pa.scalar(lo, typ)) & (field <= pa.scalar(hi, typ)) for lo, hi in value])
        elif op == "like_any":
            e = _any([pc.match_like(field, pattern) for pattern in value])
        else:
            v = pa.scalar(value, typ)
            e = {"=": field == v, ">=": field >= v, "<": field < v}[op]
        out = e if out is None else out & e
    return out


def _any(expressions):
    """OR expressions together; an always-false expression if there are none."""
    out = pc.scalar(False)
    for e in expressions:
        out = out | e
    return out
