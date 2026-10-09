"""Read a bundle written by ``ear-db-export export``: the same reads as from the database.

:class:`BundleSource` offers the interface of
:class:`~ear_db_export.read.sql.SqlSource` (``scan``, ``lookup``, ``profile``,
``id_step``, ``provides``) over a bundle directory, so views, exports and
ear-db-synth work the same on a bundle a site sent us as on a live database.
The manifest says which EAR schema profile the tables follow, and the
auto-increment step of the source (for GPU id ranges).
"""
import json
import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from ..config import ConfigError
from .sql.profile import load_profile

#: The manifest file of a bundle, written last: its presence marks a complete bundle.
MANIFEST = "manifest.json"


class BundleSource:
    """A bundle directory, read like a database.

    :param path: bundle directory.
    :param profile: profile name or ``.toml`` path, instead of the manifest's.
    :ivar manifest: the parsed ``manifest.json``.
    :ivar pseudonymized: whether user fields are already pseudonyms.
    :ivar tags: ``{node: tag}`` recorded in the manifest (for ``node_tag``).
    :raises ConfigError: if the directory has no manifest (incomplete bundle), or
        its files are not Parquet.
    """

    dry_run = False

    def __init__(self, path, profile=None):
        self.path = path
        try:
            with open(os.path.join(path, MANIFEST)) as f:
                self.manifest = json.load(f)
        except OSError as e:
            raise ConfigError("%s is not a complete bundle (no %s): %s" % (path, MANIFEST, e))
        if self.manifest.get("files", "parquet") != "parquet":
            raise ConfigError("%s holds %s files: only Parquet bundles can be read back"
                              % (path, self.manifest["files"]))
        source = self.manifest.get("source", {})
        name = profile or source.get("profile")
        self.id_step = source.get("id_step") or 1
        self.pseudonymized = bool(self.manifest.get("pseudonymized"))
        self.tags = self.manifest.get("node_tags") or {}
        self.provides = frozenset(self.manifest.get("tables", {}))
        self.profile = None
        if name:
            live = {t: {f.name: None for f in self._dataset(t).schema} for t in self.provides}
            self.profile = load_profile(name).bind(live)

    def info(self):
        """Describe the original source (a re-export keeps where the data came from)."""
        return self.manifest.get("source", {})

    def close(self):
        """Nothing to release."""

    def _dataset(self, table):
        """The Parquet files of one table, as a dataset (no partition columns added)."""
        files = sorted(self.manifest["tables"][table]["files"])
        return ds.dataset([os.path.join(self.path, f) for f in files], format="parquet")

    def scan(self, table, where=()):
        """Read the rows of a table that match a filter.

        :param table: physical table name.
        :param where: DNF filter on logical column names; empty reads every row.
        :returns: an iterator of ``pyarrow.Table``, at least one.
        :raises ConfigError: if the bundle has no such table, or a filter needs a profile.
        """
        if table not in self.provides:
            raise ConfigError("the bundle has no table %s" % table)
        data = self._dataset(table)
        expr = None
        if where:
            logical = self.profile.table_of(table) if self.profile else None
            if logical is None:
                raise ConfigError("cannot filter %s without a schema profile (give --profile)" % table)
            expr = expression(self.profile.where(logical, where), data.schema)
        empty = True
        for batch in data.to_batches(filter=expr):
            if batch.num_rows:
                empty = False
                yield pa.Table.from_batches([batch])
        if empty:
            yield data.schema.empty_table()

    def lookup(self, table, col, keys):
        """Read the rows whose (logical) column is one of ``keys``."""
        keys = sorted(set(k for k in keys if k is not None))
        return self.scan(table, [(col, "in", keys)])

    def ddl(self, tables):
        """Return the ``CREATE TABLE`` statements the bundle was written with."""
        try:
            with open(os.path.join(self.path, "schema.sql")) as f:
                return f.read()
        except OSError:
            return ""


def expression(dnf, schema):
    """Turn a DNF filter on physical column names into a pyarrow expression.

    Values are typed as their column (``uint32`` ids, UTC timestamps), so
    comparisons and ``isin`` match exactly.

    :param dnf: ``[[(column, op, value), ...], ...]``.
    :param schema: the dataset's schema.
    :returns: a ``pyarrow.compute.Expression``, or ``None`` for an empty filter.
    """
    ors = None
    for conj in dnf:
        ands = None
        for col, op, value in conj:
            field, typ = pc.field(col), schema.field(col).type
            if op in ("in", "not in"):
                e = field.isin(pa.array(list(value), typ))
                e = ~e if op == "not in" else e
            elif op == "like":
                e = pc.match_like(field, value)
            else:
                v = pa.scalar(value, typ)
                e = {"=": field == v, "==": field == v, "!=": field != v, "<": field < v,
                     "<=": field <= v, ">": field > v, ">=": field >= v}[op]
            ands = e if ands is None else ands & e
        ands = pc.scalar(True) if ands is None else ands
        ors = ands if ors is None else ors | ands
    return ors
