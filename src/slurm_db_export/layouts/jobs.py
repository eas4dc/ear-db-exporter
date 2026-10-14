"""``jobs``: one row per job allocation, decoded for analysis, from a ``raw`` bundle (never the database).

A separate step from the export, so the raw files stay as stored and this one
can be rebuilt any time. Each row is a ``job_table`` row with logical names
(``het_job_id`` even on Slurm 17.11, through the bundle's profile) and the
columns :func:`~slurm_db_export.clean.clean_jobs` adds: ``req_<tres>`` and
``alloc_<tres>`` (one per TRES of the bundle's ``tres_table``), ``mem_req_mb``,
``mem_per_cpu``, ``state_name``, ``<time>_ts``, ``wait_s``, ``run_s``,
``timelimit_s`` and ``runtime_ratio``. Nothing is filtered out.

Files: ``<cluster>/jobs/year=YYYY/month=MM/part-0.parquet`` by ``time_submit``.

Flow, as a UML activity diagram:

.. mermaid::

    flowchart TD
        A["jobs --bundle RAW"] --> M["RAW/manifest.json:<br/>profile, salt, files of each table"]
        M --> T["tres_table files: tres_names()"]
        M --> J["C/job_table files, per cluster C:<br/>pyarrow.dataset, batch by batch"]
        J --> L["profile.to_logical"]
        L --> C["clean_jobs(batch, TRES names)"]
        T --> C
        C --> O["Output.write(C/jobs, rows, time_submit)"]
"""
import logging
import os

import pyarrow as pa
import pyarrow.dataset as ds

from ear_db_export.config import ConfigError
from ear_db_export.output import read_manifest

from ..clean import clean_jobs, tres_names
from ..profile import load_profile

log = logging.getLogger(__name__)

NAME = "jobs"
HELP = "one row per job, TRES decoded, states named, wait and run times, from a raw bundle"


def add_arguments(parser):
    """Add the flags of ``jobs``: the raw bundle to read and the clusters."""
    g = parser.add_argument_group("jobs")
    g.add_argument("--bundle", required=True, metavar="DIR", help="a bundle of `export raw`")
    g.add_argument("--cluster", action="append", metavar="NAME",
                   help="cluster to write (repeat for several; default: every one in the bundle)")


def bundle_manifest(path):
    """Return the manifest of a ``raw`` bundle of slurm-db-export.

    :raises ConfigError: if the directory holds no finished bundle, or another kind.
    """
    m = read_manifest(path)
    if m is None:
        raise ConfigError("%s has no manifest.json: not a finished bundle" % path)
    if m.get("layout") != "raw" or not str(m.get("tool", "")).startswith("slurm-db-export"):
        raise ConfigError("%s is not a raw bundle of slurm-db-export" % path)
    return m


def files(bundle, manifest, name):
    """Return the paths of the files of one output of a bundle (``lenox.hu/job_table``), sorted."""
    return [os.path.join(bundle, rel) for rel in sorted(manifest["outputs"].get(name, {}).get("files", {}))]


def run(args, out, manifest):
    """Write the decoded jobs of each cluster of the bundle.

    :param args: parsed flags (``bundle``, ``cluster``).
    :param out: the :class:`~ear_db_export.output.Output`.
    :param manifest: the bundle's manifest (:func:`bundle_manifest`).
    """
    profile = load_profile(manifest["source"]["profile"])
    tres = files(args.bundle, manifest, "tres_table")
    if not tres:
        raise ConfigError("%s has no tres_table: TRES strings cannot be decoded" % args.bundle)
    names = tres_names(ds.dataset(tres).to_table())
    clusters = sorted(k.split("/")[0] for k in manifest["outputs"] if k.endswith("/job_table"))
    for c in args.cluster or clusters:
        paths = files(args.bundle, manifest, "%s/job_table" % c)
        if not paths:
            raise ConfigError("%s has no job_table of cluster %s (it has %s)"
                              % (args.bundle, c, ", ".join(clusters) or "none"))
        for batch in ds.dataset(paths).to_batches():
            t = clean_jobs(profile.to_logical("job", pa.Table.from_batches([batch])), names)
            out.write("%s/jobs" % c, t, times=t["time_submit"])
