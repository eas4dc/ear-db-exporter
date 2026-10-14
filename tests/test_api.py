"""The stable Python API: the names other programs may import (docs/api.rst).

Removing or renaming any of these is a breaking change: bump the minor version.
"""
import pytest

import inspect
import os
import re

from ear_db_export import clean, cli, config, layouts, output, read, selection
from ear_db_export.read import bundle, pseudonymized, sql
from ear_db_export.read import eacct as eacct_source
from ear_db_export.read.sql import mariadb, profile
from ear_db_export.selection import jobs, timeseries

API = {
    config: ["ConfigError", "DbConf", "DB_KEYS", "default_conf_path", "parse", "load",
             "expand_hostlist"],
    read: ["TABLES", "OPTIONAL_TABLES", "TIME_COLUMNS", "Cond", "arrow_schema", "concat"],
    sql: ["SqlSource"],
    bundle: ["BundleSource"],
    eacct_source: ["EacctSource", "COLUMNS", "HEADER"],
    pseudonymized: ["Pseudonymized"],
    mariadb: ["connect", "read_only", "grants", "rows", "scalar", "stream", "fetch", "explain",
              "live_columns", "columns", "arrow_type", "show_create", "quote", "mogrify",
              "id_step", "TYPE_FAMILY"],
    profile: ["Profile", "load_profile", "detect", "diff", "available_profiles", "ddl",
              "ddl_schema"],
    selection: ["Window", "window_from_args"],
    jobs: ["JobSelection", "JobChunk", "JobKeys", "parse_jobs", "gpu_ids"],
    timeseries: ["TimeSelection"],
    layouts: ["LAYOUTS", "ORDER"],
    clean: ["typed", "pseudonym", "pseudonymize", "USER_FIELDS", "USER_COLUMNS"],
    output: ["Output", "template_for", "read_checkpoint", "read_manifest"],
    cli: ["main"],
}


@pytest.mark.parametrize("module", API, ids=lambda m: m.__name__)
def test_api_names(module):
    """Every public name of docs/api.rst exists."""
    assert [n for n in API[module] if not hasattr(module, n)] == []


def test_main_prog(capsys):
    """main() can be mounted as another program's subcommand."""
    with pytest.raises(SystemExit) as e:
        cli.main(["--help"], prog="other-tool export")
    assert e.value.code == 0
    assert capsys.readouterr().out.startswith("usage: other-tool export")


def test_connect_is_always_read_only():
    """connect() has no way to open a writable session."""
    assert list(inspect.signature(mariadb.connect).parameters) == ["db"]


def test_package_never_writes():
    """No statement that changes data or schema anywhere in the package's code."""
    src = os.path.dirname(cli.__file__)
    # a string literal that starts a statement changing data or schema ("INSERT INTO t")
    writes = re.compile(r"[\"']\s*(INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|DROP|TRUNCATE|GRANT)"
                        r"\s+\w+\s+[\w`%(]")
    offenders = [os.path.join(d, f) for d, _, files in os.walk(src) for f in files
                 if f.endswith(".py") and writes.search(open(os.path.join(d, f)).read())]
    assert offenders == []
