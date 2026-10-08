"""The stable Python API: the names other programs may import (docs/api.rst).

Removing or renaming any of these is a breaking change: bump the minor version.
"""
import pytest

import os
import re

from ear_db_export import cli, config, dump, read
from ear_db_export.read import sql
from ear_db_export.read.sql import mariadb, profile

API = {
    config: ["ConfigError", "DbConf", "DB_KEYS", "ACCOUNTS", "default_conf_path", "parse", "load",
             "expand_hostlist"],
    read: ["TABLES", "Reader", "Records", "arrow_schema"],
    sql: ["SqlReader"],
    mariadb: ["connect", "read_only", "grants", "rows", "scalar", "stream", "fetch", "explain",
              "live_columns", "columns", "arrow_type", "show_create", "quote", "mogrify",
              "id_step", "TYPE_FAMILY"],
    profile: ["Profile", "load_profile", "detect", "diff", "available_profiles"],
    dump: ["dump", "USER_COLUMNS"],
    cli: ["main"],
}


@pytest.mark.parametrize("module", API, ids=lambda m: m.__name__)
def test_api_names(module):
    assert [n for n in API[module] if not hasattr(module, n)] == []


def test_main_prog(capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(["--help"], prog="other-tool export")
    assert e.value.code == 0
    assert capsys.readouterr().out.startswith("usage: other-tool export")


def test_package_never_opens_a_writable_connection():
    """Only callers outside the package may pass read_only=False to connect()."""
    src = os.path.dirname(cli.__file__)
    writable = re.compile(r"connect\([^)]*read_only\s*=\s*(False|0)")
    offenders = [os.path.join(d, f) for d, _, files in os.walk(src) for f in files
                 if f.endswith(".py") and writable.search(open(os.path.join(d, f)).read())]
    assert offenders == []
