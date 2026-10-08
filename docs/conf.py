"""Sphinx configuration for the ear-db-export documentation.

Build from the repository root:

    uv run --group docs sphinx-build -b html docs docs/_build/html

The user pages are guide.md, data.md (the columns) and audit.md; the
developer pages arrange the docstrings under src/ear_db_export/. One directive:

``.. logical-schema::``
    The record tables (read.TABLES) with each column's physical name in
    every bundled profile, so the tables never drift from the code.
"""
import sys
from pathlib import Path

from docutils import nodes
from docutils.parsers.rst import Directive
from docutils.statemachine import StringList
from sphinx.util.nodes import nested_parse_with_titles

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ear_db_export.read import TABLES  # noqa: E402
from ear_db_export.read.sql import profile  # noqa: E402

# -- Project -----------------------------------------------------------------
project = "ear-db-export"
author = "EAR Analytics"
copyright = "2026, EAS"

# -- General -----------------------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx_design",
    "myst_parser",
]
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
exclude_patterns = ["_build"]

# Short names: `load_profile`, not `ear_db_export.read.sql.profile.load_profile`.
add_module_names = False
toc_object_entries_show_parents = "hide"
# Keep `--flag` as two hyphens (no en-dash conversion); still curl quotes.
smartquotes_action = "qe"
autodoc_member_order = "bysource"
autodoc_typehints = "none"

# -- HTML --------------------------------------------------------------------
html_theme = "sphinx_rtd_theme"
html_title = "ear-db-export"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_show_sourcelink = False
html_theme_options = {"navigation_depth": 3, "collapse_navigation": True}


# -- Directives ----------------------------------------------------------------
class LogicalSchema(Directive):
    """``.. logical-schema::`` — per logical table: column, type, physical name per profile."""

    def run(self):
        profiles = [profile.load_profile(n) for n in profile.available_profiles()]
        lines = []
        for table, cols in TABLES.items():
            physical = ", ".join("``%s`` (%s)" % (p.tables[table], p.name) for p in profiles)
            lines += [".. rubric:: %s" % table, "", "Physical table: %s" % physical, "",
                      ".. list-table::", "   :header-rows: 1", "",
                      "   * - Logical column", "     - Type"]
            lines += ["     - %s" % p.name for p in profiles]
            for col, typ in cols.items():
                optional = " (optional)" if typ.endswith("?") else ""
                lines += ["   * - ``%s``" % col, "     - %s%s" % (typ.rstrip("?"), optional)]
                lines += ["     - ``%s``" % p.columns[table][col] if col in p.columns[table]
                          else "     - —" for p in profiles]
            lines.append("")
        container = nodes.container()
        nested_parse_with_titles(self.state, StringList(lines, source="conf.py"), container)
        return container.children


def setup(app):
    app.add_directive("logical-schema", LogicalSchema)
