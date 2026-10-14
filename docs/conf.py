"""Sphinx configuration for the ear-db-export documentation (how to build it: development.rst).

The user pages are guide.md, data.md (the columns) and audit.md; the
developer pages arrange the docstrings under src/ear_db_export/. Diagrams are
Mermaid: ``.. mermaid::`` in reStructuredText and docstrings, ```` ```mermaid ````
blocks in Markdown. Three directives write tables from the code, so they never
drift from it:

``.. logical-schema::``
    The record tables (read.TABLES) with each column's physical name in
    every bundled profile.
``.. layouts::``
    Every layout (layouts.LAYOUTS): its name, what it writes and what it reads.
``.. export-help:: LAYOUT``
    The ``--help`` of ``ear-db-export export LAYOUT``.
"""
import contextlib
import io
import os
import sys
from pathlib import Path

from docutils import nodes
from docutils.parsers.rst import Directive
from docutils.statemachine import StringList
from sphinx.util.nodes import nested_parse_with_titles

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sphinx.ext.autodoc import ModuleDocumenter  # noqa: E402

from ear_db_export import cli  # noqa: E402
from ear_db_export.layouts import LAYOUTS  # noqa: E402
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
    "sphinxcontrib.mermaid",
]
myst_fence_as_directive = ["mermaid"]  # ```mermaid blocks in .md pages are diagrams
# Diagrams at their own size, shrunk only when wider than the page (the
# fullscreen option forces every diagram into a fixed-height box)
mermaid_fullscreen = False
mermaid_version = "11.12.1"
mermaid_include_elk = ""  # no diagram uses the ELK layout
# Sequence diagrams wrap long messages instead of spreading their participants.
# A custom init must import mermaid itself.
mermaid_init_js = (
    'import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@%s/dist/mermaid.esm.min.mjs";'
    "mermaid.initialize({startOnLoad: false, sequence: {wrap: true}});" % mermaid_version)
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
html_js_files = ["toc.js"]
html_show_sourcelink = False
# Depth 4: Reference > package > module > its classes and functions
html_theme_options = {"navigation_depth": 4, "collapse_navigation": True}


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


class Layouts(Directive):
    """``.. layouts::`` — one row per layout: name, what it writes (HELP), what it reads (READS).

    Builds the table nodes itself, so it works on Markdown pages too (MyST
    would read generated reStructuredText as Markdown).
    """

    def run(self):
        tgroup = nodes.tgroup(cols=3)
        tgroup += [nodes.colspec(colwidth=w) for w in (20, 50, 30)]
        tgroup += nodes.thead("", _row(["Layout", "Writes", "Reads"]))
        tgroup += nodes.tbody("", *[_row(["`%s`" % l.NAME, l.HELP, l.READS]) for l in LAYOUTS.values()])
        return [nodes.table("", tgroup)]


def _row(cells):
    """A table row; text between backticks becomes inline code."""
    row = nodes.row()
    for text in cells:
        para = nodes.paragraph()
        for i, part in enumerate(text.split("`")):
            if part:
                para += nodes.literal(part, part) if i % 2 else nodes.Text(part)
        row += nodes.entry("", para)
    return row


class ExportHelp(Directive):
    """``.. export-help:: LAYOUT`` — the ``--help`` text of ``export LAYOUT``, as a literal block."""

    required_arguments = 1

    def run(self):
        os.environ["COLUMNS"] = "100"  # argparse wraps at the terminal width
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.suppress(SystemExit):
            cli.parser().parse_args(["export", self.arguments[0], "--help"])
        text = out.getvalue()
        return [nodes.literal_block(text, text)]


# A module's members: classes first, then functions, then constants, each group
# in source order (autodoc's "groupwise" sorts each group alphabetically).
_sort_members = ModuleDocumenter.sort_members


def _classes_first(self, documenters, order):
    """Sort a module's members by source, then group them by kind (a stable sort keeps the source order)."""
    return sorted(_sort_members(self, documenters, "bysource"), key=lambda d: d[0].member_order)


ModuleDocumenter.sort_members = _classes_first


def setup(app):
    app.add_directive("logical-schema", LogicalSchema)
    app.add_directive("layouts", Layouts)
    app.add_directive("export-help", ExportHelp)
