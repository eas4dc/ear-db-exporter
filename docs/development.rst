Development
===========

Setup and tests
---------------

.. code-block:: console

   $ uv sync                      # Python env with the dev tools
   $ uv run pytest                # unit tests, no database needed

The unit tests need no server:

- ``tests/fakedb.py`` is a fake engine on SQLite (stdlib): EAR's tables as a
  profile's DDL creates them, filled with ``tests/fakedata.py``. The sources'
  real SQL (pages, id windows, filters, ``IN`` lookups) runs against it, and
  whole commands run through :func:`ear_db_export.cli.main` with
  ``cli.ENGINE`` replaced by it.
- ``tests/test_cli.py`` runs whole commands, and checks that every layout
  written from a ``raw`` bundle equals the same layout written from the
  database, and that an ``eacct`` CSV read back with ``--eacct`` gives the same file.
- ``tests/test_read_sql.py`` checks that every profile maps every column of
  its DDL (``profiles/earN.sql``).

Integration tests read from a live EAR database, read-only:

.. code-block:: console

   $ EAR_EXPORT_TEST_CONF=$EAR_ETC/ear/ear.conf uv run pytest -m db

``EAR_EXPORT_TEST_DAY=YYYY-MM-DD`` picks a day with data and
``EAR_EXPORT_TEST_DB`` another database than ear.conf's. The tests take their
configuration from environment variables only: no hostnames or credentials in
the repository. Their reference queries must stay bounded by a primary-key
range (or read ``Jobs``, which is small): test databases may have no
``Periodic_metrics`` time index, and a full scan there takes minutes.

Documentation
-------------

.. code-block:: console

   $ uv run --group docs sphinx-build -W -E -b html docs docs/_build/html
   $ xdg-open docs/_build/html/index.html

``-W`` turns warnings into errors, so a broken docstring fails the build; ``-E``
rebuilds every page, so no page of a deleted module is left behind.

- ``README.md`` is the short version: install, quick start, basic usage. The
  full user guide is ``docs/guide.md``, the columns are in ``docs/data.md``
  and what the tool does to a database in ``docs/audit.md``; when a flag, a
  default, a query, a dataset or a format changes, update them, the README and
  ``CHANGELOG.md``.
- ``docs/reference.rst`` only lists modules; the text is in the docstrings.
  Every function and class has one, helpers included; complex ones have an
  example, and each layout module has its flow as a UML diagram.
- Diagrams are Mermaid, rendered by ``sphinxcontrib-mermaid``: ``.. mermaid::``
  in docstrings and ``.rst`` pages, fenced ``mermaid`` code blocks in ``.md`` pages
  (GitHub renders those in the README too). The browser loads mermaid.js from
  a CDN; offline, a diagram shows its source. ``sphinxcontrib-mermaid`` stays
  below 2 while the package supports Python 3.9.
- Tables that repeat the code are generated in ``docs/conf.py``
  (``logical-schema``, ``layouts``, ``export-help``): change the code, not the page.
- Docstrings use Sphinx fields: ``:param:``, ``:returns:``, ``:raises:``, and
  ``:ivar:`` for dataclass fields. Module-level constants are documented with
  ``#:`` comments.

The stable API
--------------

The modules listed in :doc:`api` are the public Python API that other
programs import.

- Don't remove or rename a name listed in ``tests/test_api.py`` without a minor
  version bump.
- Code meant to be imported goes into one of those modules and into
  ``tests/test_api.py``.
- The package stays read-only: ``connect()`` cannot open a writable session,
  and no code holds a statement that changes data (``tests/test_api.py``
  checks both).
- Keep the steps independent (:doc:`architecture`, "What each module may
  know"): a filter's meaning stays in its selection class, layouts only shape
  rows. Engine-specific code (driver, quoting, ``EXPLAIN``,
  ``information_schema``) stays in ``read/sql/mariadb.py``; a new engine is a
  module with the same functions.

Releases
--------

Bump ``version`` in ``pyproject.toml``, add the release to ``CHANGELOG.md``,
run the unit and integration tests, then tag ``vX.Y.Z``. Installs pin the tag.
