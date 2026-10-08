Development
===========

Setup and tests
---------------

.. code-block:: console

   $ uv sync                      # Python env with the dev tools
   $ uv run pytest                # unit tests, no database needed

Integration tests read from a live EAR database, read-only. They compare row
counts with plain SQL, spot-check rows field by field and check that re-runs
are identical:

.. code-block:: console

   $ EAR_EXPORT_TEST_CONF=$EAR_ETC/ear/ear.conf uv run pytest -m db

``EAR_EXPORT_TEST_DAY=YYYY-MM-DD`` and ``EAR_EXPORT_TEST_MONTH=YYYY-MM`` pick
the partitions to test. The tests take their configuration from environment
variables only: no hostnames or credentials in the repository.

Documentation
-------------

.. code-block:: console

   $ uv run --group docs sphinx-build -W -b html docs docs/_build/html
   $ xdg-open docs/_build/html/index.html

``-W`` turns warnings into errors, so a broken docstring fails the build.

- ``README.md`` is the short version: install, quick start, basic usage. The
  full user guide is ``docs/guide.md``, the columns are in ``docs/data.md``
  and what the tool does to a database in ``docs/audit.md``; when a flag, a
  default, a query, a dataset or a format changes, update them and the README.
- ``docs/reference.rst`` only lists modules; the text is in the docstrings.
- Docstrings use Sphinx fields: ``:param:``, ``:returns:``, ``:raises:``, and
  ``:ivar:`` for dataclass fields. Module-level constants are documented with
  ``#:`` comments.

The stable API
--------------

The modules listed in :doc:`api` (``config``, ``read``, ``read.sql``,
``read.sql.mariadb``, ``read.sql.profile``, ``dump``, ``cli``) are the public
Python API that other programs import.

- Don't remove or rename a name listed in ``tests/test_api.py`` without a minor
  version bump.
- Code meant to be imported goes into one of those modules and into
  ``tests/test_api.py``.
- The package stays read-only: none of its code opens a writable connection
  (``tests/test_api.py`` checks that nothing passes ``read_only=False``).
- Keep the steps independent (:doc:`architecture`, "What each step may
  know"). Engine-specific code (driver, quoting, ``EXPLAIN``,
  ``information_schema``) stays in ``read/sql/mariadb.py``; a new engine is a
  module with the same functions.
- ``tests/test_pipeline.py`` runs whole commands with an in-memory reader;
  extend it when a step's contract changes.

Releases
--------

Bump ``version`` in ``pyproject.toml``, run the unit and integration tests,
then tag ``vX.Y.Z``. Installs and downstream packages pin the tag.
