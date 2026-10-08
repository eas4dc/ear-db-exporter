ear-db-export
=============

ear-db-export exports what EAR measures (node power and energy, jobs, and
per-job signatures) from EAR's database to Parquet files, without writing to
the database. You can analyse those files with standard tools, keep them
after EAR prunes its tables, or dump the whole database to load it elsewhere.

.. grid:: 1 2 2 3
   :gutter: 3

   .. grid-item-card:: User guide
      :link: guide
      :link-type: doc

      Install, first run, the commands, daily timer, publishing to a data lake.

      +++
      Start here

   .. grid-item-card:: The data
      :link: data
      :link-type: doc

      What each file and column holds, with units. No EAR knowledge needed.

      +++
      For data users

   .. grid-item-card:: What it does to your database
      :link: audit
      :link-type: doc

      Account, read-only session, every query, what leaves the site, personal data.

      +++
      For site administrators

   .. grid-item-card:: Architecture
      :link: architecture
      :link-type: doc

      The four steps, what each may know, how to extend it.

      +++
      Start here to read the code

   .. grid-item-card:: Reference
      :link: reference
      :link-type: doc

      Every module, in the order an export runs them, and the schema profiles.

      +++
      ``read`` · ``canonical`` · ``formats`` · ``write`` · ``dump``

   .. grid-item-card:: Python API
      :link: api
      :link-type: doc

      The stable names other programs import: ear.conf, connection, tables, dump.

      +++
      ``config`` · ``mariadb`` · ``dump``

.. toctree::
   :hidden:
   :caption: Users

   guide
   data
   audit

.. toctree::
   :hidden:
   :caption: Developers

   architecture
   reference
   api
   development
