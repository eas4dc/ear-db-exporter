ear-db-export
=============

ear-db-export reads what EAR measures (node power and energy, jobs, per-job
signatures, loops, events) from EAR's database, without writing to it, and
writes it to files with one command, ``export LAYOUT``: the tables as stored
(``raw``, to archive or send elsewhere), or clean layouts for analysis with
standard tools (``node_metrics``, ``job_nodes``, ``power_signatures``, ``eacct``).

.. grid:: 1 2 2 3
   :gutter: 3

   .. grid-item-card:: User guide
      :link: guide
      :link-type: doc

      Install, layouts, filters, bundles and paths, cron with --resume, sending an extract.

      +++
      Start here

   .. grid-item-card:: The data
      :link: data
      :link-type: doc

      What each file and column holds, with units. No EAR knowledge needed.

      +++
      For anyone reading the files

   .. grid-item-card:: What it does to your database
      :link: audit
      :link-type: doc

      Account, read-only session, every query, what leaves the site, personal data.

      +++
      For sysadmins

   .. grid-item-card:: Architecture
      :link: architecture
      :link-type: doc

      The data flow, the two selections, access paths, what each module may know, how to extend it.

      +++
      Start here to read the code

   .. grid-item-card:: Reference
      :link: reference
      :link-type: doc

      Every module, in the order a run uses them, and the schema profiles.

      +++
      ``layouts`` · ``selection`` · ``read`` · ``clean`` · ``output``

   .. grid-item-card:: Python API
      :link: api
      :link-type: doc

      The stable names other programs import: ear.conf, sources, profiles, selections.

      +++
      ``config`` · ``read`` · ``selection``

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
