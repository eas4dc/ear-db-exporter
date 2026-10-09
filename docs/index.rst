ear-db-export
=============

ear-db-export reads what EAR measures (node power and energy, jobs, per-job
signatures, loops, events) from EAR's database, without writing to it, and
writes it to files: the tables as stored, filtered by dates, jobs, users or
nodes (a *bundle*, to archive or send elsewhere), or clean datasets for
analysis with standard tools.

.. grid:: 1 2 2 3
   :gutter: 3

   .. grid-item-card:: User guide
      :link: guide
      :link-type: doc

      Install, filters, the commands, bundles, daily timer, sending an extract.

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

      The data flow, logical and physical names, what each module may know, how to extend it.

      +++
      Start here to read the code

   .. grid-item-card:: Reference
      :link: reference
      :link-type: doc

      Every module, in the order a run uses them, and the schema profiles.

      +++
      ``selection`` · ``read`` · ``extract`` · ``canonical`` · ``write``

   .. grid-item-card:: Python API
      :link: api
      :link-type: doc

      The stable names other programs import: ear.conf, sources, profiles, extract.

      +++
      ``config`` · ``read`` · ``extract``

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
