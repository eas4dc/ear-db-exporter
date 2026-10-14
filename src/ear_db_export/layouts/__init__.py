"""Layouts: what ``export`` writes. One module per layout, each a pipeline you read top to bottom.

Every layout module has five names:

- ``NAME``: the subcommand (``export NAME``) and the ``{layout}`` path field.
- ``HELP``: one line, for ``export --help``.
- ``READS``: the EAR tables it reads, shown in its ``--help``.
- ``add_arguments(parser)``: its flags: the filters it accepts (from the
  selection classes of :mod:`ear_db_export.selection`) and its own options.
- ``run(source, args, out)``: read with a selection, shape each batch, and
  write it to ``out`` (an :class:`~ear_db_export.output.Output`).

What is the same for every layout is done once, by :mod:`ear_db_export.cli`:
it opens the source (with pseudonyms), works out the time window from the
flags or the checkpoint, and opens the output, which places the files and
writes the manifest. To add a layout, write a module with these five names and
add it to :data:`LAYOUTS`.

======================  ========================================  =====================================  ===============
Layout                  Writes one row per                        Reads                                  Selection
======================  ========================================  =====================================  ===============
``raw``                 table row, as stored                      the tables named (default all)         both
``node_metrics``        node, interval and metric (long format)   Periodic_metrics                       TimeSelection
``job_nodes``           job step and node                         Jobs, Applications                     JobSelection
``power_signatures``    job step and node                         … and Power_signatures                 JobSelection
``eacct``               job step and node, as ``eacct -l -c``     … Signatures, GPU_signatures           JobSelection
======================  ========================================  =====================================  ===============
"""
from . import eacct, job_nodes, node_metrics, power_signatures, raw
from .raw import ORDER  # noqa: F401  (stable API: the order a loader writes EAR's tables in)

#: Every layout, by name, in the order ``export --help`` lists them.
LAYOUTS = {m.NAME: m for m in (raw, node_metrics, job_nodes, power_signatures, eacct)}
