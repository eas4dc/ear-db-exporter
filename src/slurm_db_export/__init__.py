"""slurm-db-export: read-only export of slurmdbd's accounting database (``slurm_acct_db``).

For datasets that train scheduling models (wait time, runtime vs. time limit,
capacity and queue reconstruction). It shares ear-db-export's engine, bundle
and window code; see :mod:`slurm_db_export.cli` for the flow.
"""
