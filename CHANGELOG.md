# Changelog

## 0.7.1

- `slurm-db-export export jobs`: a TRES stored as `NO_VAL64` (`3=18446744073709551614`,
  energy without energy accounting) is null in `req_<tres>`/`alloc_<tres>`; it failed
  the export with `OverflowError: Python int too large to convert to C long`.

## 0.7.0

- `node_metrics`: `labels['job_steps']` lists every job step (`job.step`,
  `job.sb` for the batch step, comma-separated) that ran on the node during the
  interval, from `Jobs` and `Applications`. `--job-labels sb` lists the jobs
  instead (`job_ids`, from their batch steps); `--job-labels table` keeps
  `job_id`/`step_id`, the one step `Periodic_metrics` holds.
  Steps are in `Jobs` once they end: the docs now suggest a monthly cron, run
  once the month's jobs have ended.

## 0.6.1

- Arrow uses the system allocator, in both commands (`ARROW_DEFAULT_MEMORY_POOL`
  still chooses): with mimalloc a month's run peaked at 2-4 times the memory it used.
- On a pseudonymised bundle, `--users/--accounts/--groups` take names with the
  bundle's `--salt-file`; a name without it is refused (it matched nothing).
- A database that cannot be reached or refuses the account is one error line
  (`DbError`, exit 1) instead of a traceback, in both commands.
- A bundle takes only runs with the same filters (the window and `raw`'s TABLEs
  aside); its manifest lists the windows it holds in `coverage`.
- `--version`; `ear_db_export.__version__`.
- `ear_db_export.layouts.ORDER` (EAR's tables, each after the tables it points to)
  is stable API, for ear-db-synth's loader.

## 0.6.0

New command, `slurm-db-export`, installed with ear-db-export: a read-only export
of Slurm's accounting database (slurmdbd's `slurm_acct_db`), for datasets that
train scheduling models. It shares ear-db-export's engine, bundle and
pseudonyms. Documentation: the new *Slurm accounting* page (`docs/slurm.md`).

- `check`: the account's grants, the read-only session, and the schema profile
  compared with each cluster's tables.
- `export raw`: `job_table` (by `time_submit`, through a `job_db_inx` range
  and primary-key pages), the steps of those jobs, `event_table` and
  `resv_table` (by `time_start`), and snapshots of `assoc_table`, `qos_table`
  and `tres_table`. Optional: `--with-usage-hour` and `--with-scripts`. One file
  per cluster, table and month; nothing is filtered out (deleted rows and jobs
  that never started are kept). User, group and account ids are pseudonymised.
- `export jobs`: one row per job from a raw bundle, with TRES decoded into
  columns, `mem_req` decoded, `state_name`, UTC timestamps, `wait_s`, `run_s`,
  `timelimit_s` and `runtime_ratio`.
- Schema profiles `slurm17` (17.11 – 19.05) and `slurm20` (20.02 – 25.05+),
  picked from `convert_version_table`. Columns added in later releases are
  optional: a missing one is a warning and is listed in the manifest.

ear-db-export:

- `Output` takes the name of the tool for the manifest (`tool=`).
- The MariaDB engine reads `tinytext`, `mediumtext`, `longtext` and `blob` columns.

## 0.5.0

One command, `export LAYOUT`: a *layout* is what to write, and each has its own
flags, so `--help` shows which filters it takes and which tables it reads. The
code follows the same split: one module per layout, read top to bottom, and the
filters owned by two selection classes, one per family of EAR's tables.
Breaking: the command line, the files and the Python API changed. There is no
compatibility with 0.4.0.

### Commands

| 0.4.0 | 0.5.0 |
|---|---|
| `export [--tables …]` | `export raw [TABLE …]` (default every table) |
| `view node_metrics --format node_metrics=long --format-opt service=X` | `export node_metrics --service X` (long format only) |
| `view job_nodes` | `export job_nodes` (window and nodes only) |
| `view power_signatures` | `export power_signatures` |
| `view eacct` | `export eacct` (streams by chunks of job steps) |
| `view … --every day --incremental` | `export … --hive --resume --to DAY`, run by a cron script (user guide) |
| `check` | `check` |

### New

- `--eacct FILE`: read `eacct -l -c` CSV files as a source (the job layouts).
- `--path TEMPLATE`: where files go inside `--out`, with `{layout} {table} {ext}
  {from} {to}` and, per row, `{date} {year} {month} {day}`; `--hive` is
  `{layout}/date={date}/part-0.{ext}`. Every layout but `raw` can split by row time.
- `--resume`: start at the bundle's checkpoint (`checkpoint.json`, referenced by
  the manifest); `--from` is the start of the first run.
- `@FILE`: read flags from a file, one per line.
- `raw` with filters is a linked dump: the selected job steps and every row
  they point to; time-series tables by their own time, or by the selected jobs.
- Every export streams: one page of a time-series table, or one chunk of 5000
  job steps, in memory at a time (a month of `node_metrics` fits).
- Every export is a bundle (`manifest.json`, `SHA256SUMS`).

### Removed

- `view`, `--every`, `--incremental`, `--lag-days`, `--state`, `--update-state`,
  `--overwrite`: the tool no longer schedules; a cron script passes `--resume
  --to "$(date -u -d '2 days ago' +%F)"`.
- The datasets `app_signatures` and `gpu_signatures` (their values are in
  `eacct`), `node_metrics` in wide format, the `allocations` format,
  `--format`/`--format-opt`, `--sample/--seed`, `--tables` groups, CSV bundles of
  `raw`, prune-horizon warnings.
- Modules `selection.py` (now a package), `extract`, `schedule`, `pipeline`,
  `formats`, `canonical`, `write`, and `eacct` (its `COLUMNS` and `HEADER` are
  now in `read.eacct`); the Python API is listed in `docs/api.rst`.

### Changed

- `--limit N` counts job steps (eacct's `-n` counts jobs).
- `job_nodes` and `power_signatures` have every `Jobs` column, `def_gpu_f`
  included; times are UTC timestamps.
- Sources yield logical column names with the types as stored; `raw` renames
  them back, so a linked dump is still the tables as stored.
- The database source streams long reads (`Jobs`, `Loops`, tables without an
  integer key) on a second read-only connection, so each chunk of job steps
  is looked up while `Jobs` streams; a query that would make the driver drop
  a stream's unread rows is an error.
- Documentation: diagrams render (Mermaid, `sphinxcontrib-mermaid`); the
  layout table and each layout's `--help` are generated from the code.

## 0.4.0

A new command line around one idea: select rows with filters, follow EAR's
links between tables, and write either the tables as stored (a *bundle*) or
analysis datasets. Breaking: every command changed.

### Commands

| 0.3.0 | 0.4.0 |
|---|---|
| `periodic` (daily timer) | `view node_metrics job_nodes --every day --incremental` |
| `periodic --from A --to B` | `view node_metrics job_nodes --every day --from A --to B --overwrite` |
| `power-signatures` | `view power_signatures --every month` |
| `app-signatures` | `view app_signatures gpu_signatures --every month` |
| `dump [--tables X]` | `export --tables all` (or `--tables X`) |
| `check-schema` | `check` |

Flags now go **after** the command (`ear-db-export view … --out DIR --salt-file F`).
The files `view` writes are the same as 0.3.0's (same paths, columns and values),
and `--incremental` uses the same watermark file, so a timer switches without a
gap or a re-export.

**Snellius timer**: replace the `ExecStart` line (keep any `--conf`, `--format`,
`--format-opt`, `--lag-days`, `--state` flags, now after `view`):

```diff
- ExecStart=/usr/local/bin/ear-db-export --out /data/ear-db-export --salt-file /etc/ear-db-export/salt periodic
+ ExecStart=/usr/local/bin/ear-db-export view node_metrics job_nodes --every day --incremental --out /data/ear-db-export --salt-file /etc/ear-db-export/salt
```

### New

- Filters for `export` and `view`: `--from/--to` (a day or a UTC time),
  `--jobs` (eacct syntax plus ranges: `123,456.0,1000-2000,789.sb`),
  `--users --accounts --groups --etags --policies`, `--apps` (globs),
  `--nodes` (hostlist) and `--tags` (ear.conf Island tags), `--batch-steps`
  (job granularity), `--sample N --seed S`, `--limit N`, `--tables` (groups
  `jobs loops periodic events energy learning all`).
- `export` writes a bundle: the selected rows of every table as stored, and
  the rows they point to (applications, signatures, GPU signatures, loops,
  periodic metrics, events), plus `schema.sql`, `SHA256SUMS` (check with
  `sha256sum -c SHA256SUMS`) and `manifest.json` (written last). Without
  filters it needs no schema profile, so any EAR version can be exported.
- `--bundle DIR` reads a bundle instead of the database: views of data a site
  sent, re-exports, conversion to CSV.
- `--as csv` writes CSV instead of Parquet.
- Profiles cover every table and column EAR 4.3, 5-6 and 7 create (Loops,
  Events, Periodic_aggregations, Global_energy, Learning_*, EAR 7's DCGM GPU
  metrics); `profiles/ear{4,6,7}.sql` hold EAR's own DDL.

### Changed

- Day partitions that exist are skipped unless `--overwrite` (as months were).
- The session sets `net_write_timeout=600` (long streamed scans).

### Removed

- Python API: `read.Reader`, `read.Records` (now `extract.Records`),
  `read.sql.SqlReader` (now `SqlSource`), `read.sql.queries.Queries`, `dump`,
  `pipeline.COMMANDS`, `schedule.daily/monthly/pending_days` (now `periods`),
  `config.ACCOUNTS` and `config.load(account=)`, `mariadb.connect(read_only=)`:
  the package can no longer open a writable session or read EAR's daemon account.

## 0.3.0

Read-only export of EAR data to Parquet: `periodic`, `power-signatures`,
`app-signatures`, `dump`, `check-schema`.
