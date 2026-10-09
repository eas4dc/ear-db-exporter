# Changelog

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
