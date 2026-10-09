# ear-db-export

Reads the data EAR collects (node metrics, jobs, power and application
signatures, loops, events) from its database and writes it to files, without
ever writing to the database:

- **`export`**: EAR's tables **as stored**, the rows you select and every row
  they point to, as a *bundle* (Parquet + `schema.sql` + `SHA256SUMS` +
  `manifest.json`): to archive, move to another site, or send to EAS.
- **`view`**: clean **datasets** for analysis (joined, UTC timestamps, NULLs,
  units), in Parquet or CSV, including `eacct`'s own CSV layout.
- **`check`**: the account's grants and the schema profile against the database.

```
filters ─► Selection ─► source (EAR's DB, read-only, or a bundle) ─► follow EAR's links ─┬─► export ─► OUT/<Table>.parquet …
                                                                                          └─► view ──► OUT/<dataset>.parquet|csv
```

Filters: dates (`--from/--to`), jobs (`--jobs 123,456.0,1000-2000,789.sb`),
`--users --accounts --groups --apps --etags --policies`, nodes (`--nodes`
hostlist, `--tags` from ear.conf), `--batch-steps`, `--sample`, `--limit`,
`--tables`. User names, accounts and groups are pseudonymised (HMAC with your
secret salt) before anything is written.

It connects with EAR's read-only `DBCommandsUser` from `ear.conf`, in a
read-only session, and reads EAR 4.3, 5.x, 6.x and 7 (schema profiles
`ear4`, `ear6`, `ear7`, autodetected); without filters any version can be
exported whole.

## Install

Needs Python ≥ 3.9 and [uv](https://docs.astral.sh/uv/).

```bash
uv tool install git+https://github.com/eas4dc/ear-db-exporter@v0.4.0   # puts `ear-db-export` on PATH
# or from a checkout:
uv tool install /path/to/ear-db-exporter
```

Offline installs (wheels) are described in the user guide.

## Quick start

```bash
umask 077; head -c 32 /dev/urandom | base64 > /etc/ear-db-export/salt   # once; keep it forever
S="--salt-file /etc/ear-db-export/salt"

ear-db-export check                                                     # exit 0 = schema profile matches
ear-db-export export --users alice --from 2026-09-01 --to 2026-09-30 --dry-run   # queries + EXPLAIN; writes nothing
ear-db-export export --users alice --from 2026-09-01 --to 2026-09-30 --out /data/alice-sep $S
ear-db-export view node_metrics job_nodes --every day --incremental --out /data/ear-db-export $S   # daily timer
ear-db-export view eacct --as csv --jobs 123456 --out /tmp/job $S       # eacct's CSV layout
ear-db-export view app_signatures --bundle /data/alice-sep --out /tmp/a # datasets from a bundle
```

EAR prunes `Periodic_metrics` (about every 2 months by default), so the daily
`--incremental` run must not stop for long: missed days are lost.

## Basic usage

```
ear-db-export <check|export|view> [flags]
```

| Flags | Meaning |
|---|---|
| `--conf` | ear.conf to read (default `$EAR_ETC/ear/ear.conf`, else `/etc/ear/ear.conf`) |
| `--db-host --db-port --db-name --db-user --db-password-file` | connect without ear.conf (all five) |
| `--bundle DIR` | read a bundle instead of the database |
| `--from --to` | a day (`YYYY-MM-DD`, `--to` included) or a UTC time |
| `--jobs --users --accounts --groups --apps --etags --policies --nodes --tags --batch-steps --sample --limit` | filters: AND across flags, OR within a list |
| `export --tables` | `jobs,periodic` (default), `loops`, `events`, `energy`, `learning`, `all`, or table names |
| `view DATASET …` | `node_metrics`, `job_nodes`, `power_signatures`, `app_signatures`, `gpu_signatures`, `eacct` |
| `--format DATASET=FORMAT`, `--format-opt KEY=VALUE` | `long` for `node_metrics`, `allocations` for `job_nodes` |
| `--out`, `--as parquet\|csv` | output directory and file format |
| `--salt-file` / `--no-pseudonymize` | pseudonymise user fields with this secret, or write them in clear |
| `--every day\|month`, `--incremental`, `--lag-days`, `--overwrite` | one partition per day or month; continue after the last exported day |
| `--dry-run` | print SQL, `EXPLAIN`, row counts and target paths; write nothing |

Exit codes: `0` ok, `1` runtime error (DB, I/O), `2` configuration or schema
error. `ear-db-export <command> --help` lists every flag. Upgrading from
0.3.0: `CHANGELOG.md` maps the old commands to the new ones.

## Documentation

The full documentation has the user guide (filters, bundles, sending an
extract, offline install, systemd timer, publishing to a data lake), **the
data** (every column with its unit, for readers who don't know EAR), **what the
tool does to your database** (account, queries, what leaves the site, personal
data), the architecture, the reference and the Python API. Build it and open it
in a browser:

```bash
uv run --group docs sphinx-build -b html docs docs/_build/html
xdg-open docs/_build/html/index.html
```

Reading the code? Start at `src/ear_db_export/pipeline.py`, then
`extract.py` (EAR's links) and `read/` (the sources).
`read/sql/mariadb.py` is the only file that runs SQL.

## Development

```bash
uv sync
uv run pytest                                                     # unit tests, no DB (SQLite fake)
EAR_EXPORT_TEST_CONF=$EAR_ETC/ear/ear.conf uv run pytest -m db    # integration, read-only
```
