# ear-db-export

Exports the data EAR collects (node metrics, jobs, power and application
signatures) to Parquet datasets, so it can be analysed with standard tools and
kept after EAR prunes its tables, or dumps EAR's tables as stored. It only
reads, and writes only to an output directory: publishing the files elsewhere
is up to the caller.

Every export runs four steps for each day or month:

```
EAR database ─► 1. read ─► 2. canonical datasets ─► 3. formats ─► 4. write ─► OUT/<dataset>/date=…/part-0.parquet
```

1. **Read** EAR's records from its MariaDB/MySQL database, with the read-only
   `DBCommandsUser` account from `ear.conf`, in a read-only session. Schema
   profiles map each EAR version's table and column names, and are
   autodetected: `ear4` (EAR 4.3), `ear6` (EAR 5.0 – 6.x), `ear7` (EAR 7).
2. **Canonical datasets**: one clean table per subject, joined, with
   timestamps, NULLs and units; user fields pseudonymised.
3. **Formats**: each dataset in the shape you choose.
4. **Write**: one Parquet file per dataset and period.

**Commands:**

| Command | Run | Datasets | Partition |
|---|---|---|---|
| `periodic` | daily (timer/cron) | `node_metrics` (per node and interval), `job_nodes` (per job, step and node) | UTC day |
| `power-signatures` | one-off | `power_signatures` | month |
| `app-signatures` | one-off | `app_signatures`, `gpu_signatures` | month |
| `dump` | one-off | every table as stored (`--tables` to pick), `schema.sql`, `manifest.json`; any EAR version, EAR 7 included | table |
| `check-schema` | diagnostic | none: shows the account's grants, compares the DB with the schema profile; exit 2 on mismatch | |

Each dataset is written as `OUT/<dataset>/date=…/part-0.parquet` (or
`month=…`), in a format chosen with `--format DATASET=FORMAT`: `wide` (default,
one column per variable), `long` for `node_metrics` (tidy rows with
Prometheus-style metric names), or `allocations` for `job_nodes`.

## Install

Needs Python ≥ 3.9 and [uv](https://docs.astral.sh/uv/).

```bash
uv tool install git+ssh://git@github.com/eas4dc/ear-db-exporter@v0.3.0   # puts `ear-db-export` on PATH
# or from a checkout:
uv tool install /path/to/ear-db-exporter
```

Offline installs (wheels) are described in the user guide.

## Quick start

```bash
umask 077; head -c 32 /dev/urandom | base64 > /etc/ear-db-export/salt   # once; keep it forever
OPTS="--out /data/ear-db-export --salt-file /etc/ear-db-export/salt"

ear-db-export $OPTS check-schema                              # exit 0 = schema profile matches
ear-db-export $OPTS --dry-run periodic --from 2026-09-01 --to 2026-09-01   # queries + EXPLAIN; writes nothing
ear-db-export $OPTS periodic --from 2026-09-01 --to 2026-09-01             # one real day; inspect it
ear-db-export $OPTS periodic                                  # catch up, then run daily from a timer
```

EAR prunes `Periodic_metrics` (about every 2 months by default), so the daily
`periodic` run must not stop for long: missed days are lost.

## Basic usage

```
ear-db-export [global flags] <command> [command flags]
```

| Flag | Meaning |
|---|---|
| `--conf` | ear.conf to read (default `$EAR_ETC/ear/ear.conf`, else `/etc/ear/ear.conf`) |
| `--db-host --db-port --db-name --db-user --db-password-file` | connect without ear.conf (all five) |
| `--salt-file` / `--no-pseudonymize` | pseudonymise user fields with this secret, or export them in clear |
| `--out` | output directory |
| `--format DATASET=FORMAT` | output format of a dataset (repeatable; default `wide`) |
| `--format-opt KEY=VALUE` | option for the formats, e.g. `service=NAME` for `long` (repeatable) |
| `--dry-run` | print SQL, `EXPLAIN`, row counts, formats and target paths; write nothing |
| `periodic --lag-days N` | export up to N days before today (default 2) |
| `--from YYYY-MM-DD --to YYYY-MM-DD` | explicit date range (`periodic` and the signature commands; the latter export whole, finished months only) |
| `dump --tables A,B` | tables to dump (default: all) |

```bash
ear-db-export $OPTS power-signatures                        # last 12 complete months
ear-db-export $OPTS app-signatures --from 2025-10-01 --to 2026-09-30
ear-db-export $OPTS --format node_metrics=long --format-opt service=mycluster periodic
ear-db-export --out /data/ear-dump --salt-file /etc/ear-db-export/salt dump   # own --out
```

Exit codes: `0` ok, `1` runtime error (DB, I/O), `2` configuration or schema
error. `ear-db-export <command> --help` lists every flag.

## Documentation

The full documentation has the user guide (offline install, systemd timer,
publishing to a data lake), **the data** (every column with its unit, for
readers who don't know EAR), **what the tool does to your database** (account,
queries, what leaves the site, personal data), the architecture, the reference
and the Python API. Build it and open it in a browser:

```bash
uv run --group docs sphinx-build -b html docs docs/_build/html
xdg-open docs/_build/html/index.html
```

Reading the code? Start at `src/ear_db_export/pipeline.py`: it runs the four
steps, each in its own module (`read/`, `canonical.py`, `formats.py`, `write.py`).
`dump.py` is the dump; `read/sql/mariadb.py` is the only file that runs SQL.

## Development

```bash
uv sync
uv run pytest                                                     # unit tests, no DB
EAR_EXPORT_TEST_CONF=$EAR_ETC/ear/ear.conf uv run pytest -m db    # integration, read-only
```
