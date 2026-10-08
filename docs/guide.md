# User guide

`ear-db-export` exports the data EAR collects (node metrics, jobs, and power
and application signatures) out of EAR's database into Parquet files. You can
analyse those with standard tools (pandas, Polars, DuckDB, Spark) and keep
them after EAR prunes its tables.

It only reads: no writes to the database, no temporary tables, no indexes
added ({doc}`audit` has the details and how to check them). It writes only to
an output directory (`--out`). Copying the files elsewhere (S3, a data lake,
another layout) is a separate step owned by the caller; see
[](#example-publishing-to-a-data-lake). What the files hold: {doc}`data`.

## How an export works

Every command runs the same four steps for each day or month it exports:

| Step | What happens | Example (`periodic`, one day) |
|---|---|---|
| 1. Read | Read EAR's *records* for the period: rows of its own tables, as EAR stores them | 14 071 `Periodic_metrics` rows, 73 `Jobs` rows and their `Applications` |
| 2. Canonical dataset | Reorganise the records into one clean table per subject: joined, timestamps, NULLs, units | `node_metrics` (one row per node and minute), `job_nodes` (one row per job step and node) |
| 3. Format | Shape each dataset for its consumer (`--format`) | `node_metrics` in `long` format: 77 391 rows, one per node, minute and metric |
| 4. Write | One Parquet file per dataset and period | `OUT/node_metrics/date=2026-07-04/part-0.parquet` |

## Step 1: reading EAR's database

EAR stores its data in MariaDB/MySQL, and this is the reader the tool ships.

- **Connection from ear.conf** (`--conf`, default `$EAR_ETC/ear/ear.conf`,
  else `/etc/ear/ear.conf`): `DBIp`, `DBPort`, `DBDatabase`, and the
  **read-only** `DBCommandsUser` / `DBCommandsPassw` (never `DBUser`). They
  are read exactly as EAR reads them (case-insensitive keys, `MariaDB*`
  aliases, EAR's defaults for missing keys), so the tool connects where
  `eacct` connects. The `Island=` lines give each node its tag (`node_tag`).
  The password is never logged.
- **Or explicit flags:** give **all** of `--db-host --db-port --db-name
  --db-user --db-password-file`. Then ear.conf is not used for the
  connection; pass `--conf` too if you want `node_tag` filled. The password is
  read from a file so it does not show up in `ps`.
- The session is set to `READ ONLY`, so a write fails even if the account
  could write.

**Schema profiles.** EAR versions name some columns differently (the job key
is `Jobs.id` in EAR 4.3 and `Jobs.job_id` from EAR 5.0). A profile maps the
names the exporter uses to the names of one EAR version: `ear4` (EAR 4.3) or
`ear6` (EAR 5.0 – 6.x) or `ear7` (EAR 7). It is autodetected from the live database, or forced
with `--profile`. `ear-db-export check-schema` compares the profile with the
database and exits `2` on a mismatch.
See {doc}`reference`.

## Step 2: canonical datasets

| Dataset | One row per | Written by |
|---|---|---|
| `node_metrics` | node and interval (~60 s): energies, CPU frequency, temperature | `periodic` |
| `job_nodes` | job step and node it ran on: who ran where, and when | `periodic` |
| `power_signatures` | job step and node, with its power signature (`ps_*`) | `power-signatures` |
| `app_signatures` | job step and node with an EARL application signature (`sig_*`) | `app-signatures` |
| `gpu_signatures` | job step, node and GPU | `app-signatures` |

User fields (`user_id`, `user_acc`, `user_group`) are pseudonymised in this
step, before any format sees them: `--salt-file` is a secret you create once,
and each value is replaced by `HMAC-SHA256(salt, value)`, first 16 hex digits.
Keep the same file forever so pseudonyms stay joinable across runs. Without
it the export refuses to run unless you pass `--no-pseudonymize`.

Every column, with units: {doc}`data`. How each dataset is built, with
examples: {doc}`reference`.

## Step 3: formats

Each dataset is written in one format, chosen with `--format DATASET=FORMAT`
(repeatable). Options for the formats go in `--format-opt KEY=VALUE`.

| Format | Datasets | Shape |
|---|---|---|
| `wide` (default) | all | the canonical dataset as it is: one column per variable |
| `long` | `node_metrics` | long ("tidy") format: one row per (node, interval, metric), with Prometheus-style metric names and a `labels` map. Needs `--format-opt service=NAME`; `source` defaults to `ear` |
| `allocations` | `job_nodes` | Slurm-style allocation records: `job_id, step_id, local_id, node, start, end`, user fields, application, `node_tag` |


## Step 4: output layout

Hive-style partitions, one directory per dataset:

```
OUT/node_metrics/date=YYYY-MM-DD/part-0.parquet
OUT/job_nodes/date=YYYY-MM-DD/part-0.parquet
OUT/power_signatures/month=YYYY-MM/part-0.parquet
OUT/app_signatures/month=YYYY-MM/part-0.parquet
OUT/gpu_signatures/month=YYYY-MM/part-0.parquet
OUT/.ear_export_state.json                         # watermark of periodic
```

`pyarrow.dataset`, DuckDB (`read_parquet('OUT/node_metrics/*/*.parquet',
hive_partitioning=true)`), Spark and Polars read a dataset directory as one
table with a `date` or `month` column. Use one `--out` per format choice: a
partition written in another format is replaced, not kept beside it. Two runs
on the same `--out` at once are refused (the second stops with an error).

## Commands

| Command | Run | Datasets | One file per |
|---|---|---|---|
| `periodic` | daily (timer/cron) | `node_metrics`, `job_nodes` | UTC day |
| `power-signatures` | one-off | `power_signatures` | month |
| `app-signatures` | one-off | `app_signatures`, `gpu_signatures` | month |
| `dump` | one-off | every table as stored (or `--tables`) | table |
| `check-schema` | diagnostic | none: shows the account's grants and compares the database with the schema profile | |

### Daily `periodic`

It exports every day after the *watermark* (the last day fully exported,
saved in `OUT/.ear_export_state.json`, or `--state`) up to `today −
--lag-days` (default 2). The watermark moves after each finished day, so a
crash resumes from the failed day. `--from/--to` re-exports days without
moving the watermark unless `--update-state` is given. It warns when the
oldest pending day is within 7 days of the *prune horizon* (the oldest day
still in `Periodic_metrics`). **EAR prunes `Periodic_metrics` (about every 2
months by default), so a broken timer loses data.**

### Monthly signature datasets

```bash
ear-db-export $OPTS power-signatures          # default: the last 12 complete months
ear-db-export $OPTS app-signatures --from 2025-10-01 --to 2026-09-30
```

Only whole, finished months are exported: `--from` and `--to` are widened
to the months they fall in, and the current month is skipped until it ends.
Months whose files all exist are skipped; `--overwrite` redoes them.

### Dumping the database

```bash
ear-db-export --out /data/ear-dump --salt-file /etc/ear-db-export/salt dump                 # every table
ear-db-export --out /data/ear-dump --salt-file /etc/ear-db-export/salt dump --tables Jobs,Applications
```

Writes `OUT/<Table>.parquet` for each table (EAR's own names and exact
types), `OUT/schema.sql` and `OUT/manifest.json` ({doc}`data`). It needs no
schema profile, so it works with any EAR version, EAR 7 included. Use an
`--out` of its own, and try `--dry-run dump` first on a large database.

## Install

Needs Python ≥ 3.9 and [uv](https://docs.astral.sh/uv/). Runtime
dependencies: `pymysql`, `pyarrow` (and `tomli` on Python < 3.11), pinned in `uv.lock`.

```bash
uv tool install git+ssh://git@github.com/eas4dc/ear-db-exporter@v0.3.0   # puts `ear-db-export` on PATH
# or, from a checkout or an unpacked release:
uv tool install /path/to/ear-db-exporter
uv sync --no-dev && .venv/bin/ear-db-export --help
```

The repository is private for now: without access to it, install from a wheel
(`uv build` on a machine that has access, then the offline steps below).

No internet on the admin node? On a connected machine with the same OS and Python:

```bash
uv build -o wheels && uv export --no-dev --no-hashes -o req.txt
pip download -r req.txt -d wheels
# copy wheels/ over, then:
python3 -m venv /opt/ear-db-export && /opt/ear-db-export/bin/pip install --no-index -f wheels ear-db-export
```

## First run checklist

```bash
umask 077; head -c 32 /dev/urandom | base64 > /etc/ear-db-export/salt   # once, keep it
OPTS="--out /data/ear-db-export --salt-file /etc/ear-db-export/salt"

ear-db-export $OPTS check-schema                   # 1. exit 0 = profile matches
ear-db-export $OPTS --dry-run periodic --from 2026-09-01 --to 2026-09-01
                                                   # 2. SQL, EXPLAIN, row counts, formats, paths; writes nothing
ear-db-export $OPTS periodic --from 2026-09-01 --to 2026-09-01   # 3. one real day; inspect it
ear-db-export $OPTS periodic                       # 4. catch up from the oldest row, then enable the timer
```

On step 4 without a state file it starts at the prune horizon. That day is
partial because older rows were pruned.

Exit codes: `0` ok, `1` runtime error (DB, I/O), `2` config or schema error.
Logs go to stdout (UTC timestamps) and end with a summary of the partitions written.

## Scheduling

systemd (`/etc/systemd/system/ear-db-export.{service,timer}`):

```ini
# ear-db-export.service
[Service]
Type=oneshot
ExecStart=/usr/local/bin/ear-db-export --out /data/ear-db-export --salt-file /etc/ear-db-export/salt periodic

# ear-db-export.timer
[Timer]
OnCalendar=*-*-* 03:15:00 UTC
Persistent=true
[Install]
WantedBy=timers.target
```

cron: `15 3 * * * root /usr/local/bin/ear-db-export --out /data/ear-db-export --salt-file /etc/ear-db-export/salt periodic >> /var/log/ear-db-export.log 2>&1` (with `CRON_TZ=UTC`).

(example-publishing-to-a-data-lake)=
## Example: publishing to a data lake

The tool stops at `--out`. A wrapper run by cron or systemd chooses the
formats and copies the partitions into whatever layout the destination
expects. This one exports in long and allocation formats and copies each day
into a lake that partitions by service, source and date:

```bash
#!/bin/bash
set -euo pipefail
OUT=/data/ear-db-export
LAKE=remote:bucket          # an rclone remote, or a mounted path with cp
SERVICE=mycluster

ear-db-export --out "$OUT" --salt-file /etc/ear-db-export/salt \
    --format node_metrics=long --format job_nodes=allocations \
    --format-opt service="$SERVICE" periodic

for f in "$OUT"/node_metrics/date=*/part-0.parquet; do
    d=$(basename "$(dirname "$f")")                 # date=YYYY-MM-DD
    rclone copyto "$f" "$LAKE/metrics/service=$SERVICE/source=ear/$d/ear_periodic_metrics.parquet"
done
for f in "$OUT"/job_nodes/date=*/part-0.parquet; do
    d=$(basename "$(dirname "$f")")
    rclone copyto "$f" "$LAKE/jobs/service=$SERVICE/source=ear/$d/ear_job_allocations.parquet"
done
```

Copy, don't move: the monthly commands skip months whose partition is still
in `--out`, and `rclone copyto` skips files that have not changed, so a failed
upload is simply retried on the next run.
