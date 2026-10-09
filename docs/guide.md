# User guide

`ear-db-export` reads the data EAR collects (node metrics, jobs, power and
application signatures, loops, events) out of EAR's database and writes it to
files, in two ways:

- **`export`** writes the tables **as stored**, the rows you select and the
  rows they point to, as a *bundle*: Parquet files plus `schema.sql`,
  `SHA256SUMS` and `manifest.json`. For archiving, moving data between sites,
  or sending an extract to EAS.
- **`view`** writes **datasets** for analysis: one clean table per subject
  (joined, UTC timestamps, NULLs, units), in Parquet or CSV, including
  `eacct`'s own CSV layout.

It only reads: no writes to the database, no temporary tables, no indexes
added ({doc}`audit` has the details and how to check them). It writes only to
an output directory (`--out`). Copying the files elsewhere (S3, a data lake,
another site) is a separate step you own; see
[](#example-publishing-to-a-data-lake). What the files hold: {doc}`data`.

## How a run works

```
filters ─► Selection ─► source (database, or --bundle) ─► extract ─┬─► export: tables as stored ─► OUT/<Table>.parquet + manifest
                                                                    └─► view: datasets ─► formats ─► OUT/<dataset>.parquet|csv
```

| Step | What happens | Example (`view node_metrics job_nodes`, one day) |
|---|---|---|
| Select | Turn the filter flags into a Selection | `--from 2026-07-04 --to 2026-07-04` |
| Read | Read the selected job steps, then follow EAR's links to the other tables | 73 `Jobs` rows, their `Applications`; 14 071 `Periodic_metrics` rows |
| Build (`view`) | One clean table per subject | `node_metrics` (one row per node and minute), `job_nodes` (one row per job step and node) |
| Format (`view`) | Shape it for its consumer (`--format`) | `node_metrics` in `long` format: 77 391 rows |
| Write | One file per table or dataset (and period) | `OUT/node_metrics/date=2026-07-04/part-0.parquet` |

## The source: EAR's database, or a bundle

EAR stores its data in MariaDB/MySQL; this is the engine the tool ships.

- **Connection from ear.conf** (`--conf`, default `$EAR_ETC/ear/ear.conf`,
  else `/etc/ear/ear.conf`): `DBIp`, `DBPort`, `DBDatabase`, and the
  **read-only** `DBCommandsUser` / `DBCommandsPassw` (never `DBUser`). They
  are read exactly as EAR reads them (case-insensitive keys, `MariaDB*`
  aliases, EAR's defaults for missing keys), so the tool connects where
  `eacct` connects. The `Island=` lines give each node its tag (`node_tag`,
  `--tags`). The password is never logged.
- **Or explicit flags:** give **all** of `--db-host --db-port --db-name
  --db-user --db-password-file`. Then ear.conf is not used for the
  connection; pass `--conf` too if you want node tags. The password is read
  from a file so it does not show up in `ps`.
- **Or a bundle:** `--bundle DIR` reads a bundle written by `export` instead
  of the database, with the same filters: views of data a site sent, CSV from
  an archive, a smaller extract of a bigger one.
- The session is set to `READ ONLY`, so a write fails even if the account
  could write.

**Schema profiles.** EAR versions name some columns differently (the job key
is `Jobs.id` in EAR 4.3 and `Jobs.job_id` from EAR 5.0; EAR 7 renamed most
columns). A profile maps the names the tool uses to the names of one EAR
version: `ear4` (EAR 4.3), `ear6` (EAR 5.0 – 6.x) or `ear7` (EAR 7). It is
autodetected from the database, or forced with `--profile`. `ear-db-export
check` compares the profile with the database and exits `2` on a mismatch.
Filters and datasets need a profile; an `export` without filters does not,
so any EAR version can be exported whole. See {doc}`reference`.

## Filters

Every filter is optional; without any, `export` reads the whole tables.
Different flags combine with AND, the values of one flag with OR
(`--users a,b --apps 'lmp*'` is "user a or b, and an application starting
with lmp").

| Flag | Selects |
|---|---|
| `--from DATE --to DATE` | job steps that **ended** in the range, and rows (periodic metrics, events, …) that end in it. `YYYY-MM-DD` is a whole UTC day (`--to` included); `YYYY-MM-DDTHH:MM[:SS]` an exact UTC time (`--to` excluded) |
| `--jobs` | job ids, in `eacct -j` syntax plus ranges: `123,456.0,1000-2000,789.sb` (`.0` a step, `.sb` the batch step) |
| `--users --accounts --groups` | users, Slurm accounts, Unix groups (comma-separated) |
| `--apps` | application names, with `*` and `?` wildcards: `--apps 'lmp*,python'` |
| `--etags --policies` | energy tags, EAR policies |
| `--nodes` | nodes, as a Slurm hostlist: `--nodes 'cpu[001-064],gpu01'` |
| `--tags` | the nodes of these ear.conf `Island` tags: `--tags gpu` |
| `--batch-steps` | job granularity: only each job's batch step (Slurm's batch script step), with every periodic row of the job. Jobs launched with `srun` alone have no batch step and are left out |
| `--sample N --seed S` | N jobs picked at random (the same N for the same seed) |
| `--limit N` | the newest N jobs, by end time (like `eacct -n`) |
| `--tables` | `export` only: table groups `jobs` (Jobs, Applications, Power_signatures, Signatures, GPU_signatures), `loops`, `periodic`, `events`, `energy` (Periodic_aggregations, Global_energy), `learning`, `all`, or table names. Default `jobs,periodic` |

What the rows of each table are, given the filters:

- **Job steps** (`Jobs`) match the job filters. Their **applications** (one per
  step and node), **power and application signatures**, **GPU signatures**
  and **loops** follow. With `--nodes`, only those nodes' applications and
  loops are kept, and the job steps that ran there.
- **Periodic metrics and events**: with job filters (`--jobs`, `--users`, …)
  only the rows of the selected jobs (idle rows are left out), from their
  first start to their last end. Without job filters, every row in the time
  range, idle nodes included, on `--nodes` if given.
- **Periodic aggregations, global energy**: the time range.
- **Learning tables** (`Learning_*`): whole.

On a pseudonymised bundle, give user filters as pseudonyms.

## Commands

| Command | What it writes |
|---|---|
| `check` | nothing: the account's grants, whether the session is read-only, and the profile compared with the database (exit `2` on a mismatch) |
| `export` | a bundle: the selected tables as stored, `schema.sql`, `SHA256SUMS`, `manifest.json` |
| `view DATASET …` | datasets: `node_metrics`, `job_nodes`, `power_signatures`, `app_signatures`, `gpu_signatures`, `eacct` ({doc}`data`) |

```bash
ear-db-export check
ear-db-export export --users alice --from 2026-09-01 --to 2026-09-30 --tables jobs,periodic,loops --out /data/alice-sep --salt-file /etc/ear-db-export/salt
ear-db-export view node_metrics job_nodes --every day --incremental --out /data/ear --salt-file /etc/ear-db-export/salt
ear-db-export view eacct --as csv --jobs 123456 --out /tmp/job --salt-file /etc/ear-db-export/salt
ear-db-export view app_signatures --bundle /data/site-x-2026 --out /tmp/site-x
```

Flags go after the command. Exit codes: `0` ok, `1` runtime error (DB, I/O),
`2` configuration or schema error. Logs go to stdout (UTC timestamps) and end
with a summary of the files written. `ear-db-export <command> --help` lists
every flag.

### Bundles (`export`)

```
OUT/manifest.json      # what, from where, how: written last, so a bundle without it is incomplete
OUT/SHA256SUMS         # checksums of every file: sha256sum -c SHA256SUMS
OUT/schema.sql         # the CREATE TABLE statements of the tables, as the server prints them
OUT/Jobs.parquet       # one file per table, every column, EAR's own names and exact types
OUT/Periodic_metrics.parquet
…
```

With `--every day` or `--every month` each table is one partition per day or
month instead (`OUT/Periodic_metrics/date=2026-09-01/part-0.parquet`), and the
manifest and checksums are updated after each period, so a bundle is
consistent even if a long export stops half-way. The manifest records the
source (database, server, EAR schema profile, auto-increment step), the
filters (users pseudonymised), whether user fields are pseudonymised and a
fingerprint of the salt; a bundle never mixes salts.

Without filters and `--tables all`, `export` is a dump of the whole database
(any EAR version). Try `--dry-run` first on a large database: it prints every
query with its `EXPLAIN` and row count, and writes nothing.

### Sending an extract to EAS

For a large extract (months of data, a whole cluster), run `export` on your
site rather than `eacct`: it writes compressed, typed files, includes the
periodic metrics, pseudonymises users before anything leaves the site, and
can be resumed.

```bash
umask 077; head -c 32 /dev/urandom | base64 > ~/ear-salt            # once; keep it private
ear-db-export export --dry-run --from 2026-01-01 --to 2026-06-30    # what it would read
ear-db-export export --from 2026-01-01 --to 2026-06-30 --every month \
    --tables jobs,periodic,loops --out /scratch/ear-extract --salt-file ~/ear-salt
cd /scratch/ear-extract && sha256sum -c SHA256SUMS                  # all OK
rsync -av /scratch/ear-extract/ receiver:/incoming/site-x/          # resumable; or any tool
```

If it stops, run it again with `--overwrite` left out: months already written
are skipped. The receiver checks the files with `sha256sum -c SHA256SUMS`.
Loops are big and read with a full table scan ({doc}`audit`); leave them out
if they are not needed.

### Datasets (`view`)

Each dataset is written in one format, chosen with `--format DATASET=FORMAT`
(repeatable). Options for the formats go in `--format-opt KEY=VALUE`.

| Format | Datasets | Shape |
|---|---|---|
| `wide` (default) | all | the dataset as it is: one column per variable |
| `long` | `node_metrics` | long ("tidy") format: one row per (node, interval, metric), with Prometheus-style metric names and a `labels` map. Needs `--format-opt service=NAME`; `source` defaults to `ear`. Parquet only |
| `allocations` | `job_nodes` | Slurm-style allocation records: `job_id, step_id, local_id, node, start, end`, user fields, application, `node_tag` |

`--as csv` writes CSV instead of Parquet (`eacct` uses `;`, as eacct does).

User fields (`user_id`, `user_acc`, `user_group`) are pseudonymised before
anything is written: `--salt-file` is a secret you create once, and each value
is replaced by `HMAC-SHA256(salt, value)`, first 16 hex digits. Keep the same
file forever so pseudonyms stay joinable across runs. Without it the tool
refuses to run unless you pass `--no-pseudonymize`. A pseudonymised bundle
needs neither: its values are already pseudonyms.

### Periods (`--every`)

| Flags | Writes |
|---|---|
| (none) | one file per table or dataset for the whole selection: `OUT/<name>.parquet` |
| `--every day --from A --to B` | one partition per UTC day: `OUT/<name>/date=YYYY-MM-DD/part-0.parquet` |
| `--every month [--from A --to B]` | one partition per calendar month (`month=YYYY-MM`); default the last 12 complete months. Only whole, finished months: `--from/--to` are widened to the months they fall in, the current month is skipped until it ends |
| `--every day --incremental` | every day after the *watermark* up to `today − --lag-days` (default 2): the daily timer |

Partitions whose files all exist are skipped; `--overwrite` redoes them.
`--incremental` keeps the *watermark* (the last day fully exported) in
`OUT/.ear_export_state.json` (or `--state`) and moves it after each finished
day, so a crash resumes from the failed day; on its first run it starts at the
*prune horizon* (the oldest day still in `Periodic_metrics`). `--from/--to`
moves the watermark only with `--update-state`. It warns when the oldest
pending day is within 7 days of the prune horizon. **EAR prunes
`Periodic_metrics` (about every 2 months by default), so a broken timer loses
data.**

Partitions are Hive-style: `pyarrow.dataset`, DuckDB
(`read_parquet('OUT/node_metrics/*/*.parquet', hive_partitioning=true)`),
Spark and Polars read a directory as one table with a `date` or `month`
column. Use one `--out` per format choice: a partition written in another
format is replaced, not kept beside it. Two runs on the same `--out` at once
are refused (the second stops with an error).

## Install

Needs Python ≥ 3.9 and [uv](https://docs.astral.sh/uv/). Runtime
dependencies: `pymysql`, `pyarrow` (and `tomli` on Python < 3.11), pinned in `uv.lock`.

```bash
uv tool install git+https://github.com/eas4dc/ear-db-exporter@v0.4.0   # puts `ear-db-export` on PATH
# or, from a checkout or an unpacked release:
uv tool install /path/to/ear-db-exporter
```

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
DAYS="node_metrics job_nodes --every day"

ear-db-export check                                           # 1. exit 0 = profile matches
ear-db-export view $DAYS --from 2026-09-01 --to 2026-09-01 --dry-run $OPTS
                                                              # 2. SQL, EXPLAIN, row counts, paths; writes nothing
ear-db-export view $DAYS --from 2026-09-01 --to 2026-09-01 $OPTS   # 3. one real day; inspect it
ear-db-export view $DAYS --incremental $OPTS                  # 4. catch up from the oldest row, then enable the timer
```

On step 4 without a state file it starts at the prune horizon. That day is
partial because older rows were pruned.

## Scheduling

systemd (`/etc/systemd/system/ear-db-export.{service,timer}`):

```ini
# ear-db-export.service
[Service]
Type=oneshot
ExecStart=/usr/local/bin/ear-db-export view node_metrics job_nodes --every day --incremental --out /data/ear-db-export --salt-file /etc/ear-db-export/salt

# ear-db-export.timer
[Timer]
OnCalendar=*-*-* 03:15:00 UTC
Persistent=true
[Install]
WantedBy=timers.target
```

cron: `15 3 * * * root /usr/local/bin/ear-db-export view node_metrics job_nodes --every day --incremental --out /data/ear-db-export --salt-file /etc/ear-db-export/salt >> /var/log/ear-db-export.log 2>&1` (with `CRON_TZ=UTC`).

Upgrading from 0.3.0: the `periodic` command became the `view` line above;
it reads the same watermark file and writes the same files (`CHANGELOG.md`).

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

ear-db-export view node_metrics job_nodes --every day --incremental \
    --format node_metrics=long --format job_nodes=allocations --format-opt service="$SERVICE" \
    --out "$OUT" --salt-file /etc/ear-db-export/salt

for f in "$OUT"/node_metrics/date=*/part-0.parquet; do
    d=$(basename "$(dirname "$f")")                 # date=YYYY-MM-DD
    rclone copyto "$f" "$LAKE/metrics/service=$SERVICE/source=ear/$d/ear_periodic_metrics.parquet"
done
for f in "$OUT"/job_nodes/date=*/part-0.parquet; do
    d=$(basename "$(dirname "$f")")
    rclone copyto "$f" "$LAKE/jobs/service=$SERVICE/source=ear/$d/ear_job_allocations.parquet"
done
```

Copy, don't move: monthly runs skip months whose partition is still in
`--out`, and `rclone copyto` skips files that have not changed, so a failed
upload is simply retried on the next run.
