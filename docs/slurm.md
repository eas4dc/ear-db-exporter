# Slurm accounting: slurm-db-export

`slurm-db-export`, installed with ear-db-export, reads Slurm's accounting
database (slurmdbd's `slurm_acct_db`, on MariaDB or MySQL) without writing to it.
It writes the files to train scheduling models: wait-time prediction, runtime
compared with the requested time limit, and the reconstruction of capacity and
queues. It shares ear-db-export's database engine, bundle format and
pseudonyms ({doc}`guide`), but reads only Slurm's tables.

## What it writes

| Layout | Writes | Reads |
|---|---|---|
| `raw` | slurmdbd's tables as stored, for a time window: one file per cluster, table and month | the database |
| `jobs` | one row per job allocation, decoded for analysis | a `raw` bundle |

`raw` writes these tables. Each row goes to the month of its own time, so
consecutive windows split the data with no gaps and no repeats:

| Table | Rows | Month of |
|---|---|---|
| `<cluster>_job_table` | jobs submitted in `[--from, --to]`; the columns listed under *The data* below | `time_submit` |
| `<cluster>_step_table` | the steps of those jobs | the job's `time_submit` |
| `<cluster>_event_table` | node and cluster events (down, drain, …) that started in the window; every column | `time_start` |
| `<cluster>_resv_table` | reservations that started in the window; every column | `time_start` |
| `<cluster>_assoc_table`, `qos_table`, `tres_table` | the whole table: a snapshot | the window's last day |
| `<cluster>_assoc_usage_hour_table` | with `--with-usage-hour`: hourly usage per association that started in the window | `time_start` |
| `<cluster>_job_script_table` | with `--with-scripts` (Slurm 22.05+): the batch scripts of the jobs | its first job's `time_submit` |

Nothing is filtered out:
- **`deleted` rows** are kept; whether to drop them is a decision for the analysis.
- **Jobs that never started** (`time_start = 0`: still pending, or cancelled while
  pending) are censored observations for wait-time models, and are kept.

Files: `OUT/<cluster>/<table>/year=YYYY/month=MM/part-0.parquet`; the global
tables are `OUT/<table>/…`. `--path` changes this, with the fields of
ear-db-export ({doc}`guide`) and `{table}` = `<cluster>/<table>`. Each bundle
also has `SHA256SUMS` and `manifest.json`. The manifest records:
- the window and the clusters;
- the MariaDB server version;
- `convert_version_table.version` and the Slurm release it indicates;
- the schema profile;
- the columns of each table, and those that were missing;
- rows and checksums per file.

## Running it

It needs a **read-only** MySQL account: never slurmdbd's own `StorageUser`,
which is the account slurmdbd writes with. A database administrator creates
it once:

```sql
CREATE USER 'slurm_ro'@'exporthost' IDENTIFIED BY '...';
GRANT SELECT ON slurm_acct_db.* TO 'slurm_ro'@'exporthost';
```

Keep the connection flags in a file (one flag per line, `--db-host=slurmmaster`)
and pass it with `@`. The password stays in its own file, so it never appears in `ps`:

```bash
A=@/etc/slurm-db-export/db.args       # --db-host, --db-user, --db-password-file
S="--salt-file /etc/slurm-db-export/salt"
slurm-db-export check $A                                   # grants, read-only session, profile vs. tables
slurm-db-export export raw $A --cluster lenox.hu --from 2026-09-01 --to 2026-10-13 \
    --dry-run --out /tmp/x                                 # each query, its EXPLAIN and row count
slurm-db-export export raw $A $S --cluster lenox.hu --from 2026-09-01 --to 2026-10-13 \
    --out /data/slurm/raw
slurm-db-export export jobs --bundle /data/slurm/raw --out /data/slurm/jobs
```

The defaults are port `3306` (MySQL's; slurmdbd's own 6819 is not the database)
and database `slurm_acct_db`. Without `--cluster`, every cluster of
`cluster_table` that is not marked deleted is exported. Exit codes are
ear-db-export's: `0` ok, `1` runtime error, `2` configuration or schema error.

```{export-help} raw
:tool: slurm
```

```{export-help} jobs
:tool: slurm
```

### Keeping it up to date

There is no watermark. A job's row changes until the job ends (its state, start
and end time), so a daily run re-exports the **last two months whole**: each run
rewrites those month files, and the jobs that were pending or running last time
are written again with their new state. A month older than that is final, unless
a job ran for longer than a month.

```bash
#!/bin/sh
# /etc/cron.daily/slurm-db-export: last month and this month, up to yesterday
first=$(date -u -d "$(date -u +%Y-%m-01) -1 month" +%F)
slurm-db-export export raw @/etc/slurm-db-export/db.args --salt-file /etc/slurm-db-export/salt \
    --from "$first" --to "$(date -u -d yesterday +%F)" --out /data/slurm/raw \
  && slurm-db-export export jobs --bundle /data/slurm/raw --out /data/slurm/jobs
```

`--from` must be the first day of a month (with `--path`, the start of its
smallest period: a day, a month or a year). Otherwise the run would replace a
month file with part of that month, and `raw` refuses it.

## What it does to the database

- **Read only.** It connects with the account given, in a `READ ONLY`
  session in UTC. It runs only `SELECT` statements, each on one table, with no locks and no joins.
- **job_table.** `time_submit` leads no index. The window is first mapped to a
  `job_db_inx` range by binary search on the primary key, about 40 single-row
  lookups, with a 7-day margin for jobs that slurmdbd stored late. The range is
  then read in pages of `--page-rows` rows (default 100000) in key order.
- **Steps** are read by `job_db_inx IN (…)`, 5000 jobs per query: the prefix of
  the primary key. Scripts are read by their primary key.
- **Events** are read through their index on `time_start`. **Reservations** are a
  small table, read with a plain condition.
- **`--with-usage-hour` is a full scan.** That table has no index on
  `time_start`, so check its cost with `--dry-run` before using it on a large site.

## Privacy

`--salt-file` (or an explicit `--no-pseudonymize`) is required, as in
ear-db-export. User names are never looked up or joined in. These columns are
replaced by HMAC pseudonyms (16 hex characters, the same for the same id and salt):
- `job_table`: `id_user`, `id_group`, `account`, `kill_requid`;
- `assoc_table`: `user`, `acct`, `parent_acct`;
- `event_table`: `reason_uid`.

`assoc_table.lineage` (`/root/acct/0-alice/`) is dropped. Numeric ids are hashed
as their decimal text.

What stays in clear: `job_name`, `submit_line`, `nodelist`, reservation and
event texts, and the batch scripts of `--with-scripts`. Names and paths in them
can identify a person, so review them before sending an extract off site.

## Slurm versions

Slurm names a few columns differently across releases. Two schema profiles map
them to one set of names, and the profile is picked from `convert_version_table`
(`--profile` overrides it):

| Profile | Slurm | Schema version | Difference |
|---|---|---|---|
| `slurm17` | 17.11 – 19.05 | 5 – 7 | `het_job_id`/`het_job_offset` are stored as `pack_job_id`/`pack_job_offset`. 17.11 stores step usage as `max_rss`, `ave_cpu`, … instead of `tres_usage_*`; both are exported as stored. |
| `slurm20` | 20.02 – 25.05 and later | 8 – 16 | — |

A column added in a later release than the database's is left out with a
warning, and the manifest lists it:
- `job_table`: `constraints`, `flags` and `state_reason_prev` (19.05), `submit_line`
  (21.08), `script_hash_inx` (22.05);
- `step_table`: `step_het_comp` (20.11).

`raw` keeps the physical names; `jobs` uses the profile's names. `check`
compares the profile with every table. A new Slurm release usually only adds
columns; {doc}`reference` explains how to extend a profile.

## The data

### `job_table` (raw) and `jobs`

Times are Unix seconds (UTC), and `0` means "not yet". Slurm's "no value"
(`NO_VAL`) is 4294967294, and "unlimited" (`INFINITE`) is 4294967295.

| Column | Meaning |
|---|---|
| `job_db_inx` | slurmdbd's row id: one per allocation (a requeued job may get a new row). The key that steps use. |
| `id_job` | The Slurm job id. |
| `id_array_job`, `id_array_task` | Job array id and task (`0` / `NO_VAL` when not an array). |
| `het_job_id`, `het_job_offset` | Heterogeneous job id and component (`0` / `NO_VAL` when not heterogeneous). |
| `id_user`, `id_group`, `account` | Owner, group and account: pseudonyms. |
| `id_assoc`, `id_qos`, `id_wckey`, `id_resv` | Association, QOS (`qos_table.id`), WCKey and reservation (`resv_table.id_resv`, `0` = none). |
| `partition`, `job_name`, `constraints`, `submit_line` | As submitted. |
| `flags` | Slurm's job flag bits, as stored. |
| `time_submit`, `time_eligible`, `time_start`, `time_end` | When the job was submitted, became eligible to run, started and ended. |
| `time_suspended` | Seconds spent suspended: a duration, not a time. |
| `timelimit` | Requested limit, in **minutes**. |
| `tres_req`, `tres_alloc` | Requested and allocated trackable resources, as `id=count` strings. The ids are in `tres_table`. |
| `cpus_req`, `nodes_alloc`, `nodelist`, `node_inx` | CPUs requested, nodes allocated, their names and their index ranges. |
| `mem_req` | Memory requested in MB, encoded: the top bit (`0x8000000000000000`) means per CPU. |
| `state` | State code (below), possibly with flag bits. |
| `exit_code` | The job's wait status, as stored: `exit_code >> 8` is the exit code and `exit_code & 0x7f` the signal, which `sacct` shows as `ExitCode`. |
| `priority`, `state_reason_prev` | Priority, and the last pending reason (Slurm's reason enum). |
| `kill_requid` | Who cancelled the job: a pseudonym. |
| `deleted`, `mod_time` | slurmdbd's deleted flag, and when the row last changed. |
| `script_hash_inx` | The script's row in `job_script_table` (`0` = none). |

`jobs` adds these columns to every row:

| Column | Type | Meaning |
|---|---|---|
| `req_<tres>`, `alloc_<tres>` | int | One column per TRES of `tres_table`, named `type` or `type_name`: `req_cpu`, `req_mem` (MB), `req_node`, `alloc_billing`, `alloc_gres_gpu`, … Null when the string lacks that TRES. |
| `mem_req_mb`, `mem_per_cpu` | int, bool | `mem_req` decoded: the MB, and whether they are per CPU (else per node). |
| `state_name` | string | As `sacct` names it: a flag first (`REQUEUED`, `RESIZING`, `REVOKED`, …), else `PENDING`, `RUNNING`, `SUSPENDED`, `COMPLETED`, `CANCELLED`, `FAILED`, `TIMEOUT`, `NODE_FAIL`, `PREEMPTED`, `BOOT_FAIL`, `DEADLINE`, `OUT_OF_MEMORY`. |
| `time_submit_ts`, `time_eligible_ts`, `time_start_ts`, `time_end_ts`, `mod_time_ts` | timestamp (UTC) | The times; null for `0`. |
| `wait_s` | int | `time_start - time_eligible`, in seconds. Null until the job starts. |
| `run_s` | int | `time_end - time_start`, in seconds. Null until it ends, and for jobs that never started. |
| `timelimit_s` | int | `timelimit × 60`. Null for `INFINITE` or `NO_VAL`. |
| `runtime_ratio` | float | `run_s / timelimit_s`. |

### `step_table`

One row per job step:
- `id_step`: since 20.11, `-5` is the batch script, `-4` the extern step and `-6` the
  interactive step; before 20.11, `-2` was the batch script and `-1` the extern step.
- `step_het_comp`: the heterogeneous component.
- `time_start`, `time_end`, `state`, `exit_code`, `nodes_alloc`, `tres_alloc`: as for jobs.
- `tres_usage_{in,out}_{ave,max,tot}`: the TRES strings of measured usage (memory, disk
  I/O, energy, …), as `sacct` reports them.
- `user_sec`, `user_usec`, `sys_sec`, `sys_usec`: CPU time.

### Other tables

Events, reservations, associations, QOS, TRES, hourly usage and scripts are
written with every column as slurmdbd stores them. See Slurm's
[`sacctmgr`](https://slurm.schedmd.com/sacctmgr.html) and
[`sreport`](https://slurm.schedmd.com/sreport.html) documentation for their
meaning. `event_table` rows with an empty `node_name` are cluster-wide events,
whose `tres` is the cluster's capacity from `time_start` on.

## Not covered

This tool reads only what slurmdbd stores. It does not take queue snapshots
(`squeue`, `sprio`), track versions of `slurm.conf`, or parse `slurmctld` logs.
Each of those needs its own collector on the cluster.
