# The data: what each file holds

This page is for the people who analyse the files, and assumes no knowledge
of EAR. What the tool does to the database and which fields are hidden is in
{doc}`audit`.

## EAR in one paragraph

EAR is an energy manager for HPC clusters. A daemon on every node (**EARD**)
measures the node's power about once a minute, and the power of every job
step that runs on it. Jobs can also load the **EAR library (EARL)**, which
reads the CPU's performance counters while the application runs and may
change CPU frequencies according to an energy **policy**. EAR stores all of
this in a MariaDB/MySQL (or PostgreSQL) database, and deletes old node
measurements after a while.

## Glossary

| Term | Meaning |
|---|---|
| job, step | A Slurm job and one of its steps (`srun`, batch script, …). EAR records one row per **job step**. `step_id` values near 2³² are Slurm's special steps (see the table at the end of this page). |
| `local_id` | EAR's number for one application inside a step (since EAR 5.0; called `app_id` in EAR 7's database), when a step runs several. With `job_id` and `step_id` it identifies an application. Empty on EAR 4.3. |
| node, `node_tag` | Node name, and the tag of its group of nodes in EAR's configuration (`ear.conf` `Island=` lines), e.g. `cpu` or `gpu`. |
| power signature (`ps_*`) | What EARD measured for one job step on one node: average power, duration, frequency. Exists for **every** job step. |
| application signature | What EARL measured for one application on one node: power plus performance counters (CPI, bandwidth, FLOPS…). Exists only when the application ran with EARL. In `eacct`'s signature columns. |
| GPU signature | The GPU part of an application signature, one per GPU. |
| policy, threshold | EAR's energy policy for the job (`monitoring`, `min_energy`, `min_time`, …) and its setting. |
| energy tag (`e_tag`) | A named set of frequencies a user may request instead of a policy. |
| default frequency (`def_f`) | The CPU frequency the job asked for, in kHz. |
| interval | One node measurement period (`start` to `end`, about 60 s). |

## Files and times

Every export writes a *bundle* into `--out` ({doc}`guide` has the commands):
the data files, `SHA256SUMS`, `manifest.json` and, when the run had a time
window, `checkpoint.json`. A layout is one file (`OUT/node_metrics.parquet`),
or with `--hive` one file per UTC day of the rows' time:

```
OUT/node_metrics/date=YYYY-MM-DD/part-0.parquet
OUT/job_nodes/date=YYYY-MM-DD/part-0.parquet
```

pandas, Polars, DuckDB (`read_parquet('OUT/node_metrics/*/*.parquet',
hive_partitioning=true)`) and Spark read such a directory as one table with a
`date` column. A row without a time goes to `date=__HIVE_DEFAULT_PARTITION__`,
which these readers read as a null date. With `--as csv` the files are CSV
instead (not `node_metrics`). Times are UTC timestamps (`eacct` keeps eacct's
own: epoch seconds and UTC date strings). Missing values are NULL; EAR stores "no value" as
`0`, and the layouts turn those zeros into NULL where a zero cannot be a real
value. A job step belongs to the window in which it **ended**. Column names are
the same whatever the EAR version. Rows come roughly in time order, as EAR
inserted them: sort them if you need an order.

User names, accounts and groups are replaced by pseudonyms: 16 hex digits,
the same for the same user in every file made with the same secret
(`--salt-file`).

## node_metrics: node measurements, long format

One row per node, interval and metric that has a value. `export node_metrics`.

| column | type | meaning |
|---|---|---|
| `timestamp` | UTC | interval end |
| `duration_s` | s | interval length (about 60 s) |
| `service` | | from `--service` (e.g. the cluster's name) |
| `source` | | `ear` |
| `metric_name` | | one of the metrics below |
| `value` | float64 | the value, in the metric's unit |
| `labels` | map | `node`; and the job steps that ran on the node during the interval, as `--job-labels` says (none when idle) |

| `metric_name` | unit | meaning |
|---|---|---|
| `ear_node_dc_energy_joules` | J | energy of the whole node (DC) in the interval |
| `ear_node_dram_energy_joules` | J | memory (DRAM) energy |
| `ear_node_pck_energy_joules` | J | CPU package energy |
| `ear_node_gpu_energy_joules` | J | GPU energy; absent on nodes without GPUs |
| `ear_node_avg_cpu_freq_khz` | kHz | average CPU frequency |
| `ear_node_temp_celsius` | °C | highest CPU socket temperature |

Average power in W is `value / duration_s` for the energies. EAR replaces
implausible readings by the tag's `max_power`, so a constant `max_power ×
duration` (e.g. 5000 W) is an EAR error value, not a measurement. A metric EAR
could not read (DRAM energy on some AMD CPUs, frequency or temperature when
reading failed) has no row.

The job labels list the steps whose `[start_time, end_time)` overlaps the
interval on that node, from `Jobs` and `Applications` as in `job_nodes`,
comma-separated and sorted. `--job-labels` chooses which:

| `--job-labels` | label | example | lists |
|---|---|---|---|
| `all` (default) | `job_steps` | `4521.0,4521.3,4521.sb` | every step, `job.step`; `job.sb` for the batch step |
| `sb` | `job_ids` | `4521,4533` | the jobs whose batch step ran there: one name per job. Jobs without a batch step (`salloc` + `srun`) are not listed |
| `table` | `job_id`, `step_id` | `4521`, `0` | the one step the `Periodic_metrics` row holds; `Jobs` is not read. On a shared node that is one of the steps only |

The batch step spans the whole job, so with `all` a job that runs `srun`
steps is listed twice (`4521.0,4521.sb`); a job whose work runs in the batch
script itself has only `4521.sb`. A step that ran 2 s of the interval is listed
like one that ran all of it. The labels tell **who was on the node**, the
cluster's occupancy: the value is still the whole node's, and is not split
between the steps (each step's own power is in `power_signatures`). To expand
the list: `unnest(string_split(labels['job_steps'], ','))` (DuckDB).

```{important}
EAR writes a step to `Jobs` when it **ends**. A step still running when the
export runs is not listed, and a later run does not go back to add it. Export a
period once every job of it has ended: at least the cluster's longest walltime
after its last day (see the monthly cron in {doc}`guide`).
```

Comparing with `table` on a test cluster: EAR's own label also covers steps
it measured but never wrote to `Jobs` (failed jobs), and lingers on the node
for up to half an hour after a step ends.

## job_nodes: who ran where, and when

One row per job step and node it ran on, for steps that ended in the window.
`export job_nodes`.

| column | unit | meaning |
|---|---|---|
| `job_id`, `step_id`, `local_id` | | the application (see glossary) |
| `user_id`, `user_acc`, `user_group` | | user, Slurm account and Unix group, **pseudonymised** |
| `app_name` | | job or application name, **in clear** |
| `start_time`, `end_time` | UTC | step start and end |
| `start_mpi_time`, `end_mpi_time` | UTC | when EARL started and stopped in the application; without EARL, `start_mpi_time` is NULL and `end_mpi_time` is 2038-01-19 03:14:07 (EAR's "not set") |
| `policy`, `threshold` | | EAR energy policy and its setting |
| `procs` | | number of processes |
| `job_type` | | EAR's internal job type code (NULL from EAR 7) |
| `def_f` | kHz | default CPU frequency requested |
| `e_tag` | | energy tag requested, NULL if none |
| `def_gpu_f` | | default GPU frequency requested, as EAR 7 stores it (NULL before EAR 7) |
| `node`, `node_tag` | | node it ran on, and its tag |

To find every step on node N at time t: `node = N AND start_time <= t AND t <
end_time`. To attribute node metrics: `labels['node'] = node AND timestamp >
start_time AND timestamp - duration_s < end_time`. A step is in the file of
the day it **ended**, which may be days after it started: read the following
days' files too.

## power_signatures: power of every job step on every node

One row per job step and node. `export power_signatures`.

The `job_*` columns are those of `job_nodes` with a `job_` prefix
(`job_job_id`, `job_user_id`, …), then `node`, `node_tag`, then:

| column | unit | meaning |
|---|---|---|
| `ps_dc_power` | W | average node power during the step |
| `ps_dram_power` | W | average memory power |
| `ps_pck_power` | W | average CPU package power |
| `ps_edp` | J·s | energy-delay product, as EAR stores it (may be 0) |
| `ps_max_dc_power`, `ps_min_dc_power` | W | highest and lowest node power during the step |
| `ps_time` | s | duration of the step on the node |
| `ps_avg_f` | kHz | average CPU frequency |
| `ps_def_f` | kHz | default CPU frequency |

Energy of the step on the node: `ps_dc_power × ps_time` (J). Rows whose
power signature is missing are kept with NULL `ps_*` columns. With
`--batch-steps`, only each job's batch step: one row per job and node.

## eacct: eacct's own CSV layout

One row per job step and node, with the 153 columns `eacct -l -c` writes (EAR
6 and 7): `JOBID;STEPID;APPID;USERID;…;NODENAME`, the EARL signature
(`AVG_CPUFREQ_KHZ` … `DPOPS_512`), four sockets' `TEMP*`, `CPU_POWER*` and
`DRAM_POWER*`, `GPU0_*` … `GPU7_*` and the EARD power signature (`NODEMGR_*`). Tools that read eacct files (EAR's analytics, and
this tool's `--eacct`) can read it: `export eacct --as csv` writes it with `;`,
like eacct.

Differences with eacct's own output: times and dates are UTC (eacct prints
local time), numbers are in their shortest form (`310.523`, not `310.523000`),
empty strings are empty (eacct prints a space), and user fields are
pseudonymised. Columns EAR's database does not store (stalls, cache hits and
rates, per-socket temperature and power, GPU temperature,
`EAR_LEARNING_PHASE`) are `0`, as eacct
prints them when it reads the database; steps without an EARL signature have
`EARL_ENABLED` 0 and zeros in the signature columns.

(bundles-the-tables-as-stored)=
## raw: the tables as stored

`export raw` writes EAR's tables as they are in the database, for archiving,
moving them to another site, or analyses the layouts above do not cover
(loops, events):

```
OUT/<Table>.parquet      # e.g. Jobs.parquet: the selected rows, every column
OUT/schema.sql           # the CREATE TABLE statement of each table
OUT/SHA256SUMS           # checksums: sha256sum -c SHA256SUMS
OUT/manifest.json        # written last: source, profile, filters, rows and checksum per file
```

Unlike the layouts, `raw` keeps EAR's own table and column names, which
change between EAR versions (EAR 7 renamed many; `manifest.json` says which
schema profile applies), its epoch-second times and its `0` for "no value".
Column types are exact (unsigned integers stay unsigned, `FLOAT` stays 32-bit:
read through a layout, a stored 0.1 is 0.1 again). User columns
(`user_id`, `user_name`, `user_acc`, `user_group`, `pod_user_id`) are
pseudonymised as above. A bundle can be read back with `--bundle DIR`.

With filters, the rows are the selected job steps and every row they point
to: their applications, power and application signatures, GPU signatures
(EAR stores a signature's GPUs as an id range, stepping by the server's
auto-increment step, which the manifest records) and loops. The time-series
tables (periodic metrics, events, aggregations, global energy) have the rows
of the window and nodes when no job filter is given. With job filters they are
read from the first start to the last end of the selected steps plus one hour
(a node reports its last interval after a step ends). Periodic metrics and
events then keep the rows of those jobs (of every step of a job, or only of the
steps `--jobs` names); aggregations and global energy have no job, so they keep
every row of that time span. Tables are read one after the other while EAR keeps writing, so a
bundle is not a snapshot of one instant.

Special values worth knowing:

| Value | Meaning |
|---|---|
| `step_id` ≥ 4294967280 (0xFFFFFFF0) | Slurm's special steps: 4294967291 the batch script (Slurm ≥ 20.11), 4294967294 the batch step on older Slurm and PBS, 4294967292 extern, 4294967293 interactive |
| `end_mpi_time` / `earl_end_time` 2147483647 | "not set" (the largest 32-bit time): the step ran without EARL; datasets show it as 2038-01-19 03:14:07 |
| `job_id` 0 in `Periodic_metrics` | the node was idle |
