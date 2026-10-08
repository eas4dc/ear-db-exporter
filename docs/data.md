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
| job, step | A Slurm job and one of its steps (`srun`, batch script, …). EAR records one row per **job step**. `step_id` values near 2³² (e.g. 4294967291, 4294967294) are Slurm's special steps (batch script, extern), depending on the Slurm version. |
| `local_id` | EAR's number for one application inside a step (since EAR 5.0; called `app_id` in EAR 7's database), when a step runs several. With `job_id` and `step_id` it identifies an application. Empty on EAR 4.3. |
| node, `node_tag` | Node name, and the tag of its group of nodes in EAR's configuration (`ear.conf` `Island=` lines), e.g. `cpu` or `gpu`. |
| power signature (`ps_*`) | What EARD measured for one job step on one node: average power, duration, frequency. Exists for **every** job step. |
| application signature (`sig_*`) | What EARL measured for one application on one node: power plus performance counters (CPI, bandwidth, FLOPS…). Exists only when the application ran with EARL. |
| GPU signature | The GPU part of an application signature, one per GPU. |
| policy, threshold | EAR's energy policy for the job (`monitoring`, `min_energy`, `min_time`, …) and its setting. |
| energy tag (`e_tag`) | A named set of frequencies a user may request instead of a policy. |
| default frequency (`def_f`) | The CPU frequency the job asked for, in kHz. |
| interval | One node measurement period (`start` to `end`, about 60 s). |

## Files and times

Every dataset is a directory of Parquet files, one per day or month (see
{doc}`guide` for the commands):

```
OUT/node_metrics/date=YYYY-MM-DD/part-0.parquet       # periodic, one file per UTC day
OUT/job_nodes/date=YYYY-MM-DD/part-0.parquet
OUT/power_signatures/month=YYYY-MM/part-0.parquet     # one file per calendar month
OUT/app_signatures/month=YYYY-MM/part-0.parquet
OUT/gpu_signatures/month=YYYY-MM/part-0.parquet
```

pandas, Polars, DuckDB (`read_parquet('OUT/node_metrics/*/*.parquet',
hive_partitioning=true)`) and Spark read a directory as one table with a
`date` or `month` column. Times are UTC timestamps. Missing values are NULL;
EAR stores "no value" as `0`, and the tool turns those zeros into NULL where
a zero cannot be a real value. A job step belongs to the day or month in
which it **ended**. Column names are the same whatever the EAR version.

User names, accounts and groups are replaced by pseudonyms: 16 hex digits,
the same for the same user in every file made with the same secret
(`--salt-file`).

## node_metrics: node measurements

One row per node and interval, sorted by node and time. A row belongs to the
day of its `end`. Written by `periodic`.

| column | unit | meaning |
|---|---|---|
| `node`, `node_tag` | | node name, and its tag |
| `job_id`, `step_id` | | the job step EAR saw on the node in that interval; NULL when the node was idle. Informational: to attribute energy to jobs use `job_nodes` (below) |
| `start`, `end` | UTC | interval start and end, about 60 s apart |
| `dc_energy_j` | J | energy of the whole node (DC) in the interval |
| `dram_energy_j` | J | memory (DRAM) energy |
| `pck_energy_j` | J | CPU package energy |
| `gpu_energy_j` | J | GPU energy; NULL on nodes without GPUs |
| `avg_cpu_freq_khz` | kHz | average CPU frequency |
| `temp_c` | °C | highest CPU socket temperature |

Average power in W is `dc_energy_j / (end - start)`. EAR replaces implausible
readings by the tag's `max_power`, so a constant `max_power × duration`
(e.g. 5000 W) is an EAR error value, not a measurement. DRAM energy is NULL
on CPUs without DRAM sensors (some AMD); frequency and temperature are NULL
when EAR could not read them.

## job_nodes: who ran where, and when

One row per job step and node it ran on, for steps that ended that day. Written
by `periodic`.

| column | unit | meaning |
|---|---|---|
| `job_id`, `step_id`, `local_id` | | the application (see glossary) |
| `user_id`, `user_acc`, `user_group` | | user, Slurm account and Unix group, **pseudonymised** |
| `app_name` | | job or application name, **in clear** |
| `start_time`, `end_time` | UTC | step start and end |
| `start_mpi_time`, `end_mpi_time` | UTC | when EARL started and stopped in the application; NULL without EARL |
| `policy`, `threshold` | | EAR energy policy and its setting |
| `procs` | | number of processes |
| `job_type` | | EAR's internal job type code (NULL from EAR 7) |
| `def_f` | kHz | default CPU frequency requested |
| `e_tag` | | energy tag requested, NULL if none |
| `node`, `node_tag` | | node it ran on, and its tag |

To attribute node energy to jobs, join on the node and overlapping time:
`node_metrics.node = job_nodes.node AND node_metrics.end > job_nodes.start_time
AND node_metrics.start < job_nodes.end_time`. A step can run for days before
the day it ends, so join across several days of `node_metrics`.

## power_signatures: power of every job step on every node

One row per job step and node. Written by `power-signatures`.

The `job_*` columns are those of `job_nodes` with a `job_` prefix
(`job_job_id`, `job_user_id`, …), then `node`, `node_tag`, then:

| column | unit | meaning |
|---|---|---|
| `ps_dc_power` | W | average node power during the step |
| `ps_dram_power` | W | average memory power |
| `ps_pck_power` | W | average CPU package power |
| `ps_max_dc_power`, `ps_min_dc_power` | W | highest and lowest node power during the step |
| `ps_time` | s | duration of the step on the node |
| `ps_edp` | J·s | energy-delay product, as EAR stores it (may be 0) |
| `ps_avg_f` | kHz | average CPU frequency |
| `ps_def_f` | kHz | default CPU frequency |

Energy of the step on the node: `ps_dc_power × ps_time` (J). Rows whose
power signature is missing are kept with NULL `ps_*` columns.

## app_signatures: application performance on every node

One row per job step and node where the application ran with EARL. Written by
`app-signatures`. Columns: `job_*` and `node`, `node_tag` as above, then:

| column | unit | meaning |
|---|---|---|
| `sig_dc_power`, `sig_dram_power`, `sig_pck_power` | W | average node, memory and CPU package power |
| `sig_time` | s | application time on the node |
| `sig_edp` | J·s | energy-delay product: time² × node power (NULL from EAR 7) |
| `sig_cpi` | | cycles per instruction (higher: more stalls) |
| `sig_tpi` | | memory transactions (cache lines) per instruction |
| `sig_gbs` | GB/s | memory bandwidth |
| `sig_io_mbs` | MB/s | I/O throughput |
| `sig_gflops` | GFLOP/s | CPU floating-point rate |
| `sig_perc_mpi` | % | share of time spent in MPI |
| `sig_avg_f`, `sig_avg_imc_f`, `sig_def_f` | kHz | average CPU, average memory-controller and default CPU frequency |
| `sig_cpu_util` | % | CPU utilisation (EAR ≥ 6; NULL before) |
| `sig_instructions`, `sig_cycles` | count | instructions and cycles |
| `sig_l1_misses`, `sig_l2_misses`, `sig_l3_misses` | count | cache misses |
| `sig_flops_1` … `sig_flops_4` | count | single-precision floating-point instructions: scalar, 128-, 256-, 512-bit vectors |
| `sig_flops_5` … `sig_flops_8` | count | double precision, same widths |

The counts (instructions to FLOPS) are NULL when the site sets
`DBReportSigDetail=0`.

## gpu_signatures: GPUs of each application

One row per GPU of each application signature. Written by `app-signatures`.

| column | unit | meaning |
|---|---|---|
| `job_id`, `step_id`, `local_id`, `node` | | the application and node (no prefix here) |
| `gpu_index` | | GPU number on the node (0, 1, …); kept even when a GPU in the middle has no row |
| `gpu_power` | W | average GPU power |
| `gpu_freq`, `gpu_mem_freq` | kHz | average GPU and GPU memory frequency |
| `gpu_util`, `gpu_mem_util` | % | GPU and GPU memory utilisation |
| `gpu_gflops` | GFLOP/s | GPU floating-point rate (EAR ≥ 6; NULL before) |

## Other formats

`--format DATASET=FORMAT` writes a dataset in another shape.

**`long`** (`node_metrics`): one row per node, interval and metric, sorted by
`metric_name, node, timestamp`. Missing values are left out.

| column | meaning |
|---|---|
| `timestamp` | interval end |
| `duration_s` | interval length (≈ 60 s) |
| `service`, `source` | from `--format-opt service=…` (required) and `source=…` (default `ear`) |
| `metric_name` | `ear_node_dc_energy_joules`, `ear_node_dram_energy_joules`, `ear_node_pck_energy_joules`, `ear_node_gpu_energy_joules`, `ear_node_avg_cpu_freq_khz`, `ear_node_temp_celsius` |
| `value` | float64 |
| `labels` | map: `node`, and `job_id`/`step_id` of the job EAR saw in that interval |

**`allocations`** (`job_nodes`): `job_id, step_id, local_id, node, start, end,
user_id, user_acc, user_group, app_name, policy, job_type, procs, node_tag`,
where `start`/`end` are the step's `start_time`/`end_time`. To attribute
`long` metrics: `metrics.labels['node'] = alloc.node AND metrics.timestamp >
alloc.start AND metrics.timestamp - duration_s < alloc.end`.

## dump: the database as stored

`dump` writes EAR's tables as they are in the database, for loading them
elsewhere or for analyses the datasets above do not cover (loops, events):

```
OUT/<Table>.parquet      # e.g. Jobs.parquet, Periodic_metrics.parquet: every row and column
OUT/schema.sql           # the CREATE TABLE statement of each table
OUT/manifest.json        # database, server version, start/end time, rows per table, pseudonymised?
```

Unlike the datasets, a dump keeps EAR's own table and column names, which
change between EAR versions (EAR 7 renamed many), its epoch-second times and
its `0` for "no value". Column types are exact (unsigned integers stay
unsigned, `FLOAT` stays 32-bit). User columns (`user_id`, `user_name`,
`user_acc`, `user_group`, `pod_user_id`) are pseudonymised as above. Tables
are read one after the other while EAR keeps writing, so a dump is not a
snapshot of one instant: `manifest.json` has its start and end time.
