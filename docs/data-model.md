# What is stored, and how every figure is defined

HPCsizer stores what Slurm reported, in base units, and derives everything
else when a report is made. A changed rule therefore applies to all history at
once, and nothing derived can go stale.

- [From sacct to a stored job](#from-sacct-to-a-stored-job)
- [Tables](#tables)
- [Derived when read](#derived-when-read)
- [The in-job series](#the-in-job-series)
- [Files](#files)
- [Versions](#versions)

## From sacct to a stored job

One query, the same for every harvest:

```
TZ=UTC SLURM_TIME_FORMAT=%s sacct -a -A <accounts> -S <start> [-E <end>] -n -p --delimiter=<0x1F> -o <fields>
```

`-a` asks for every user's jobs (without it only the caller's are returned).
Times are epoch seconds in both directions, so nothing depends on a time zone
or on daylight saving. The field separator is the ASCII unit separator, and
the output is split into records of a fixed number of fields rather than into
lines, so job names and submit lines may contain `|` and newlines. Output that
does not end on a record boundary is rejected as truncated and the query is
retried.

`<fields>` are the fields listed below that the installed `sacct` has: it is
asked once per harvest (`sacct --helpformat`), because it refuses a whole query
that names a field its version does not know. A field it lacks is stored as
unknown; the harvest log and `hpg preflight` name it.

`sacct` prints a job as one line for the job followed by one line per step
(`.batch`, `.extern`, `.0`, `.interactive`, ...). They are combined as follows.

| Stored value | From | Rule |
|---|---|---|
| requested memory | `ReqTRES` `mem` (else `AllocTRES`) | the job's total, whatever form it was asked in (`--mem`, `--mem-per-cpu`, `--mem-per-gpu`) |
| requested / allocated CPUs, nodes, GPUs | `ReqCPUS`, `AllocCPUS`, `ReqNodes`, `NNodes`, `gres/gpu` in the TRES fields | as reported |
| time limit | `TimelimitRaw` | minutes; empty for unlimited |
| elapsed | `ElapsedRaw` | seconds |
| state | `State` | first word (`CANCELLED by 1234` → `CANCELLED`); the original is kept as `state_raw` |
| CPU time | `TotalCPU` and `TRESUsageInTot` `cpu` of every step | per step the larger of the two, then summed. `TotalCPU` comes from process accounting, which Slurm does not collect for a step it kills; the polled value survives. `cpu_src` says which was used: `rusage`, `polled` or `mixed` |
| peak memory | `MaxRSS` / `TRESUsageInMax` `mem` of every step | the largest sum over steps that ran at the same time. A multi-task step counts its total over tasks, capped at the allocation. `mem_src`: `step` (one step dominates), `overlap_sum`, `ntasks_est`, `job` |
| out of memory | `State` of the job or of any step | 1 when either is `OUT_OF_MEMORY` |
| disk read / written | `TRESUsageInTot` / `TRESUsageOutTot` `fs/disk` | summed over steps |
| GPU use | `TRESUsageInMax` `gres/gpuutil`, `gres/gpumem` | maximum over steps |

Durations are read in every form `sacct` prints, including `MM:SS.mmm` for
values under an hour. A value that cannot be read is stored as unknown, never
as zero.

Jobs that have not finished are skipped and picked up by a later harvest.
HPCsizer's own jobs (names starting with `hpcsizer-`) are never stored.

## Tables

### `jobs`

One row per job, array task, or heterogeneous-job component. Identity is
`(job_id_raw, submit_ts)`: Slurm reuses job ids once its counter wraps, and a
reused id must become a new row rather than overwrite an archived one.

| Column | Unit / form | Note |
|---|---|---|
| `id` | integer | internal key |
| `job_id_raw`, `submit_ts` | integer, epoch s | identity |
| `job_id` | text | as Slurm displays it: `123`, `123_4`, `123+0` |
| `array_job_id`, `array_task_id`, `het_offset` | integer | set for array tasks and heterogeneous components |
| `source` | `sacct` \| `legacy` | `legacy` rows were migrated from version 2 and not found again in `sacct` |
| `user`, `account`, `partition`, `qos` | text | |
| `job_name`, `submit_line`, `work_dir` | text | the submit line is what identifies a `--wrap` job's program |
| `state`, `state_raw`, `exit_code`, `derived_exit_code` | text | |
| `eligible_ts`, `start_ts`, `end_ts` | epoch s | |
| `elapsed_sec` | s | |
| `req_time_min` | min | empty = unlimited |
| `req_cpus`, `alloc_cpus`, `req_nodes`, `alloc_nodes`, `node_list` | | |
| `req_mem_mb`, `alloc_mem_mb` | MB | job total |
| `req_gpus`, `alloc_gpus`, `gpu_type` | | |
| `cpu_sec`, `cpu_src`, `user_cpu_sec`, `sys_cpu_sec` | s | |
| `peak_mem_bytes`, `mem_src` | bytes | |
| `oom` | 0 / 1 | |
| `disk_read_bytes`, `disk_write_bytes` | bytes | |
| `gpu_util_max`, `gpu_util_ave`, `gpu_mem_max_bytes` | %, bytes | |
| `n_steps`, `n_tasks`, `has_interactive_step`, `restarts` | | |
| `parser_version` | integer | version of the reading rules that produced the row |
| `first_seen_ts`, `updated_ts` | epoch s | |
| `raw_json` | JSON | the job's `sacct` line as received |

### `steps`

One row per step of a job: `step_id`, `state`, `exit_code`, `start_ts`,
`end_ts`, `elapsed_sec`, `ntasks`, `alloc_cpus`, `cpu_sec`, `cpu_src`,
`max_rss_bytes`, `mem_tot_bytes`, `disk_read_bytes`, `disk_write_bytes`,
`gpu_util_max`, `gpu_util_ave`, `gpu_mem_max_bytes`, `raw_json`.

### `job_monitor`

Summary of the in-job series, one row per profiled job.

| Column | Meaning |
|---|---|
| `source` | `cgroup-v2`, `cgroup-v1`, `proc` (process tree), or `mixed` across nodes |
| `n_samples`, `interval_sec` | number of samples; median time between them |
| `peak_mem_bytes` | largest sampled memory that cannot be dropped (anonymous + shared); over several nodes, the peak of their sum over time |
| `peak_usage_bytes` | the kernel's high-water mark of memory charged to the job, page cache included |
| `cpu_sec` | CPU seconds of everything the job ran |
| `avg_cores`, `p95_cores`, `max_cores` | cores in use: mean; the value 95% of the sampled time stays under ("sustained"); busiest interval |
| `gpu_util_max`, `gpu_mem_max_bytes` | from `nvidia-smi` |
| `oom_kills` | processes the kernel killed inside the job for memory |
| `top_comm` | the largest processes at the memory peak, `name:MB,...` |
| `coverage` | share of the job's run time the samples span |
| `summary_json` | per-node figures, I/O totals, node-wide counters, notes |
| `series_path` | where the archived series are |

### `job_context`

What `hpg submit` recorded at submission: `script_hash`, `tools_json` (packages
and tools the script uses), `input_gb` (size of the data files it names),
`detail_json` (the request, what was suggested, what was applied).

### `harvest_runs` and `meta`

`harvest_runs` has one row per successful harvest: when, where, the window it
read, and its counters (records, new, updated, still open, internal, unparsed,
late, users, profiles). A failed harvest publishes nothing, so it leaves no row;
its trace is the log and the age of the last row here.

`meta` holds `schema_version`, `watermark_ts` (time of the last successful
harvest) and `last_deep_ts`.

## Derived when read

### Kind and family (`lib/classify.py`)

| Kind | A job is of this kind when |
|---|---|
| `ood:<app>` | its name contains `sys/dashboard/`: an Open OnDemand app |
| a session name | its name equals a configured `session_names` entry (default `vscode`) |
| `interactive` | it has an `.interactive` step, was started with `salloc`/`srun`, or is named `hpg-interactive…` |
| `wrap` | its submit line contains `--wrap` |
| `batch` | otherwise |

The first three are *session* kinds.

The **family** is `user/kind/name_key`, plus `+gpu` when the job has a GPU; for
sessions it is `user/kind`. `name_key` is the job name with every run of digits
replaced by `#`. For a `--wrap` job, an unnamed job, or a job whose name is in
`generic_names`, it is `cmd:` followed by the *program signature* taken from
the submit line: the script's file name, or for a wrapped command the program
it runs (`python:read_xenium.py`, `Rscript:fit.R`, `python:scimap.cli`), with
shell set-up (`module load`, `cd`, `conda run`, `apptainer exec`, `srun`, ...)
skipped and arguments ignored.

### Metrics (`lib/metrics.py`)

A value is *unknown* when it was not measured — never zero.

| Metric | Definition | Unknown when |
|---|---|---|
| `mem_req_gb` | `req_mem_mb` / 1024 | no request stored |
| `mem_used_gb` | the profile's peak when a cgroup profile covers ≥ 90% of the run; otherwise the larger of the profile's and accounting's peak | no profile, and the job ran less than two accounting samples (2 × 60 s) |
| `mem_eff` | used / requested | either is unknown |
| `cores_alloc` | `alloc_cpus` (else `req_cpus`) | |
| `cores_used` | CPU seconds / elapsed; CPU seconds chosen like memory | the job ran under 60 s; or it was killed and neither a polled value nor a profile exists |
| `cpu_eff` | used / allocated | |
| `time_eff` | elapsed / limit | no limit |
| `gpu_util` | busiest moment, profile first | not recorded |
| `mem_gbh_req`, `mem_gbh_used`, `mem_gbh_unused` | GB × hours of the run | |
| `core_h_alloc`, `core_h_used`, `core_h_unused` | cores × hours | |
| `gpu_h`, `gpu_h_idle` | GPUs × hours; idle when the busiest moment was under 5% | |
| `wait_sec` | start − eligible (or submit) | |

### Flags

| Flag | Condition |
|---|---|
| `oom` | `oom` = 1 or state `OUT_OF_MEMORY` |
| `timeout` | state `TIMEOUT`, not a session |
| `mem_overrequest` | `mem_eff` < 0.25 and requested − used ≥ 2 GB |
| `cpu_overrequest` | `cpu_eff` < 0.5, ≥ 2 cores, elapsed ≥ 300 s, not a session |
| `time_overrequest` | `COMPLETED`, limit ≥ 60 min, `time_eff` < 0.25, not a session |
| `gpu_idle` | has a GPU, `gpu_util` < 5, elapsed ≥ 300 s |
| `unmeasured` | ran, but memory is unknown |

### Group totals (`lib/report.py`)

Ratios are sums over jobs, not averages of per-job ratios:
`used = Σ used resource-hours / Σ reserved resource-hours`, over the jobs for
which both are known. `reserved` columns cover every job that ran, measured or
not; the note under the table states the difference.

### Suggestions

See [sizing-jobs.md](sizing-jobs.md) for the rules and
`lib/recommender.py` for their implementation. Policy constants are the
`[policy]` section of `hpcsizer.ini`.

## The in-job series

`bin/monitor.py` writes one tab-separated file per job and node. The first
line describes it; the second names the columns; then one line per sample.

```
#hpcsizer-sampler v1 job=51000040 node=c0700a-s1 source=cgroup-v2 pid=18233 cpus=16 start=1795058629 user=alice
t	mem	usage	usage_peak	cpu	nproc	threads	rd	wr	gpu_util	gpu_mem	oom	top	node_lustre_rd	...
```

| Column | Meaning |
|---|---|
| `t` | epoch seconds |
| `mem` | bytes that cannot be dropped: anonymous + shared memory of the job's cgroup (the sum of resident sizes in process-tree mode) |
| `usage`, `usage_peak` | bytes charged to the job including page cache; the kernel's high-water mark of it (`memory.peak`, or the largest value seen) |
| `cpu` | CPU seconds so far, cumulative |
| `nproc`, `threads` | processes and threads alive |
| `rd`, `wr` | bytes read and written by the processes alive, cumulative per process |
| `gpu_util`, `gpu_mem` | busiest GPU in %, memory of all the job's GPUs in bytes |
| `oom` | out-of-memory kills so far |
| `top` | the three largest processes, `name:MB` |
| `node_lustre_rd`, `node_lustre_wr`, `node_lustre_meta` | **node-wide** Lustre client counters, cumulative |
| `node_numa_hit`, `node_numa_miss` | **node-wide** NUMA allocation counters, cumulative |
| `perf_ipc`, `perf_cmiss` | with `perf = yes`: instructions per cycle and cache misses per reference of the busiest process |

Cumulative columns are turned into rates between samples when the series is
summarised. The `node_` columns describe the whole node and say something
about the job only when it has the node to itself; notes derived from them are
labelled "node-wide".

Samples are taken every 2 s for the first two minutes, every 10 s to ten
minutes, every 30 s to an hour, and every 60 s after that.

The file is appended to and flushed after every sample, so it is complete up
to the moment the job ends, however it ends. A last line cut off mid-write is
ignored when the file is read.

## Files

| File | Written by | Read by |
|---|---|---|
| `profiles.db` | a harvest (the owner), by building a copy and renaming it into place | everyone, read-only |
| `spool/<jobid>.<node>.tsv` | the sampler, inside the job, as the job's user (`<jobid>.<node>-<pid>-<n>.tsv` when that name is taken by something it must not write to) | the next harvest |
| `spool/submit/<jobid>.json` | `hpg submit`, after `sbatch` returns the job id | the next harvest |
| `timeseries/<jobid>-<submit_ts>.<node>.tsv.gz` | the harvest that ingested the series | `hpg plot` |

## Text that comes from jobs

A job's name, command line and working directory are written by whoever
submits it, and a process names itself. The database stores them as Slurm
reported them. Reports clean them once, before a family name is derived:
control characters (escape sequences, newlines, tabs) and the invisible
characters that reorder or hide text are replaced by their escape notation
(`\x1b`, `\x0a`, `\u202e`), so that a name cannot change a terminal, break a
table, or add a line to the commands `hpg queue` prints. This also holds for
the JSON forms. In CSV output every row is one physical line, and a text cell
that a spreadsheet would evaluate as a formula (it begins with `=`, `+`, `-` or
`@`) is given a leading apostrophe.

## Versions

- **Schema version** (`meta.schema_version`, currently 3). A version 2
  database is converted once with `hpg db migrate`; reports refuse to read an
  unconverted one and say so.
- **Parser version** (`jobs.parser_version`, currently 1). Raised when the
  rules that read `sacct` output change. Each row keeps the `sacct` record it
  came from (`raw_json`, also for its steps), and `hpg db reparse` rebuilds
  every row from it — including jobs Slurm has since deleted.
