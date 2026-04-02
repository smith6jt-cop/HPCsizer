# HPCsizer

Automated SLURM job profiling and resource sizing for HPC clusters. HPCsizer
monitors jobs during execution (including multi-node jobs), records
resource-usage statistics, detects anomalies, builds per-tool regression
models, and provides data-driven recommendations for memory and CPU requests.

## Features

### Sidecar monitoring

A lightweight sidecar process (`monitor.py`) is injected into SLURM jobs at
submit time. It polls `/proc` at adaptive intervals and collects:

- **Memory**: RSS, HWM, swap (per-process, aggregated across PIDs)
- **CPU**: user time, system time, CPU fraction, thread count
- **Disk I/O**: read/write throughput from `/proc/<pid>/io`
- **NUMA**: miss rate from `/sys/devices/system/node/*/numastat`
- **Lustre filesystem**: read/write byte throughput and metadata operation
  rates from `/proc/fs/lustre/llite/*/stats` (node-level counters for
  `/blue` and `/orange` mounts on HiPerGator)
- **Hardware counters**: CPI (cycles per instruction) and cache miss rate
  via `perf stat`, sampled every 5 minutes when available
  (`perf_event_paranoid <= 1`)

### Multi-node support

For jobs spanning multiple nodes (`SLURM_JOB_NUM_NODES > 1`), HPCsizer
launches the monitor on every node via `srun --overlap --ntasks-per-node=1`.
Each node writes its own time-series file with the hostname embedded in the
filename. At finalization, per-node data is:

- Loaded and parsed independently
- Merged into an aggregate series (RSS/IO summed, CPU averaged)
- Analyzed for cross-node imbalance (coefficient of variation of per-node
  mean CPU fraction)

### Anomaly detection (14 flags)

HPCsizer detects 14 anomaly flags across four categories:

**Resource misuse:**
- `oom_killed` — job killed by OOM
- `mem_overrequest` — peak RSS < 25% of requested memory
- `idle_cpu` — >50% of samples with CPU fraction < 0.05
- `single_threaded` — effective core usage < 1.5 with >2 CPUs requested
- `mem_spike_plateau` — peak in first 10% of runtime, then stable at <60%

**I/O patterns:**
- `io_dominant` — >50% of wall time in high disk I/O with low CPU
- `lustre_metadata_heavy` — average Lustre metadata ops/s > 100
- `lustre_io_dominant` — >50% of wall time in high Lustre I/O (read+write
  > 50 MB/s) with CPU < 10%

**Hardware efficiency:**
- `high_cpi` — average cycles per instruction > 1.0
- `cache_thrashing` — cache miss rate > 0.5
- `numa_misplaced` — average NUMA miss rate > 0.20
- `catastrophe` — step-function drop in CPU activity mid-job

**Multi-node:**
- `node_imbalance` — coefficient of variation of per-node CPU > 1.0
- `idle_nodes` — total CPU time < 50% of single-node capacity

### Visualization

Six-panel time-series plots for each job:

1. Memory RSS (GB) with requested memory line
2. CPU fraction
3. Disk I/O read (MB/s)
4. Thread count
5. Lustre I/O read + write (MB/s)
6. NUMA miss rate with 0.20 threshold line

### Recommendation engine

Three-tier system: historical database median > per-tool regression model >
cold-start heuristics. Supports Seurat, SCTransform, scanpy, Cell Ranger,
QuPath, and generic fallbacks.

### Database

SQLite with WAL mode. Schema includes job metadata, resource requests, sacct
usage, sidecar measurements, Lustre I/O totals, hardware counters (CPI,
cache miss rate), multi-node metrics, efficiency scores, and anomaly flags.
An idempotent `migrate_db()` function adds new columns to existing databases
on startup.

---

## Validation guide

Replace the placeholder values below with your own:

| Placeholder | Meaning | Example |
|---|---|---|
| `<GROUP_DIR>` | Writable group directory on shared storage | `/blue/mygroup` |
| `<SLURM_ACCOUNT>` | Your SLURM account string | `mygroup` |

### 1. Clone and create the environment

```bash
cd <GROUP_DIR>
git clone https://github.com/smith6jt-cop/HPCsizer.git hpg-sizer
cd hpg-sizer
mamba env create -f environment.yml   # or: conda env create -f environment.yml
conda activate hpcsizer
pip install -e .
```

### 2. Validate setup and run tests

```bash
python bin/validate.py
python -m pytest tests/ -v
```

`validate.py` creates the runtime directories (`timeseries/`, `plots/`,
`logs/`), initializes the database, and confirms all tables exist.

All tests should pass. If `test_plotter.py` tests fail on a headless login node
(no display), that is acceptable as long as all other test files pass.

### 3. Set environment variables

```bash
export HPCSIZER_DB="<GROUP_DIR>/hpg-sizer/profiles.db"
export HPCSIZER_ACCT="<SLURM_ACCOUNT>"
export HPCSIZER_ROOT="<GROUP_DIR>/hpg-sizer"
```

### 4. Confirm sacct access

```bash
sacct -S "$(date -d '-30 days' +%Y-%m-%d)" -a -A "$HPCSIZER_ACCT" \
  --noheader --parsable2 \
  -o JobID,User,JobName,State,ReqMem,MaxRSS,Elapsed | head -10
```

If that returns data, proceed.

> **Note:** Without `-X`, sacct returns both parent and `.batch` step lines.
> `MaxRSS` is populated on the `.batch` lines; `harvest.sh` and `backfill.py`
> both use a two-pass merge to capture it.

### 5. Backfill historical data

The harvester's 20-minute lookback window won't capture history. Run the
one-off backfill script which queries sacct *without* `-X` and does a two-pass
merge of parent + `.batch` lines so that `MaxRSS` is actually populated:

```bash
python bin/backfill.py --since 2026-01-01
```

Use `--since` to control how far back to query (default: `2026-01-01`).

### 6. Verify the database

```bash
python bin/validate.py --db
```

This prints total job count, per-user summaries, MaxRSS coverage, and anomaly
flag distribution.

### 7. Test the CLI commands

```bash
cd "$HPCSIZER_ROOT"

# Efficiency report
python bin/hpg report --days 90

# Anomaly-flagged jobs
python bin/hpg flags --days 90

# Per-tool distributional history
python bin/hpg history --days 90

# Recommend against the bundled test script
python bin/hpg recommend tests/fixtures/sample_seurat.sbatch
```

The `recommend` output should detect R as the language and Seurat,
SCTransform, and FindMarkers as tools. With no sidecar data yet it will
return cold-start heuristics.

To test input-file detection (and the memory-estimation multipliers), point
the script at a real dataset on your cluster:

```bash
cp tests/fixtures/sample_seurat.sbatch /tmp/test.sbatch
# Append an actual file path so the analyzer can stat and size it
echo 'Rscript analysis.R /blue/mygroup/data/counts.rds' >> /tmp/test.sbatch
python bin/hpg recommend /tmp/test.sbatch
```

The recommendation should now include an input-size estimate based on the
`.rds` file size multiplied by 2.5x.

### 8. Start the scheduler

Regular users on HiPerGator do not have access to `crontab`. Instead,
HPCsizer uses a lightweight self-resubmitting SLURM job that runs every
15 minutes (and triggers the nightly model update at 3 AM).

Start the scheduler:

```bash
sbatch bin/scheduler.sh
```

The job requests minimal resources (1 CPU, 512 MB, 5-min time limit). Each
run resubmits itself with `--begin=now+15minutes` before doing any work, so
the chain continues even if a single run fails.

To stop the scheduler:

```bash
scancel --name=hpcsizer-scheduler
```

Verify it is running:

```bash
squeue -u "$USER" --name=hpcsizer-scheduler
```

After 15-20 minutes, check the logs:

```bash
cat logs/harvest.log
```

> **Note:** The `--qos=minimal` line in `scheduler.sh` may need to be
> adjusted or removed depending on your cluster's available QOS tiers. Edit
> the `#SBATCH --qos` line if the job is rejected.

> **Note:** `harvest.sh` now queries without `-X` and merges `.batch` MaxRSS,
> matching the approach used by `backfill.py` and `finalize.py`.

### 9. Persist environment variables

```bash
cat >> ~/.bashrc << EOF
export HPCSIZER_DB="<GROUP_DIR>/hpg-sizer/profiles.db"
export HPCSIZER_ACCT="<SLURM_ACCOUNT>"
export HPCSIZER_ROOT="<GROUP_DIR>/hpg-sizer"
export PATH="<GROUP_DIR>/hpg-sizer/bin:\$PATH"
EOF
```

---

## Architecture overview

```
bin/hpg              CLI entry point (submit, recommend, interactive, report, history, plot, flags)
bin/backfill.py      One-time sacct backfill with .batch MaxRSS merging
bin/validate.py      Setup and database validation checks
bin/monitor.py       Sidecar process — polls /proc, Lustre stats, and perf counters
bin/finalize.py      Post-job collector — multi-node merge, sacct, flags, DB insert
bin/scheduler.sh     Self-resubmitting SLURM job — replaces crontab
bin/harvest.sh       Harvests sacct data every 15 minutes (called by scheduler)
bin/update_models.py Fits per-tool linear regression models nightly (called by scheduler)

lib/analyzer.py      Static analysis of sbatch scripts (language, tools, input files)
lib/db.py            SQLite database layer (WAL mode, schema migration)
lib/flags.py         Anomaly detection (14 flag types)
lib/recommender.py   Three-tier recommendation engine (DB > model > heuristic)
lib/plotter.py       Six-panel time-series visualization (memory, CPU, I/O, threads, Lustre, NUMA)
```
