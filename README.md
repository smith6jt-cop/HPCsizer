# HPCsizer

Automated SLURM job profiling and resource sizing. HPCsizer monitors jobs
during execution, records resource-usage statistics, detects anomalies, builds
per-tool regression models, and provides data-driven recommendations for memory
and CPU requests.

## Recent changes

* `environment.yml` added with `name: hpcsizer` (named env, not a path prefix).
* `harvest.sh` now tries `conda env list | grep hpcsizer` before falling back
  to `env/bin/python` or system Python.
* `analyzer.py`: `_GRES_RE` handles typed GPUs (`gpu:a100:4`); `_FILE_PATH_RE`
  adds `/red` and `/home`.
* `finalize.py`: same GPU regex fix.
* `bin/hpg`: `_apply_recommendations` preserves `--mem-per-cpu` when the
  submission script uses it.
* `setup.py`: switched from broken `entry_points` to `scripts=["bin/hpg"]`.
* `finalize.py` and `harvest.sh`: sacct field changed from `CPUTime` (a billing
  metric equal to Elapsed x AllocCPUS) to `TotalCPU` (actual user+system CPU
  time), which fixes CPU efficiency calculations and enables the `idle_cpu` and
  `single_threaded` anomaly flags.
* Tests: added cases for typed GPUs, `--mem-per-cpu`, `/home` paths, `/red`
  paths.
* `bin/validate.py` added: replaces manual `mkdir`/`sqlite3` commands from the
  old steps 2–3 so environment variables (`HPCSIZER_DB`, etc.) are now set in
  the step *after* validation and testing, not before.
* `harvest.sh`: removed `-X` flag and added two-pass parent + `.batch` merge
  (same approach as `backfill.py`) so ongoing harvests now capture MaxRSS.

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

# Recommend against a test script
cat > /tmp/test.sbatch << 'EOF'
#!/bin/bash
#SBATCH --job-name=seurat_test
#SBATCH --mem=256G
#SBATCH --cpus-per-task=8
#SBATCH --time=12:00:00

module load R/4.3
Rscript -e 'library(Seurat); obj <- readRDS("/blue/group/data/test.rds")'
EOF

python bin/hpg recommend /tmp/test.sbatch
```

The `recommend` output should detect R as the language, Seurat as a tool, and
return a cold-start heuristic since there is no sidecar data yet.

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
bin/hpg              CLI entry point (submit, recommend, report, history, plot, flags)
bin/backfill.py      One-time sacct backfill with .batch MaxRSS merging
bin/validate.py      Setup and database validation checks
bin/monitor.py       Sidecar process — polls /proc during job execution
bin/finalize.py      Post-job collector — queries sacct, computes flags, stores to DB
bin/scheduler.sh     Self-resubmitting SLURM job — replaces crontab
bin/harvest.sh       Harvests sacct data every 15 minutes (called by scheduler)
bin/update_models.py Fits per-tool linear regression models nightly (called by scheduler)

lib/analyzer.py      Static analysis of sbatch scripts
lib/db.py            SQLite database layer (WAL mode)
lib/flags.py         Anomaly detection (8 flag types)
lib/recommender.py   Three-tier recommendation engine (DB > model > heuristic)
lib/plotter.py       Multi-panel time-series visualization
```
