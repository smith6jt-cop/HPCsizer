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
  --noheader --parsable2 -X \
  -o JobID,User,JobName,State,ReqMem,MaxRSS,Elapsed | head -10
```

If that returns data, proceed.

> **Note:** `-X` returns parent job lines only. `MaxRSS` will be empty on most
> parent lines because sacct only populates it on `.batch` step lines. This is a
> known limitation of `harvest.sh` that needs a future fix (see step 5).

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

### 8. Set up cron

Resolve the Python path inside the named conda environment:

```bash
HPCSIZER_PYTHON="$(conda run -n hpcsizer which python)"
echo "Python path: $HPCSIZER_PYTHON"
```

Then edit your crontab:

```bash
crontab -e
```

Add the following, substituting `<GROUP_DIR>`, `<SLURM_ACCOUNT>`, and
`<PYTHON_PATH>` (the output from the `conda run` command above):

```cron
HPCSIZER_DB=<GROUP_DIR>/hpg-sizer/profiles.db
HPCSIZER_ACCT=<SLURM_ACCOUNT>
HPCSIZER_ROOT=<GROUP_DIR>/hpg-sizer

*/15 * * * * <GROUP_DIR>/hpg-sizer/bin/harvest.sh >> <GROUP_DIR>/hpg-sizer/logs/harvest.log 2>&1
0 3 * * * <PYTHON_PATH> <GROUP_DIR>/hpg-sizer/bin/update_models.py --db <GROUP_DIR>/hpg-sizer/profiles.db >> <GROUP_DIR>/hpg-sizer/logs/models.log 2>&1
```

Verify:

```bash
crontab -l
```

Wait 15-20 minutes, then check:

```bash
cat logs/harvest.log
```

> **Note:** `harvest.sh` still uses `-X`, so ongoing harvests won't capture
> `.batch` MaxRSS. The same two-pass parent+batch merge logic from the backfill
> script should eventually be incorporated into `harvest.sh`. For now the
> backfill covers historical data, and `finalize.py` (called by the sidecar
> monitor) does its own sacct query without `-X`, so jobs submitted through
> `hpg submit` will have correct MaxRSS.

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
bin/harvest.sh       Cron job — bulk-harvests sacct data every 15 minutes
bin/update_models.py Cron job — fits per-tool linear regression models nightly

lib/analyzer.py      Static analysis of sbatch scripts
lib/db.py            SQLite database layer (WAL mode)
lib/flags.py         Anomaly detection (8 flag types)
lib/recommender.py   Three-tier recommendation engine (DB > model > heuristic)
lib/plotter.py       Multi-panel time-series visualization
```
