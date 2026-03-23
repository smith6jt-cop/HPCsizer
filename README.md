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

### 2. Initialize runtime directories and database

```bash
mkdir -p timeseries plots logs
python -c "from lib.db import init_db; init_db()"
sqlite3 profiles.db ".tables"
```

Expected output:

```
daily_summary  jobs           tool_models
```

### 3. Run the test suite

```bash
python -m pytest tests/ -v
```

All tests should pass. If `test_plotter.py` tests fail on a headless login node
(no display), that is acceptable as long as all other test files pass.

### 4. Set environment variables

```bash
export HPCSIZER_DB="<GROUP_DIR>/hpg-sizer/profiles.db"
export HPCSIZER_ACCT="<SLURM_ACCOUNT>"
export HPCSIZER_ROOT="<GROUP_DIR>/hpg-sizer"
```

### 5. Confirm sacct access

```bash
sacct -S 2026-03-01 -a -A "$HPCSIZER_ACCT" --noheader --parsable2 -X \
  -o JobID,User,JobName,State,ReqMem,MaxRSS,Elapsed | head -10
```

If that returns data, proceed.

> **Note:** `-X` returns parent job lines only. `MaxRSS` will be empty on most
> parent lines because sacct only populates it on `.batch` step lines. This is a
> known limitation of `harvest.sh` that needs a future fix (see step 6).

### 6. Backfill historical data

The harvester's 20-minute lookback window won't capture history. Run a one-off
backfill that queries sacct *without* `-X` and does a two-pass merge of parent
and `.batch` lines so that `MaxRSS` is actually populated.

Create and run the backfill script:

```bash
cat > /tmp/backfill.py << 'PYEOF'
#!/usr/bin/env python3
"""One-time backfill: queries sacct WITHOUT -X to get .batch MaxRSS,
then merges parent + .batch lines before inserting."""

import subprocess, sys, os, json, re

sys.path.insert(0, os.environ.get("HPCSIZER_ROOT", "."))
from lib.db import init_db, insert_job

DB = os.environ["HPCSIZER_DB"]
ACCT = os.environ["HPCSIZER_ACCT"]
init_db(DB)

FIELDS = ("JobID,User,JobName,Account,QOS,State,Submit,Start,End,"
          "ReqMem,NCPUS,Timelimit,ReqTRES,MaxRSS,Elapsed,TotalCPU")

result = subprocess.run(
    ["sacct", "-S", "2026-01-01", "-a", "-A", ACCT,
     "--noheader", "--parsable2", "-o", FIELDS],
    capture_output=True, text=True,
)
if result.returncode != 0:
    print(f"sacct failed: {result.stderr}", file=sys.stderr)
    sys.exit(1)


def parse_mem(s):
    if not s or s.strip() in ("", "0"):
        return None
    s = s.strip().rstrip("nc")
    suffix = s[-1].upper() if s[-1] in "KMGTkmgt" else "M"
    try:
        n = float(s[:-1] if s[-1] in "KMGTkmgt" else s)
    except ValueError:
        return None
    return n * {"K": 1 / 1048576, "M": 1 / 1024, "G": 1.0, "T": 1024.0}.get(suffix, 1 / 1024)


def parse_elapsed(t):
    t = t.strip()
    m = re.match(r"(\d+)-(\d+):(\d+):(\d+)", t)
    if m:
        d, h, mi, s = [int(x) for x in m.groups()]
        return d * 86400 + h * 3600 + mi * 60 + s
    m = re.match(r"(\d+):(\d+):(\d+)", t)
    if m:
        h, mi, s = [int(x) for x in m.groups()]
        return h * 3600 + mi * 60 + s
    return 0


def parse_gpus(tres):
    m = re.search(r"gres/gpu(?::\w+)?=(\d+)", tres or "")
    return int(m.group(1)) if m else 0


# Two-pass: collect parent lines, then merge .batch MaxRSS
parents = {}
batch_rss = {}
fnames = FIELDS.split(",")

for line in result.stdout.strip().split("\n"):
    if not line.strip():
        continue
    parts = line.split("|")
    data = dict(zip(fnames, parts + [""] * (len(fnames) - len(parts))))
    job_id = data.get("JobID", "").strip()

    if ".batch" in job_id:
        parent_id = job_id.split(".")[0]
        rss = parse_mem(data.get("MaxRSS", ""))
        if rss is not None:
            batch_rss[parent_id] = rss
    elif ".extern" in job_id:
        continue
    elif "." not in job_id:
        state = data.get("State", "").strip()
        if state in ("RUNNING", "PENDING"):
            continue
        parents[job_id] = data

count = 0
for job_id, data in parents.items():
    state = data.get("State", "").strip()
    elapsed_sec = parse_elapsed(data.get("Elapsed", ""))
    cpu_time_sec = parse_elapsed(data.get("TotalCPU", ""))
    req_mem_gb = parse_mem(data.get("ReqMem", ""))
    ncpus = int(data.get("NCPUS", 1) or 1)

    # Prefer .batch MaxRSS over parent line
    rss_gb = batch_rss.get(job_id) or parse_mem(data.get("MaxRSS", ""))

    cpu_eff = (cpu_time_sec / (elapsed_sec * ncpus)) if elapsed_sec and ncpus else None
    mem_eff = (rss_gb / req_mem_gb) if rss_gb and req_mem_gb else None
    waste = max(req_mem_gb - (rss_gb or 0), 0) if req_mem_gb else None

    flags = []
    if state == "OUT_OF_MEMORY":
        flags.append("oom_killed")
    if mem_eff is not None and mem_eff < 0.25:
        flags.append("mem_overrequest")
    if cpu_eff is not None and cpu_eff < 0.05:
        flags.append("idle_cpu")
    if ncpus > 2 and cpu_eff is not None and cpu_eff < (1.5 / ncpus):
        flags.append("single_threaded")

    insert_job({
        "job_id": job_id,
        "user": data.get("User", ""),
        "job_name": data.get("JobName", ""),
        "account": data.get("Account", ""),
        "qos": data.get("QOS", ""),
        "state": state,
        "submit_time": data.get("Submit", ""),
        "start_time": data.get("Start", ""),
        "end_time": data.get("End", ""),
        "req_mem_gb": req_mem_gb,
        "req_cpus": ncpus,
        "req_gpus": parse_gpus(data.get("ReqTRES", "")),
        "sacct_peak_rss_gb": rss_gb,
        "sacct_elapsed_sec": elapsed_sec,
        "sacct_cpu_time_sec": cpu_time_sec,
        "cpu_efficiency": cpu_eff,
        "mem_efficiency": mem_eff,
        "waste_gb": waste,
        "flags": json.dumps(flags),
    }, db_path=DB)
    count += 1

print(f"Backfilled {count} jobs ({len(batch_rss)} had .batch MaxRSS).")
PYEOF

python /tmp/backfill.py
```

This differs from the regular harvester by querying without `-X` and doing a
two-pass merge of parent + `.batch` lines so that `MaxRSS` is actually
populated.

### 7. Verify the database

```bash
# Total jobs
sqlite3 "$HPCSIZER_DB" "SELECT count(*) FROM jobs;"

# Per-user summary
sqlite3 "$HPCSIZER_DB" \
  "SELECT user, count(*),
          round(avg(mem_efficiency),3) as avg_mem_eff,
          sum(sacct_peak_rss_gb IS NOT NULL) as has_rss
   FROM jobs GROUP BY user;"

# Confirm MaxRSS is populated (should be nonzero)
sqlite3 "$HPCSIZER_DB" \
  "SELECT count(*) as total,
          sum(sacct_peak_rss_gb IS NOT NULL) as with_rss
   FROM jobs;"

# Anomaly flags
sqlite3 "$HPCSIZER_DB" \
  "SELECT flags, count(*)
   FROM jobs WHERE flags != '[]'
   GROUP BY flags ORDER BY count(*) DESC;"
```

### 8. Test the CLI commands

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

### 9. Set up cron

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

### 10. Persist environment variables

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
