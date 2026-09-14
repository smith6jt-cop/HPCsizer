# Sizing a job from your history

How to use past jobs to decide what a planned job should request.

The short version:

```bash
# 1. What did jobs like this actually use?
hpg history --tool Seurat --days 180

# 2. What does HPCsizer suggest for this script?
hpg recommend my_job.sbatch

# 3. Where am I wasting resources today?
hpg report --days 90
hpg flags --days 90
```

Then write the numbers into your `#SBATCH` lines, or let `hpg submit` do it.

---

## 1. Start with what your jobs actually used

`hpg history` is the ground truth. It reads every job in the database — including
ones harvested from `sacct` that HPCsizer never monitored directly — and prints
the distribution of what they used.

```bash
hpg history --tool Seurat --days 365
```

```
Historical summary for Seurat (30 jobs, last 365 days):

  Runtime (hrs)      : min=0.83  p25=1.15  median=1.62  p75=2.61  p90=12.00  max=12.00
  Peak memory (GB)   : min=39.30  p25=54.80  median=63.80  p75=72.00  p90=88.50  max=94.90
  CPU efficiency (%) : min=37.10  p25=43.40  median=51.62  p75=62.49  p90=70.09  max=77.51
  Mem efficiency (%) : min=15.35  p25=21.41  median=27.73  p75=30.31  p90=99.69  max=99.69
  Input size (GB)    : min=9.40  p25=15.50  median=20.30  p75=22.00  p90=26.10  max=26.90
```

Reading this:

| Row | What to do with it |
|---|---|
| `Peak memory (GB)` | Size `--mem` from **p90**, not the median — the median leaves half your jobs short. |
| `Runtime (hrs)` | Size `--time` from p90 with margin. Watch for a p90 pinned to a round number (here `12.00`) — that is jobs hitting their limit, not finishing. |
| `CPU efficiency (%)` | Consistently low means you are asking for cores the job never uses. |
| `Mem efficiency (%)` | `peak / requested`. A median of 27.7% means the typical job asks for ~3.6× what it needs. |
| `Input size (GB)` | Correlate with peak memory. If memory scales with input, a fitted model can predict it. |

Add `--user $USER` to see only your own jobs. Leave it off to pool the whole
group, which gives more samples.

> **Note:** `--tool` is a substring match against the tool list stored with each
> job, so `--tool R` matches almost everything. Use a specific name.

## 2. Ask for a recommendation

```bash
hpg recommend my_job.sbatch
```

```
============================================================
  HPCsizer Resource Recommendation
============================================================
  Script       : /home/user/jobs/seurat_integration.sbatch
  Language     : R
  Tools found  : SCTransform, Seurat, readRDS
  Input data   : 22.0GB

  Recommended Memory : 101.3GB
  Recommended CPUs   : 12
  Recommended Time   : 04:58:00 (n=14)
  Source             : database (n=16)

  Current request    : 256.0GB mem, 8 CPUs, 12:00:00
============================================================
```

The last two lines are the point: this script asks for 256 GB and 12 hours, and
16 comparable jobs say 101 GB and 5 hours would do.

Options:

| Flag | Default | Effect |
|---|---|---|
| `--user USER` | pooled | Size against one person's history instead of the whole group |
| `--days N` | 90 | How far back to look |

### Read the `Source` line before you trust the number

| Source | How it was computed | How much to trust it |
|---|---|---|
| `database (n=N)` | p90 of peak memory across N past jobs, × 1.10 | Best available. Check `N` is not barely 5. **Ignores your input size** — a 1 GB and a 500 GB run of the same tool get the same answer. |
| `model (n=N)` | Fitted `slope × your_input_gb + baseline`, × 1.10 | Good when `Model R²` is high (> 0.7). This is the only tier that scales with *your* dataset. |
| `heuristic (n=0)` | Static per-tool multiplier against detected input size | A guess from a lookup table. Sanity-check against `hpg history`. |
| `insufficient-data` | Nothing to compute from | No number is printed. Use `hpg history` and decide yourself. |

Memory carries 10% headroom; walltime carries 50%, because overrunning `--time`
throws away every CPU-hour already spent while overrunning a memory request
merely queues you more slowly.

`Recommended Time` is computed separately from memory, from the elapsed time of
**completed** jobs only. `TIMEOUT` jobs are excluded deliberately: their runtime
only proves where they were cut off, so including them would anchor the estimate
to the limit that already failed.

## 3. Turn the output into `#SBATCH` lines

```bash
#SBATCH --mem=104G              # Recommended Memory, rounded up
#SBATCH --cpus-per-task=12      # Recommended CPUs
#SBATCH --time=05:00:00         # Recommended Time, rounded up
```

Round up, never down. If the recommendation is far below your current request,
step down over two or three jobs rather than all at once, and confirm with
`hpg history` afterwards.

Or let HPCsizer write them for you:

```bash
hpg submit my_job.sbatch
```

It prints the same recommendation, asks whether to apply it, rewrites the
directives, injects the monitoring sidecar, and submits. `--yes` accepts without
prompting; `--as-is` keeps your numbers but still monitors the job. Directives
that are missing from your script are added; a script using `--mem-per-cpu`
keeps that form, with the total divided by the CPU count.

Jobs submitted this way are measured far more precisely than `sacct` can manage,
which makes every future recommendation better.

## 4. Find what is already being wasted

```bash
hpg report --days 90
```

```
  Total jobs           : 53
  Flagged jobs         : 25 (47.2%)
  OOM-killed jobs      : 3
  Avg memory efficiency: 29.0%
  Avg CPU efficiency   : 51.6%
  Est. wasted GB total : 7339.2 GB

  Per-user breakdown:
  User                   Jobs  Flags  MemEff%   Waste GB
  ------------------------------------------------------
  kwong                     9      0     29.6      760.3
  rlopez                   14      9     20.4     1426.4
  smith6jt                 30     16     32.9     5152.5

  Anomaly flags:
    mem_overrequest                  22 (41.5%)
    oom_killed                        3 (5.7%)
```

`hpg flags --days 90` lists the individual jobs. Of the 14 anomaly flags, these
are the ones a resource change fixes:

| Flag | Meaning | Action |
|---|---|---|
| `mem_overrequest` | Peak was under 25% of the request | Lower `--mem` |
| `oom_killed` | Killed by the OOM killer | Raise `--mem` |
| `idle_cpu` | Cores idle for most of the run | Lower `--cpus-per-task` |
| `single_threaded` | Asked for > 2 CPUs, used under 1.5 | Drop to 1–2 CPUs |
| `idle_nodes` | Allocated nodes largely unused | Lower `--nodes` |
| `node_imbalance` | Work spread unevenly across nodes | Fix distribution, or use fewer nodes |

The rest — `high_cpi`, `cache_thrashing`, `numa_misplaced`, `io_dominant`,
`lustre_metadata_heavy`, `lustre_io_dominant`, `catastrophe`,
`mem_spike_plateau` — point at code, placement or storage behavior. Changing the
request will not help; they are worth investigating separately.

> **Note:** `oom_killed` jobs are excluded from the history used for
> recommendations, because their peak memory is the point at which they died,
> not what they needed. A history full of OOMs will not raise the number on its
> own — fix one job by hand first.

To see a single job in detail:

```bash
hpg plot 60000012
```

## 5. When the answer is weak

**`Source: insufficient-data`, or `Input data : 0MB`**

No input files were detected. Only absolute paths under `/blue`, `/orange`,
`/red`, `/scratch` and `/home` are recognised; relative paths, `$SCRATCH`, `~/`
and files that do not exist yet are invisible. Reference your input by full
absolute path in the script, or size the job from `hpg history` instead.

**`Tools found : none`**

Tools are detected by reading the sbatch script itself. If your analysis lives
in a separate `analysis.R` or `analysis.py` that the script merely calls, there
is nothing to detect. Either inline the code with `Rscript -e '...'` or size
from `hpg history` with an explicit `--tool`.

**`Source: heuristic (n=0)` even though the database has thousands of jobs**

The tool name in your script does not match the tool names stored with those
jobs. Check what is actually recorded:

```bash
hpg history --days 365          # all tools, no filter
```

**No `model` tier ever appears**

Models are fitted only from jobs whose input files could be measured, and need
at least 10 of them. Jobs whose paths never matched the recognised prefixes are
silently excluded. The fit runs nightly at 3 AM via the scheduler; after a
backfill, run it immediately rather than waiting:

```bash
python bin/update_models.py --db "$HPCSIZER_DB" --days 365
```

Inspect what was fitted:

```bash
sqlite3 "$HPCSIZER_DB" \
  "select tool, round(mem_per_input_gb,3), round(baseline_gb,2), optimal_cpus,
          round(r_squared,3), sample_count, updated_at
     from tool_models order by sample_count desc;"
```

**The recommendation looks too low**

Check `n`. Five samples is the minimum and the p90 of five values is just the
maximum of five. Widen the window with `--days 365`, or drop `--user` to pool
the group.

---

## What HPCsizer cannot tell you

Sizing covers memory, CPUs and walltime. There is no recommendation for GPU
count, node count, partition or QOS — the `Recommended GPUs` line, when it
appears, is simply echoing the `--gres` your script already requested.
