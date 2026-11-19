# Sizing jobs with HPCsizer

How to read what `hpg` tells you about a job, and how its suggestions are made.

## The report

```console
$ hpg recommend --name align
Job         align  [batch, user bjones]
Family      bjones/batch/align
Based on    14 runs named align by bjones, 2026-10-23 to 2026-11-18: 12 COMPLETED, 1 TIMEOUT, 1 OUT_OF_MEMORY

        Requested            Used by past runs (p50 / p95 / max)        Suggested  Change
Memory  16 GB (last run)     11 / 16 / 16 GB                      n=14  24 GB      +50%
CPUs    8 (last run)         7.6 / 7.6 / 7.6 cores                n=13  8          keep
Time    02:00:00 (last run)  01:04:00 / 01:18:00 / 01:18:00       n=12  03:00:00   +50%

Why
  Memory  1.5 x the request of job 51000093, which ran out of memory
  Time    1.5 x the limit of job 51000088, which timed out

Notes
  - job 51000093 ran out of memory on 2026-11-12 with 16 GB requested
  - job 51000083 kept all 8 of its cores busy, so fewer are not suggested
  - job 51000088 hit its 02:00:00 limit on 2026-11-02

Confidence  high -- 14 runs of this job by this user

Suggested request
  #SBATCH --mem=24G
  #SBATCH --cpus-per-task=8
  #SBATCH --time=03:00:00
```

| Part | Meaning |
|---|---|
| **Family** | the earlier jobs this one is compared with (next section) |
| **Based on** | how many runs, over which dates, and how they ended |
| **Requested** | what you are asking for now. `(last run)` means you gave no value and the most recent run's request is shown; `(default)` means sbatch's default |
| **Used by past runs** | median, 95th percentile and maximum over the runs, and how many runs had a measurement (`n`) |
| **Suggested** | what the rules below arrive at; `-` when there is no basis |
| **Change** | `keep`, or the difference from the request |
| **Why** | the figure each change rests on, with the job it comes from |
| **Confidence** | `high` from ten or more runs of your own job, `medium` from fewer; one step lower when the runs are found only through the program they share (third level below), or when their peak memory varies more than threefold; `low` when the runs include other users' sessions |

The same table is printed by `hpg submit` before it submits.

## Which earlier jobs count

A job is compared with runs of the same **family**: your own earlier runs of
the same thing. "The same thing" is tried at these levels, and the first one
with enough runs (three by default) is used:

1. **the same job name** — `mplx45k-spleen`
2. **the same name apart from numbers** — `mplx12k-spleen`, `mplx30k-spleen`
   and `mplx45k-spleen` are one family, written `mplx#k-spleen`
3. **the same script or program under any name** — `run_mplx.sh`, or
   `python:archive.py`

A name that says nothing about the job — `wrap` (what Slurm calls every
`sbatch --wrap` job), `job`, `test`, `python` and the like — is skipped, and
the job is identified by its program instead: two `--wrap` jobs running
different programs are different families, and arguments never matter.

Other people's batch jobs are never used: a job called `align` is different
work for different people.

**Interactive sessions** are the exception. Open OnDemand apps (`ood:rstudio`,
`ood:jupyter`, ...), jobs named like a configured session (`vscode`), and
`salloc`/`srun --pty` shells are compared with your own sessions of that kind
first, and with everybody's sessions of that kind when you have too few (five
are then needed).

A job with a GPU and one without are always different families, even under
one name.

Only the most recent 20 runs within 180 days are used, so a job that has
changed is sized from how it runs now. Runs that say nothing — cancelled while
pending, failed within seconds — are not counted.

```bash
hpg families --user $USER
```

lists your families. To see the runs of one:

```bash
hpg history 'mplx#k-spleen'
```

## How each figure is arrived at

A request that is too small kills the job and wastes what it had computed; one
that is too large costs queue priority. The rules lean accordingly.

### Memory

- The **largest peak** among the runs that ended normally, times 1.2, rounded
  up to a whole GB (256 MB steps below 2 GB; never less than 512 MB).
- Never below the largest peak of **any** run, however it ended.
- Never above the largest request under which a run **completed** — unless a
  run **ran out of memory**: then the suggestion is at least 1.5 times that
  run's request, and the job is named.
- A request within 10% of the suggestion is left alone.

Slurm samples memory once a minute. A run shorter than two minutes has no
usable peak and does not enter the calculation; if every run was that short,
no memory figure is given.

`--mem` is always treated as the total of the job. If you write
`--mem-per-cpu`, the suggestion is given in that form, recomputed for the
suggested CPU count.

### CPUs

- Only ever **lowered**: to the highest number of cores a run kept busy on
  average, times 1.25, rounded up.
- Not lowered when that run used (nearly) all the cores it had — four busy
  cores out of four say the job can use at least four, not that sixteen would
  be too many.
- Not changed for jobs with more than one task or node.
- When the CPU count is lowered, memory is always stated as a total, so that a
  per-CPU request or a per-CPU cluster default does not shrink with it.

For **sessions** the average is not used at all: an editor waits for you most
of the time, and 0.4 cores on average says nothing about the 16 cores of a
compile. A session's CPU count is only judged from in-job profiles (below),
which record how many cores were busy for 95% of the samples.

### Time limit

- The 95th percentile of the completed runs times 1.5, and at least the
  longest run times 1.1; rounded up (5-minute steps to an hour, 15 to six
  hours, then whole hours); at least 15 minutes.
- Raised to 1.5 times the limit of any run that **timed out**, and that job is
  named.
- Not above the longest limit under which runs completed, when none timed out.
- **Not shortened in the same suggestion that lowers the CPU count** — fewer
  cores can make the run longer. The report says what history alone would
  suggest; apply it once the job has run with its new CPU count.
- For sessions, ending at the limit is normal. Their limit is never raised.

### GPUs

As requested — unless at least three runs held a GPU and none of them used it
(busiest moment under 5%), in which case the report says to drop it.

## Applying a suggestion

```bash
hpg submit --apply job.sh
```

adds the changed options to the sbatch command line, before the script name.
Your `#SBATCH` lines stay as they are; the command line overrides them. Nothing
is applied when

- the figures do not come from the job's own history (see "No history" below),
- the job has more than one task or node, or is a heterogeneous job,
- the request is `--mem=0` (the whole node),
- an sbatch option is used that `hpg` does not know — the job is then
  submitted exactly as given.

`hpg submit` without `--apply` only reports. It never prompts unless you asked
for `--apply` at a terminal.

## Jobs submitted together

Fifty array tasks or fifty `sbatch` calls in a loop cannot be sized when they
are submitted: none of them has finished. Once the first ones have,

```console
$ hpg queue
Queue of account mygroup at 2026-11-19 18:44: 2 pending, 1 running

Job                User    State    Name            Run / limit          CPUs  Memory  Its family so far                                                Would suggest
51000163           alice   RUNNING  vscode          02:59:30 / 12:00:00  16    256 GB  20 runs, memory up to 30 GB, 2.6 cores busy, longest 12:00:00    --mem=37G
51000164           alice   PENDING  mplx30k-spleen  - / 2-00:00:00       16    64 GB   10 runs, memory up to 10.3 GB, 2.8 cores busy, longest 01:00:00  --cpus-per-task=4 --mem=13G
51000165_[1-50%5]  bjones  PENDING  align           - / 02:00:00         8     16 GB   14 runs, memory up to 16 GB, 7.6 cores busy, longest 01:18:00    --mem=24G --time=03:00:00

Pending jobs that ask for more than their family has used can be resized in place:
  scontrol update JobId=51000164 MinMemoryNode=13312   # mplx30k-spleen: memory 64 GB -> 13 GB (10 runs)
  Only a job's owner can run these, and only while it is pending. A different CPU count needs a resubmission.

Pending jobs that ask for less than their family needed:
  51000165_[1-50%5] (bjones, align): asks for 16 GB; job 51000093 ran out of memory on 2026-11-12 with 16 GB requested -- resubmit with --mem=24G
```

The `scontrol update` lines are printed for you to review and run; `hpg` does
not run them.

## Profiles: what the job really did

A job submitted with `hpg submit` records its own use every few seconds
(every 2 s for the first two minutes, then every 10, 30 and 60 s). Afterwards:

```console
$ hpg job 51000040
Job 51000040  vscode  [vscode]  user alice, account mygroup, partition hpg-default, QOS mygroup
Family    alice/vscode
State     CANCELLED (CANCELLED by 1001), exit 0:15
Times     submitted 2026-11-19 03:43, waited 00:00:30, ran 08:00:00, ended 2026-11-19 11:43
Command   sbatch /home/alice/vscode_cpu.sh

        Requested  Used
Memory  256 GB     27.1 GB (11%)    peak; profile
CPUs    16         2.6 cores (16%)  average; CPU time from profile, busiest moment 14.5 cores
Time    12:00:00   08:00:00 (67%)

Flags
  mem_overrequest   peak 27.1 GB of 256 GB requested (11%) for 8 h: 1,831 GB-h unused

Profile (in-job sampler, cgroup-v2)
  480 samples every 60 s; peak memory 27.1 GB; cores average 2.6, sustained 14.5, busiest 14.5
  largest processes: node:9200,python:6100,rg:310
  time series: hpg plot 51000040
```

This is what makes three kinds of job sizeable that accounting alone cannot
measure: jobs of a minute or two, jobs that were killed, and sessions.

The recording is one line added to a temporary copy of your script. It starts
a small program in the background that ends with your script; it cannot change
your job's exit status, and nothing runs when the job ends. `--no-profile`
leaves it out. Scripts that are not run by `sh` or `bash` are submitted
unprofiled, with a note.

Because the job runs from that temporary copy, `scontrol show job` reports the
copy's path as the job's `Command`, and the copy is gone once the job has been
submitted. A script that reads its own location from that field is therefore
submitted unprofiled, with a note; use `$SLURM_SUBMIT_DIR` in such a script to
have it profiled.

```bash
hpg plot 51000040
```

draws memory, busy cores, I/O and GPU use over time against the request (it
needs matplotlib; without it the command names the series files instead).

## No history

For a job that has never run, `hpg recommend` falls back, in this order, to

1. a **fit of peak memory against input size** over other jobs that used the
   same tool and recorded their inputs (jobs submitted with `hpg submit`),
   when at least ten such runs exist and the fit explains half of the
   variance; the largest miss above the fitted line is added, and the usual
   memory headroom applied;
2. a **rule of thumb** per tool (for example three times the size of the input
   for a Seurat object), when the script names a known tool and its input
   files exist.

Both are labelled as what they are — confidence `low` and `guess` — and
neither is ever applied by `--apply`. Without either, no figure is given.

## Flags

`hpg flags` lists job families whose requests are far from their use, largest
first, each with the measurement behind it:

| Flag | Raised when |
|---|---|
| `oom` | the job ran out of memory |
| `timeout` | the job hit its time limit (not for sessions) |
| `mem_overrequest` | peak memory under 25% of the request, and at least 2 GB apart |
| `cpu_overrequest` | under half of the cores busy on average, in a job of at least five minutes with two or more cores (not for sessions) |
| `time_overrequest` | completed in under 25% of a limit of an hour or more (not for sessions) |
| `gpu_idle` | a GPU whose busiest moment was under 5% |
| `unmeasured` | too short for accounting to sample its memory |

## Choosing the constants

The headrooms, the number of runs and the look-back window are settings
(`[policy]` in `hpcsizer.ini`). To choose them on the group's own jobs rather
than by feel:

```bash
hpg backtest --set mem_headroom=1.5
```

sizes every stored job from the runs that had ended before it was submitted,
under the configured policy, the changed one, and the rule of the previous
version, and reports for each how many jobs would have been left short and how
many resource-hours it would have handed back.

## Questions

**The suggestion is higher than what I use.** Check the *Why* line: a run that
ran out of memory or timed out raises the figure for as long as it is among
the last 20 runs, and a failed run with a high peak sets a floor. `hpg history`
shows the run.

**My job's memory depends on its input.** The suggestion covers the largest
recent run. If jobs differ a lot, give them names that tell them apart —
families split by name — or expect the largest to set the figure. When the
input of the job you are about to submit is larger than in any earlier run,
the report says so.

**It says "not measured".** Every run was shorter than two minutes, or was
killed before Slurm recorded its CPU time. Submit with `hpg submit` once and
the profile fills it in.

**Can I see someone else's jobs?** Yes: `--user NAME` on `recommend`, `report`,
`history`, `flags`, `families`, `export` and `queue`. Everything stored comes
from Slurm's accounting, which members of an account can already query.
