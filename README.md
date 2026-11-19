# HPCsizer

HPCsizer compares what Slurm jobs **requested** with what they **used**, keeps
that record for every job of a group's account, and feeds it back — before a
job is submitted and on demand — so that the next request is closer to what the
job needs.

It has three parts:

| | |
|---|---|
| **Collect** | Every finished job of the account, of every member, is read from `sacct` once an hour and stored with the raw values Slurm reported. Slurm deletes its own records after a retention period; the database is the durable copy. |
| **Analyze** | Jobs are grouped into *families* — the same user running the same thing — and requested memory, CPUs, time and GPUs are set against what the runs used. Nothing derived is stored, so a corrected rule applies to all history at once. |
| **Report** | `hpg recommend` and `hpg submit` show requested / used / suggested before a job runs. `hpg report`, `history`, `flags`, `job` and `queue` answer the same question for a user, a family, a single job, or the group. |

It needs Python 3.9 or newer and nothing else: no packages to install, no
environment to activate. Any member of the group can run it from the shared
checkout.

## For members of the group

Put the checkout's `bin/` on your `PATH` (or call `bin/hpg` by its full path).

### Before you submit

```console
$ hpg recommend run_mplx.sh 45
Job         mplx45k-spleen  [batch, user alice]
Family      alice/batch/mplx#k-spleen
Based on    10 runs named mplx45k-spleen by alice, 2026-10-22 to 2026-11-18: 10 COMPLETED

        Requested   Used by past runs (p50 / p95 / max)        Suggested   Change
Memory  64 GB       12.4 / 12.4 / 12.4 GB                n=10  15 GB       -77%
CPUs    16          3.1 / 3.1 / 3.1 cores                n=10  4           -75%
Time    2-00:00:00  01:10:00 / 01:10:00 / 01:10:00       n=10  2-00:00:00  keep

Why
  Memory  largest peak 12.4 GB (job 51000042) x 1.2
  CPUs    highest average cores in use: 3.1 (job 51000042) x 1.25

Notes
  - not shortened together with the CPU count: fewer cores can lengthen the run (history alone suggests 01:45:00)

Confidence  high -- 10 runs of this job by this user

Suggested request
  #SBATCH --mem=15G
  #SBATCH --cpus-per-task=4
  #SBATCH --time=2-00:00:00
Data as of 2026-11-19 18:43 (0 s old)
```

The script is read the way `sbatch` reads it, and options you add on the command
line override it, as they do for `sbatch`:

```bash
hpg recommend --mem=32G -c 8 run_mplx.sh 45
```

No script is needed to ask about a job you have run before:

```bash
hpg recommend --name align
```

```bash
hpg recommend --like 51000093
```

```bash
hpg recommend --kind ood:rstudio
```

```bash
hpg recommend --wrap "python archive.py --sample S100"
```

When a job has never run, there is nothing to compare with, and `hpg` says so
instead of inventing a number (exit status 3).

### Submitting

`hpg submit` takes exactly what you would give `sbatch`:

```bash
hpg submit run_mplx.sh 45
```

It prints the same report (on standard error), submits the job **as you wrote
it**, and adds one line to a temporary copy of the script that records what the
job uses while it runs. `sbatch`'s own output and exit status are passed
through, so `jid=$(hpg submit --parsable job.sh)` works. Your script file is
never changed.

To submit with the suggested request instead:

```console
$ hpg submit --apply run_mplx.sh 45
...
Submitting with: --mem=15G --cpus-per-task=4
The job is profiled; afterwards: hpg job <jobid>
Submitted batch job 51000200
```

At a terminal `--apply` asks first; add `--yes` in scripts. `--as-is` skips the
report, `--no-profile` skips the recording.

### Afterwards

```bash
hpg job 51000040
```

shows one job: requested and used side by side, where each figure comes from,
its steps, and its profile if it was submitted with `hpg submit`.

```bash
hpg history align
```

lists the runs of a job family and their spread; `hpg history` alone lists your
recent jobs.

```bash
hpg queue
```

lists what is pending and running next to what each job's family has used so
far. For a pending job that asks for more than its family ever used, it prints
the `scontrol update` line that resizes it without resubmitting — useful for a
batch of jobs submitted together, which cannot be sized until the first of them
has finished.

[docs/sizing-jobs.md](docs/sizing-jobs.md) explains how to read all of this and
how the suggestions are made.

## For the group

```console
$ hpg report
HPCsizer report: account mygroup, 2026-10-20 to 2026-11-19 (30 days)
145 finished jobs of 4 user(s) in 13 job families; data as of 2026-11-19 18:44

Requested and used, by user
  User    Jobs  Core-h reserved  busy  Memory GB-h reserved  used  GPU-h  idle  OOM  Timeout
  alice     61            4,764    7%                70,511   10%   52.0  100%    0        0
  bjones    48              632   25%                 6,237   39%      -     -    1        1
  cchen     28              340   70%                 3,044   58%   40.0    0%    0        0
  dpatel     8              102   59%                 1,524   61%      -     -    0        0
  Total    145            5,838   13%                81,316   15%   92.0   57%    1        1

By kind of job
  Kind         Jobs  Core-h reserved  busy  Memory GB-h reserved  used  GPU-h  idle  OOM  Timeout
  vscode         30            4,288    6%                68,608   10%   52.0  100%    0        0
  batch          80            1,035   48%                 6,693   47%   40.0    0%    1        1
  ood:rstudio    10              480    5%                 5,760   37%      -     -    0        0
  wrap           24             28.8   75%                   230   69%      -     -    0        0
  interactive     1              6.0    8%                  24.0   20%      -     -    0        0

Memory requested and not used: the 10 largest job families
  Family                                 Jobs  Requested p50  Used p50 / max  Unused GB-h  Suggested --mem  Last run
  alice/vscode                             24         256 GB      25 / 30 GB       49,705            37 GB  2026-11-19
  alice/vscode+gpu                          6         256 GB      25 / 29 GB       11,967            37 GB  2026-11-15
  bjones/ood:rstudio                       10          96 GB      35 / 40 GB        3,629            49 GB  2026-11-17
  alice/batch/mplx#k-spleen                30          64 GB  10.3 / 12.4 GB        1,589            15 GB  2026-11-19
  ...
```

(The example is a synthetic account; see `tests/synthetic.py`.) The report goes
on with the families holding the most unused cores and idle GPU time,
interactive sessions, and the jobs that did not finish. Ratios are weighted by
resource-hours and taken over measured jobs only; what a ratio leaves out is
stated under it.

| Command | Shows |
|---|---|
| `hpg report [--user U] [--kind K] [--days N] [--format text\|md\|json\|csv] [--out FILE]` | requested against used by user, by kind of job, by family, with the suggested next request |
| `hpg families [--sort cores\|memory\|cpu\|jobs\|recent\|name]` | one line per job family |
| `hpg history [FAMILY]` | the runs of a family, or your recent jobs |
| `hpg flags [--flag F] [--jobs]` | flagged jobs by family, with the measurement behind each flag |
| `hpg job ID` | one job in full |
| `hpg queue [--user U]` | unfinished jobs next to their family's history |
| `hpg export [--format csv\|json] [--out FILE]` | one line per job, for a spreadsheet or a notebook |
| `hpg plot ID` | time series of a profiled job (needs matplotlib) |
| `hpg recommend ...` | requested / used / suggested for a job about to be submitted |
| `hpg submit ...` | `sbatch`, with the report first and a sampler inside the job |
| `hpg backtest [--set KEY=VALUE]` | what the sizing policy would have done on every stored job |
| `hpg status` | state of collection; non-zero exit when stale or incomplete |
| `hpg harvest`, `hpg schedule`, `hpg db`, `hpg preflight` | administration, see [docs/operations.md](docs/operations.md) |

Every report ends with the age of its data. `--format md` is meant to be pasted
into a message to the group; `--format csv` gives one line per family.

## Setting it up

One person, the owner of the checkout, sets up collection. In a directory every
member can read and nobody else can write:

```bash
git clone https://github.com/smith6jt-cop/HPCsizer.git hpc-sizer
```

```bash
cd hpc-sizer && cp hpcsizer.ini.example hpcsizer.ini
```

Set `accounts` in `hpcsizer.ini` to the Slurm account(s) of the group. Then:

```bash
bin/hpg preflight
```

```bash
bin/hpg harvest
```

```bash
bin/hpg schedule install
```

```bash
bin/hpg status
```

`preflight` checks what this code assumes about the cluster and prints PASS or
FAIL per item. The first `harvest` reads everything Slurm still has (182 days
by default) and creates the spool that running jobs write their profiles to.
`schedule install` adds one `scrontab` entry that runs the harvest every hour;
`--mode chain` is the alternative where `scrontab` is not available.

Moving an existing version 2 installation over is described step by step in
[docs/operations.md](docs/operations.md).

## What is measured, and what is not

HPCsizer reports what Slurm's accounting measured, and says so when Slurm did
not measure.

- **Memory** is sampled by Slurm once a minute. A job shorter than two samples
  has no usable peak; it is counted as *unmeasured*, not as having wasted its
  request.
- **CPU time** is not recorded for a job Slurm had to kill (time limit,
  `scancel`, node failure); the last sampled value is used when there is one.
- **Interactive sessions** — editors, RStudio, shells — always end by being
  stopped, and wait for input most of the time. Their average CPU use says
  little about what they need while working.

The in-job sampler added by `hpg submit` fills these gaps: it reads the job's
cgroup every few seconds (every 2 s for the first two minutes), so short jobs,
killed jobs and sessions get a real peak, real CPU time and a *sustained* core
count. A CPU reduction for a session is only ever suggested from such profiles.

## How it is built

- `lib/slurm.py` is the only place that talks to and parses `sacct`.
- `lib/db.py` stores jobs in one SQLite file. The file is never written in
  place on the shared file system: a harvest builds the new database on local
  disk and publishes it by renaming it over the old one, so readers need no
  locks and a crash cannot corrupt it.
- `lib/classify.py`, `lib/metrics.py`, `lib/history.py` and
  `lib/recommender.py` derive kinds, families, metrics, flags and suggestions
  when a report is produced.
- `bin/monitor.py` is the in-job sampler; `lib/profile.py` ingests what it wrote.
- `bin/harvest.sh` is the launcher the schedule runs.

[docs/data-model.md](docs/data-model.md) lists what is stored and how every
derived figure is defined.

## Development

```bash
python3 -m pytest tests/ -q
```

```bash
ruff check . bin/hpg && ruff format --check . bin/hpg
```

The suite needs only `pytest`. It runs the collector, the reports and
`hpg submit` end to end against stub Slurm commands and a synthetic account
with every kind of job (`tests/synthetic.py`), and runs the sampler against
simulated cgroup trees and a real process tree. The plot tests are skipped
without matplotlib (`environment.yml` describes an environment that has it).

## Acknowledgements

The idea of sampling a job from inside, without administrator access, follows
TACC Stats. HPCsizer was written for the University of Florida's HiPerGator and
makes no assumption about a site beyond Slurm with accounting.

## License

See [LICENSE](LICENSE).
