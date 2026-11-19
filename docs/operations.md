# Operating HPCsizer

For the person who owns the checkout and keeps collection running.

- [What runs where](#what-runs-where)
- [Fresh installation](#fresh-installation)
- [Moving from version 2](#moving-from-version-2)
- [Day to day](#day-to-day)
- [The schedule](#the-schedule)
- [Files and how they are written](#files-and-how-they-are-written)
- [Permissions](#permissions)
- [Profiling](#profiling)
- [Upgrading and going back](#upgrading-and-going-back)
- [What only the cluster can confirm](#what-only-the-cluster-can-confirm)
- [When something is wrong](#when-something-is-wrong)

## What runs where

| Where | What | As whom |
|---|---|---|
| a small Slurm job once an hour (`scrontab`) | `bin/harvest.sh` → `hpg harvest`: asks `sacct` for the account's jobs, stores them, ingests profiles | the owner |
| a login node, on demand | every `hpg` report; the owner's reports refresh the data first when it is older than 15 minutes | any member |
| inside a job submitted with `hpg submit` | `bin/monitor.py`, appending samples to the spool | the job's user |

Nothing else runs. There is no daemon, no watchdog, and nothing is executed
when a job ends.

## Fresh installation

Choose a directory that every member of the group can read and only you can
write.

```bash
git clone https://github.com/smith6jt-cop/HPCsizer.git hpc-sizer
```

```bash
cd hpc-sizer && cp hpcsizer.ini.example hpcsizer.ini
```

Edit `hpcsizer.ini`: set `accounts` under `[slurm]`, and under `[schedule]`
whatever your site requires for a one-CPU, 20-minute job (`account`, `qos`,
`partition`). The scheduled job starts without your shell environment, so these
must be in the file, not in `~/.bashrc`.

```bash
bin/hpg preflight
```

Stop at any `FAIL`; the line says what is wrong. `WARN` lines are explained in
[What only the cluster can confirm](#what-only-the-cluster-can-confirm).

```bash
bin/hpg harvest
```

The first harvest reads all of Slurm's retention (182 days unless `first_days`
says otherwise), in 30-day slices, and creates the spool directory.

```bash
bin/hpg schedule install
```

```bash
bin/hpg status
```

## Moving from version 2

Version 2 kept its database in SQLite's WAL mode on the shared file system,
ran as a chain of self-resubmitting jobs with watchdogs, and wrote into the
checkout. The steps below stop it, clear what it left behind, convert the
database and start version 3. Run them in the old checkout, after the pull
request with version 3 has been merged.

**1. Stop the old chain.** Nothing else may write the database during the
migration; `hpg db migrate` refuses while these jobs are queued.

```bash
scancel --name=hpcsizer-scheduler; scancel --name=hpcsizer-watchdog
```

**2. Remove core dumps and stale job output** — before any `git stash`, which
would otherwise copy them.

```bash
rm -f core.* slurm-*.out
```

**3. Put local changes aside.** Untracked helper files go to `backup/`; a local
edit of the old scheduler is stashed (its settings now belong in
`hpcsizer.ini`).

```bash
mkdir -p backup && mv bin/harvest_ingest.py bin/harvest.sh.bak backup/ 2>/dev/null; git stash push -m pre-v3 -- bin/scheduler.sh
```

**4. Update.**

```bash
git pull --ff-only origin main
```

**5. Configure.** Carry over what the local edit of `bin/scheduler.sh` set
(account, partition) to the `[schedule]` section.

```bash
cp hpcsizer.ini.example hpcsizer.ini
```

**6. Check the cluster.**

```bash
bin/hpg preflight
```

**7. Convert the database.** The version 2 file and its `-wal` are copied to
local disk, read there, and written out as a new version 3 file; the originals
are moved to `backup/`. Rows the old parser got wrong keep what was right
(peak memory, elapsed time) and have their CPU time marked unknown rather than
zero.

```bash
bin/hpg db migrate
```

**8. Re-read everything Slurm still has.** This is what brings in the other
members' jobs, the final states the old harvester dropped, and correct CPU
times; a re-read job replaces its migrated twin.

```bash
bin/hpg harvest --since 182d
```

**9. Check.** Every member with jobs should be listed, and coverage against
`sacct` should be complete.

```bash
bin/hpg status
```

**10. Schedule it.**

```bash
bin/hpg schedule install --every hour
```

**11. Try a profiled job**, then look at it once it has finished and the next
harvest has run (or run `bin/hpg harvest` yourself).

```bash
bin/hpg submit --wrap "sleep 150"
```

```bash
bin/hpg job JOBID
```

The job view should show a `Profile` section with a non-zero peak and a source
of `cgroup-v2` or `cgroup-v1`.

### Going back to version 2

```bash
bin/hpg schedule remove
```

```bash
git checkout 07999dc && git stash pop
```

Copy `backup/profiles.v2.<date>.db` back as `profiles.db`, and
`backup/profiles.v2.<date>.db-wal`, if there is one, as `profiles.db-wal`; then
`sbatch bin/scheduler.sh`.

## Day to day

```console
$ hpg status --no-sacct
HPCsizer 3.0.0
  Database        : /blue/mygroup/hpc-sizer/profiles.db (schema 3, 392.0 KB)
  Accounts        : mygroup
  Last harvest    : 2026-11-19 18:47 (0 s ago) on login5, manual
                    167 records: 0 new, 2 changed, 156 unchanged, 3 still open, 6 internal, 0 unparsed, 0 late
  Deep pass       : 2026-11-19 18:47
  Rows            : 158 (sacct 158)
  Last 30 days    : alice 61 | bjones 48 | cchen 28 | dpatel 8
                    COMPLETED 96 | CANCELLED 29 | TIMEOUT 13 | FAILED 5 | OUT_OF_MEMORY 1 | NODE_FAIL 1
  Schedule        : scrontab: 17 * * * * /blue/mygroup/hpc-sizer/bin/harvest.sh
  Spool           : /blue/mygroup/hpc-sizer/spool drwxrws--T
  Interpreter     : /usr/local/bin/python3, Python 3.13.18, SQLite 3.53.1
STATUS: OK
```

Without `--no-sacct` it also asks `sacct` which jobs finished in the last 30
days and adds a `Coverage` line saying how many of them are stored.

It exits non-zero when the last successful harvest is older than `stale_hours`
(26), when finished jobs known to `sacct` are missing from the database, or
when there is no database. Lines starting with `!` name each problem — for
instance that only one user has jobs (the sign that other members' jobs are not
visible to the collector) or that nothing is scheduled.

What a harvest does about the usual trouble:

| Situation | Behaviour |
|---|---|
| `sacct` fails or its output is cut short | retried three times with growing pauses; then nothing is published and the next run starts from the same point |
| one scheduled run fails | the Slurm job still exits 0, so no mail; it fails (and mails, with `mail_type = FAIL`) only once no harvest has succeeded for `stale_hours` |
| the accounting database was down for a day | each run starts three days before the last success, so the gap is re-read |
| a job's record reaches accounting late | once a week a deep pass re-reads 35 days |
| two harvests at once | the second finds the lock and leaves; the first publishes the same records |
| a harvest is killed mid-way | the published database is untouched; its lock is taken over at once by a harvest on the same node, after 30 minutes from another |

To re-read a period by hand:

```bash
bin/hpg harvest --since 2026-09-01
```

The log of scheduled runs is `logs/harvest.log` (one line per run; rotated at
5 MB, one generation kept).

## The schedule

```console
$ hpg schedule install --dry-run
Would install (scrontab mode):
# >>> hpcsizer harvest (managed by 'hpg schedule'; edits here are overwritten) >>>
#SCRON --job-name=hpcsizer-harvest
#SCRON --account=mygroup
#SCRON --time=00:20:00
#SCRON --mem=1gb
#SCRON --ntasks=1
#SCRON --cpus-per-task=1
#SCRON --output=/dev/null
#SCRON --error=/dev/null
#SCRON --mail-type=FAIL
17 * * * * /blue/mygroup/hpc-sizer/bin/harvest.sh
# <<< hpcsizer harvest <<<
```

`scrontab` is Slurm's own cron: the entry runs as a batch job and keeps one job
id for all its runs. Other entries in your scrontab are left alone; only the
block between the two marker lines is managed.

`--every 15min`, `hour` (default) or `day` sets the cadence. `hpg schedule show`
prints what is installed; `hpg schedule remove` removes it.

Where `scrontab` is not available:

```bash
bin/hpg schedule install --mode chain
```

starts a single job that queues its own successor before doing anything else,
only when none is pending, and with `--dependency=singleton`, so that a second
chain started by accident merges into the first. There is no watchdog.

## Files and how they are written

Everything lives under the checkout unless `[paths]` in `hpcsizer.ini` says
otherwise.

| Path | Content |
|---|---|
| `profiles.db` | the database: one SQLite file, no `-wal` or `-shm` beside it |
| `profiles.db.prev` | the file as it was before the last harvest |
| `profiles.db.lock` | present while a harvest is writing; names host and process |
| `spool/` | series being written by running jobs; `spool/submit/` holds what `hpg submit` recorded |
| `timeseries/` | compressed series of finished jobs |
| `logs/harvest.log` | scheduled runs |
| `backup/` | the version 2 database, after migration |
| `hpcsizer.ini` | settings (not tracked by git) |

The database is never modified in place. A harvest copies it to node-local
disk, applies all changes there in one transaction, checks the result, copies
it back under a temporary name, and renames it over the published file.
Consequences:

- A reader always sees a complete database, old or new; it takes no locks, and
  a member with read-only access can run every report.
- A harvest that dies — node failure, time limit, a stall of the shared file
  system — leaves the published file as it was.
- **To back up, copy `profiles.db` at any time.** It is always consistent.
- `profiles.db.prev` is a one-step undo: `cp profiles.db.prev profiles.db`.

SQLite's write-ahead log is not used because its shared-memory file cannot be
mapped reliably on a network file system; on Lustre that failure kills the
process with SIGBUS.

```bash
bin/hpg db check
```

runs SQLite's integrity check and prints row counts.

Slurm reuses job ids once its counter wraps. A job is therefore identified by
its id **and** its submit time, and an id that comes round again becomes a new
row instead of overwriting an archived one; `hpg job ID` shows the most recent
and says when there are others.

## Permissions

| What | Who needs what |
|---|---|
| the checkout (`bin/`, `lib/`) | members: read. **Nobody but you: write.** A profiled job runs `bin/monitor.py` as the member who submitted it; if others could edit that file they could run code inside everyone's jobs. `hpg status` and `hpg preflight` warn when it is group- or world-writable. |
| `profiles.db`, `timeseries/` | members: read |
| `spool/`, `spool/submit/` | members: write. Created by the first harvest with mode 3770 — group-writable, setgid, sticky: members' jobs can add files, nobody can remove another's. |

Every member can write to the spool, so any member can put anything there
under any name. Nothing in it is trusted because of its name:

- The harvest, which runs as you, opens a spool entry without following links
  and reads it only if it is a plain file with a single name whose owner is the
  user the job belongs to according to Slurm. So a member cannot attach a
  made-up profile to someone else's job, cannot make the harvest read a file of
  yours through a link, and cannot stall it with a pipe or exhaust it with a
  huge file (series are read up to 64 MB, submit records up to 1 MB). From a
  submit record only the fields `hpg submit` writes are stored. What is
  rejected is named in the harvest log and removed a day after its job ended.
- The sampler, which runs as the job's user, creates its file exclusively and
  never writes through a link or into a file that is not a plain file of that
  user; if the name is taken it writes to `<jobid>.<node>-<pid>-<n>.tsv`.
  `hpg submit` writes its record to a new file and renames it into place.
- `spool/submit/` is created by the harvest only, and is used only while it is
  a real directory that belongs to the owner of the spool.

Keep the sticky bit: without it members could remove or replace each other's
files. To profile only your own jobs, set `spool_mode = 0700` under `[profile]`
before the first harvest (or `chmod` the existing directory).

## Profiling

`hpg submit` adds this line after the last `#SBATCH` line of a temporary copy
of the script (or in front of a `--wrap` command):

```bash
( HPCSIZER=sampler exec python3 -S /blue/mygroup/hpc-sizer/bin/monitor.py --spool /blue/mygroup/hpc-sizer/spool --pid $$ </dev/null >/dev/null 2>&1 & )
```

- The subshell means the sampler is not a child of the script: a `wait` in the
  script does not wait for it and `set -e` cannot see it.
- It stops when the script's shell (`$$`) ends or on SIGTERM, and always exits 0.
- If Python, the spool or the sampler file is missing, the line does nothing.

`compute_python` under `[slurm]` sets the interpreter named in that line
(`python3` by default; any Python from 3.6 works). The sampler reads the job's
cgroup; `hpg preflight --compute` runs a one-minute job that reports which
cgroup version the compute nodes use.

A job on several nodes is profiled on the node that runs its batch script.
Such a profile is marked "profiled on 1 of N nodes" and counts as a lower
bound for the job; accounting's per-step figures cover the other nodes.

With `multi_node = yes` under `[profile]` the sampler starts one more sampler
on each other node through `srun --overlap --mem=0`; each writes its own file
and the harvester combines them. That is one more step in the job, visible in
its accounting. It is off by default because it has been tried on Slurm 23.11
only, without cgroups: there the job's own `srun` steps ran unaffected whether
they started before or after the sampler's step, for jobs requested with
`--mem` and with `--mem-per-cpu`. Submit one multi-node test job of your own
with the setting on (`hpg job ID` should then list every node) before turning
it on for the group.

Where no cgroup of the job is found (a cluster without the cgroup plugins) the
sampler follows the batch script's process tree instead. That does not include
what `srun` starts, so such a profile (`proc`) is only ever a lower bound and
is not used to judge a CPU count.

A series is ingested once its job is stored, and archived under `timeseries/`.
The spool copy is removed by the following harvest; series whose job never
appears in the account's accounting are removed after 30 days.

`perf = yes` under `[profile]` additionally samples hardware counters of the
busiest process every five minutes. It is off by default.

## Upgrading and going back

```bash
git pull --ff-only origin main
```

Derived values — kinds, families, metrics, flags, suggestions — are computed
when a report is made, so a new version applies to all stored jobs at once and
nothing needs converting. If a release changes how `sacct` output is read (the
release notes will say; `parser_version` is stored with every row), apply the
new rules to everything stored:

```bash
bin/hpg db reparse
```

Every row keeps the `sacct` record it came from (`hpg job ID --json` shows it),
so this also corrects jobs that Slurm has since deleted. Rows migrated from
version 2 have no such record and stay as they are.

## What only the cluster can confirm

This code was developed against recorded `sacct` output and stub Slurm
commands, not on the cluster. `hpg preflight` turns each assumption into a
check:

| Check | Assumption | If it does not hold |
|---|---|---|
| interpreter | Python ≥ 3.9 and SQLite ≥ 3.24 on the login node | set `HPCSIZER_PYTHON` for the scheduled job |
| sacct fields | this version of `sacct` has every field HPCsizer stores | WARN names the fields it lacks; they are left out of the query and stored as unknown (Slurm 23.11, for one, has no `Restarts`) |
| sacct query | the query and the field separator are accepted | FAIL shows sacct's message; report it |
| epoch timestamps | `SLURM_TIME_FORMAT=%s` is honoured | WARN: ISO times are then read as UTC, which the query also sets |
| other members visible | `sacct -a` returns other users' jobs | WARN: only your jobs can be collected; ask the administrators about `PrivateData` |
| value forms | `TotalCPU` and `MaxRSS` are in a known form | FAIL shows the value |
| full-retention query | 182 days can be queried in one go | lower `first_days` or `slice_days` |
| unfinished jobs query | `sacct -s PENDING,RUNNING` lists the present | only `hpg queue` is affected |
| scrontab enabled / answers | `scrontab` can be used | `hpg schedule install --mode chain` |
| code permissions | nobody else can write the code | `chmod go-w` |
| compute node (`--compute`) | which cgroup version jobs run under; Python on the nodes | set `compute_python` |

Three things have no check and should be looked at once:

1. **The first profiled job** (step 11 above): the `Profile` section should
   name a cgroup source. A source of `proc` means the job's cgroup was not
   found and only the process tree below the script was measured.
2. **`scontrol update` lines from `hpg queue`.** They are printed, never run.
   Try one on a pending job of your own before recommending them to others;
   whether a user may lower `MinMemoryNode` on a pending job is site policy.
3. **Multi-node profiling.** Submit one small two-node job with `hpg submit`
   and check that `hpg job` lists both nodes.

`sacct` is called without `--noconvert`: memory values are read in the forms
observed on the target cluster (`86984640K`, `4000M`, `16G`) and in plain
bytes, so either setting works.

## When something is wrong

| Symptom | Look at |
|---|---|
| `STATUS: STALE` | `logs/harvest.log`; `hpg schedule show`; `squeue --name=hpcsizer-harvest`; run `bin/hpg harvest` by hand and read the error |
| only your own jobs are stored | `hpg preflight`, line "other members visible" |
| `... is a version 2 database` | `bin/hpg db migrate` (once) |
| `another HPCsizer writer holds ...profiles.db.lock` and none is running | the message names host and process; the lock is taken over automatically after 30 minutes |
| a job shows `not measured` | it ran under two minutes, or was killed before its CPU time was recorded; `hpg submit` profiles fill that in |
| a submitted job has no profile | `hpg status` (spool line); `ls spool/`; the script must be run by `sh` or `bash`; `hpg submit` says when it does not profile, and why |
| `hpg plot` asks for matplotlib | use an environment that has it (`environment.yml`) |
| a member gets `no database at ...` | they need read access to the checkout and `profiles.db` |
| suggestions look wrong for a whole family | `hpg history FAMILY` shows the runs they rest on; `hpg backtest` shows what the policy would have done on past jobs |
