"""Scheduling the harvest: one scrontab entry, or one self-resubmitting job.

scrontab is Slurm's own cron: the entry runs as a batch job and keeps a single
job id across runs, so it replaces the chain of self-resubmitting jobs and
watchdogs (about 190 job submissions a day) with nothing to keep alive.

The ``chain`` mode exists for clusters without scrontab. It is one job, with no
watchdog: each run queues its successor only if none is pending, and
``--dependency=singleton`` lets one job of the name run at a time, so an
accidental second chain merges into the first instead of persisting.
"""

import os
import subprocess
import tempfile
from typing import Any, Dict, List, Optional

from lib.config import Config

JOB_NAME = "hpcsizer-harvest"
BEGIN = "# >>> hpcsizer harvest (managed by 'hpg schedule'; edits here are overwritten) >>>"
END = "# <<< hpcsizer harvest <<<"

# Minute 17 keeps the job off the top of the hour, when most cron-style jobs start.
CRON = {"15min": "*/15 * * * *", "hour": "17 * * * *", "day": "17 3 * * *"}
CHAIN_INTERVAL = {"15min": "15minutes", "hour": "1hour", "day": "1day"}


class ScheduleError(Exception):
    pass


class ToolMissing(ScheduleError):
    """The command is not installed here -- as opposed to having failed."""


def launcher(cfg: Config) -> str:
    return os.path.join(cfg.root, "bin", "harvest.sh")


def _sbatch_options(cfg: Config) -> List[str]:
    opts = ["--job-name=" + JOB_NAME]
    if cfg.sched_account:
        opts.append("--account=" + cfg.sched_account)
    if cfg.sched_qos:
        opts.append("--qos=" + cfg.sched_qos)
    if cfg.sched_partition:
        opts.append("--partition=" + cfg.sched_partition)
    opts += [
        "--time=" + cfg.sched_time,
        "--mem=" + cfg.sched_mem,
        "--ntasks=1",
        "--cpus-per-task=1",
        "--output=/dev/null",
        "--error=/dev/null",
    ]
    if cfg.mail_type:
        opts.append("--mail-type=" + cfg.mail_type)
    if cfg.mail_user:
        opts.append("--mail-user=" + cfg.mail_user)
    return opts


def scron_block(cfg: Config, every: str = "hour") -> str:
    """The managed scrontab block.

    scrontab jobs start in the home directory without the user's environment,
    so the entry uses the absolute launcher path and the launcher reads its
    settings from ``hpcsizer.ini``.
    """
    if every not in CRON:
        raise ScheduleError("unknown interval %r (use one of: %s)" % (every, ", ".join(CRON)))
    lines = [BEGIN]
    lines += ["#SCRON " + opt for opt in _sbatch_options(cfg)]
    lines.append("%s %s" % (CRON[every], launcher(cfg)))
    lines.append(END)
    return "\n".join(lines) + "\n"


def strip_block(text: str) -> str:
    """Remove the managed block, leaving every other scrontab line untouched."""
    out: List[str] = []
    inside = False
    for line in text.splitlines():
        if line.strip() == BEGIN:
            inside = True
            continue
        if line.strip() == END:
            inside = False
            continue
        if not inside:
            out.append(line)
    while out and not out[-1].strip():
        out.pop()
    return ("\n".join(out) + "\n") if out else ""


def find_block(text: str) -> Optional[str]:
    lines = text.splitlines()
    try:
        start = next(i for i, ln in enumerate(lines) if ln.strip() == BEGIN)
        end = next(i for i, ln in enumerate(lines) if ln.strip() == END and i > start)
    except StopIteration:
        return None
    return "\n".join(lines[start : end + 1])


def _run(
    cmd: List[str], timeout: int = 60, env: Optional[Dict[str, str]] = None
) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=timeout,
            env=env,
        )
    except FileNotFoundError:
        raise ToolMissing("'%s' not found" % cmd[0]) from None
    except subprocess.TimeoutExpired:
        raise ScheduleError("'%s' did not answer within %d s" % (cmd[0], timeout)) from None


def read_scrontab(cfg: Config) -> str:
    """Current scrontab text; empty when the user has none."""
    proc = _run([cfg.scrontab, "-l"])
    if proc.returncode != 0:
        if "no crontab" in (proc.stderr + proc.stdout).lower():
            return ""
        raise ScheduleError((proc.stderr or proc.stdout).strip() or "scrontab -l failed")
    return proc.stdout


def write_scrontab(cfg: Config, text: str) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".scrontab", delete=False) as handle:
        handle.write(text)
        path = handle.name
    try:
        proc = _run([cfg.scrontab, path])
        if proc.returncode != 0:
            raise ScheduleError(
                (proc.stderr or proc.stdout).strip() or "scrontab rejected the entry"
            )
    finally:
        os.unlink(path)


def chain_command(cfg: Config, every: str = "hour") -> List[str]:
    if every not in CHAIN_INTERVAL:
        raise ScheduleError("unknown interval %r" % every)
    return (
        [cfg.sbatch, "--parsable", "--dependency=singleton"]
        + _sbatch_options(cfg)
        + ["--export=ALL", launcher(cfg), "--chain", CHAIN_INTERVAL[every]]
    )


def chain_environment(cfg: Config) -> Dict[str, str]:
    """Environment the first chain job is submitted with, and every successor inherits.

    Each run queues the next one itself, from a copy of the launcher on the
    node, before it touches the shared file system. It can therefore read
    neither the configuration nor this code at that moment: the root and the
    sbatch options have to travel with the job (``--export=ALL``).
    """
    env = dict(os.environ)
    env["HPCSIZER_ROOT"] = cfg.root
    env["HPCSIZER_SBATCH_OPTS"] = " ".join(_sbatch_options(cfg))
    return env


def chain_jobs(cfg: Config) -> List[str]:
    """``jobid state`` lines for queued chain jobs of the calling user."""
    user = os.environ.get("USER") or ""
    cmd = [cfg.squeue, "-h", "--name=" + JOB_NAME, "-o", "%i %T"]
    if user:
        cmd += ["-u", user]
    proc = _run(cmd)
    if proc.returncode != 0:
        raise ScheduleError((proc.stderr or proc.stdout).strip() or "squeue failed")
    return [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]


def install(cfg: Config, every: str = "hour", mode: str = "scrontab", dry_run: bool = False) -> str:
    """Install the schedule and return a description of what was done."""
    if not os.path.exists(launcher(cfg)):
        raise ScheduleError("launcher not found: %s" % launcher(cfg))
    if mode == "scrontab":
        block = scron_block(cfg, every)
        if dry_run:
            return block
        current = read_scrontab(cfg)
        rest = strip_block(current)
        write_scrontab(cfg, (rest + "\n" if rest else "") + block)
        return block
    if mode == "chain":
        cmd = chain_command(cfg, every)
        if dry_run:
            return " ".join(cmd) + "\n"
        if chain_jobs(cfg):
            raise ScheduleError(
                "a %s job is already queued; run 'hpg schedule remove' first" % JOB_NAME
            )
        proc = _run(cmd, env=chain_environment(cfg))
        if proc.returncode != 0:
            raise ScheduleError((proc.stderr or proc.stdout).strip() or "sbatch failed")
        return "submitted chain job %s\n" % proc.stdout.strip()
    raise ScheduleError("unknown mode %r (use scrontab or chain)" % mode)


def remove(cfg: Config) -> List[str]:
    """Remove the scrontab block and cancel chain jobs. Returns what was removed.

    Both are always attempted. A command that is not installed is not an error
    (a cluster has scrontab or it does not); one that fails is, and is raised
    after the other has been tried -- reporting success while a chain keeps
    resubmitting itself would be worse than failing.
    """
    done: List[str] = []
    failed: List[str] = []
    missing = 0
    try:
        current = read_scrontab(cfg)
        if find_block(current):
            write_scrontab(cfg, strip_block(current))
            done.append("scrontab entry removed")
    except ToolMissing:
        missing += 1
    except ScheduleError as exc:
        failed.append("scrontab: %s" % exc)
    try:
        if chain_jobs(cfg):
            user = os.environ.get("USER") or ""
            cmd = ["scancel", "--name=" + JOB_NAME] + (["-u", user] if user else [])
            proc = _run(cmd)
            if proc.returncode != 0:
                raise ScheduleError((proc.stderr or proc.stdout).strip() or "scancel failed")
            done.append("chain job(s) cancelled")
    except ToolMissing:
        missing += 1
    except ScheduleError as exc:
        failed.append("chain: %s" % exc)
    if missing == 2:
        failed.append("neither scrontab nor squeue was found")
    if failed:
        raise ScheduleError(
            "; ".join(failed) + (" (done before that: %s)" % "; ".join(done) if done else "")
        )
    return done


def describe(cfg: Config) -> Dict[str, Any]:
    """What is scheduled right now: ``{"mode": scrontab|chain|none|unknown, "detail": ...}``."""
    errors: List[str] = []
    try:
        block = find_block(read_scrontab(cfg))
        if block:
            cron = [ln for ln in block.splitlines() if ln and not ln.startswith("#")]
            return {"mode": "scrontab", "detail": cron[0] if cron else "", "block": block}
    except ScheduleError as exc:
        errors.append("scrontab: %s" % exc)
    try:
        jobs = chain_jobs(cfg)
        if jobs:
            return {"mode": "chain", "detail": "; ".join(jobs)}
    except ScheduleError as exc:
        errors.append("squeue: %s" % exc)
    if len(errors) == 2:
        return {"mode": "unknown", "detail": "; ".join(errors)}
    return {"mode": "none", "detail": "no scheduled harvest"}
