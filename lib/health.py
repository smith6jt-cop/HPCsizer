"""Health reporting: ``hpg status`` and ``hpg preflight``.

For five months the scheduled harvest stored nothing and the only trace was a
log file nobody read. ``status`` makes the state of collection a one-line
answer with an exit code, and every report prints the age of its data.
"""

import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Sequence

from lib import __version__, collect, db, queue, schedule, slurm
from lib.config import Config

DAY = 86400


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def spool_state(cfg: Config) -> Dict[str, Any]:
    path = cfg.spool_dir
    info: Dict[str, Any] = {"path": path, "exists": os.path.isdir(path)}
    if not info["exists"]:
        info["note"] = "in-job profiles are disabled until it exists"
        return info
    mode = os.stat(path).st_mode
    info["mode"] = stat.filemode(mode)
    info["writable"] = os.access(path, os.W_OK)
    info["shared"] = bool(mode & stat.S_IWGRP) and bool(mode & stat.S_ISVTX)
    if not info["shared"]:
        info["note"] = "only your own jobs can be profiled (for the group: chmod 3770)"
    return info


def status(cfg: Config, now: Optional[int] = None, with_sacct: bool = True) -> Dict[str, Any]:
    """Everything ``hpg status`` shows, as a dict. ``state`` is the verdict."""
    now = int(time.time()) if now is None else int(now)
    out: Dict[str, Any] = {
        "version": __version__,
        "db_path": cfg.db_path,
        "accounts": list(cfg.accounts),
        "stale_hours": cfg.stale_hours,
        "interpreter": {
            "path": sys.executable,
            "python": "%d.%d.%d" % sys.version_info[:3],
            "sqlite": sqlite3.sqlite_version,
        },
        "problems": [],
    }
    problems: List[str] = out["problems"]
    if not cfg.accounts:
        problems.append("no Slurm account configured (HPCSIZER_ACCT or [slurm] accounts)")

    version = db.detect_version(cfg.db_path)
    out["schema"] = version
    if version is None:
        out["state"] = "missing"
        problems.append("no database yet: run 'hpg harvest'")
    elif version == 0:
        out["state"] = "needs-migration"
        problems.append("version 2 database: run 'hpg db migrate' once")
    elif version != db.SCHEMA_VERSION:
        out["state"] = "unsupported"
        problems.append("unsupported schema version %s" % version)
    else:
        out["db_bytes"] = os.path.getsize(cfg.db_path)
        with db.reader(cfg.db_path) as conn:
            mark = db.get_meta_int(conn, "watermark_ts")
            out["watermark_ts"] = mark
            out["last_deep_ts"] = db.get_meta_int(conn, "last_deep_ts")
            out["last_harvest"] = db.last_harvest(conn)
            out["rows"] = db.summary_counts(conn)
            out["recent"] = db.summary_counts(conn, since_ts=now - 30 * DAY)
        if mark is None:
            out["state"] = "empty"
            problems.append("database exists but no harvest has succeeded: run 'hpg harvest'")
        else:
            out["age_sec"] = max(0, now - mark)
            out["stale"] = out["age_sec"] > cfg.stale_hours * 3600
            out["state"] = "stale" if out["stale"] else "ok"
            if out["stale"]:
                problems.append(
                    "no successful harvest for %.1f h (limit %g h)"
                    % (out["age_sec"] / 3600.0, cfg.stale_hours)
                )
        recent_users = [u for u, _n in out["recent"]["users"]]
        if mark is not None and len(recent_users) == 1 and out["recent"]["total"] >= 20:
            problems.append(
                "only %s has jobs in the last 30 days; other members' jobs may not be visible"
                % recent_users[0]
            )
        if with_sacct and mark is not None and cfg.accounts and shutil.which(cfg.sacct):
            out["coverage"] = collect.coverage(cfg, days=30, now=now)
            if out["coverage"].get("available") and out["coverage"]["missing"]:
                problems.append(
                    "%d finished job(s) in sacct are not stored (first ids: %s)"
                    % (
                        out["coverage"]["missing"],
                        ", ".join(map(str, out["coverage"]["missing_examples"])),
                    )
                )

    out["schedule"] = schedule.describe(cfg)
    if out["schedule"]["mode"] == "none":
        problems.append("no scheduled harvest: run 'hpg schedule install'")
    out["spool"] = spool_state(cfg)
    out["ok"] = out.get("state") == "ok" and not out.get("coverage", {}).get("missing")
    return out


# ---------------------------------------------------------------------------
# preflight
# ---------------------------------------------------------------------------

PASS, FAIL, WARN, SKIP, INFO = "PASS", "FAIL", "WARN", "SKIP", "INFO"


def _check(name: str, state: str, detail: str) -> Dict[str, str]:
    return {"check": name, "status": state, "detail": detail}


def _run(cmd: Sequence[str], timeout: int = 120, env: Optional[Dict[str, str]] = None):
    try:
        proc = subprocess.run(
            list(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            errors="replace",
            timeout=timeout,
            env=env,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except FileNotFoundError:
        return 127, "", "%s: not found" % cmd[0]
    except subprocess.TimeoutExpired:
        return 124, "", "%s: no answer within %d s" % (cmd[0], timeout)


def preflight(
    cfg: Config, compute: bool = False, now: Optional[int] = None
) -> List[Dict[str, str]]:
    """Check the assumptions this code makes about the cluster.

    Each item was verified against documentation or an earlier audit, not on
    the machine where the code was written; this turns them into one command.
    """
    now = int(time.time()) if now is None else int(now)
    checks: List[Dict[str, str]] = []

    ok_py = sys.version_info >= (3, 9)
    ok_sql = sqlite3.sqlite_version_info >= db.MIN_SQLITE
    checks.append(
        _check(
            "interpreter",
            PASS if ok_py and ok_sql else FAIL,
            "%s, Python %d.%d.%d, SQLite %s (need Python 3.9+, SQLite 3.24+)"
            % ((sys.executable,) + tuple(sys.version_info[:3]) + (sqlite3.sqlite_version,)),
        )
    )

    if not cfg.accounts:
        checks.append(_check("accounts", FAIL, "set HPCSIZER_ACCT or [slurm] accounts"))
        return checks
    checks.append(_check("accounts", PASS, ", ".join(cfg.accounts)))

    folder = os.path.dirname(os.path.abspath(cfg.db_path))
    checks.append(
        _check(
            "database directory",
            PASS if collect.can_write(cfg) else WARN,
            "%s is %s" % (folder, "writable" if collect.can_write(cfg) else "read-only for you"),
        )
    )

    # The exact query the collector runs, over two days.
    cmd = slurm.build_sacct_cmd(cfg.accounts, now - 2 * DAY, None, sacct=cfg.sacct)
    code, out, err = _run(cmd, timeout=300, env=slurm.sacct_env())
    rows: List[Dict[str, str]] = []
    if code != 0:
        checks.append(_check("sacct query", FAIL, (err.strip().splitlines() or ["failed"])[-1]))
    else:
        try:
            rows = slurm.records_to_dicts(slurm.split_records(out))
            checks.append(
                _check(
                    "sacct query",
                    PASS,
                    "%d records, all fields and the separator accepted" % len(rows),
                )
            )
        except slurm.SacctFormatError as exc:
            checks.append(_check("sacct query", FAIL, "output not parseable: %s" % exc))

    job_rows = [r for r in rows if "." not in r.get("JobIDRaw", "")]
    if job_rows:
        stamps = [r["Submit"] for r in job_rows if r.get("Submit")]
        epoch = bool(stamps) and all(s.isdigit() for s in stamps)
        checks.append(
            _check(
                "epoch timestamps",
                PASS if epoch else WARN,
                "Submit=%s%s"
                % (
                    stamps[0] if stamps else "?",
                    "" if epoch else " (SLURM_TIME_FORMAT ignored; ISO times are read as UTC)",
                ),
            )
        )
        users = sorted({r["User"] for r in job_rows if r.get("User")})
        checks.append(
            _check(
                "other members visible",
                PASS if len(users) > 1 else WARN,
                "users in the last 2 days: %s" % (", ".join(users) or "none"),
            )
        )
        step_rows = [r for r in rows if "." in r.get("JobIDRaw", "") and r.get("TotalCPU")]
        cpu = [r["TotalCPU"] for r in step_rows]
        unread = [c for c in cpu if slurm.parse_duration(c) is None]
        mem = [r["MaxRSS"] for r in step_rows if r.get("MaxRSS")]
        bad_mem = [m for m in mem if slurm.parse_mem(m) is None]
        checks.append(
            _check(
                "value forms",
                FAIL if unread or bad_mem else PASS,
                "TotalCPU e.g. %s, MaxRSS e.g. %s%s"
                % (
                    cpu[0] if cpu else "-",
                    mem[0] if mem else "-",
                    "; unreadable: %s" % ", ".join((unread + bad_mem)[:3])
                    if unread or bad_mem
                    else "",
                ),
            )
        )
        wraps = [r["SubmitLine"] for r in job_rows if "--wrap" in r.get("SubmitLine", "")]
        sample = (
            wraps[0]
            if wraps
            else next((r["SubmitLine"] for r in job_rows if r.get("SubmitLine")), "")
        )
        checks.append(
            _check("SubmitLine form", INFO, sample[:140] or "no submit line in the sample")
        )
    elif code == 0:
        checks.append(_check("sample jobs", WARN, "no jobs in the last 2 days to inspect"))

    start = time.time()
    code, out, err = _run(
        [cfg.sacct, "-a", "-A", ",".join(cfg.accounts), "-X", "-n", "-P", "-S",
         slurm.utc_stamp(now - int(cfg.first_days * DAY)), "-o", "JobIDRaw"],
        timeout=900,
        env=slurm.sacct_env(),
    )  # fmt: skip
    if code == 0:
        checks.append(
            _check(
                "full-retention query",
                PASS,
                "%d job records over %g days in %.0f s"
                % (len(out.splitlines()), cfg.first_days, time.time() - start),
            )
        )
    else:
        checks.append(
            _check("full-retention query", FAIL, (err.strip().splitlines() or ["failed"])[-1])
        )

    # What `hpg queue` asks: jobs that are pending or running right now.
    code, out, err = _run(queue.open_jobs_cmd(cfg), timeout=300, env=slurm.sacct_env())
    if code == 0:
        try:
            open_rows = [
                r
                for r in slurm.records_to_dicts(slurm.split_records(out))
                if "." not in r.get("JobIDRaw", "") and slurm.is_open(r)
            ]
            checks.append(
                _check(
                    "unfinished jobs query",
                    PASS,
                    "%d pending or running job(s) listed by sacct -s (used by hpg queue)"
                    % len(open_rows),
                )
            )
        except slurm.SacctFormatError as exc:
            checks.append(_check("unfinished jobs query", WARN, "output not parseable: %s" % exc))
    else:
        checks.append(
            _check("unfinished jobs query", WARN, (err.strip().splitlines() or ["failed"])[-1])
        )

    code, out, err = _run([cfg.scontrol, "show", "config"], timeout=60)
    if code == 0:
        wanted = ("MaxJobId", "FirstJobId", "MaxMemPerCPU", "DefMemPerCPU", "ScronParameters",
                  "JobAcctGatherFrequency")  # fmt: skip
        found = {}
        for line in out.splitlines():
            key, _, value = line.partition("=")
            if key.strip() in wanted:
                found[key.strip()] = value.strip()
        checks.append(
            _check("cluster limits", INFO, ", ".join("%s=%s" % kv for kv in sorted(found.items())))
        )
        scron = found.get("ScronParameters", "")
        checks.append(
            _check(
                "scrontab enabled",
                PASS if "enable" in scron.lower() else WARN,
                "ScronParameters=%s%s"
                % (scron or "(unset)", "" if "enable" in scron.lower() else "; use --mode chain"),
            )
        )
    else:
        checks.append(_check("cluster limits", WARN, err.strip() or "scontrol not available"))

    code, out, err = _run([cfg.scrontab, "-l"], timeout=60)
    text = (out + err).lower()
    if code == 0 or "no crontab" in text:
        checks.append(
            _check(
                "scrontab answers",
                PASS,
                "scrontab -l: %s" % ("entries present" if code == 0 else "no entries yet"),
            )
        )
    else:
        checks.append(_check("scrontab answers", WARN, (err or out).strip() or "scrontab failed"))

    spool = spool_state(cfg)
    checks.append(
        _check(
            "spool directory",
            PASS if spool.get("exists") and spool.get("writable") else WARN,
            "%s %s%s"
            % (
                spool["path"],
                spool.get("mode", "(missing)"),
                "; " + spool["note"] if spool.get("note") else "",
            ),
        )
    )

    probe = (
        "cat /proc/self/cgroup; %s -c 'import sys,sqlite3;"
        'print("python", sys.version_info[:3], "sqlite", sqlite3.sqlite_version)\''
        % cfg.compute_python
    )
    srun = ["srun", "--time=1", "--mem=200M", "--ntasks=1"]
    if cfg.sched_account:
        srun.append("--account=" + cfg.sched_account)
    if cfg.sched_qos:
        srun.append("--qos=" + cfg.sched_qos)
    if cfg.sched_partition:
        srun.append("--partition=" + cfg.sched_partition)
    srun += ["sh", "-c", probe]
    if compute:
        code, out, err = _run(srun, timeout=600)
        if code == 0:
            lines = [ln for ln in out.splitlines() if ln.strip()]
            v2 = any(ln.startswith("0::") for ln in lines)
            checks.append(
                _check(
                    "compute node",
                    PASS,
                    "cgroup %s; %s" % ("v2" if v2 else "v1", lines[-1] if lines else ""),
                )
            )
        else:
            checks.append(
                _check("compute node", WARN, (err.strip().splitlines() or ["srun failed"])[-1])
            )
    else:
        checks.append(
            _check(
                "compute node", SKIP, "runs a 1-minute job; repeat with: hpg preflight --compute"
            )
        )
    return checks
