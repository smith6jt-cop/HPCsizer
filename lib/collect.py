"""Collect finished jobs from Slurm accounting into the database.

One code path serves the scheduled harvest, an on-demand refresh before a
report, and a backfill (``--since``). It replaces the Python embedded in
``bin/harvest.sh`` and ``bin/backfill.py``.

Nothing is lost between runs because of three properties together:

* sacct returns every job whose end time is at or after the query start, so a
  query starting at "last successful harvest minus an overlap" covers an outage
  of any length;
* upserts are idempotent, so the overlap costs nothing;
* the watermark is stored in the database and published with the data, so it
  can only advance when the records it vouches for are really there.
"""

import contextlib
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from lib import db, slurm
from lib.config import Config

DAY = 86400
SACCT_TIMEOUT = 900

Logger = Callable[[str], None]


class HarvestError(Exception):
    """The harvest could not complete; nothing was published."""


def _stderr(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Talking to sacct
# ---------------------------------------------------------------------------


def _run_sacct_once(cmd: Sequence[str]) -> str:
    try:
        proc = subprocess.run(
            list(cmd),
            env=slurm.sacct_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            errors="replace",
            timeout=SACCT_TIMEOUT,
        )
    except FileNotFoundError:
        raise
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise HarvestError(str(exc)) from exc
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip().splitlines()
        raise HarvestError(
            detail[-1] if detail else "sacct exited with status %d" % proc.returncode
        )
    return proc.stdout


run_sacct = _run_sacct_once


def fetch_window(
    cfg: Config,
    start_ts: int,
    end_ts: Optional[int],
    log: Logger = _stderr,
    sleep: Callable[[float], None] = time.sleep,
) -> Tuple[List[Tuple[Dict[str, str], List[Dict[str, str]]]], int]:
    """Query one window and return ``(job groups, orphan step count)``.

    The query and its parsing are retried together: output that is cut short
    is as much a transient failure as a non-zero exit, and storing it would
    silently drop the jobs that were cut off.
    """
    cmd = slurm.build_sacct_cmd(cfg.accounts, start_ts, end_ts, sacct=cfg.sacct)
    delay = cfg.retry_delay
    attempts = max(1, int(cfg.retries))
    error = "unknown error"
    for attempt in range(1, attempts + 1):
        try:
            text = _run_sacct_once(cmd)
            rows = slurm.records_to_dicts(slurm.split_records(text))
            return slurm.group_records(rows)
        except FileNotFoundError:
            raise HarvestError("'%s' not found; is Slurm on PATH?" % cfg.sacct) from None
        except (HarvestError, slurm.SacctFormatError) as exc:
            error = str(exc)
        if attempt < attempts:
            log(
                "[harvest] sacct attempt %d/%d failed (%s); retrying in %gs"
                % (attempt, attempts, error, delay)
            )
            if delay > 0:
                sleep(delay)
            delay *= 3
    raise HarvestError("sacct failed after %d attempt(s): %s" % (attempts, error))


def _slices(start: int, end: int, width: float) -> List[Tuple[int, Optional[int]]]:
    """Split ``[start, end]`` into consecutive windows; the last is open-ended.

    A long catch-up is queried in pieces so that one failed or truncated answer
    costs a retry of that piece, not of half a year.
    """
    width = max(int(width), 3600)
    out: List[Tuple[int, Optional[int]]] = []
    cursor = int(start)
    while end - cursor > width:
        out.append((cursor, cursor + width))
        cursor += width
    out.append((cursor, None))
    return out


# ---------------------------------------------------------------------------
# Harvest
# ---------------------------------------------------------------------------

_SINCE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([dhm])$", re.IGNORECASE)


def parse_since(spec: str, now: Optional[int] = None) -> int:
    """``182d``, ``36h``, ``90m``, ``2026-06-01``, ``2026-06-01T12:00:00`` or an epoch."""
    now = int(time.time()) if now is None else int(now)
    spec = spec.strip()
    match = _SINCE_RE.match(spec)
    if match:
        unit = {"d": DAY, "h": 3600, "m": 60}[match.group(2).lower()]
        return now - int(float(match.group(1)) * unit)
    if spec.isdigit() and len(spec) >= 9:
        return int(spec)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        with contextlib.suppress(ValueError):
            return int(time.mktime(time.strptime(spec, fmt)))
    raise ValueError("cannot read %r as a start time (try 30d, 36h or 2026-06-01)" % spec)


def harvest(
    cfg: Config,
    since_ts: Optional[int] = None,
    deep: bool = False,
    trigger: str = "manual",
    now: Optional[int] = None,
    log: Logger = _stderr,
    sleep: Callable[[float], None] = time.sleep,
    ingest: Optional[Callable[..., int]] = None,
) -> Dict[str, Any]:
    """Run one harvest and return its counters. Raises on failure, publishing nothing."""
    now = int(time.time()) if now is None else int(now)
    overlap = int(cfg.overlap_days * DAY)
    stats: Dict[str, Any] = {
        "started_ts": now,
        "host": socket.gethostname(),
        "trigger": trigger,
        "deep": 0,
        "n_records": 0,
        "n_inserted": 0,
        "n_updated": 0,
        "n_open": 0,
        "n_internal": 0,
        "n_unparsed": 0,
        "n_late": 0,
        "n_users": 0,
        "n_profiles": 0,
        "parser_version": slurm.PARSER_VERSION,
    }
    if not cfg.accounts:
        raise HarvestError("no Slurm account configured (set HPCSIZER_ACCT or [slurm] accounts)")

    with db.SnapshotWriter(cfg.db_path, stale_seconds=cfg.lock_stale_minutes * 60) as conn:
        watermark = db.get_meta_int(conn, "watermark_ts")
        last_deep = db.get_meta_int(conn, "last_deep_ts")
        if watermark is not None and watermark > now:
            watermark = now  # a clock that ran ahead must not create a gap

        if since_ts is not None:
            start = int(since_ts)
        elif watermark is None:
            start = now - int(cfg.first_days * DAY)
            log("[harvest] first harvest: reading the last %g days" % cfg.first_days)
        else:
            start = watermark - overlap
            due = last_deep is None or now - last_deep >= cfg.deep_every_days * DAY
            if deep or due:
                start = min(start, now - int(cfg.deep_days * DAY))
        if start <= now - int(cfg.deep_days * DAY):
            deep = True
        stats["window_start_ts"] = start
        stats["deep"] = int(deep)

        # A job that spans two slices is returned by both; every counter is per
        # distinct job, keyed the way the database keys it.
        seen: Dict[Tuple[str, str], str] = {}
        users = set()
        late_before = (watermark - overlap) if watermark is not None else None

        for slice_start, slice_end in _slices(start, now, cfg.slice_days * DAY):
            groups, orphans = fetch_window(cfg, slice_start, slice_end, log=log, sleep=sleep)
            stats["n_unparsed"] += orphans
            for job_row, step_rows in groups:
                ident = (job_row.get("JobIDRaw", ""), job_row.get("Submit", ""))
                first_time = ident not in seen
                if db.is_internal_job(job_row.get("JobName")):
                    seen.setdefault(ident, "internal")
                    continue
                if slurm.is_open(job_row):
                    seen.setdefault(ident, "open")
                    continue
                try:
                    record, steps = slurm.merge_job(job_row, step_rows)
                except (ValueError, TypeError, KeyError) as exc:
                    seen.setdefault(ident, "unparsed")
                    log("[harvest] could not parse job %s: %s" % (ident[0], exc))
                    continue
                if record["job_id_raw"] is None or record["submit_ts"] is None:
                    seen.setdefault(ident, "unparsed")
                    continue
                result = db.upsert_job(conn, record, steps, now=now)
                if first_time:
                    seen[ident] = result
                    if result == "inserted" and late_before is not None:
                        if (record["end_ts"] or now) < late_before:
                            stats["n_late"] += 1
                if record["user"]:
                    users.add(record["user"])

        outcome = list(seen.values())
        stats["n_records"] = len(outcome)
        stats["n_inserted"] = outcome.count("inserted")
        stats["n_updated"] = outcome.count("updated")
        stats["n_open"] = outcome.count("open")
        stats["n_internal"] = outcome.count("internal")
        stats["n_unparsed"] += outcome.count("unparsed")
        stats["n_users"] = len(users)
        if ingest is not None:
            stats["n_profiles"] = ingest(conn, cfg, log=log, now=now)

        db.set_meta(conn, "watermark_ts", now)
        if deep:
            db.set_meta(conn, "last_deep_ts", now)
        stats["finished_ts"] = int(time.time())
        db.insert_harvest_run(conn, stats)

    log(
        "[harvest] %s: %d records since %s -> %d new, %d updated, %d still open, "
        "%d internal, %d unparsed, %d late, %d user(s)%s"
        % (
            time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(now)),
            stats["n_records"],
            slurm.utc_stamp(start) + "Z",
            stats["n_inserted"],
            stats["n_updated"],
            stats["n_open"],
            stats["n_internal"],
            stats["n_unparsed"],
            stats["n_late"],
            stats["n_users"],
            " (deep pass)" if deep else "",
        )
    )
    return stats


# ---------------------------------------------------------------------------
# Refresh on demand
# ---------------------------------------------------------------------------


def watermark(cfg: Config) -> Optional[int]:
    """Time of the last successful harvest, or None."""
    try:
        with db.reader(cfg.db_path) as conn:
            return db.get_meta_int(conn, "watermark_ts")
    except db.DatabaseError:
        return None


def can_write(cfg: Config) -> bool:
    """True when the caller may publish a new snapshot (i.e. owns the data directory)."""
    folder = os.path.dirname(os.path.abspath(cfg.db_path))
    if not os.path.isdir(folder) or not os.access(folder, os.W_OK):
        return False
    return not os.path.exists(cfg.db_path) or os.access(cfg.db_path, os.W_OK)


def maybe_refresh(
    cfg: Config,
    enabled: bool = True,
    now: Optional[int] = None,
    log: Logger = _stderr,
    ingest: Optional[Callable[..., int]] = None,
) -> str:
    """Harvest before a report when the data is old and the caller may write it.

    Returns why it did or did not run: ``refreshed``, ``fresh``, ``disabled``,
    ``read-only``, ``busy``, ``unavailable`` or ``failed``. A report never
    fails because a refresh could not run; it prints the age of its data.
    """
    now = int(time.time()) if now is None else int(now)
    if not enabled or os.environ.get("HPCSIZER_NO_REFRESH"):
        return "disabled"
    if not cfg.accounts or shutil.which(cfg.sacct) is None:
        return "unavailable"
    if not can_write(cfg):
        return "read-only"
    if db.detect_version(cfg.db_path) == 0:
        return "unavailable"  # a v2 database: migration is an explicit step
    mark = watermark(cfg)
    if mark is not None and now - mark < cfg.refresh_minutes * 60:
        return "fresh"
    try:
        harvest(cfg, trigger="refresh", now=now, log=lambda _msg: None, ingest=ingest)
        return "refreshed"
    except db.LockBusy:
        return "busy"  # another harvest is running; read the current snapshot
    except (HarvestError, db.DatabaseError, OSError) as exc:
        log("hpg: could not refresh from sacct (%s); using stored data" % exc)
        return "failed"


# ---------------------------------------------------------------------------
# Coverage: is everything sacct has also in the database?
# ---------------------------------------------------------------------------

_COVERAGE_FIELDS = ("JobIDRaw", "Submit", "End", "State", "JobName")


def coverage(cfg: Config, days: float = 30.0, now: Optional[int] = None) -> Dict[str, Any]:
    """Compare finished jobs in sacct with stored rows over the last ``days``.

    Only jobs that ended before the last harvest are expected to be stored.
    """
    now = int(time.time()) if now is None else int(now)
    since = now - int(days * DAY)
    mark = watermark(cfg)
    if mark is None:
        return {"available": False, "reason": "no successful harvest yet"}
    cmd = slurm.build_sacct_cmd(cfg.accounts, since, None, sacct=cfg.sacct, fields=_COVERAGE_FIELDS)
    cmd.insert(1, "-X")
    try:
        text = _run_sacct_once(cmd)
        rows = slurm.records_to_dicts(
            slurm.split_records(text, nfields=len(_COVERAGE_FIELDS)), _COVERAGE_FIELDS
        )
    except (FileNotFoundError, HarvestError, slurm.SacctFormatError) as exc:
        return {"available": False, "reason": str(exc) or "sacct not available"}

    expected = set()
    for row in rows:
        if db.is_internal_job(row.get("JobName")) or slurm.is_open(row):
            continue
        end, submit = slurm.parse_ts(row.get("End")), slurm.parse_ts(row.get("Submit"))
        raw = slurm.parse_int(row.get("JobIDRaw"))
        if raw is None or submit is None or end is None or not (since <= end <= mark):
            continue
        expected.add((raw, submit))
    with db.reader(cfg.db_path) as conn:
        stored = {
            (r[0], r[1])
            for r in conn.execute(
                "SELECT job_id_raw, submit_ts FROM jobs WHERE source='sacct' AND end_ts >= ?",
                (since,),
            )
        }
    missing = expected - stored
    return {
        "available": True,
        "days": days,
        "expected": len(expected),
        "stored": len(expected) - len(missing),
        "missing": len(missing),
        "missing_examples": sorted(raw for raw, _submit in missing)[:5],
    }
