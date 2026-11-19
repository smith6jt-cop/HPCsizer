"""The single interface to Slurm accounting.

Everything HPCsizer knows about a job comes from one ``sacct`` call built and
parsed here. Parsers return ``None`` for anything they cannot read -- never 0 --
so "unknown" and "zero" stay distinguishable downstream.
"""

import calendar
import json
import os
import re
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

PARSER_VERSION = 1

# ASCII unit separator: cannot appear in a job name or submit line by accident,
# so '|' and newlines inside those fields need no special handling.
SEP = "\x1f"

# Free-text fields go last. SubmitLine is the final field so that a stray
# separator inside it can only spill forward, never shift the fields before it.
FIELDS: Tuple[str, ...] = (
    "JobIDRaw",
    "JobID",
    "User",
    "Account",
    "Partition",
    "QOS",
    "State",
    "ExitCode",
    "DerivedExitCode",
    "Submit",
    "Eligible",
    "Start",
    "End",
    "ElapsedRaw",
    "TimelimitRaw",
    "ReqCPUS",
    "AllocCPUS",
    "ReqNodes",
    "NNodes",
    "NTasks",
    "ReqTRES",
    "AllocTRES",
    "TotalCPU",
    "UserCPU",
    "SystemCPU",
    "MaxRSS",
    "TRESUsageInTot",
    "TRESUsageInMax",
    "TRESUsageInAve",
    "TRESUsageOutTot",
    "Restarts",
    "NodeList",
    "JobName",
    "WorkDir",
    "SubmitLine",
)

# A job in one of these states is not finished; it is skipped now and picked up
# by a later harvest once it ends (sacct returns any job whose end time falls at
# or after the query start).
OPEN_STATES = frozenset(
    {
        "PENDING",
        "RUNNING",
        "SUSPENDED",
        "REQUEUED",
        "RESIZING",
        "COMPLETING",
        "CONFIGURING",
        "STAGE_OUT",
        "SIGNALING",
        "STOPPED",
        "REQUEUE_HOLD",
        "REQUEUE_FED",
        "RESV_DEL_HOLD",
        "SPECIAL_EXIT",
    }
)

_JOBIDRAW_RE = re.compile(r"^\d+(?:\.[A-Za-z0-9_+-]+)?$")
_UNSET = frozenset({"", "Unknown", "None", "N/A", "INVALID", "NONE"})


class SacctFormatError(ValueError):
    """sacct output did not have the expected shape."""


class TruncatedOutput(SacctFormatError):
    """sacct output stopped in the middle of a record."""


# ---------------------------------------------------------------------------
# Building the query
# ---------------------------------------------------------------------------


def utc_stamp(ts: float) -> str:
    """Epoch seconds as the UTC timestamp sacct accepts for -S/-E."""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(ts)))


def build_sacct_cmd(
    accounts: Sequence[str],
    start_ts: float,
    end_ts: Optional[float] = None,
    sacct: str = "sacct",
    fields: Sequence[str] = FIELDS,
) -> List[str]:
    """Return the sacct argv for every job of ``accounts`` active since ``start_ts``.

    ``-a`` is not optional: without it sacct returns only the caller's own jobs,
    which is how other group members went uncollected. An account list is
    required because ``-a`` alone would select the whole cluster.
    """
    if not accounts:
        raise ValueError("no Slurm account configured (set HPCSIZER_ACCT or [slurm] accounts)")
    cmd = [sacct, "-a", "-A", ",".join(accounts), "-S", utc_stamp(start_ts)]
    if end_ts is not None:
        cmd += ["-E", utc_stamp(end_ts)]
    cmd += ["-n", "-p", "--delimiter=" + SEP, "-o", ",".join(fields)]
    return cmd


def sacct_env(base: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """Environment for sacct: epoch timestamps, interpreted and printed in UTC."""
    env = dict(os.environ if base is None else base)
    env["TZ"] = "UTC"
    env["SLURM_TIME_FORMAT"] = "%s"
    return env


# ---------------------------------------------------------------------------
# Splitting the stream
# ---------------------------------------------------------------------------


def _starts_record(token: str) -> bool:
    return token[:1] in ("\n", "\r") and bool(_JOBIDRAW_RE.match(token.lstrip("\r\n")))


def split_records(text: str, nfields: int = len(FIELDS), sep: str = SEP) -> List[List[str]]:
    """Split ``sacct -p --delimiter=SEP`` output into records of ``nfields`` strings.

    With ``-p`` every field, including the last, is followed by the separator
    and every record by a newline. Splitting the whole stream on the separator
    therefore yields exactly ``nfields`` tokens per record no matter what the
    free-text fields contain. A stream that does not end on a record boundary
    raises ``TruncatedOutput``: storing a partial answer and advancing the
    watermark would lose the rest for good.
    """
    if not text.strip():
        return []
    tokens = text.split(sep)
    tail = tokens.pop()
    if tail.strip():
        raise TruncatedOutput("sacct output does not end on a record boundary")

    records: List[List[str]] = []
    i, total = 0, len(tokens)
    while i < total:
        if total - i < nfields:
            raise TruncatedOutput(
                f"last record has {total - i} of {nfields} fields; output was cut short"
            )
        rec = tokens[i : i + nfields]
        i += nfields
        rec[0] = rec[0].lstrip("\r\n")
        # A separator typed into a submit line spills extra tokens; they belong
        # to that SubmitLine until the next token that starts a new record.
        while i < total and not _starts_record(tokens[i]):
            rec[-1] += sep + tokens[i]
            i += 1
        if not _JOBIDRAW_RE.match(rec[0]):
            raise SacctFormatError(f"record does not start with a job id: {rec[0][:40]!r}")
        records.append(rec)
    return records


def records_to_dicts(
    records: Sequence[Sequence[str]], fields: Sequence[str] = FIELDS
) -> List[Dict[str, str]]:
    return [dict(zip(fields, rec)) for rec in records]


def group_records(
    rows: Sequence[Dict[str, str]],
) -> Tuple[List[Tuple[Dict[str, str], List[Dict[str, str]]]], int]:
    """Group step rows under the job row that precedes them.

    sacct prints a job line followed by its step lines. Grouping by adjacency
    rather than by job id keeps two incarnations of a reused job id apart.
    Returns ``(groups, orphan_step_count)``.
    """
    groups: List[Tuple[Dict[str, str], List[Dict[str, str]]]] = []
    orphans = 0
    current_base: Optional[str] = None
    for row in rows:
        raw = row.get("JobIDRaw", "")
        base, dot, _step = raw.partition(".")
        if not dot:
            groups.append((row, []))
            current_base = base
        elif groups and base == current_base:
            groups[-1][1].append(row)
        else:
            orphans += 1
    return groups, orphans


# ---------------------------------------------------------------------------
# Field parsers
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"^(?:(\d+)-)?(\d+(?::\d+){0,2})(\.\d+)?$")


def parse_duration(value: Optional[str]) -> Optional[float]:
    """Seconds from a Slurm duration, or None when it cannot be read.

    Handles every form sacct prints: ``MM:SS.mmm`` (TotalCPU under one hour --
    the common case), ``HH:MM:SS``, ``D-HH:MM:SS`` with optional fraction, and
    the ``D-HH`` / ``D-HH:MM`` forms accepted for time limits.
    """
    if value is None:
        return None
    value = value.strip()
    match = _DURATION_RE.match(value)
    if not match:
        return None
    parts = [int(p) for p in match.group(2).split(":")]
    frac = float(match.group(3) or 0)
    if match.group(1) is not None:
        hours, minutes, seconds = (parts + [0, 0])[:3]
        days = int(match.group(1))
    else:
        days = 0
        if len(parts) == 3:
            hours, minutes, seconds = parts
        elif len(parts) == 2:
            hours, (minutes, seconds) = 0, parts
        else:
            hours, minutes, seconds = 0, 0, parts[0]
    return days * 86400 + hours * 3600 + minutes * 60 + seconds + frac


_MEM_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([KMGTPE]?)(?:i?B)?([cn])?$", re.IGNORECASE)
_MEM_UNIT = {
    "B": 1,
    "K": 1024,
    "M": 1024**2,
    "G": 1024**3,
    "T": 1024**4,
    "P": 1024**5,
    "E": 1024**6,
}


def parse_mem(value: Optional[str], default_unit: str = "B") -> Optional[int]:
    """Bytes from a Slurm size such as ``86984640K``, ``64G`` or ``4000M``.

    ``default_unit`` applies to bare numbers: bytes for usage counters,
    megabytes for requested/allocated ``mem`` TRES. A trailing ``c``/``n``
    (per-CPU / per-node, printed by old Slurm versions) is accepted and ignored;
    requested memory is taken from the ``mem`` TRES, which is always the job
    total.
    """
    if value is None:
        return None
    match = _MEM_RE.match(value.strip())
    if not match:
        return None
    unit = (match.group(2) or default_unit).upper()
    return int(round(float(match.group(1)) * _MEM_UNIT[unit]))


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    value = value.strip()
    if value in _UNSET:
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return int(float(value))
        except ValueError:
            return None


def parse_ts(value: Optional[str]) -> Optional[int]:
    """Epoch seconds from a sacct timestamp; None for Unknown/None/0.

    sacct is run with ``SLURM_TIME_FORMAT=%s`` so values are epochs. ISO
    timestamps are still accepted (read as UTC, matching ``TZ=UTC``) in case a
    site build ignores the variable.
    """
    if value is None:
        return None
    value = value.strip()
    if value in _UNSET:
        return None
    if value.isdigit():
        return int(value) or None
    try:
        return calendar.timegm(time.strptime(value[:19], "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None


def parse_tres(value: Optional[str]) -> Dict[str, str]:
    """``billing=8,cpu=8,gres/gpu:a100=1,mem=64G`` -> dict of strings."""
    out: Dict[str, str] = {}
    for item in (value or "").split(","):
        key, eq, val = item.partition("=")
        if eq and key.strip():
            out[key.strip()] = val.strip()
    return out


def tres_mem_bytes(tres: Mapping[str, str]) -> Optional[int]:
    return parse_mem(tres.get("mem"), default_unit="M")


def tres_gpus(tres: Mapping[str, str]) -> Tuple[int, Optional[str]]:
    """GPU count and type(s) from a requested/allocated TRES dict."""
    typed = [
        (key.split(":", 1)[1], parse_int(val) or 0)
        for key, val in tres.items()
        if key.startswith("gres/gpu:")
    ]
    total = parse_int(tres.get("gres/gpu"))
    if total is None:
        total = sum(count for _name, count in typed)
    gpu_type = ",".join(sorted(name for name, _count in typed)) or None
    return total, gpu_type


def parse_tres_usage(value: Optional[str]) -> Dict[str, float]:
    """Usage TRES (``TRESUsageIn*``/``Out*``) -> numbers in base units.

    Keys: ``cpu_sec``, ``mem_bytes``, ``vmem_bytes``, ``disk_bytes``,
    ``gpu_util`` (percent), ``gpu_mem_bytes``, ``energy``.
    """
    tres = parse_tres(value)
    out: Dict[str, float] = {}

    def put(key: str, parsed: Optional[float]) -> None:
        if parsed is not None:
            out[key] = parsed

    if "cpu" in tres:
        raw = tres["cpu"]
        put("cpu_sec", parse_duration(raw))
    put("mem_bytes", parse_mem(tres.get("mem")))
    put("vmem_bytes", parse_mem(tres.get("vmem")))
    put("disk_bytes", parse_mem(tres.get("fs/disk")))
    put("gpu_mem_bytes", parse_mem(tres.get("gres/gpumem")))
    for key, name in (("gres/gpuutil", "gpu_util"), ("energy", "energy")):
        raw = tres.get(key)
        if raw is not None:
            try:
                out[name] = float(raw)
            except ValueError:
                pass
    return out


def parse_state(value: Optional[str]) -> Tuple[str, str, Optional[str]]:
    """``(state, raw, cancelled_by)``.

    Job lines read ``CANCELLED by 6084``; matching the whole string against a
    list of state names silently dropped every cancelled job. Only the first
    word is the state.
    """
    raw = (value or "").strip()
    if not raw:
        return "UNKNOWN", raw, None
    state = raw.split()[0].upper().rstrip("+")
    match = re.match(r"^CANCELLED\s+by\s+(\S+)", raw, re.IGNORECASE)
    return state, raw, match.group(1) if match else None


def parse_jobid(job_id: str, job_id_raw: str) -> Dict[str, Any]:
    """Decompose the displayed and raw job ids.

    ``job_id`` forms: ``123``, ``123_4`` (array task), ``123_[5-9]`` (array
    tasks that never started), ``123+0`` (heterogeneous component); steps carry
    ``.batch``, ``.extern``, ``.0``, ``.interactive``.
    """
    raw_base, _, raw_step = (job_id_raw or "").strip().partition(".")
    display = (job_id or "").strip().partition(".")[0]
    out: Dict[str, Any] = {
        "job_id_raw": int(raw_base) if raw_base.isdigit() else None,
        "step": raw_step or None,
        "job_id": display or raw_base,
        "array_job_id": None,
        "array_task_id": None,
        "het_offset": None,
    }
    match = re.match(r"^(\d+)_(\d+)$", display)
    if match:
        out["array_job_id"], out["array_task_id"] = int(match.group(1)), int(match.group(2))
        return out
    match = re.match(r"^(\d+)_\[", display)
    if match:
        out["array_job_id"] = int(match.group(1))
        return out
    match = re.match(r"^(\d+)\+(\d+)$", display)
    if match:
        out["het_offset"] = int(match.group(2))
    return out


def is_open(row: Mapping[str, str]) -> bool:
    """True while the job has not reached a final state."""
    state, _raw, _by = parse_state(row.get("State"))
    return state in OPEN_STATES or parse_ts(row.get("End")) is None


# ---------------------------------------------------------------------------
# Job + steps -> one stored record
# ---------------------------------------------------------------------------


def _compact(row: Mapping[str, str]) -> str:
    return json.dumps({k: v for k, v in row.items() if v not in ("", None)}, sort_keys=True)


def _step_record(row: Mapping[str, str], alloc_mem_bytes: Optional[int]) -> Dict[str, Any]:
    ids = parse_jobid(row.get("JobID", ""), row.get("JobIDRaw", ""))
    state, _raw, _by = parse_state(row.get("State"))
    in_tot = parse_tres_usage(row.get("TRESUsageInTot"))
    in_max = parse_tres_usage(row.get("TRESUsageInMax"))
    out_tot = parse_tres_usage(row.get("TRESUsageOutTot"))
    ntasks = parse_int(row.get("NTasks"))

    # CPU: rusage (TotalCPU) is exact but is not collected for a step that was
    # killed; the value polled by jobacct_gather survives. Take the larger.
    rusage = parse_duration(row.get("TotalCPU"))
    polled = in_tot.get("cpu_sec")
    if rusage is None and polled is None:
        cpu_sec, cpu_src = None, None
    elif polled is not None and (rusage is None or polled > rusage):
        cpu_sec, cpu_src = polled, "polled"
    else:
        cpu_sec, cpu_src = rusage, "rusage"

    # Memory: MaxRSS is the largest single task. For a multi-task step the
    # tasks hold memory at the same time, so use the total over tasks (or
    # MaxRSS x tasks when that is absent), capped at what the job was allocated.
    max_rss = parse_mem(row.get("MaxRSS"))
    if max_rss is None:
        max_rss = int(in_max["mem_bytes"]) if "mem_bytes" in in_max else None
    mem_tot = int(in_tot["mem_bytes"]) if "mem_bytes" in in_tot else None
    step_mem = max_rss
    estimated = False
    if max_rss is not None and ntasks and ntasks > 1:
        est = mem_tot if mem_tot is not None else max_rss * ntasks
        est = max(est, max_rss)
        if alloc_mem_bytes:
            est = min(est, max(alloc_mem_bytes, max_rss))
        estimated = est != max_rss
        step_mem = est

    return {
        "step_id": ids["step"] or "",
        "state": state,
        "exit_code": row.get("ExitCode") or None,
        "start_ts": parse_ts(row.get("Start")),
        "end_ts": parse_ts(row.get("End")),
        "elapsed_sec": parse_int(row.get("ElapsedRaw")),
        "ntasks": ntasks,
        "alloc_cpus": parse_int(row.get("AllocCPUS")),
        "cpu_sec": cpu_sec,
        "cpu_src": cpu_src,
        "max_rss_bytes": max_rss,
        "mem_tot_bytes": mem_tot,
        "disk_read_bytes": int(in_tot["disk_bytes"]) if "disk_bytes" in in_tot else None,
        "disk_write_bytes": int(out_tot["disk_bytes"]) if "disk_bytes" in out_tot else None,
        "gpu_util_max": in_max.get("gpu_util"),
        "gpu_util_ave": parse_tres_usage(row.get("TRESUsageInAve")).get("gpu_util"),
        "gpu_mem_max_bytes": (int(in_max["gpu_mem_bytes"]) if "gpu_mem_bytes" in in_max else None),
        "raw_json": _compact(row),
        "_mem": step_mem,
        "_estimated": estimated,
    }


def _peak_memory(
    steps: Sequence[Dict[str, Any]], job_start: Optional[int], job_end: Optional[int]
) -> Tuple[Optional[int], Optional[str]]:
    """Peak job memory as the largest sum of peaks of steps that ran together.

    The batch step, the extern step and any ``srun`` steps overlap in time, so
    the largest single step under-reports a job that runs steps in parallel.
    Summing the peaks of overlapping steps can only over-estimate, which is the
    safe direction for sizing.
    """
    spans = []
    for step in steps:
        mem = step.get("_mem")
        if mem is None:
            continue
        start = step.get("start_ts") or job_start or 0
        end = step.get("end_ts") or job_end or float("inf")
        if end < start:
            start, end = job_start or 0, job_end or float("inf")
        spans.append((start, end, mem, step.get("_estimated", False)))
    if not spans:
        return None, None

    # At equal timestamps process ends before starts, so steps that run one
    # after another are not added together.
    events = []
    for start, end, mem, _est in spans:
        events.append((start, 1, mem))
        events.append((end, 0, mem))
    current = peak = 0
    for _when, is_start, mem in sorted(events):
        current += mem if is_start else -mem
        peak = max(peak, current)

    largest = max(mem for _s, _e, mem, _est in spans)
    if any(est for _s, _e, _m, est in spans):
        src = "ntasks_est"
    elif largest >= 0.95 * peak:
        src = "step"
    else:
        src = "overlap_sum"
    return int(peak), src


def _max(values: Sequence[Optional[float]]) -> Optional[float]:
    present = [v for v in values if v is not None]
    return max(present) if present else None


def _sum(values: Sequence[Optional[float]]) -> Optional[float]:
    present = [v for v in values if v is not None]
    return sum(present) if present else None


def merge_job(
    job: Mapping[str, str], steps: Sequence[Mapping[str, str]]
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Combine a job line and its step lines into ``(job_record, step_records)``.

    The job record holds only what sacct reported, in base units. Efficiency,
    waste and flags are derived when a report is produced, so a change to those
    rules never requires re-collecting.
    """
    ids = parse_jobid(job.get("JobID", ""), job.get("JobIDRaw", ""))
    state, state_raw, _by = parse_state(job.get("State"))
    req_tres = parse_tres(job.get("ReqTRES"))
    alloc_tres = parse_tres(job.get("AllocTRES"))

    req_mem = tres_mem_bytes(req_tres)
    alloc_mem = tres_mem_bytes(alloc_tres)
    if req_mem is None:
        req_mem = alloc_mem
    req_gpus, req_gpu_type = tres_gpus(req_tres)
    alloc_gpus, alloc_gpu_type = tres_gpus(alloc_tres)

    start_ts = parse_ts(job.get("Start"))
    end_ts = parse_ts(job.get("End"))
    step_recs = [_step_record(s, alloc_mem) for s in steps]

    # CPU time: per step the better of rusage and polled, then summed. The job
    # line's own TotalCPU is the rusage sum, so it is only a floor.
    step_cpu = _sum([s["cpu_sec"] for s in step_recs])
    job_cpu = parse_duration(job.get("TotalCPU"))
    sources = {s["cpu_src"] for s in step_recs if s["cpu_sec"]}
    if step_cpu is None or (job_cpu is not None and job_cpu > step_cpu):
        cpu_sec, cpu_src = job_cpu, ("rusage" if job_cpu is not None else None)
    else:
        cpu_sec = step_cpu
        cpu_src = "mixed" if len(sources) > 1 else (sources.pop() if sources else "rusage")

    peak_mem, mem_src = _peak_memory(step_recs, start_ts, end_ts)
    if peak_mem is None:
        peak_mem = parse_mem(job.get("MaxRSS"))
        mem_src = "job" if peak_mem is not None else None

    timelimit = parse_int(job.get("TimelimitRaw"))
    record: Dict[str, Any] = {
        "job_id_raw": ids["job_id_raw"],
        "submit_ts": parse_ts(job.get("Submit")),
        "job_id": ids["job_id"],
        "array_job_id": ids["array_job_id"],
        "array_task_id": ids["array_task_id"],
        "het_offset": ids["het_offset"],
        "source": "sacct",
        "user": job.get("User") or None,
        "account": job.get("Account") or None,
        "partition": job.get("Partition") or None,
        "qos": job.get("QOS") or None,
        "job_name": job.get("JobName") or None,
        "submit_line": job.get("SubmitLine") or None,
        "work_dir": job.get("WorkDir") or None,
        "state": state,
        "state_raw": state_raw,
        "exit_code": job.get("ExitCode") or None,
        "derived_exit_code": job.get("DerivedExitCode") or None,
        "eligible_ts": parse_ts(job.get("Eligible")),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "elapsed_sec": parse_int(job.get("ElapsedRaw")),
        "req_time_min": timelimit,
        "req_cpus": parse_int(job.get("ReqCPUS")),
        "alloc_cpus": parse_int(job.get("AllocCPUS")),
        "req_nodes": parse_int(job.get("ReqNodes")),
        "alloc_nodes": parse_int(job.get("NNodes")),
        "node_list": job.get("NodeList") or None,
        "req_mem_mb": int(round(req_mem / 1024**2)) if req_mem is not None else None,
        "alloc_mem_mb": int(round(alloc_mem / 1024**2)) if alloc_mem is not None else None,
        "req_gpus": req_gpus,
        "alloc_gpus": alloc_gpus,
        "gpu_type": alloc_gpu_type or req_gpu_type,
        "cpu_sec": cpu_sec,
        "cpu_src": cpu_src,
        "user_cpu_sec": parse_duration(job.get("UserCPU")),
        "sys_cpu_sec": parse_duration(job.get("SystemCPU")),
        "peak_mem_bytes": peak_mem,
        "mem_src": mem_src,
        "oom": int(
            state == "OUT_OF_MEMORY" or any(s["state"] == "OUT_OF_MEMORY" for s in step_recs)
        ),
        "disk_read_bytes": _sum([s["disk_read_bytes"] for s in step_recs]),
        "disk_write_bytes": _sum([s["disk_write_bytes"] for s in step_recs]),
        "gpu_util_max": _max([s["gpu_util_max"] for s in step_recs]),
        "gpu_util_ave": _max([s["gpu_util_ave"] for s in step_recs]),
        "gpu_mem_max_bytes": _max([s["gpu_mem_max_bytes"] for s in step_recs]),
        "n_steps": len(step_recs),
        "n_tasks": _max([s["ntasks"] for s in step_recs]),
        "has_interactive_step": int(any(s["step_id"] == "interactive" for s in step_recs)),
        "restarts": parse_int(job.get("Restarts")),
        "parser_version": PARSER_VERSION,
        "raw_json": _compact(job),
    }
    for step in step_recs:
        step.pop("_mem", None)
        step.pop("_estimated", None)
    return record, step_recs
