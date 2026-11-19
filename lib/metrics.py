"""Requested versus used, derived from stored records at read time.

Nothing here is written to the database. The previous version computed
efficiency and flags once at ingest from a parser that was wrong, stored them,
and could not recompute them once Slurm had purged the records.

A value is ``None`` when it was not measured -- never 0. The cases:

* memory: Slurm samples a task's memory every ``acct_sample_sec`` (60 s on the
  target cluster), so a job shorter than two samples has no usable peak;
* CPU: ``TotalCPU`` comes from process rusage, which Slurm does not collect for
  a step it had to kill. For timed-out and cancelled jobs it reads near zero
  whatever the job did, so only a polled value or an in-job profile counts.
"""

import math
from typing import Any, Dict, List, Mapping, Optional, Sequence

from lib.config import Config

GIB = 1024.0**3
MIB_PER_GIB = 1024.0

# Flags that can be decided from accounting alone.
ACCOUNTING_FLAGS = (
    "oom",
    "timeout",
    "mem_overrequest",
    "cpu_overrequest",
    "time_overrequest",
    "gpu_idle",
    "unmeasured",
)

FLAG_HELP = {
    "oom": "ran out of memory: raise --mem",
    "timeout": "hit its time limit: raise --time",
    "mem_overrequest": "peak memory under 25% of the request: lower --mem",
    "cpu_overrequest": "used under half of its cores on average: lower --cpus-per-task",
    "time_overrequest": "finished in under 25% of its time limit: lower --time",
    "gpu_idle": "held a GPU it did not use",
    "unmeasured": "too short for accounting to sample memory",
}

# Thresholds. A flag needs a minimum duration and a minimum absolute size so
# that seconds-long jobs and megabyte-sized differences do not raise it.
MEM_OVERREQUEST_RATIO = 0.25
MEM_OVERREQUEST_MIN_GB = 2.0
CPU_OVERREQUEST_RATIO = 0.5
CPU_MIN_ELAPSED_SEC = 300
TIME_OVERREQUEST_RATIO = 0.25
TIME_MIN_LIMIT_MIN = 60
GPU_IDLE_PERCENT = 5.0
PROFILE_COVERAGE = 0.9

_KILLED = frozenset({"TIMEOUT", "CANCELLED", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY", "DEADLINE"})


def quantile(values: Sequence[float], q: float) -> Optional[float]:
    """Nearest-rank quantile: the smallest value with at least ``q`` of the data at or below it.

    For small samples this never interpolates below an observed value, and
    ``q=1`` is the maximum.
    """
    data = sorted(v for v in values if v is not None)
    if not data:
        return None
    rank = max(1, int(math.ceil(q * len(data))))
    return data[min(rank, len(data)) - 1]


def describe(values: Sequence[Optional[float]]) -> Optional[Dict[str, float]]:
    """``{n, min, p50, p90, p95, max}`` of the present values, or None when there are none."""
    data = sorted(v for v in values if v is not None)
    if not data:
        return None
    return {
        "n": len(data),
        "min": data[0],
        "p50": quantile(data, 0.50),
        "p90": quantile(data, 0.90),
        "p95": quantile(data, 0.95),
        "max": data[-1],
    }


def job_metrics(
    job: Mapping[str, Any], cfg: Config, session: Optional[bool] = None
) -> Dict[str, Any]:
    """Derived metrics and flags for one stored job.

    ``session`` says whether the job is an interactive session (from
    ``lib.classify``); it defaults to the job's own ``session`` key.
    """
    if session is None:
        session = bool(job.get("session"))
    state = job.get("state") or ""
    elapsed = job.get("elapsed_sec") or 0
    elapsed_h = elapsed / 3600.0

    # A profile is the whole truth when it read the job's cgroup for (nearly)
    # the whole run. Otherwise it is one more lower bound, like accounting, and
    # the larger of the two is used.
    trusted = (
        str(job.get("mon_source") or "").startswith("cgroup")
        and (job.get("mon_coverage") or 0) >= PROFILE_COVERAGE
    )

    # -- memory --------------------------------------------------------------
    req_mem_mb = job.get("req_mem_mb")
    mem_req_gb = req_mem_mb / MIB_PER_GIB if req_mem_mb else None
    mem_used_gb: Optional[float] = None
    mem_source: Optional[str] = None
    monitor_peak = job.get("mon_peak_mem_bytes")
    sacct_peak = job.get("peak_mem_bytes") if elapsed >= 2 * cfg.acct_sample_sec else None
    if monitor_peak and (trusted or not sacct_peak or monitor_peak >= sacct_peak):
        mem_used_gb, mem_source = monitor_peak / GIB, "profile"
    elif sacct_peak:
        mem_used_gb, mem_source = sacct_peak / GIB, "sacct"
    mem_eff = mem_used_gb / mem_req_gb if mem_used_gb is not None and mem_req_gb else None

    # -- CPU -----------------------------------------------------------------
    cores_alloc = job.get("alloc_cpus") or job.get("req_cpus") or None
    cpu_sec: Optional[float] = None
    cpu_source: Optional[str] = None
    sacct_cpu = job.get("cpu_sec")
    if sacct_cpu is not None and state in _KILLED and job.get("cpu_src") not in ("polled", "mixed"):
        sacct_cpu = None  # rusage of a step Slurm killed: near zero whatever it did
    monitor_cpu = job.get("mon_cpu_sec")
    if monitor_cpu is not None and (trusted or sacct_cpu is None or monitor_cpu >= sacct_cpu):
        cpu_sec, cpu_source = monitor_cpu, "profile"
    elif sacct_cpu is not None:
        cpu_sec, cpu_source = sacct_cpu, job.get("cpu_src") or "rusage"
    cores_used = cpu_sec / elapsed if cpu_sec is not None and elapsed >= 60 else None
    cores_peak = job.get("mon_max_cores")
    cores_p95 = job.get("mon_p95_cores")
    cpu_eff = cores_used / cores_alloc if cores_used is not None and cores_alloc else None

    # -- time ----------------------------------------------------------------
    time_req_min = job.get("req_time_min")
    time_eff = elapsed / (time_req_min * 60.0) if time_req_min and elapsed else None

    # -- GPU -----------------------------------------------------------------
    gpus = job.get("alloc_gpus") or 0
    gpu_util = job.get("mon_gpu_util_max")
    if gpu_util is None:
        gpu_util = job.get("gpu_util_max")
    gpu_mem = job.get("mon_gpu_mem_max_bytes") or job.get("gpu_mem_max_bytes")

    # -- resource-hours ------------------------------------------------------
    mem_gbh_req = mem_req_gb * elapsed_h if mem_req_gb is not None else None
    mem_gbh_used = mem_used_gb * elapsed_h if mem_used_gb is not None else None
    mem_gbh_unused = (
        max(mem_gbh_req - mem_gbh_used, 0.0)
        if mem_gbh_req is not None and mem_gbh_used is not None
        else None
    )
    core_h_alloc = cores_alloc * elapsed_h if cores_alloc else None
    core_h_used = cpu_sec / 3600.0 if cores_used is not None else None
    core_h_unused = (
        max(core_h_alloc - core_h_used, 0.0)
        if core_h_alloc is not None and core_h_used is not None
        else None
    )
    gpu_h = gpus * elapsed_h if gpus else 0.0
    gpu_idle = bool(
        gpus
        and gpu_util is not None
        and gpu_util < GPU_IDLE_PERCENT
        and elapsed >= CPU_MIN_ELAPSED_SEC
    )

    # -- flags ---------------------------------------------------------------
    flags: List[str] = []
    if job.get("oom") or state == "OUT_OF_MEMORY":
        flags.append("oom")
    if state == "TIMEOUT" and not session:
        flags.append("timeout")
    if (
        mem_eff is not None
        and mem_eff < MEM_OVERREQUEST_RATIO
        and (mem_req_gb - mem_used_gb) >= MEM_OVERREQUEST_MIN_GB
    ):
        flags.append("mem_overrequest")
    if (
        not session
        and cpu_eff is not None
        and cores_alloc >= 2
        and cpu_eff < CPU_OVERREQUEST_RATIO
        and elapsed >= CPU_MIN_ELAPSED_SEC
    ):
        flags.append("cpu_overrequest")
    if (
        not session
        and state == "COMPLETED"
        and time_eff is not None
        and time_req_min >= TIME_MIN_LIMIT_MIN
        and time_eff < TIME_OVERREQUEST_RATIO
    ):
        flags.append("time_overrequest")
    if gpu_idle:
        flags.append("gpu_idle")
    if elapsed > 0 and mem_used_gb is None:
        flags.append("unmeasured")

    start = job.get("start_ts")
    queued_from = job.get("eligible_ts") or job.get("submit_ts")
    return {
        "elapsed_h": elapsed_h,
        "mem_req_gb": mem_req_gb,
        "mem_used_gb": mem_used_gb,
        "mem_source": mem_source,
        "mem_eff": mem_eff,
        "cores_alloc": cores_alloc,
        "cores_used": cores_used,
        "cores_peak": cores_peak,
        "cores_p95": cores_p95,
        "cpu_source": cpu_source,
        "cpu_eff": cpu_eff,
        "time_req_min": time_req_min,
        "time_eff": time_eff,
        "gpus": gpus,
        "gpu_util": gpu_util,
        "gpu_mem_gb": gpu_mem / GIB if gpu_mem else None,
        "mem_gbh_req": mem_gbh_req,
        "mem_gbh_used": mem_gbh_used,
        "mem_gbh_unused": mem_gbh_unused,
        "core_h_alloc": core_h_alloc,
        "core_h_used": core_h_used,
        "core_h_unused": core_h_unused,
        "gpu_h": gpu_h,
        "gpu_h_idle": gpu_h if gpu_idle else 0.0,
        "wait_sec": (start - queued_from) if start and queued_from else None,
        "flags": flags,
    }


def annotate(jobs: Sequence[Dict[str, Any]], cfg: Config) -> Sequence[Dict[str, Any]]:
    """Attach ``job["m"]`` (metrics) to each classified job dict in place."""
    for job in jobs:
        job["m"] = job_metrics(job, cfg)
    return jobs
