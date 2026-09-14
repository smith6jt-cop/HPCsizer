"""Recommendation engine for HPCsizer.

Produces memory, CPU and walltime recommendations based on:
  1. Profile DB (when ≥5 similar jobs exist)
  2. Tool-specific linear regression models (when ≥10 samples)
  3. Cold-start heuristics (otherwise)

Memory and CPUs come from the tiered chain above. Walltime is computed
independently from historical elapsed time, because no runtime model exists.
When there is no history *and* no detectable input size, no number is
returned at all -- an invented figure is worse than an explicit "unknown".
"""

from typing import Any, Dict, List, Optional

from lib.db import get_tool_model, query_jobs

# ---------------------------------------------------------------------------
# Cold-start heuristics
# ---------------------------------------------------------------------------

# (mem_multiplier, baseline_gb, cpu_hint)
# detect_tools() emits bare tool names, so these keys must be bare names too;
# earlier "Seurat_SCTransform"-style composite keys were unreachable.
_COLD_START: Dict[str, tuple] = {
    "readRDS": (3.0, 0.0, 1),
    "SCTransform": (4.5, 0.0, 4),  # 3.0 × file + 50%
    "FindMarkers": (2.5, 0.0, 4),
    "Seurat": (3.0, 0.0, 4),
    "scanpy": (2.0, 0.0, 4),
    "anndata": (2.0, 0.0, 4),
    "scVI": (2.0, 0.0, 4),
    "QuPath": (0.0, 2.0, None),  # 2 GB per core
    "cellranger": (0.0, 32.0, 8),
    "scimap": (2.0, 16.0, 4),
    "DESeq2": (2.0, 4.0, 4),
    "edgeR": (1.5, 2.0, 4),
    "limma": (1.5, 2.0, 4),
    "STAR": (0.0, 32.0, 8),
    "salmon": (0.0, 16.0, 8),
    "kallisto": (0.0, 12.0, 4),
}

_GENERIC_MULTIPLIER = 2.0
_GENERIC_BASELINE_GB = 16.0


def _cold_start_mem(
    tools: List[str],
    total_input_gb: float,
    req_cpus: int = 1,
) -> Optional[float]:
    """Return a cold-start memory estimate in GB, or None if none is possible.

    Takes the most demanding matching tool rather than the first one. detect_tools()
    returns a sorted list, so picking the first match meant the estimate was decided
    by alphabetical order -- FindMarkers (2.5x) beating SCTransform (4.5x) in the
    same script. Under-requesting kills the job; over-requesting only costs queue
    priority, so the maximum is the safe direction.

    Returns None when the only matching entries scale purely with input size and no
    input was detected: there is genuinely nothing to estimate from, and a confident
    small number (the old 1.0 GB floor) is actively dangerous.
    """
    estimates = []
    for tool in tools:
        if tool not in _COLD_START:
            continue
        mult, baseline, _ = _COLD_START[tool]
        if tool == "QuPath":
            estimates.append(max(baseline * req_cpus, 4.0))
        elif mult > 0 and total_input_gb <= 0 and baseline <= 0:
            continue  # multiplier-driven entry with nothing to multiply
        else:
            estimates.append(max(mult * total_input_gb + baseline, baseline))
    if estimates:
        return max(estimates)
    if total_input_gb > 0:
        return max(_GENERIC_MULTIPLIER * total_input_gb + _GENERIC_BASELINE_GB, 16.0)
    if any(t in _COLD_START for t in tools):
        return None
    # No recognised tool at all: the generic baseline is independent of input size.
    return _GENERIC_BASELINE_GB


def _cold_start_cpus(tools: List[str], req_cpus: int = 1) -> int:
    """Return cold-start CPU recommendation (most demanding matching tool)."""
    hints = [_COLD_START[t][2] for t in tools if t in _COLD_START and _COLD_START[t][2] is not None]
    if hints:
        return max(hints)
    return req_cpus


# ---------------------------------------------------------------------------
# DB-backed recommendation
# ---------------------------------------------------------------------------

MIN_SAMPLES_DB = 5
MIN_SAMPLES_MODEL = 10

# Memory headroom over the historical p90. Walltime gets more: running out of
# memory and running out of time both kill the job, but a too-short --time wastes
# every CPU-hour already spent, while over-asking only costs queue priority.
_MEM_HEADROOM = 1.10
_WALLTIME_HEADROOM = 1.5
_MIN_WALLTIME_MIN = 15


def _peak_gb(job: Dict[str, Any]) -> Optional[float]:
    """Peak memory for a job, preferring the sidecar measurement over sacct MaxRSS.

    Only jobs submitted through ``hpg submit``/``hpg interactive`` carry
    ``sidecar_peak_gb``; anything harvested from sacct or loaded by backfill.py has
    only ``sacct_peak_rss_gb``. Requiring the sidecar value made the entire history
    tier invisible to backfilled databases. ``hpg history`` and update_models.py
    already fall back this way.
    """
    return job.get("sidecar_peak_gb") or job.get("sacct_peak_rss_gb")


def _percentile(values: List[float], pct: float) -> float:
    """Index-based percentile, matching the convention used by ``hpg history``."""
    ordered = sorted(values)
    return ordered[min(int(pct * len(ordered)), len(ordered) - 1)]


def _query_kwargs(
    days: int, user: Optional[str] = None, db_path: Optional[str] = None
) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {"days": days}
    if user:
        kwargs["user"] = user
    if db_path:
        kwargs["db_path"] = db_path
    return kwargs


def _db_recommendation(
    tools: List[str],
    days: int = 90,
    user: Optional[str] = None,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return recommendation from historical job data if ≥5 samples exist."""
    import statistics

    kwargs = _query_kwargs(days, user, db_path)

    for tool in tools:
        jobs = query_jobs(tool=tool, **kwargs)
        completed = [
            j
            for j in jobs
            if j.get("state") in ("COMPLETED", "TIMEOUT") and _peak_gb(j) is not None
        ]
        if len(completed) >= MIN_SAMPLES_DB:
            peaks = [_peak_gb(j) for j in completed]
            mem_rec = _percentile(peaks, 0.9) * _MEM_HEADROOM

            cpus = [j.get("req_cpus") or 1 for j in completed]
            cpu_rec = round(statistics.median(cpus))

            return {
                "mem_gb": mem_rec,
                "cpus": cpu_rec,
                "source": "database",
                "samples": len(completed),
                "sidecar_samples": sum(1 for j in completed if j.get("sidecar_peak_gb")),
                "tool": tool,
            }
    return None


def _walltime_recommendation(
    tools: List[str],
    days: int = 90,
    user: Optional[str] = None,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return a walltime estimate in minutes from historical elapsed time.

    Independent of the memory tier chain: update_models.py fits memory against input
    size only, so there is no runtime model to consult.

    TIMEOUT jobs are excluded. They are fine for memory -- the peak RSS observed was
    real -- but their elapsed time only proves the job was cut off at its limit, so
    including them would anchor the estimate to the old wrong limit.
    """
    kwargs = _query_kwargs(days, user, db_path)

    for tool in tools:
        jobs = query_jobs(tool=tool, **kwargs)
        elapsed = [
            j["sacct_elapsed_sec"]
            for j in jobs
            if j.get("state") == "COMPLETED" and j.get("sacct_elapsed_sec")
        ]
        if len(elapsed) >= MIN_SAMPLES_DB:
            p90_min = _percentile(elapsed, 0.9) / 60.0
            return {
                "time_min": max(int(round(p90_min * _WALLTIME_HEADROOM)), _MIN_WALLTIME_MIN),
                "time_samples": len(elapsed),
                "time_tool": tool,
            }
    return None


def _model_recommendation(
    tools: List[str],
    total_input_gb: float,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return recommendation from linear regression model if ≥10 samples."""
    kwargs: Dict[str, Any] = {}
    if db_path:
        kwargs["db_path"] = db_path

    for tool in tools:
        model = get_tool_model(tool, **kwargs)
        if model and model.get("sample_count", 0) >= MIN_SAMPLES_MODEL:
            mem_gb = model["mem_per_input_gb"] * total_input_gb + model["baseline_gb"]
            return {
                "mem_gb": mem_gb * _MEM_HEADROOM,
                "cpus": model["optimal_cpus"],
                "source": "model",
                "r_squared": model["r_squared"],
                "samples": model["sample_count"],
                "tool": tool,
            }
    return None


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def recommend(
    tools: List[str],
    total_input_gb: float,
    req_cpus: int = 1,
    req_gpus: int = 0,
    db_path: Optional[str] = None,
    user: Optional[str] = None,
    days: int = 90,
) -> Dict[str, Any]:
    """Return the best available resource recommendation.

    Memory and CPUs: DB historical data > regression model > cold-start heuristics.
    Walltime is looked up separately and may be present or absent regardless of
    which memory tier answered.

    ``mem_gb`` is None when no tier could produce a defensible number; callers must
    handle that rather than printing it as a figure. ``warnings`` carries any caveats
    worth showing the user.
    """
    warnings: List[str] = []

    rec = _db_recommendation(tools, days=days, user=user, db_path=db_path)
    if rec is None:
        rec = _model_recommendation(tools, total_input_gb, db_path=db_path)
        if rec is not None and total_input_gb <= 0:
            warnings.append(
                "No input files detected, so the fitted model contributed only its "
                "baseline; the estimate does not reflect your dataset size."
            )
    if rec is None:
        mem_gb = _cold_start_mem(tools, total_input_gb, req_cpus)
        cpus = _cold_start_cpus(tools, req_cpus)
        if mem_gb is None:
            rec = {"mem_gb": None, "cpus": cpus, "source": "insufficient-data", "samples": 0}
            warnings.append(
                "No job history for these tools and no input files detected, so no "
                "memory estimate is possible. Check what similar jobs actually used: "
                "hpg history --tool <TOOL> --days 180"
            )
        else:
            rec = {"mem_gb": mem_gb, "cpus": cpus, "source": "heuristic", "samples": 0}
            if total_input_gb <= 0:
                warnings.append(
                    "No input files detected — only absolute paths under /blue, /orange, "
                    "/red, /scratch or /home are recognised. This estimate ignores your "
                    "dataset size; verify it against hpg history."
                )

    walltime = _walltime_recommendation(tools, days=days, user=user, db_path=db_path)
    if walltime:
        rec.update(walltime)

    rec["req_gpus"] = req_gpus
    rec["warnings"] = warnings
    return rec
