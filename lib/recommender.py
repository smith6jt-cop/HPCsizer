"""Recommendation engine for HPCsizer.

Produces memory and CPU recommendations based on:
  1. Profile DB (when ≥5 similar jobs exist)
  2. Tool-specific linear regression models (when ≥10 samples)
  3. Cold-start heuristics (otherwise)
"""

from typing import Any, Dict, List, Optional

from lib.db import get_tool_model, query_jobs


# ---------------------------------------------------------------------------
# Cold-start heuristics
# ---------------------------------------------------------------------------

# (mem_multiplier, baseline_gb, cpu_hint)
_COLD_START: Dict[str, tuple] = {
    "Seurat_readRDS":       (3.0,  0.0,  1),
    "Seurat_SCTransform":   (4.5,  0.0,  4),   # 3.0 × file + 50%
    "Seurat_FindMarkers":   (2.5,  0.0,  4),
    "Seurat":               (3.0,  0.0,  4),
    "scanpy":               (2.0,  0.0,  4),
    "anndata":              (2.0,  0.0,  4),
    "scVI":                 (2.0,  0.0,  4),
    "QuPath":               (0.0,  2.0,  None),  # 2 GB per core
    "cellranger":           (0.0, 32.0,  8),
    "scimap":               (0.0,  0.0,  4),     # computed separately
    "DESeq2":               (2.0,  4.0,  4),
    "edgeR":                (1.5,  2.0,  4),
    "limma":                (1.5,  2.0,  4),
    "STAR":                 (0.0, 32.0,  8),
    "salmon":               (0.0, 16.0,  8),
    "kallisto":             (0.0, 12.0,  4),
}

_GENERIC_MULTIPLIER = 2.0
_GENERIC_BASELINE_GB = 16.0


def _cold_start_mem(
    tools: List[str],
    total_input_gb: float,
    req_cpus: int = 1,
) -> float:
    """Return cold-start memory estimate in GB."""
    for tool in tools:
        if tool in _COLD_START:
            mult, baseline, cpu_hint = _COLD_START[tool]
            if tool == "QuPath":
                return max(baseline * req_cpus, 4.0)
            return max(mult * total_input_gb + baseline, baseline, 1.0)
    # Generic fallback
    return max(_GENERIC_MULTIPLIER * total_input_gb + _GENERIC_BASELINE_GB, 16.0)


def _cold_start_cpus(tools: List[str], req_cpus: int = 1) -> int:
    """Return cold-start CPU recommendation."""
    for tool in tools:
        if tool in _COLD_START:
            _, _, cpu_hint = _COLD_START[tool]
            if cpu_hint is not None:
                return cpu_hint
    return req_cpus


# ---------------------------------------------------------------------------
# DB-backed recommendation
# ---------------------------------------------------------------------------

MIN_SAMPLES_DB = 5
MIN_SAMPLES_MODEL = 10


def _db_recommendation(
    tools: List[str],
    days: int = 90,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return recommendation from historical job data if ≥5 samples exist."""
    import json
    import statistics

    kwargs: Dict[str, Any] = {"days": days}
    if db_path:
        kwargs["db_path"] = db_path

    for tool in tools:
        jobs = query_jobs(tool=tool, **kwargs)
        completed = [
            j for j in jobs
            if j.get("state") in ("COMPLETED", "TIMEOUT")
            and j.get("sidecar_peak_gb") is not None
        ]
        if len(completed) >= MIN_SAMPLES_DB:
            peaks = [j["sidecar_peak_gb"] for j in completed]
            # Use 90th percentile + 10% headroom
            peaks_sorted = sorted(peaks)
            p90 = peaks_sorted[int(0.9 * len(peaks_sorted))]
            mem_rec = p90 * 1.10

            cpus = [j.get("req_cpus") or 1 for j in completed]
            cpu_rec = round(statistics.median(cpus))

            return {
                "mem_gb": mem_rec,
                "cpus": cpu_rec,
                "source": "database",
                "samples": len(completed),
                "tool": tool,
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
            mem_gb = (
                model["mem_per_input_gb"] * total_input_gb + model["baseline_gb"]
            )
            return {
                "mem_gb": mem_gb * 1.10,
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
) -> Dict[str, Any]:
    """Return the best available resource recommendation.

    Priority: DB historical data > regression model > cold-start heuristics.
    """
    # Try DB-backed recommendation
    rec = _db_recommendation(tools, db_path=db_path)
    if rec is None:
        rec = _model_recommendation(tools, total_input_gb, db_path=db_path)
    if rec is None:
        mem_gb = _cold_start_mem(tools, total_input_gb, req_cpus)
        cpus = _cold_start_cpus(tools, req_cpus)
        rec = {
            "mem_gb": mem_gb,
            "cpus": cpus,
            "source": "heuristic",
            "samples": 0,
        }
    rec["req_gpus"] = req_gpus
    return rec
