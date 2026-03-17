#!/usr/bin/env python3
"""Nightly model fitting for HPCsizer.

Fits per-tool linear regression models (memory vs input file size) from the
profile database and stores updated parameters in the tool_models table.

Run nightly from cron:
    0 3 * * * /blue/GROUP/hpg-sizer/bin/update_models.py
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.db import init_db, query_jobs, upsert_tool_model

MIN_SAMPLES = 10  # Minimum samples to fit a model

try:
    import numpy as np
    from scipy import stats as scipy_stats
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False


def _extract_tool_jobs(jobs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group jobs by detected tool."""
    tool_jobs: Dict[str, List[Dict[str, Any]]] = {}
    for job in jobs:
        raw_tools = job.get("static_tools") or job.get("runtime_tools")
        if not raw_tools:
            continue
        if isinstance(raw_tools, str):
            try:
                tools = json.loads(raw_tools)
            except (json.JSONDecodeError, ValueError):
                tools = [raw_tools]
        else:
            tools = list(raw_tools)
        for tool in tools:
            tool_jobs.setdefault(tool, []).append(job)
    return tool_jobs


def _parse_input_gb(job: Dict[str, Any]) -> Optional[float]:
    """Return total input file size in GB from a job record."""
    raw = job.get("input_files")
    if not raw:
        return None
    if isinstance(raw, str):
        try:
            files = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return None
    else:
        files = raw
    total = sum(f.get("size_gb") or 0 for f in files if isinstance(f, dict))
    return total if total > 0 else None


def fit_model(
    jobs: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Fit a linear regression: peak_mem_gb = a * input_gb + b.

    Returns model dict or None if insufficient data.
    """
    points: List[Tuple[float, float]] = []
    for job in jobs:
        peak = job.get("sidecar_peak_gb") or job.get("sacct_peak_rss_gb")
        input_gb = _parse_input_gb(job)
        if peak is not None and input_gb is not None and input_gb > 0:
            points.append((input_gb, peak))

    if len(points) < MIN_SAMPLES:
        return None

    xs = [p[0] for p in points]
    ys = [p[1] for p in points]

    if _HAS_NUMPY:
        slope, intercept, r_value, _, _ = scipy_stats.linregress(xs, ys)
        r_squared = r_value ** 2
    else:
        # Manual least-squares
        n = len(xs)
        sx = sum(xs)
        sy = sum(ys)
        sxy = sum(x * y for x, y in zip(xs, ys))
        sxx = sum(x * x for x in xs)
        denom = n * sxx - sx * sx
        if denom == 0:
            return None
        slope = (n * sxy - sx * sy) / denom
        intercept = (sy - slope * sx) / n
        # R²
        y_mean = sy / n
        ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
        ss_tot = sum((y - y_mean) ** 2 for y in ys)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Optimal CPUs: median of req_cpus for completed jobs
    cpus = [j.get("req_cpus") or 1 for j in jobs if j.get("state") == "COMPLETED"]
    if not cpus:
        cpus = [j.get("req_cpus") or 1 for j in jobs]
    cpus_sorted = sorted(cpus)
    optimal_cpus = cpus_sorted[len(cpus_sorted) // 2]

    return {
        "mem_per_input_gb": max(slope, 0),
        "baseline_gb": max(intercept, 0),
        "optimal_cpus": optimal_cpus,
        "r_squared": r_squared,
        "sample_count": len(points),
    }


def update_models(
    db_path: Optional[str] = None,
    days: int = 365,
) -> None:
    """Fit and store models for all tools with sufficient data."""
    db_kwargs: Dict[str, Any] = {}
    if db_path:
        db_kwargs["db_path"] = db_path
    init_db(**({"db_path": db_path} if db_path else {}))

    all_jobs = query_jobs(days=days, **({"db_path": db_path} if db_path else {}))
    print(f"[update_models] Loaded {len(all_jobs)} jobs from last {days} days.")

    tool_jobs = _extract_tool_jobs(all_jobs)
    fitted = 0
    for tool, jobs in tool_jobs.items():
        model = fit_model(jobs)
        if model:
            model["tool"] = tool
            upsert_tool_model(model, **({"db_path": db_path} if db_path else {}))
            print(
                f"[update_models] {tool}: n={model['sample_count']}, "
                f"R²={model['r_squared']:.3f}, "
                f"slope={model['mem_per_input_gb']:.3f} GB/GB, "
                f"intercept={model['baseline_gb']:.2f} GB"
            )
            fitted += 1
    print(f"[update_models] Fitted {fitted} tool models.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HPCsizer nightly model updater")
    parser.add_argument("--db", default=None, help="Path to profiles.db")
    parser.add_argument("--days", type=int, default=365, help="Days of history to use")
    args = parser.parse_args()
    update_models(db_path=args.db, days=args.days)
