"""Time-series visualization for HPCsizer.

Generates multi-panel plots inspired by TACC Stats Figure 4.
"""

import csv
import gzip
import io
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    _HAS_MATPLOTLIB = True
except ImportError:  # pragma: no cover
    _HAS_MATPLOTLIB = False


def load_timeseries(ts_path: str) -> List[Dict[str, Any]]:
    """Load a compressed time-series CSV into a list of dicts."""
    rows = []
    opener = gzip.open if ts_path.endswith(".gz") else open
    with opener(ts_path, "rt", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            parsed: Dict[str, Any] = {}
            for k, v in row.items():
                try:
                    parsed[k] = float(v) if v not in ("", "None", "NA") else None
                except (ValueError, TypeError):
                    parsed[k] = v
            rows.append(parsed)
    return rows


def plot_job(
    job_id: str,
    timeseries_dir: str,
    output_dir: str,
    job_meta: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Generate a multi-panel time-series plot for *job_id*.

    Returns the output file path, or None if no time-series data is found.
    Raises ImportError if matplotlib is not available.
    """
    if not _HAS_MATPLOTLIB:
        raise ImportError("matplotlib is required for plotting")

    ts_path_gz = os.path.join(timeseries_dir, f"{job_id}.csv.gz")
    ts_path_plain = os.path.join(timeseries_dir, f"{job_id}.csv")
    ts_path = ts_path_gz if os.path.exists(ts_path_gz) else (
        ts_path_plain if os.path.exists(ts_path_plain) else None
    )
    if ts_path is None:
        return None

    rows = load_timeseries(ts_path)
    if not rows:
        return None

    elapsed = [r.get("elapsed_sec") for r in rows]
    elapsed_hr = [e / 3600 if e is not None else None for e in elapsed]
    x = elapsed_hr

    rss = [r.get("rss_gb") for r in rows]
    cpu_frac = [r.get("cpu_frac") for r in rows]
    io_read = [r.get("io_read_mb_s") for r in rows]
    threads = [r.get("threads") for r in rows]
    numa = [r.get("numa_miss_rate") for r in rows]

    n_panels = 5
    fig = plt.figure(figsize=(12, 14))
    gs = gridspec.GridSpec(n_panels, 1, hspace=0.45)

    title = f"Job {job_id}"
    if job_meta:
        user = job_meta.get("user", "")
        name = job_meta.get("job_name", "")
        if user or name:
            title += f"  ({user}  {name})"
    fig.suptitle(title, fontsize=13, fontweight="bold")

    def _plot_panel(idx, y_vals, ylabel, color, ylim_bottom=0):
        ax = fig.add_subplot(gs[idx])
        xs = [xi for xi, yi in zip(x, y_vals) if xi is not None and yi is not None]
        ys = [yi for xi, yi in zip(x, y_vals) if xi is not None and yi is not None]
        if xs:
            ax.plot(xs, ys, color=color, linewidth=0.9)
            ax.fill_between(xs, ys, alpha=0.20, color=color)
        ax.set_ylabel(ylabel, fontsize=9)
        ax.set_xlim(left=0)
        if ylim_bottom is not None:
            ax.set_ylim(bottom=ylim_bottom)
        ax.tick_params(labelsize=8)
        ax.grid(True, linestyle="--", alpha=0.4)
        return ax

    ax0 = _plot_panel(0, rss, "Memory RSS (GB)", "#2196F3")
    if job_meta and job_meta.get("req_mem_gb"):
        ax0.axhline(
            job_meta["req_mem_gb"],
            color="red",
            linestyle="--",
            linewidth=0.8,
            label=f"Requested {job_meta['req_mem_gb']:.0f} GB",
        )
        ax0.legend(fontsize=7)

    _plot_panel(1, cpu_frac, "CPU Fraction", "#4CAF50")
    _plot_panel(2, io_read, "Disk I/O Read (MB/s)", "#FF9800")
    _plot_panel(3, threads, "Thread Count", "#9C27B0")
    ax4 = _plot_panel(4, numa, "NUMA Miss Rate", "#F44336")
    ax4.set_xlabel("Elapsed Time (hours)", fontsize=9)
    if numa and any(v is not None for v in numa):
        ax4.axhline(0.20, color="red", linestyle=":", linewidth=0.8, label="Threshold 0.20")
        ax4.legend(fontsize=7)

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"{job_id}.png")
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return out_path
