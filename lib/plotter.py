"""Time-series plot of a profiled job: memory, cores, I/O and GPU against the request.

matplotlib is the one optional dependency of HPCsizer; everything else runs
without it, and ``hpg plot`` says so plainly when it is missing.
"""

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from lib import profile

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - depends on the environment
    plt = None

GIB = 1024.0**3
MIB = 1024.0**2

MISSING = (
    "hpg plot needs matplotlib, which this Python does not have. Use an environment that has it "
    "(for example: conda env create -f environment.yml), or read the series directly:"
)


class PlotError(Exception):
    """The plot cannot be made; the message says why."""


def available() -> bool:
    return plt is not None


def _hours(series: Sequence[Tuple[float, float]], start: float, scale: float = 1.0):
    return [(t - start) / 3600.0 for t, _v in series], [v / scale for _t, v in series]


def plot_job(
    job: Mapping[str, Any],
    node_series: Sequence[Tuple[Dict[str, str], Sequence[Dict[str, Any]]]],
    out_path: str,
) -> str:
    """Write a PNG for one job from its per-node series. Returns ``out_path``."""
    if plt is None:
        raise PlotError(MISSING)
    usable = [(meta, rows) for meta, rows in node_series if rows]
    if not usable:
        raise PlotError("the job's series holds no samples")
    start = min(rows[0]["t"] for _meta, rows in usable)

    mem = profile.aligned_sum([profile.value_series(rows, "mem") for _m, rows in usable])
    usage = profile.aligned_sum([profile.value_series(rows, "usage") for _m, rows in usable])
    cores = profile.aligned_sum([profile.rate_series(rows, "cpu") for _m, rows in usable])
    read = profile.aligned_sum([profile.rate_series(rows, "rd") for _m, rows in usable])
    write = profile.aligned_sum([profile.rate_series(rows, "wr") for _m, rows in usable])
    gpu = [s for s in (profile.value_series(rows, "gpu_util") for _m, rows in usable) if s]
    lustre = profile.aligned_sum(
        [
            profile.rate_series(rows, key)
            for _m, rows in usable
            for key in ("node_lustre_rd", "node_lustre_wr")
        ]
    )

    panels: List[str] = ["memory", "cores", "io"]
    if gpu:
        panels.append("gpu")
    if any(v for _t, v in lustre):
        panels.append("lustre")
    fig, axes = plt.subplots(len(panels), 1, figsize=(11, 2.7 * len(panels)), sharex=True)
    axes = [axes] if len(panels) == 1 else list(axes)
    by_name = dict(zip(panels, axes))

    title = "Job %s  %s  (%s)" % (
        job.get("job_id"),
        job.get("job_name") or "",
        job.get("user") or "",
    )
    if len(usable) > 1:
        title += "  -- %d nodes" % len(usable)
    fig.suptitle(title, fontsize=12, fontweight="bold")

    ax = by_name["memory"]
    if mem:
        xs, ys = _hours(mem, start, GIB)
        ax.plot(xs, ys, color="#1f77b4", linewidth=1.2, label="in use")
        ax.fill_between(xs, ys, alpha=0.18, color="#1f77b4")
    if usage:
        xs, ys = _hours(usage, start, GIB)
        ax.plot(xs, ys, color="#7f7f7f", linewidth=0.8, linestyle=":", label="with file cache")
    if len(usable) > 1:
        for meta, rows in usable:
            xs, ys = _hours(profile.value_series(rows, "mem"), start, GIB)
            ax.plot(xs, ys, linewidth=0.6, alpha=0.6, label=meta.get("node", "?"))
    if job.get("req_mem_mb"):
        ax.axhline(
            job["req_mem_mb"] / 1024.0,
            color="#d62728",
            linestyle="--",
            linewidth=0.9,
            label="requested %.0f GB" % (job["req_mem_mb"] / 1024.0),
        )
    ax.set_ylabel("Memory (GB)")
    ax.legend(fontsize=7, loc="upper right", ncol=2)

    ax = by_name["cores"]
    if cores:
        xs, ys = _hours(cores, start)
        ax.plot(xs, ys, color="#2ca02c", linewidth=1.0, label="busy")
        ax.fill_between(xs, ys, alpha=0.18, color="#2ca02c")
    if job.get("alloc_cpus"):
        ax.axhline(
            job["alloc_cpus"],
            color="#d62728",
            linestyle="--",
            linewidth=0.9,
            label="allocated %d" % job["alloc_cpus"],
        )
    ax.set_ylabel("Cores")
    ax.legend(fontsize=7, loc="upper right")

    ax = by_name["io"]
    for series, color, label in ((read, "#ff7f0e", "read"), (write, "#9467bd", "written")):
        if series:
            xs, ys = _hours(series, start, MIB)
            ax.plot(xs, ys, color=color, linewidth=0.9, label=label)
    ax.set_ylabel("I/O (MB/s)")
    ax.legend(fontsize=7, loc="upper right")

    if "gpu" in by_name:
        ax = by_name["gpu"]
        for index, series in enumerate(gpu):
            xs, ys = _hours(series, start)
            ax.plot(
                xs,
                ys,
                linewidth=0.9,
                label="node %d" % (index + 1) if len(gpu) > 1 else "busiest GPU",
            )
        ax.set_ylabel("GPU (%)")
        ax.set_ylim(0, 105)
        ax.legend(fontsize=7, loc="upper right")

    if "lustre" in by_name:
        ax = by_name["lustre"]
        xs, ys = _hours(lustre, start, MIB)
        ax.plot(xs, ys, color="#8c564b", linewidth=0.9)
        ax.set_ylabel("Lustre, whole node (MB/s)")

    for ax in axes:
        ax.set_ylim(bottom=0)
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.tick_params(labelsize=8)
    axes[-1].set_xlabel("Hours since the first sample")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(out_path, dpi=110)
    plt.close(fig)
    return out_path


def load(paths: Sequence[str]) -> List[Tuple[Dict[str, str], List[Dict[str, Any]]]]:
    """Read the per-node series files of a job, naming each by its node."""
    out = []
    for path in paths:
        meta, rows = profile.read_series(path)
        parts = profile.split_name(path)
        if parts:
            meta.setdefault("node", parts[1])
        out.append((meta, rows))
    return out


def default_name(job: Mapping[str, Any]) -> Optional[str]:
    return "hpg-job-%s.png" % str(job.get("job_id")).replace("/", "_")
