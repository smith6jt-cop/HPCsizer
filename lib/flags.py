"""Anomaly detection for HPCsizer job profiles.

Implements flags described in Section 3.4 of the design document.
"""

from typing import Any, Dict, List, Optional


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / b


def detect_flags(
    job: Dict[str, Any], timeseries: Optional[List[Dict[str, Any]]] = None
) -> List[str]:
    """Return a list of anomaly flag names for a job record.

    Parameters
    ----------
    job:
        A dict matching the ``jobs`` table schema.
    timeseries:
        Optional list of per-poll dicts (keys: elapsed_sec, rss_gb, cpu_frac,
        io_read_mb_s, threads, numa_miss_rate).  If not provided, only
        sacct-level flags are attempted.
    """
    flags: List[str] = []

    state = job.get("state", "")
    if state == "OUT_OF_MEMORY":
        flags.append("oom_killed")

    # mem_overrequest: peak RSS < 25% of requested memory
    peak_gb = job.get("sidecar_peak_gb") or job.get("sacct_peak_rss_gb")
    req_mem_gb = job.get("req_mem_gb")
    mem_eff = _safe_div(peak_gb, req_mem_gb)
    if mem_eff is not None and mem_eff < 0.25:
        flags.append("mem_overrequest")

    # high_cpi: average cycles per instruction above threshold
    cpi = job.get("cpi")
    if cpi is not None and cpi > 1.0:
        flags.append("high_cpi")

    # cache_thrashing: high cache miss rate indicating poor data locality
    cache_miss = job.get("cache_miss_rate")
    if cache_miss is not None and cache_miss > 0.5:
        flags.append("cache_thrashing")

    # node_imbalance: high coefficient of variation in CPU use across nodes
    node_cv = job.get("node_imbalance_cv")
    num_nodes = job.get("num_nodes") or 1
    if node_cv is not None and num_nodes > 1 and node_cv > 1.0:
        flags.append("node_imbalance")

    # idle_nodes: multi-node job but effectively single-node CPU usage
    if num_nodes > 1:
        cpu_time = job.get("sacct_cpu_time_sec")
        elapsed = job.get("sacct_elapsed_sec") or 1
        req_cpus = job.get("req_cpus") or 1
        if cpu_time is not None and elapsed > 0 and req_cpus > 0:
            # req_cpus (NCPUS) is total across all nodes; compute per-node
            per_node_cpus = req_cpus / num_nodes
            # If total CPU time is less than what 1 node could produce,
            # some nodes are likely idle
            single_node_capacity = elapsed * per_node_cpus
            if cpu_time < single_node_capacity * 0.5:
                flags.append("idle_nodes")

    # CPU-related flags require time-series
    if timeseries and len(timeseries) > 2:
        cpu_fracs = [r.get("cpu_frac") for r in timeseries if r.get("cpu_frac") is not None]
        elapsed = job.get("sacct_elapsed_sec") or 1
        req_cpus = job.get("req_cpus") or 1

        if cpu_fracs:
            idle_frac = sum(1 for c in cpu_fracs if c < 0.05) / len(cpu_fracs)
            if idle_frac > 0.50:
                flags.append("idle_cpu")

            cpu_time = job.get("sacct_cpu_time_sec")
            if cpu_time is not None and req_cpus > 2:
                eff_cores = cpu_time / elapsed if elapsed else 0
                if eff_cores < 1.5:
                    flags.append("single_threaded")

        # mem_spike_plateau: peak in first 10% of runtime, then stable
        rss_vals = [r.get("rss_gb") for r in timeseries if r.get("rss_gb") is not None]
        if rss_vals and len(rss_vals) >= 10:
            cutoff = max(1, len(rss_vals) // 10)
            early_peak = max(rss_vals[:cutoff])
            late_vals = rss_vals[cutoff:]
            if late_vals:
                p95_late = sorted(late_vals)[int(0.95 * len(late_vals))]
                if early_peak > 0 and p95_late < 0.6 * early_peak:
                    flags.append("mem_spike_plateau")

        # io_dominant: majority of wall time in high I/O with low CPU
        io_rates = [r.get("io_read_mb_s", 0) or 0 for r in timeseries]
        if io_rates and cpu_fracs:
            high_io = sum(1 for io, cpu in zip(io_rates, cpu_fracs) if io > 10 and cpu < 0.10)
            if high_io / len(timeseries) > 0.50:
                flags.append("io_dominant")

        # numa_misplaced
        numa_rates = [
            r.get("numa_miss_rate") for r in timeseries if r.get("numa_miss_rate") is not None
        ]
        if numa_rates:
            avg_numa = sum(numa_rates) / len(numa_rates)
            if avg_numa > 0.20:
                flags.append("numa_misplaced")

        # catastrophe: step-function drop in activity mid-job
        if len(cpu_fracs) >= 10:
            mid = len(cpu_fracs) // 2
            first_half = sum(cpu_fracs[:mid]) / mid
            second_half = sum(cpu_fracs[mid:]) / (len(cpu_fracs) - mid)
            if first_half > 0.10 and second_half < 0.05:
                flags.append("catastrophe")

        # lustre_metadata_heavy: excessive metadata operations per second
        lustre_meta_rates = [
            r.get("lustre_metadata_ops_s")
            for r in timeseries
            if r.get("lustre_metadata_ops_s") is not None
        ]
        if lustre_meta_rates:
            avg_meta = sum(lustre_meta_rates) / len(lustre_meta_rates)
            if avg_meta > 100.0:
                flags.append("lustre_metadata_heavy")

        # lustre_io_dominant: most wall time spent in high Lustre I/O (read+write) with low CPU
        lustre_cpu_samples = [
            ((r.get("lustre_read_mb_s") or 0) + (r.get("lustre_write_mb_s") or 0), r["cpu_frac"])
            for r in timeseries
            if r.get("lustre_read_mb_s") is not None and r.get("cpu_frac") is not None
        ]
        if lustre_cpu_samples:
            high_lustre_io = sum(
                1 for io_rate, cpu in lustre_cpu_samples if io_rate > 50 and cpu < 0.10
            )
            if high_lustre_io / len(lustre_cpu_samples) > 0.50:
                flags.append("lustre_io_dominant")

    return flags


def compute_efficiency(job: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """Compute memory and CPU efficiency metrics for a job."""
    peak_gb = job.get("sidecar_peak_gb") or job.get("sacct_peak_rss_gb")
    req_mem_gb = job.get("req_mem_gb")
    mem_eff = _safe_div(peak_gb, req_mem_gb)

    sacct_cpu = job.get("sacct_cpu_time_sec")
    elapsed = job.get("sacct_elapsed_sec")
    req_cpus = job.get("req_cpus") or 1
    if elapsed and req_cpus:
        cpu_eff = _safe_div(sacct_cpu, elapsed * req_cpus)
    else:
        cpu_eff = None

    waste_gb: Optional[float] = None
    if peak_gb is not None and req_mem_gb is not None:
        waste_gb = max(req_mem_gb - peak_gb, 0.0)

    return {
        "mem_efficiency": mem_eff,
        "cpu_efficiency": cpu_eff,
        "waste_gb": waste_gb,
    }
