#!/usr/bin/env python3
"""Sidecar job monitor for HPCsizer.

Polls /proc for resource usage at adaptive intervals and writes a compressed
time-series CSV.  Designed to be injected into SLURM jobs via hpg submit.

Usage (internal, injected by hpg):
    python monitor.py <job_id> <output_path.csv.gz> [<pid> ...]
"""

import csv
import gzip
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# /proc helpers
# ---------------------------------------------------------------------------


def _read_proc_status(pid: int) -> Dict[str, Any]:
    """Read VmRSS, VmHWM, VmSwap from /proc/<pid>/status."""
    result: Dict[str, Any] = {}
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    result["rss_kb"] = int(line.split()[1])
                elif line.startswith("VmHWM:"):
                    result["hwm_kb"] = int(line.split()[1])
                elif line.startswith("VmSwap:"):
                    result["swap_kb"] = int(line.split()[1])
                elif line.startswith("Threads:"):
                    result["threads"] = int(line.split()[1])
    except OSError:
        pass
    return result


def _read_proc_stat(pid: int) -> Dict[str, Any]:
    """Read utime, stime, rss, majflt from /proc/<pid>/stat."""
    result: Dict[str, Any] = {}
    try:
        with open(f"/proc/{pid}/stat") as fh:
            fields = fh.read().split()
        # field indices per proc(5)
        result["utime"] = int(fields[13])
        result["stime"] = int(fields[14])
        result["majflt"] = int(fields[11])
    except (OSError, IndexError, ValueError):
        pass
    return result


def _read_proc_io(pid: int) -> Dict[str, Any]:
    """Read read_bytes, write_bytes from /proc/<pid>/io."""
    result: Dict[str, Any] = {}
    try:
        with open(f"/proc/{pid}/io") as fh:
            for line in fh:
                if line.startswith("read_bytes:"):
                    result["read_bytes"] = int(line.split()[1])
                elif line.startswith("write_bytes:"):
                    result["write_bytes"] = int(line.split()[1])
    except OSError:
        pass
    return result


def _read_vmstat() -> Dict[str, Any]:
    """Read pgfault and pgmajfault from /proc/vmstat."""
    result: Dict[str, Any] = {}
    try:
        with open("/proc/vmstat") as fh:
            for line in fh:
                if line.startswith("pgfault "):
                    result["pgfault"] = int(line.split()[1])
                elif line.startswith("pgmajfault "):
                    result["pgmajfault"] = int(line.split()[1])
    except OSError:
        pass
    return result


def _find_job_pids(job_id: Optional[str] = None) -> List[int]:
    """Return PIDs belonging to the current job (SLURM_JOB_ID env)."""
    slurm_pid = os.environ.get("SLURM_TASK_PID")
    if slurm_pid:
        return [int(slurm_pid)]
    pids = []
    try:
        for entry in os.scandir("/proc"):
            if entry.name.isdigit():
                try:
                    cgroup_path = f"/proc/{entry.name}/cgroup"
                    if job_id and os.path.exists(cgroup_path):
                        with open(cgroup_path) as fh:
                            if job_id in fh.read():
                                pids.append(int(entry.name))
                except OSError:
                    pass
    except OSError:
        pass
    if not pids:
        pids = [os.getpid()]
    return pids


def _read_numastat() -> float:
    """Return aggregate NUMA miss rate from /sys/.../numastat."""
    hit_total = 0
    miss_total = 0
    numa_base = "/sys/devices/system/node"
    if not os.path.isdir(numa_base):
        return 0.0
    try:
        for node_dir in Path(numa_base).iterdir():
            stats_path = node_dir / "numastat"
            if not stats_path.exists():
                continue
            with open(stats_path) as fh:
                for line in fh:
                    if line.startswith("numa_hit"):
                        hit_total += int(line.split()[1])
                    elif line.startswith("numa_miss"):
                        miss_total += int(line.split()[1])
    except OSError:
        pass
    total = hit_total + miss_total
    return miss_total / total if total > 0 else 0.0


def _read_lustre_stats():
    """Read Lustre client stats from /proc/fs/lustre/llite/*/stats.

    Returns cumulative byte counters and metadata op counts across all mounts.
    These are node-level counters, not per-process.
    """
    result = {
        "lustre_read_bytes": 0,
        "lustre_write_bytes": 0,
        "lustre_open_count": 0,
        "lustre_close_count": 0,
        "lustre_mmap_count": 0,
        "lustre_seek_count": 0,
    }
    lustre_base = "/proc/fs/lustre/llite"
    if not os.path.isdir(lustre_base):
        return result
    try:
        for mount_dir in Path(lustre_base).iterdir():
            stats_path = mount_dir / "stats"
            if not stats_path.exists():
                continue
            try:
                with open(stats_path) as fh:
                    for line in fh:
                        parts = line.split()
                        if not parts:
                            continue
                        key = parts[0]
                        # read_bytes and write_bytes lines have 7 fields:
                        # name count samples unit min max sum
                        if key == "read_bytes" and len(parts) >= 7:
                            result["lustre_read_bytes"] += int(parts[6])
                        elif key == "write_bytes" and len(parts) >= 7:
                            result["lustre_write_bytes"] += int(parts[6])
                        elif key == "open":
                            result["lustre_open_count"] += int(parts[1])
                        elif key == "close":
                            result["lustre_close_count"] += int(parts[1])
                        elif key == "mmap":
                            result["lustre_mmap_count"] += int(parts[1])
                        elif key == "seek":
                            result["lustre_seek_count"] += int(parts[1])
            except OSError:
                pass
    except OSError:
        pass
    return result


# ---------------------------------------------------------------------------
# Poll interval logic
# ---------------------------------------------------------------------------


def _poll_interval(elapsed_sec: float) -> int:
    if elapsed_sec < 600:
        return 10
    if elapsed_sec < 3600:
        return 30
    return 60


# ---------------------------------------------------------------------------
# Optional perf stat
# ---------------------------------------------------------------------------


def _check_perf_available() -> bool:
    try:
        paranoid_path = "/proc/sys/kernel/perf_event_paranoid"
        with open(paranoid_path) as fh:
            val = int(fh.read().strip())
        return val <= 1
    except OSError:
        return False


def _collect_perf(pid: int, duration_sec: int = 5) -> Dict[str, Optional[float]]:
    """Run perf stat on PID and parse CPI and cache-miss rate."""
    result: Dict[str, Optional[float]] = {"cpi": None, "cache_miss_rate": None}
    try:
        out = subprocess.run(
            ["perf", "stat", "-p", str(pid), "--", "sleep", str(duration_sec)],
            capture_output=True,
            text=True,
            timeout=duration_sec + 5,
        )
        text = out.stderr
        cpi_m = re.search(r"([\d.]+)\s+insns per cycle", text)
        if cpi_m:
            result["cpi"] = 1.0 / float(cpi_m.group(1))
        cm_m = re.search(r"([\d,.]+)\s+cache-misses", text)
        if cm_m:
            result["cache_miss_rate"] = float(cm_m.group(1).replace(",", ""))
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Main monitoring loop
# ---------------------------------------------------------------------------

FIELDNAMES = [
    "elapsed_sec",
    "rss_gb",
    "hwm_gb",
    "swap_gb",
    "threads",
    "utime",
    "stime",
    "majflt",
    "io_read_mb_s",
    "io_write_mb_s",
    "pgmajfault",
    "cpu_frac",
    "numa_miss_rate",
    "lustre_read_mb_s",
    "lustre_write_mb_s",
    "lustre_metadata_ops_s",
    "lustre_open_count",
]


def monitor(
    job_id: str,
    output_path: str,
    pids: Optional[List[int]] = None,
    max_duration_sec: int = 30 * 24 * 3600,
) -> None:
    """Run the monitoring loop; write output_path when done."""
    start_time = time.time()
    perf_available = _check_perf_available()
    perf_cpi_samples: List[float] = []
    perf_cache_miss_samples: List[float] = []

    if pids is None or not pids:
        pids = _find_job_pids(job_id)

    rows = []
    prev_io: Dict[int, Dict[str, int]] = {}
    prev_wall: float = start_time
    prev_utime_sum: int = 0
    prev_lustre: Dict[str, int] = {}

    try:
        while True:
            now = time.time()
            elapsed = now - start_time
            if elapsed > max_duration_sec:
                break

            # Aggregate across all tracked PIDs
            rss_kb = 0
            hwm_kb = 0
            swap_kb = 0
            threads = 0
            utime_sum = 0
            stime = 0
            majflt = 0
            read_bytes = 0
            write_bytes = 0

            for pid in list(pids):
                status = _read_proc_status(pid)
                stat = _read_proc_stat(pid)
                io = _read_proc_io(pid)

                rss_kb += status.get("rss_kb", 0)
                hwm_kb = max(hwm_kb, status.get("hwm_kb", 0))
                swap_kb += status.get("swap_kb", 0)
                threads += status.get("threads", 0)
                utime_sum += stat.get("utime", 0)
                stime += stat.get("stime", 0)
                majflt += stat.get("majflt", 0)
                read_bytes += io.get("read_bytes", 0)
                write_bytes += io.get("write_bytes", 0)

            vmstat = _read_vmstat()
            numa_miss = _read_numastat()
            lustre = _read_lustre_stats()

            wall_delta = now - prev_wall
            utime_delta = utime_sum - prev_utime_sum
            # utime is in clock ticks (typically 100/s)
            clk_tck = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
            cpu_frac = (utime_delta / clk_tck) / wall_delta if wall_delta > 0 else 0.0

            prev_io_total = sum(v.get("read_bytes", 0) for v in prev_io.values())
            prev_write_total = sum(v.get("write_bytes", 0) for v in prev_io.values())
            io_read_mb_s = (
                (read_bytes - prev_io_total) / 1024**2 / wall_delta if wall_delta > 0 else 0.0
            )
            io_write_mb_s = (
                (write_bytes - prev_write_total) / 1024**2 / wall_delta if wall_delta > 0 else 0.0
            )

            lustre_read_delta = lustre["lustre_read_bytes"] - prev_lustre.get(
                "lustre_read_bytes", lustre["lustre_read_bytes"]
            )
            lustre_write_delta = lustre["lustre_write_bytes"] - prev_lustre.get(
                "lustre_write_bytes", lustre["lustre_write_bytes"]
            )
            lustre_meta_delta = (
                (
                    lustre["lustre_open_count"]
                    - prev_lustre.get("lustre_open_count", lustre["lustre_open_count"])
                )
                + (
                    lustre["lustre_close_count"]
                    - prev_lustre.get("lustre_close_count", lustre["lustre_close_count"])
                )
                + (
                    lustre["lustre_mmap_count"]
                    - prev_lustre.get("lustre_mmap_count", lustre["lustre_mmap_count"])
                )
                + (
                    lustre["lustre_seek_count"]
                    - prev_lustre.get("lustre_seek_count", lustre["lustre_seek_count"])
                )
            )
            lustre_read_mb_s = (lustre_read_delta / 1024**2 / wall_delta) if wall_delta > 0 else 0.0
            lustre_write_mb_s = (
                (lustre_write_delta / 1024**2 / wall_delta) if wall_delta > 0 else 0.0
            )
            lustre_metadata_ops_s = (lustre_meta_delta / wall_delta) if wall_delta > 0 else 0.0

            prev_wall = now
            prev_utime_sum = utime_sum
            prev_io = {0: {"read_bytes": read_bytes, "write_bytes": write_bytes}}
            prev_lustre = lustre

            row = {
                "elapsed_sec": round(elapsed, 1),
                "rss_gb": round(rss_kb / 1024**2, 4),
                "hwm_gb": round(hwm_kb / 1024**2, 4),
                "swap_gb": round(swap_kb / 1024**2, 4),
                "threads": threads,
                "utime": utime_sum,
                "stime": stime,
                "majflt": majflt,
                "io_read_mb_s": round(max(io_read_mb_s, 0), 3),
                "io_write_mb_s": round(max(io_write_mb_s, 0), 3),
                "pgmajfault": vmstat.get("pgmajfault"),
                "cpu_frac": round(min(cpu_frac, float(max(len(pids), 1))), 4),
                "numa_miss_rate": round(numa_miss, 5),
                "lustre_read_mb_s": round(max(lustre_read_mb_s, 0), 3),
                "lustre_write_mb_s": round(max(lustre_write_mb_s, 0), 3),
                "lustre_metadata_ops_s": round(lustre_metadata_ops_s, 3),
                "lustre_open_count": lustre["lustre_open_count"],
            }
            rows.append(row)

            # Collect perf stats every 5 minutes (not every poll)
            if perf_available and elapsed > 60:
                poll_interval = _poll_interval(elapsed)
                if int(elapsed) % 300 < poll_interval:
                    for pid in pids[:1]:  # Sample first PID only
                        perf_data = _collect_perf(pid, duration_sec=3)
                        if perf_data.get("cpi") is not None:
                            perf_cpi_samples.append(perf_data["cpi"])
                        if perf_data.get("cache_miss_rate") is not None:
                            perf_cache_miss_samples.append(perf_data["cache_miss_rate"])

            interval = _poll_interval(elapsed)
            time.sleep(interval)

    except KeyboardInterrupt:
        pass
    finally:
        _write_output(output_path, rows)
        # Write perf summary if we collected any samples
        if perf_cpi_samples or perf_cache_miss_samples:
            perf_summary = {}
            if perf_cpi_samples:
                perf_summary["cpi"] = sum(perf_cpi_samples) / len(perf_cpi_samples)
            if perf_cache_miss_samples:
                perf_summary["cache_miss_rate"] = sum(perf_cache_miss_samples) / len(
                    perf_cache_miss_samples
                )
            perf_path = output_path.replace(".csv.gz", ".perf.json")
            try:
                import json as _json

                with open(perf_path, "w") as fh:
                    _json.dump(perf_summary, fh)
            except OSError:
                pass


def _write_output(output_path: str, rows: list) -> None:
    """Write collected rows to a gzip-compressed CSV."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with gzip.open(output_path, "wt", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <job_id> <output_template.csv.gz> [pid ...]", file=sys.stderr)
        sys.exit(1)
    _job_id = sys.argv[1]
    _output_template = sys.argv[2]

    # In multi-node mode, embed hostname in output filename
    import socket

    _num_nodes = int(os.environ.get("SLURM_JOB_NUM_NODES", "1"))
    if _num_nodes > 1:
        hostname = socket.gethostname().split(".")[0]
        # Convert /path/to/JOB_ID.csv.gz -> /path/to/JOB_ID.node_HOSTNAME.csv.gz
        _output = _output_template.replace(".csv.gz", f".node_{hostname}.csv.gz")
    else:
        _output = _output_template

    _pids = [int(p) for p in sys.argv[3:]] if len(sys.argv) > 3 else None
    monitor(_job_id, _output, _pids)
