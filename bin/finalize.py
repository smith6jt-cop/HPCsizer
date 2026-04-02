#!/usr/bin/env python3
"""Post-job collector for HPCsizer.

Called by the EXIT trap of the sidecar monitor after a job finishes.
Queries sacct, loads the compressed time-series, computes summary stats and
anomaly flags, and inserts everything into the profile DB.

Usage:
    python finalize.py <job_id> [--db DB_PATH] [--ts-dir TS_DIR]
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow importing lib from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.db import init_db, insert_job
from lib.flags import compute_efficiency, detect_flags
from lib.plotter import load_timeseries

_DEFAULT_DB = os.environ.get(
    "HPCSIZER_DB",
    str(Path(__file__).parent.parent / "profiles.db"),
)
_DEFAULT_TS_DIR = str(Path(__file__).parent.parent / "timeseries")


# ---------------------------------------------------------------------------
# sacct helpers
# ---------------------------------------------------------------------------

_SACCT_FIELDS = (
    "JobID,User,JobName,Account,QOS,State,Submit,Start,End,"
    "ReqMem,NCPUS,Timelimit,ReqTRES,"
    "MaxRSS,Elapsed,TotalCPU"
)


def _wait_for_sacct(job_id: str, max_wait: int = 120, poll: int = 10) -> bool:
    """Wait until sacct reports the job as completed."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        result = subprocess.run(
            ["sacct", "-j", job_id, "--noheader", "-o", "State"],
            capture_output=True,
            text=True,
        )
        if "COMPLETED" in result.stdout or "FAILED" in result.stdout or "TIMEOUT" in result.stdout:
            return True
        time.sleep(poll)
    return False


def _parse_mem_sacct(mem_str: str) -> Optional[float]:
    """Parse sacct memory strings like '128Gn', '4096Mc', etc."""
    if not mem_str or mem_str.strip() in ("", "0"):
        return None
    mem_str = mem_str.strip().rstrip("nc")
    m_val = mem_str[:-1] if mem_str and mem_str[-1] in "KMGTkmgt" else mem_str
    suffix = mem_str[-1].upper() if mem_str and mem_str[-1] in "KMGTkmgt" else "M"
    try:
        n = float(m_val)
    except ValueError:
        return None
    multipliers = {"K": 1 / 1024 / 1024, "M": 1 / 1024, "G": 1.0, "T": 1024.0}
    return n * multipliers.get(suffix, 1 / 1024)


def _parse_elapsed(t: str) -> int:
    """Parse D-HH:MM:SS or HH:MM:SS to seconds."""
    t = t.strip()
    import re

    m = re.match(r"(\d+)-(\d+):(\d+):(\d+)", t)
    if m:
        d, h, mi, s = (int(x) for x in m.groups())
        return d * 86400 + h * 3600 + mi * 60 + s
    m = re.match(r"(\d+):(\d+):(\d+)", t)
    if m:
        h, mi, s = (int(x) for x in m.groups())
        return h * 3600 + mi * 60 + s
    return 0


def _parse_gpus(tres_str: str) -> int:
    """Extract GPU count from TRES string like 'gres/gpu=1'."""
    import re

    m = re.search(r"gres/gpu(?::\w+)?=(\d+)", tres_str or "")
    return int(m.group(1)) if m else 0


def query_sacct(job_id: str) -> Optional[Dict[str, Any]]:
    """Query sacct and return a partial job dict."""
    result = subprocess.run(
        [
            "sacct",
            "-j",
            job_id,
            "--noheader",
            "--parsable2",
            "-o",
            _SACCT_FIELDS,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    lines = [l for l in result.stdout.splitlines() if l.strip() and not l.startswith("---")]
    if not lines:
        return None
    # Take the first (job-level, not step-level) record
    for line in lines:
        if "." not in line.split("|")[0]:
            parts = line.split("|")
            fields = _SACCT_FIELDS.split(",")
            data = dict(zip(fields, parts + [""] * max(0, len(fields) - len(parts))))
            break
    else:
        parts = lines[0].split("|")
        fields = _SACCT_FIELDS.split(",")
        data = dict(zip(fields, parts + [""] * max(0, len(fields) - len(parts))))

    elapsed_sec = _parse_elapsed(data.get("Elapsed", ""))
    cpu_time_str = data.get("TotalCPU", "")
    cpu_time_sec = _parse_elapsed(cpu_time_str) if cpu_time_str else None
    req_mem_gb = _parse_mem_sacct(data.get("ReqMem", ""))

    return {
        "job_id": job_id,
        "user": data.get("User", ""),
        "job_name": data.get("JobName", ""),
        "account": data.get("Account", ""),
        "qos": data.get("QOS", ""),
        "state": data.get("State", ""),
        "submit_time": data.get("Submit", ""),
        "start_time": data.get("Start", ""),
        "end_time": data.get("End", ""),
        "req_mem_gb": req_mem_gb,
        "req_cpus": int(data.get("NCPUS", 1) or 1),
        "req_gpus": _parse_gpus(data.get("ReqTRES", "")),
        "sacct_peak_rss_gb": _parse_mem_sacct(data.get("MaxRSS", "")),
        "sacct_elapsed_sec": elapsed_sec,
        "sacct_cpu_time_sec": cpu_time_sec,
    }


# ---------------------------------------------------------------------------
# Time-series summary
# ---------------------------------------------------------------------------


def _summarize_timeseries(
    rows: List[Dict[str, Any]],
) -> Dict[str, Optional[float]]:
    """Compute summary statistics from time-series rows."""
    rss = [r["rss_gb"] for r in rows if r.get("rss_gb") is not None]
    io_read = [r["io_read_mb_s"] for r in rows if r.get("io_read_mb_s") is not None]
    io_write = [r["io_write_mb_s"] for r in rows if r.get("io_write_mb_s") is not None]
    threads = [r["threads"] for r in rows if r.get("threads") is not None]
    numa = [r["numa_miss_rate"] for r in rows if r.get("numa_miss_rate") is not None]

    def _pct(vals, p):
        if not vals:
            return None
        idx = int(p / 100 * len(vals))
        return sorted(vals)[min(idx, len(vals) - 1)]

    # Lustre metrics
    lustre_r = [r["lustre_read_mb_s"] for r in rows if r.get("lustre_read_mb_s") is not None]
    lustre_w = [r["lustre_write_mb_s"] for r in rows if r.get("lustre_write_mb_s") is not None]
    lustre_meta = [
        r["lustre_metadata_ops_s"] for r in rows if r.get("lustre_metadata_ops_s") is not None
    ]

    # Compute total Lustre bytes by integrating rate * interval duration
    lustre_total_read_mb = 0.0
    lustre_total_write_mb = 0.0
    for i in range(1, len(rows)):
        dt = (rows[i].get("elapsed_sec") or 0) - (rows[i - 1].get("elapsed_sec") or 0)
        if dt > 0:
            lustre_total_read_mb += (rows[i].get("lustre_read_mb_s") or 0) * dt
            lustre_total_write_mb += (rows[i].get("lustre_write_mb_s") or 0) * dt

    return {
        "sidecar_peak_gb": max(rss) if rss else None,
        "sidecar_p95_gb": _pct(rss, 95),
        "sidecar_median_gb": _pct(rss, 50),
        "sidecar_peak_read_mb_s": max(io_read) if io_read else None,
        "sidecar_peak_write_mb_s": max(io_write) if io_write else None,
        "sidecar_avg_threads": sum(threads) / len(threads) if threads else None,
        "sidecar_numa_miss_rate": sum(numa) / len(numa) if numa else None,
        "lustre_peak_read_mb_s": max(lustre_r) if lustre_r else None,
        "lustre_peak_write_mb_s": max(lustre_w) if lustre_w else None,
        "lustre_avg_metadata_ops_s": sum(lustre_meta) / len(lustre_meta) if lustre_meta else None,
        "lustre_total_read_gb": lustre_total_read_mb / 1024 if lustre_r else None,
        "lustre_total_write_gb": lustre_total_write_mb / 1024 if lustre_w else None,
    }


def _load_multinode_timeseries(ts_dir: str, job_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load per-node time-series files for a multi-node job.

    Looks for files matching <job_id>.node_<hostname>.csv.gz.
    Returns a dict of {hostname: [rows]}.
    """
    import glob

    pattern = os.path.join(ts_dir, f"{job_id}.node_*.csv.gz")
    node_data: Dict[str, List[Dict[str, Any]]] = {}
    for path in sorted(glob.glob(pattern)):
        basename = os.path.basename(path)
        # Extract hostname from: JOB_ID.node_HOSTNAME.csv.gz
        parts = basename.replace(".csv.gz", "").split(".node_")
        if len(parts) == 2:
            hostname = parts[1]
            try:
                rows = load_timeseries(path)
                if rows:
                    node_data[hostname] = rows
            except Exception as exc:
                print(
                    f"[finalize] WARNING: could not load {path}: {exc}",
                    file=sys.stderr,
                )
    return node_data


def _compute_node_imbalance(
    node_data: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Optional[float]]:
    """Compute cross-node imbalance metrics from per-node time-series.

    Returns:
        node_imbalance_cv: coefficient of variation of mean CPU fraction
                           across nodes.  CV > 1.0 indicates severe imbalance.
    """
    if len(node_data) < 2:
        return {"node_imbalance_cv": None}

    node_mean_cpu: List[float] = []
    for _hostname, rows in node_data.items():
        cpu_vals = [r.get("cpu_frac") for r in rows if r.get("cpu_frac") is not None]
        if cpu_vals:
            node_mean_cpu.append(sum(cpu_vals) / len(cpu_vals))

    if len(node_mean_cpu) < 2:
        return {"node_imbalance_cv": None}

    mean_val = sum(node_mean_cpu) / len(node_mean_cpu)
    if mean_val == 0:
        return {"node_imbalance_cv": None}

    variance = sum((x - mean_val) ** 2 for x in node_mean_cpu) / len(node_mean_cpu)
    stddev = variance**0.5
    cv = stddev / mean_val

    return {"node_imbalance_cv": round(cv, 4)}


def _merge_multinode_timeseries(node_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Merge per-node time-series into a single aggregate series.

    RSS and IO are summed across nodes. CPU fraction is averaged.
    This merged series is used for flag detection and summary stats.
    """
    if not node_data:
        return []
    if len(node_data) == 1:
        return list(node_data.values())[0]

    # Use the node with the most data points as the time reference
    ref_hostname = max(node_data, key=lambda h: len(node_data[h]))
    ref_rows = node_data[ref_hostname]
    merged: List[Dict[str, Any]] = []

    for i, ref_row in enumerate(ref_rows):
        row: Dict[str, Any] = {"elapsed_sec": ref_row.get("elapsed_sec")}
        # Fields to sum across nodes
        for field in (
            "rss_gb",
            "swap_gb",
            "io_read_mb_s",
            "io_write_mb_s",
            "lustre_read_mb_s",
            "lustre_write_mb_s",
        ):
            total = 0.0
            count = 0
            for _hostname, rows in node_data.items():
                if i < len(rows) and rows[i].get(field) is not None:
                    total += rows[i][field]
                    count += 1
            row[field] = total if count > 0 else None

        # Fields to take max across nodes
        for field in ("hwm_gb",):
            vals = []
            for _hostname, rows in node_data.items():
                if i < len(rows) and rows[i].get(field) is not None:
                    vals.append(rows[i][field])
            row[field] = max(vals) if vals else None

        # Fields to average across nodes
        for field in ("cpu_frac", "numa_miss_rate", "lustre_metadata_ops_s"):
            vals = []
            for _hostname, rows in node_data.items():
                if i < len(rows) and rows[i].get(field) is not None:
                    vals.append(rows[i][field])
            row[field] = (sum(vals) / len(vals)) if vals else None

        # Fields to sum
        for field in ("threads", "utime", "stime", "majflt"):
            total_int = 0
            count = 0
            for _hostname, rows in node_data.items():
                if i < len(rows) and rows[i].get(field) is not None:
                    total_int += int(rows[i][field])
                    count += 1
            row[field] = total_int if count > 0 else None

        merged.append(row)

    return merged


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def finalize(
    job_id: str,
    db_path: str = _DEFAULT_DB,
    ts_dir: str = _DEFAULT_TS_DIR,
    static_context: Optional[Dict[str, Any]] = None,
) -> None:
    """Collect sacct data, load time-series, and insert into DB."""
    init_db(db_path)

    # Wait briefly for sacct to catch up
    _wait_for_sacct(job_id, max_wait=120)

    job = query_sacct(job_id)
    if job is None:
        print(f"[finalize] WARNING: sacct returned no data for job {job_id}", file=sys.stderr)
        job = {"job_id": job_id}

    # Load time-series — check for multi-node files first
    import glob

    multinode_pattern = os.path.join(ts_dir, f"{job_id}.node_*.csv.gz")
    multinode_files = sorted(glob.glob(multinode_pattern))

    timeseries: List[Dict[str, Any]] = []

    if multinode_files:
        # Multi-node job
        node_data = _load_multinode_timeseries(ts_dir, job_id)
        if node_data:
            job["has_sidecar"] = 1
            job["num_nodes"] = len(node_data)
            # Compute cross-node imbalance
            imbalance = _compute_node_imbalance(node_data)
            job.update(imbalance)
            # Merge into single aggregate series for downstream processing
            timeseries = _merge_multinode_timeseries(node_data)
    else:
        # Single-node job — original behavior
        ts_path = os.path.join(ts_dir, f"{job_id}.csv.gz")
        if os.path.exists(ts_path):
            try:
                timeseries = load_timeseries(ts_path)
                job["has_sidecar"] = 1
                job["num_nodes"] = 1
            except Exception as exc:
                print(
                    f"[finalize] WARNING: could not load timeseries: {exc}",
                    file=sys.stderr,
                )

    # Load perf summary if available
    perf_path = os.path.join(ts_dir, f"{job_id}.perf.json")
    if os.path.exists(perf_path):
        try:
            with open(perf_path) as fh:
                perf_data = json.load(fh)
            if "cpi" in perf_data:
                job["cpi"] = perf_data["cpi"]
            if "cache_miss_rate" in perf_data:
                job["cache_miss_rate"] = perf_data["cache_miss_rate"]
        except Exception as exc:
            print(f"[finalize] WARNING: could not load perf data: {exc}", file=sys.stderr)

    if timeseries:
        job.update(_summarize_timeseries(timeseries))

    # Merge in static context from pre-submit analysis
    if static_context:
        for key in ("static_tools", "input_files", "conda_env", "script_hash"):
            if key in static_context:
                job[key] = static_context[key]

    # Compute efficiency metrics and anomaly flags
    eff = compute_efficiency(job)
    job.update(eff)
    job["flags"] = detect_flags(job, timeseries or None)

    insert_job(job, db_path=db_path)
    print(f"[finalize] Job {job_id} stored. Flags: {job['flags']}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HPCsizer post-job collector")
    parser.add_argument("job_id", help="SLURM job ID")
    parser.add_argument("--db", default=_DEFAULT_DB, help="Path to profiles.db")
    parser.add_argument("--ts-dir", default=_DEFAULT_TS_DIR, help="Directory for time-series CSVs")
    parser.add_argument("--context", default=None, help="JSON file with static analyzer context")
    args = parser.parse_args()

    ctx = None
    if args.context and os.path.exists(args.context):
        with open(args.context) as fh:
            ctx = json.load(fh)

    finalize(args.job_id, db_path=args.db, ts_dir=args.ts_dir, static_context=ctx)
