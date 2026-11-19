"""Stored job rows for tests of everything that reads the database."""

GIB = 1024**3
DAY = 86400
NOW = 1_794_000_000


def run(i, age_days=None, **kwargs):
    """A stored job row. Runs are one day apart by default, the highest ``i`` the most recent."""
    age = (60 - i) if age_days is None else age_days
    end = NOW - int(age * DAY)
    base = {
        "id": i,
        "job_id": str(5_000_000 + i),
        "job_id_raw": 5_000_000 + i,
        "user": "alice",
        "job_name": "mplx12k-spleen",
        "submit_line": "sbatch run_mplx.sh",
        "state": "COMPLETED",
        "elapsed_sec": 3600,
        "req_mem_mb": 64 * 1024,
        "req_cpus": 16,
        "alloc_cpus": 16,
        "req_time_min": 2880,
        "peak_mem_bytes": 10 * GIB,
        "cpu_sec": 3 * 3600.0,
        "cpu_src": "rusage",
        "req_gpus": 0,
        "alloc_gpus": 0,
        "oom": 0,
        "submit_ts": end - 3700,
        "end_ts": end,
    }
    base.update(kwargs)
    return base
