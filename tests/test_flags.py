"""Tests for lib/flags.py"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.flags import compute_efficiency, detect_flags


def _make_job(**kwargs):
    defaults = {
        "job_id": "123",
        "state": "COMPLETED",
        "req_mem_gb": 128.0,
        "req_cpus": 8,
        "sacct_peak_rss_gb": None,
        "sidecar_peak_gb": None,
        "sacct_elapsed_sec": 3600,
        "sacct_cpu_time_sec": 7200,
    }
    defaults.update(kwargs)
    return defaults


def _make_ts(n=100, cpu_frac=0.5, rss_gb=20.0, io=0.0, threads=4, numa=0.0):
    return [
        {
            "elapsed_sec": i * 30,
            "cpu_frac": cpu_frac,
            "rss_gb": rss_gb,
            "io_read_mb_s": io,
            "threads": threads,
            "numa_miss_rate": numa,
        }
        for i in range(n)
    ]


class TestDetectFlags:
    def test_oom_killed(self):
        job = _make_job(state="OUT_OF_MEMORY")
        assert "oom_killed" in detect_flags(job)

    def test_mem_overrequest(self):
        job = _make_job(sidecar_peak_gb=10.0, req_mem_gb=128.0)
        assert "mem_overrequest" in detect_flags(job)

    def test_no_mem_overrequest_when_efficient(self):
        job = _make_job(sidecar_peak_gb=96.0, req_mem_gb=128.0)
        flags = detect_flags(job)
        assert "mem_overrequest" not in flags

    def test_idle_cpu(self):
        job = _make_job(req_cpus=8)
        ts = _make_ts(n=100, cpu_frac=0.01)
        assert "idle_cpu" in detect_flags(job, ts)

    def test_no_idle_cpu_when_busy(self):
        job = _make_job(req_cpus=8)
        ts = _make_ts(n=100, cpu_frac=0.8)
        assert "idle_cpu" not in detect_flags(job, ts)

    def test_single_threaded_flag(self):
        # cpu_time < 1.5 * elapsed with >2 cpus
        job = _make_job(
            req_cpus=8,
            sacct_elapsed_sec=3600,
            sacct_cpu_time_sec=3600,  # only 1 core equivalent
        )
        ts = _make_ts(n=100, cpu_frac=0.5)
        flags = detect_flags(job, ts)
        assert "single_threaded" in flags

    def test_no_single_threaded_when_parallel(self):
        job = _make_job(
            req_cpus=8,
            sacct_elapsed_sec=3600,
            sacct_cpu_time_sec=28800,  # 8 cores used
        )
        ts = _make_ts(n=100, cpu_frac=0.8)
        flags = detect_flags(job, ts)
        assert "single_threaded" not in flags

    def test_numa_misplaced(self):
        job = _make_job()
        ts = _make_ts(n=100, numa=0.30)
        assert "numa_misplaced" in detect_flags(job, ts)

    def test_no_numa_when_ok(self):
        job = _make_job()
        ts = _make_ts(n=100, numa=0.05)
        assert "numa_misplaced" not in detect_flags(job, ts)

    def test_mem_spike_plateau(self):
        # First 10% of rows have high RSS, rest are low
        n = 100
        ts = []
        for i in range(n):
            rss = 80.0 if i < 10 else 5.0
            ts.append(
                {
                    "elapsed_sec": i * 30,
                    "cpu_frac": 0.5,
                    "rss_gb": rss,
                    "io_read_mb_s": 0,
                    "threads": 4,
                    "numa_miss_rate": 0.0,
                }
            )
        job = _make_job()
        flags = detect_flags(job, ts)
        assert "mem_spike_plateau" in flags

    def test_catastrophe_flag(self):
        # First half busy, second half idle
        n = 100
        ts = []
        for i in range(n):
            cpu = 0.8 if i < 50 else 0.01
            ts.append(
                {
                    "elapsed_sec": i * 30,
                    "cpu_frac": cpu,
                    "rss_gb": 20.0,
                    "io_read_mb_s": 0,
                    "threads": 4,
                    "numa_miss_rate": 0.0,
                }
            )
        job = _make_job()
        flags = detect_flags(job, ts)
        assert "catastrophe" in flags

    def test_no_flags_for_clean_job(self):
        job = _make_job(sidecar_peak_gb=96.0, req_mem_gb=128.0)
        ts = _make_ts(n=100, cpu_frac=0.8)
        flags = detect_flags(job, ts)
        assert "mem_overrequest" not in flags
        assert "idle_cpu" not in flags
        assert "oom_killed" not in flags

    # --- Lustre flags ---

    def test_lustre_metadata_heavy(self):
        job = _make_job()
        ts = [
            {
                "elapsed_sec": i * 30,
                "cpu_frac": 0.5,
                "rss_gb": 20.0,
                "io_read_mb_s": 0,
                "threads": 4,
                "numa_miss_rate": 0.0,
                "lustre_metadata_ops_s": 200.0,
                "lustre_read_mb_s": 0.0,
            }
            for i in range(100)
        ]
        assert "lustre_metadata_heavy" in detect_flags(job, ts)

    def test_no_lustre_metadata_heavy_when_low(self):
        job = _make_job()
        ts = [
            {
                "elapsed_sec": i * 30,
                "cpu_frac": 0.5,
                "rss_gb": 20.0,
                "io_read_mb_s": 0,
                "threads": 4,
                "numa_miss_rate": 0.0,
                "lustre_metadata_ops_s": 10.0,
                "lustre_read_mb_s": 0.0,
            }
            for i in range(100)
        ]
        assert "lustre_metadata_heavy" not in detect_flags(job, ts)

    def test_lustre_io_dominant(self):
        job = _make_job()
        ts = [
            {
                "elapsed_sec": i * 30,
                "cpu_frac": 0.02,
                "rss_gb": 20.0,
                "io_read_mb_s": 0,
                "threads": 4,
                "numa_miss_rate": 0.0,
                "lustre_metadata_ops_s": 0.0,
                "lustre_read_mb_s": 100.0,
            }
            for i in range(100)
        ]
        assert "lustre_io_dominant" in detect_flags(job, ts)

    # --- CPI / Cache flags ---

    def test_high_cpi(self):
        job = _make_job(cpi=1.5)
        assert "high_cpi" in detect_flags(job)

    def test_no_high_cpi_when_efficient(self):
        job = _make_job(cpi=0.6)
        assert "high_cpi" not in detect_flags(job)

    def test_no_high_cpi_when_none(self):
        job = _make_job()
        assert "high_cpi" not in detect_flags(job)

    def test_cache_thrashing(self):
        job = _make_job(cache_miss_rate=0.8)
        assert "cache_thrashing" in detect_flags(job)

    def test_no_cache_thrashing_when_low(self):
        job = _make_job(cache_miss_rate=0.1)
        assert "cache_thrashing" not in detect_flags(job)

    # --- Multi-node flags ---

    def test_node_imbalance(self):
        job = _make_job(num_nodes=4, node_imbalance_cv=1.5)
        assert "node_imbalance" in detect_flags(job)

    def test_no_node_imbalance_single_node(self):
        job = _make_job(num_nodes=1, node_imbalance_cv=2.0)
        assert "node_imbalance" not in detect_flags(job)

    def test_no_node_imbalance_when_balanced(self):
        job = _make_job(num_nodes=4, node_imbalance_cv=0.3)
        assert "node_imbalance" not in detect_flags(job)

    def test_idle_nodes(self):
        # 4 nodes, 8 total CPUs (2 per node), but CPU time < single-node capacity
        job = _make_job(
            num_nodes=4,
            req_cpus=8,
            sacct_elapsed_sec=3600,
            sacct_cpu_time_sec=1000,  # far below single-node capacity of 3600*2=7200
        )
        assert "idle_nodes" in detect_flags(job)

    def test_no_idle_nodes_when_using_all(self):
        job = _make_job(
            num_nodes=4,
            req_cpus=8,
            sacct_elapsed_sec=3600,
            sacct_cpu_time_sec=100000,  # well above single-node capacity
        )
        assert "idle_nodes" not in detect_flags(job)


class TestComputeEfficiency:
    def test_mem_efficiency(self):
        job = _make_job(sidecar_peak_gb=32.0, req_mem_gb=128.0)
        eff = compute_efficiency(job)
        assert eff["mem_efficiency"] == pytest.approx(0.25)

    def test_cpu_efficiency(self):
        job = _make_job(
            req_cpus=8,
            sacct_elapsed_sec=3600,
            sacct_cpu_time_sec=14400,  # 4 cores used
        )
        eff = compute_efficiency(job)
        assert eff["cpu_efficiency"] == pytest.approx(0.5)

    def test_waste_gb(self):
        job = _make_job(sidecar_peak_gb=20.0, req_mem_gb=128.0)
        eff = compute_efficiency(job)
        assert eff["waste_gb"] == pytest.approx(108.0)

    def test_zero_waste_when_efficient(self):
        job = _make_job(sidecar_peak_gb=120.0, req_mem_gb=128.0)
        eff = compute_efficiency(job)
        assert eff["waste_gb"] >= 0.0

    def test_none_when_missing_data(self):
        job = _make_job()
        eff = compute_efficiency(job)
        assert eff["mem_efficiency"] is None
