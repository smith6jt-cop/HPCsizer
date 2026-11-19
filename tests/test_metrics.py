"""Tests for lib/metrics.py: derived values, measurement gating and flags."""

import pytest

from lib.config import load_config
from lib.metrics import describe, job_metrics, quantile

GIB = 1024**3


@pytest.fixture
def conf(tmp_path):
    return load_config({"HPCSIZER_ROOT": str(tmp_path)})


def job(**kwargs):
    base = {
        "state": "COMPLETED",
        "elapsed_sec": 3600,
        "req_mem_mb": 16 * 1024,
        "alloc_cpus": 4,
        "req_cpus": 4,
        "req_time_min": 120,
        "peak_mem_bytes": 8 * GIB,
        "cpu_sec": 2 * 3600.0,
        "cpu_src": "rusage",
        "alloc_gpus": 0,
        "oom": 0,
    }
    base.update(kwargs)
    return base


class TestQuantile:
    def test_nearest_rank_never_goes_below_an_observation(self):
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        assert quantile(values, 0.5) == 6
        assert quantile(values, 0.9) == 10
        assert quantile(values, 0.95) == 11
        assert quantile(values, 1.0) == 11
        assert quantile([5], 0.9) == 5

    def test_empty(self):
        assert quantile([], 0.9) is None and describe([None]) is None

    def test_describe(self):
        d = describe([4.0, None, 1.0, 3.0, 2.0])
        assert (d["n"], d["min"], d["p50"], d["max"]) == (4, 1.0, 2.0, 4.0)


class TestBasics:
    def test_requested_versus_used(self, conf):
        m = job_metrics(job(), conf)
        assert m["mem_req_gb"] == 16 and m["mem_used_gb"] == 8 and m["mem_eff"] == 0.5
        assert m["cores_used"] == 2 and m["cpu_eff"] == 0.5
        assert m["time_eff"] == 0.5
        assert m["mem_gbh_req"] == 16 and m["mem_gbh_unused"] == 8
        assert m["core_h_alloc"] == 4 and m["core_h_used"] == 2 and m["core_h_unused"] == 2
        assert m["mem_source"] == "sacct" and m["cpu_source"] == "rusage"
        assert m["flags"] == []

    def test_wait_time(self, conf):
        assert (
            job_metrics(job(submit_ts=100, eligible_ts=110, start_ts=170), conf)["wait_sec"] == 60
        )


class TestMeasurementGating:
    def test_job_shorter_than_two_samples_has_no_memory_measurement(self, conf):
        """Charging the whole request as waste for these inflated every total."""
        m = job_metrics(job(elapsed_sec=90, peak_mem_bytes=1024, cpu_sec=80.0), conf)
        assert m["mem_used_gb"] is None and m["mem_eff"] is None and m["mem_gbh_unused"] is None
        assert "unmeasured" in m["flags"] and "mem_overrequest" not in m["flags"]

    def test_zero_peak_is_not_a_measurement(self, conf):
        m = job_metrics(job(peak_mem_bytes=0), conf)
        assert m["mem_used_gb"] is None

    def test_killed_job_without_polled_cpu_has_no_cpu_measurement(self, conf):
        """TotalCPU is ~0 for a job Slurm killed, whatever it did."""
        m = job_metrics(job(state="TIMEOUT", cpu_sec=0.004, cpu_src="rusage"), conf)
        assert m["cores_used"] is None and m["cpu_eff"] is None
        assert "cpu_overrequest" not in m["flags"]

    def test_killed_job_with_polled_cpu_is_measured(self, conf):
        m = job_metrics(job(state="CANCELLED", cpu_sec=3 * 3600.0, cpu_src="polled"), conf)
        assert m["cores_used"] == 3 and m["cpu_source"] == "polled"

    def test_unknown_cpu_time_from_a_migrated_row(self, conf):
        m = job_metrics(job(cpu_sec=None, cpu_src=None), conf)
        assert m["cpu_eff"] is None and m["core_h_used"] is None

    def test_profile_values_take_precedence(self, conf):
        m = job_metrics(
            job(
                state="TIMEOUT",
                cpu_sec=0.004,
                peak_mem_bytes=0,
                mon_peak_mem_bytes=12 * GIB,
                mon_cpu_sec=3.5 * 3600,
                mon_max_cores=3.9,
            ),
            conf,
        )
        assert m["mem_used_gb"] == 12 and m["mem_source"] == "profile"
        assert m["cores_used"] == 3.5 and m["cpu_source"] == "profile" and m["cores_peak"] == 3.9

    def test_job_that_never_started(self, conf):
        m = job_metrics(job(elapsed_sec=0, peak_mem_bytes=None, cpu_sec=None, alloc_cpus=0), conf)
        assert m["flags"] == [] and m["mem_gbh_req"] == 0


class TestFlags:
    def test_memory_overrequest(self, conf):
        m = job_metrics(job(req_mem_mb=64 * 1024, peak_mem_bytes=4 * GIB), conf)
        assert "mem_overrequest" in m["flags"]

    def test_small_absolute_difference_is_not_flagged(self, conf):
        m = job_metrics(job(req_mem_mb=1024, peak_mem_bytes=GIB // 10), conf)
        assert "mem_overrequest" not in m["flags"]

    def test_cpu_overrequest(self, conf):
        m = job_metrics(job(alloc_cpus=16, cpu_sec=3600.0), conf)
        assert "cpu_overrequest" in m["flags"]
        assert "cpu_overrequest" not in job_metrics(job(alloc_cpus=1, cpu_sec=10.0), conf)["flags"]

    def test_time_overrequest_only_for_long_limits_that_completed(self, conf):
        assert "time_overrequest" in job_metrics(job(req_time_min=720), conf)["flags"]
        assert (
            "time_overrequest"
            not in job_metrics(job(req_time_min=30, elapsed_sec=300), conf)["flags"]
        )
        failed = job_metrics(job(req_time_min=720, state="FAILED"), conf)
        assert "time_overrequest" not in failed["flags"]

    def test_oom_and_timeout(self, conf):
        assert "oom" in job_metrics(job(state="OUT_OF_MEMORY", oom=1), conf)["flags"]
        assert "timeout" in job_metrics(job(state="TIMEOUT", cpu_src="polled"), conf)["flags"]

    def test_idle_gpu(self, conf):
        m = job_metrics(job(alloc_gpus=1, gpu_util_max=0.0), conf)
        assert "gpu_idle" in m["flags"] and m["gpu_h"] == 1 and m["gpu_h_idle"] == 1
        busy = job_metrics(job(alloc_gpus=1, gpu_util_max=38.0, gpu_mem_max_bytes=GIB), conf)
        assert "gpu_idle" not in busy["flags"] and busy["gpu_mem_gb"] == 1
        unknown = job_metrics(job(alloc_gpus=1, gpu_util_max=None), conf)
        assert "gpu_idle" not in unknown["flags"]

    def test_sessions_are_not_flagged_for_idling_or_ending_at_the_limit(self, conf):
        """An interactive session ends when its user stops or the limit cuts it off."""
        m = job_metrics(
            job(state="TIMEOUT", alloc_cpus=16, cpu_sec=600.0, cpu_src="polled", req_time_min=2880),
            conf,
            session=True,
        )
        assert "timeout" not in m["flags"] and "cpu_overrequest" not in m["flags"]
        assert "time_overrequest" not in m["flags"]

    def test_sessions_are_still_flagged_for_memory(self, conf):
        m = job_metrics(job(req_mem_mb=512 * 1024, peak_mem_bytes=34 * GIB), conf, session=True)
        assert "mem_overrequest" in m["flags"]


class TestProfileAgainstAccounting:
    def test_cgroup_profile_of_the_whole_run_is_believed(self, conf):
        """Accounting's sum over steps can only overestimate; the cgroup figure is the job."""
        m = job_metrics(
            job(
                peak_mem_bytes=30 * GIB,
                cpu_sec=5 * 3600.0,
                mon_source="cgroup-v2",
                mon_coverage=0.99,
                mon_peak_mem_bytes=12 * GIB,
                mon_cpu_sec=4 * 3600.0,
            ),
            conf,
        )
        assert m["mem_used_gb"] == 12 and m["mem_source"] == "profile"
        assert m["cores_used"] == 4 and m["cpu_source"] == "profile"

    def test_partial_or_process_tree_profile_is_only_a_lower_bound(self, conf):
        for extra in (
            {"mon_source": "cgroup-v2", "mon_coverage": 0.4},
            {"mon_source": "proc", "mon_coverage": 1.0},
        ):
            m = job_metrics(
                job(
                    peak_mem_bytes=30 * GIB,
                    cpu_sec=5 * 3600.0,
                    mon_peak_mem_bytes=12 * GIB,
                    mon_cpu_sec=4 * 3600.0,
                    **extra,
                ),
                conf,
            )
            assert m["mem_used_gb"] == 30 and m["mem_source"] == "sacct"
            assert m["cores_used"] == 5 and m["cpu_source"] == "rusage"

    def test_the_larger_of_two_lower_bounds(self, conf):
        m = job_metrics(
            job(
                peak_mem_bytes=8 * GIB,
                mon_source="proc",
                mon_peak_mem_bytes=12 * GIB,
                mon_cpu_sec=3600.0,
            ),
            conf,
        )
        assert m["mem_used_gb"] == 12 and m["mem_source"] == "profile"
        assert m["cores_used"] == 2 and m["cpu_source"] == "rusage"
