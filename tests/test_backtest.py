"""Tests for lib/backtest.py: replaying stored jobs against sizing policies."""

import pytest
from rows import DAY, GIB, NOW, run

from lib import backtest, history
from lib.config import load_config


@pytest.fixture
def conf(tmp_path):
    return load_config({"HPCSIZER_ROOT": str(tmp_path)})


def replay(conf, rows, **kwargs):
    result = backtest.backtest(history.annotate(list(rows), conf), conf, **kwargs)
    return result, {p["name"]: p for p in result["policies"]}


class TestWalkForward:
    def test_a_job_is_sized_only_from_runs_that_ended_before_it(self, conf):
        """Ten daily runs: the first three have nothing to go on, the other seven do."""
        result, by = replay(conf, [run(i) for i in range(10)])
        assert result["jobs"] == 10
        assert by["configured"]["mem"]["n"] == 10 and by["configured"]["mem"]["sized"] == 7
        assert by["as requested"]["mem"]["sized"] == 0

    def test_resource_hours(self, conf):
        """64 GB requested, 10 GB used, 12 GB suggested once there is history; 1 h runs."""
        _result, by = replay(conf, [run(i) for i in range(10)])
        asked = by["as requested"]["mem"]
        assert asked["gbh_requested"] == asked["gbh_suggested"] == 640
        assert asked["gbh_used"] == 100
        sized = by["configured"]["mem"]
        assert sized["gbh_suggested"] == 3 * 64 + 7 * 12
        assert sized["short"] == 0

    def test_runs_still_going_at_submission_are_not_history(self, conf):
        """A burst submitted together: none has finished when the others are sized."""
        rows = [run(i, age_days=5, submit_ts=NOW - 6 * DAY) for i in range(10)]
        _result, by = replay(conf, rows)
        assert by["configured"]["mem"]["sized"] == 0

    def test_jobs_that_never_ran_are_not_scored(self, conf):
        rows = [run(i) for i in range(4)]
        rows.append(run(9, state="CANCELLED", elapsed_sec=0, peak_mem_bytes=None, cpu_sec=None))
        result, _by = replay(conf, rows)
        assert result["jobs"] == 4 and result["stored"] == 5


class TestMemory:
    def test_a_job_that_needed_more_than_its_history_counts_as_short(self, conf):
        rows = [run(i) for i in range(4)] + [run(5, peak_mem_bytes=30 * GIB)]
        _result, by = replay(conf, rows)
        assert by["configured"]["mem"]["short"] == 1, "12 GB suggested, 30 GB used"
        assert by["as requested"]["mem"]["short"] == 0

    def test_a_job_that_ran_out_of_memory_is_short_as_requested(self, conf):
        rows = [run(i, req_mem_mb=8 * 1024, peak_mem_bytes=6 * GIB) for i in range(4)]
        rows.append(
            run(5, req_mem_mb=8 * 1024, state="OUT_OF_MEMORY", oom=1, peak_mem_bytes=8 * GIB)
        )
        _result, by = replay(conf, rows)
        assert by["as requested"]["mem"]["short"] == 1
        assert by["configured"]["mem"]["short"] == 1, "history suggested 8 GB again"

    def test_more_than_a_failed_request_is_unknown_not_a_success(self, conf):
        rows = [run(i, req_mem_mb=8 * 1024, peak_mem_bytes=6 * GIB) for i in range(4)]
        rows.append(
            run(5, req_mem_mb=8 * 1024, state="OUT_OF_MEMORY", oom=1, peak_mem_bytes=8 * GIB)
        )
        rows.append(
            run(6, req_mem_mb=8 * 1024, state="OUT_OF_MEMORY", oom=1, peak_mem_bytes=8 * GIB)
        )
        _result, by = replay(conf, rows)
        configured = by["configured"]["mem"]
        assert (configured["short"], configured["unknown"]) == (1, 1), (
            "after the first failure the suggestion is 12 GB: above the 8 GB that failed again"
        )
        assert by["as requested"]["mem"]["short"] == 2

    def test_unmeasured_jobs_are_left_out(self, conf):
        rows = [run(i, elapsed_sec=30, peak_mem_bytes=100, cpu_sec=10.0) for i in range(6)]
        _result, by = replay(conf, rows)
        assert by["configured"]["mem"]["n"] == 0


class TestCpusAndTime:
    def test_fewer_cores_than_a_job_kept_busy_counts_as_slowed(self, conf):
        rows = [run(i, cpu_sec=2 * 3600.0) for i in range(4)] + [run(5, cpu_sec=8 * 3600.0)]
        _result, by = replay(conf, rows)
        cpus = by["configured"]["cpus"]
        assert cpus["sized"] == 2 and cpus["slowed"] == 1  # 3 cores suggested, 8 were busy
        assert cpus["coreh_requested"] == 80 and cpus["coreh_suggested"] == 3 * 16 + 2 * 3
        assert by["as requested"]["cpus"]["slowed"] == 0

    def test_a_limit_shorter_than_the_run_counts_as_short(self, conf):
        rows = [
            run(i, req_cpus=4, alloc_cpus=4, cpu_sec=3.9 * 3600, elapsed_sec=1800) for i in range(4)
        ]
        rows.append(run(5, req_cpus=4, alloc_cpus=4, cpu_sec=3.9 * 3600, elapsed_sec=5 * 3600))
        _result, by = replay(conf, rows)
        time_ = by["configured"]["time"]
        assert time_["sized"] == 2 and time_["short"] == 1  # 45 minutes suggested, 5 hours needed
        assert time_["h_requested"] == 5 * 48

    def test_timeouts(self, conf):
        rows = [run(i, req_time_min=60, elapsed_sec=1200) for i in range(4)]
        rows.append(run(5, req_time_min=60, state="TIMEOUT", elapsed_sec=3600, cpu_src="polled"))
        rows.append(run(6, req_time_min=60, state="TIMEOUT", elapsed_sec=3600, cpu_src="polled"))
        _result, by = replay(conf, rows)
        assert by["as requested"]["time"]["short"] == 2
        configured = by["configured"]["time"]
        assert (configured["short"], configured["unknown"]) == (1, 1)

    def test_sessions_are_not_scored_on_time(self, conf):
        rows = [run(i, job_name="vscode", state="TIMEOUT", cpu_src="polled") for i in range(6)]
        _result, by = replay(conf, rows)
        assert by["configured"]["time"]["n"] == 0 and by["configured"]["mem"]["n"] == 6


class TestPolicies:
    def test_legacy_rule_needs_five_runs_and_uses_p90(self, conf):
        peaks = [10, 10, 10, 10, 10, 10, 10, 10, 10, 40, 10, 10]
        rows = [run(i, peak_mem_bytes=p * GIB) for i, p in enumerate(peaks)]
        _result, by = replay(conf, rows)
        legacy = by["legacy p90"]["mem"]
        assert legacy["sized"] == 7, "jobs 6 to 12 have five or more earlier runs"
        assert legacy["short"] == 1, "p90 x 1.10 = 11 GB when the 40 GB run came"
        # The job after the 40 GB run: p90 of ten peaks by index is the 40 GB one itself.
        assert by["configured"]["mem"]["short"] == 1

    def test_custom_settings(self, conf):
        custom = backtest.parse_settings(["mem_headroom=2", "min_runs=5"], conf)
        assert custom.mem_headroom == 2.0 and custom.min_runs == 5 and conf.min_runs == 3
        _result, by = replay(
            conf,
            [run(i) for i in range(10)],
            custom=custom,
            custom_label="mem_headroom=2, min_runs=5",
        )
        assert by["custom"]["mem"]["sized"] == 5
        assert by["custom"]["mem"]["gbh_suggested"] == 5 * 64 + 5 * 20
        assert by["custom"]["description"] == "mem_headroom=2, min_runs=5"

    @pytest.mark.parametrize(
        "bad",
        [
            "mem_headroom",
            "sacct=x",
            "mem_headroom=lots",
            "db_path=/x",
            "last_n=0",
            "min_runs=0",
            "mem_headroom=nan",
            "mem_headroom=0.5",
            "time_percentile=2",
        ],
    )
    def test_bad_settings(self, conf, bad):
        with pytest.raises(ValueError):
            backtest.parse_settings([bad], conf)

    def test_filters_choose_what_is_scored_not_what_is_history(self, conf):
        rows = [run(i) for i in range(10)]
        rows += [run(20 + i, user="bob", job_name="other") for i in range(5)]
        result, by = replay(conf, rows, since_ts=NOW - 53 * DAY - 3700, user="alice")
        assert result["jobs"] == 3 and result["stored"] == 15
        assert by["configured"]["mem"]["sized"] == 3, "older runs still serve as history"
        result, _by = replay(conf, rows, kind="vscode")
        assert result["jobs"] == 0


class TestText:
    def test_tables(self, conf):
        result, _by = replay(conf, [run(i) for i in range(10)])
        text = backtest.render_text(result)
        assert "Backtest of 10 jobs" in text
        assert "Memory -- 10 jobs" in text and "CPUs -- 10 jobs" in text and "Time limit" in text
        configured = next(ln for ln in text.splitlines() if ln.strip().startswith("configured"))
        assert "7 (70%)" in configured and "276" in configured and "57%" in configured
        assert "legacy p90" in text and "the rule before version 3" in text

    def test_nothing_to_score(self, conf):
        result, _by = replay(conf, [])
        assert result["jobs"] == 0 and "Backtest of 0 jobs" in backtest.render_text(result)
