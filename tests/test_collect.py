"""End-to-end tests of the collector against a stub sacct."""

import os
import re
import time

import pytest
from sacct_fixture import BASE_TS, job_row, simple_job, stream

from lib import collect, db
from lib.slurm import FIELDS, SEP

DAY = 86400
NOW = BASE_TS + 10 * DAY


def quiet(_msg):
    pass


def run(cfg, **kwargs):
    kwargs.setdefault("now", NOW)
    kwargs.setdefault("log", quiet)
    kwargs.setdefault("sleep", lambda _s: None)
    return collect.harvest(cfg, **kwargs)


def stored(cfg):
    with db.reader(cfg.db_path) as conn:
        return {j["job_id"]: j for j in db.fetch_jobs(conn)}


def since_of(call):
    stamp = re.search(r"-S (\S+)", call).group(1)
    import calendar

    return calendar.timegm(time.strptime(stamp, "%Y-%m-%dT%H:%M:%S"))


class TestQuery:
    def test_asks_for_every_user_of_the_account_in_utc_epochs(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        run(cfg)
        call = stubs.sacct_calls()[0]
        assert " -a " in call, "without -a only the caller's jobs are returned"
        assert "-A maigan" in call
        assert "--delimiter=" + SEP in call and " -p " in call
        assert set(stubs.sacct_env()) == {"TZ=UTC SLURM_TIME_FORMAT=%s"}

    def test_first_harvest_reads_the_full_retention_in_slices(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        stats = run(cfg)
        calls = stubs.sacct_calls()
        assert len(calls) == 7  # 182 days in 30-day slices
        assert since_of(calls[0]) == NOW - 182 * DAY
        assert all(" -E " in c for c in calls[:-1]) and " -E " not in calls[-1]
        assert stats["deep"] == 1

    def test_no_account_is_an_error_not_a_cluster_wide_query(self, cfg, stubs):
        cfg.accounts = []
        with pytest.raises(collect.HarvestError):
            run(cfg)
        assert stubs.sacct_calls() == []


class TestWhatIsStored:
    def test_every_final_state_and_every_user(self, cfg, stubs):
        rows = []
        states = [
            "COMPLETED",
            "FAILED",
            "TIMEOUT",
            "CANCELLED by 6084",
            "OUT_OF_MEMORY",
            "NODE_FAIL",
            "PREEMPTED",
        ]
        for i, state in enumerate(states):
            rows += simple_job(100 + i, state=state, user=["smith6jt", "bjones"][i % 2])
        stubs.sacct_default(rows)
        stats = run(cfg)
        jobs = stored(cfg)
        assert sorted(j["state"] for j in jobs.values()) == sorted(
            [
                "COMPLETED",
                "FAILED",
                "TIMEOUT",
                "CANCELLED",
                "OUT_OF_MEMORY",
                "NODE_FAIL",
                "PREEMPTED",
            ]
        )
        assert {j["user"] for j in jobs.values()} == {"smith6jt", "bjones"}
        assert stats["n_users"] == 2 and stats["n_inserted"] == 7
        assert jobs["103"]["state_raw"] == "CANCELLED by 6084"

    def test_values_are_parsed_not_just_counted(self, cfg, stubs):
        stubs.sacct_default(
            simple_job(
                200,
                cpus=4,
                mem="16G",
                elapsed=600,
                timelimit=120,
                batch_total_cpu="02:07.732",
                batch_max_rss="2097152K",
            )
        )
        run(cfg)
        job = stored(cfg)["200"]
        assert job["cpu_sec"] == pytest.approx(127.733)
        assert job["req_time_min"] == 120 and job["req_mem_mb"] == 16384
        assert job["peak_mem_bytes"] == (2097152 + 12288) * 1024
        with db.reader(cfg.db_path) as conn:
            assert len(db.fetch_steps(conn, job["id"])) == 2

    def test_own_infrastructure_jobs_are_counted_not_stored(self, cfg, stubs):
        stubs.sacct_default(
            simple_job(1)
            + simple_job(2, name="hpcsizer-harvest")
            + simple_job(3, name="hpcsizer-scheduler")
        )
        stats = run(cfg)
        assert set(stored(cfg)) == {"1"}
        assert stats["n_internal"] == 2 and stats["n_inserted"] == 1 and stats["n_records"] == 3

    def test_running_job_is_picked_up_once_it_ends(self, cfg, stubs):
        stubs.sacct_default([job_row(5, state="RUNNING", ended=False)])
        first = run(cfg)
        assert first["n_open"] == 1 and stored(cfg) == {}
        stubs.sacct_default(simple_job(5))
        second = run(cfg, now=NOW + 3600)
        assert second["n_inserted"] == 1 and stored(cfg)["5"]["state"] == "COMPLETED"

    def test_reused_job_id_is_kept_as_two_jobs(self, cfg, stubs):
        stubs.sacct_default(
            simple_job(77, submit=BASE_TS - 100 * DAY, name="old")
            + simple_job(77, submit=BASE_TS, name="new")
        )
        assert run(cfg)["n_inserted"] == 2
        with db.reader(cfg.db_path) as conn:
            assert [j["job_name"] for j in db.find_jobs(conn, "77")] == ["new", "old"]

    def test_harvest_run_is_recorded(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        run(cfg, trigger="schedule")
        with db.reader(cfg.db_path) as conn:
            last = db.last_harvest(conn)
            assert db.get_meta_int(conn, "watermark_ts") == NOW
        assert (
            last["trigger"] == "schedule" and last["n_inserted"] == 1 and last["started_ts"] == NOW
        )


class TestNothingIsLost:
    def test_resumes_from_the_last_success_minus_the_overlap(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        run(cfg)
        run(cfg, now=NOW + 3600)
        assert since_of(stubs.sacct_calls()[-1]) == NOW - 3 * DAY

    def test_an_outage_of_any_length_is_covered(self, cfg, stubs):
        """The previous harvester clamped the catch-up to 7 days."""
        stubs.sacct_default(simple_job(1))
        run(cfg)
        run(cfg, now=NOW + 60 * DAY)
        resumed = [c for c in stubs.sacct_calls()[7:]]
        assert since_of(resumed[0]) <= NOW - 3 * DAY
        assert len(resumed) >= 2, "a long catch-up is sliced"

    def test_completion_that_reaches_the_database_late_is_still_collected(self, cfg, stubs):
        """A job that ended before a harvest but was still shown as RUNNING by it."""
        late = simple_job(9, submit=NOW - 2 * 3600, elapsed=600)  # ended ~1.8 h before NOW
        stubs.sacct_default([job_row(9, state="RUNNING", ended=False)])
        run(cfg)
        stubs.sacct_default(late)
        run(cfg, now=NOW + 3600)
        assert since_of(stubs.sacct_calls()[-1]) < int(late[0]["End"])
        assert "9" in stored(cfg)

    def test_weekly_deep_pass_and_late_count(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        run(cfg)
        quick = run(cfg, now=NOW + DAY)
        assert quick["deep"] == 0
        old = simple_job(2, submit=NOW - 20 * DAY)  # ended long before the watermark
        stubs.sacct_default(simple_job(1) + old)
        deep = run(cfg, now=NOW + 8 * DAY)
        assert deep["deep"] == 1 and deep["n_late"] == 1
        assert since_of(stubs.sacct_calls()[-2]) == NOW + 8 * DAY - 35 * DAY

    def test_same_data_twice_changes_nothing(self, cfg, stubs):
        stubs.sacct_default(simple_job(1) + simple_job(2))
        first = run(cfg)
        second = run(cfg, now=NOW + 60)
        assert (first["n_inserted"], second["n_inserted"], second["n_updated"]) == (2, 0, 2)
        assert len(stored(cfg)) == 2

    def test_a_clock_that_ran_ahead_does_not_open_a_gap(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        run(cfg, now=NOW + 5 * DAY)
        run(cfg, now=NOW)
        assert since_of(stubs.sacct_calls()[-1]) == NOW - 3 * DAY


class TestFailures:
    def test_retries_then_succeeds(self, cfg, stubs):
        stubs.sacct_file("good", simple_job(1))
        stubs.sacct_plan("fail", "fail", "good")
        cfg.first_days = 1
        messages = []
        stats = run(cfg, log=messages.append)
        assert stats["n_inserted"] == 1
        assert sum("attempt" in m for m in messages) == 2

    def test_exhausted_retries_publish_nothing(self, cfg, stubs):
        stubs.sacct_plan("fail", "fail", "fail")
        cfg.first_days = 1
        with pytest.raises(collect.HarvestError):
            run(cfg)
        assert not os.path.exists(cfg.db_path)
        assert not os.path.exists(cfg.lock_path)

    def test_failure_leaves_data_and_watermark_untouched(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        run(cfg)
        before = open(cfg.db_path, "rb").read()
        stubs.sacct_plan("fail", "fail", "fail")
        with pytest.raises(collect.HarvestError):
            run(cfg, now=NOW + 3600)
        assert open(cfg.db_path, "rb").read() == before
        assert collect.watermark(cfg) == NOW

    def test_truncated_output_is_retried_and_never_stored(self, cfg, stubs):
        text = stream(simple_job(1) + simple_job(2))
        stubs.sacct_file("cut", text=text[: len(text) - 25])
        stubs.sacct_file("good", simple_job(1) + simple_job(2))
        cfg.first_days = 1
        stubs.sacct_plan("cut", "good")
        assert run(cfg)["n_inserted"] == 2

        stubs.sacct_plan("cut", "cut", "cut")
        with pytest.raises(collect.HarvestError) as err:
            run(cfg, now=NOW + 3600)
        assert "record" in str(err.value)
        assert collect.watermark(cfg) == NOW

    def test_one_bad_slice_fails_the_whole_catch_up(self, cfg, stubs):
        """Advancing the watermark past an unread slice would lose it for good."""
        stubs.sacct_file("good", simple_job(1))
        stubs.sacct_plan("good", "good", "fail", "fail", "fail")
        with pytest.raises(collect.HarvestError):
            run(cfg)
        assert not os.path.exists(cfg.db_path)

    def test_missing_sacct_is_reported_plainly(self, cfg, stubs):
        stubs.remove("sacct")
        cfg.sacct = "sacct-does-not-exist"
        with pytest.raises(collect.HarvestError) as err:
            run(cfg)
        assert "not found" in str(err.value)

    def test_unreadable_record_is_counted_and_the_rest_stored(self, cfg, stubs):
        bad = job_row(3)
        bad["Submit"] = "Unknown"
        stubs.sacct_default(simple_job(1) + [bad])
        stats = run(cfg)
        assert stats["n_inserted"] == 1 and stats["n_unparsed"] == 1


class TestParseSince:
    @pytest.mark.parametrize(
        "spec,expected",
        [
            ("182d", NOW - 182 * DAY),
            ("36h", NOW - 36 * 3600),
            ("90m", NOW - 5400),
            (str(BASE_TS), BASE_TS),
        ],
    )
    def test_relative_and_epoch(self, spec, expected):
        assert collect.parse_since(spec, NOW) == expected

    def test_date(self):
        assert collect.parse_since("2026-06-01", NOW) == int(
            time.mktime((2026, 6, 1, 0, 0, 0, 0, 0, -1))
        )

    def test_rejects_nonsense(self):
        with pytest.raises(ValueError):
            collect.parse_since("yesterday-ish", NOW)

    def test_since_backfills_in_slices(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        run(cfg, since_ts=NOW - 70 * DAY)
        assert len(stubs.sacct_calls()) == 3


class TestRefreshOnDemand:
    def test_refreshes_when_old_and_skips_when_fresh(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        cfg.first_days = 1
        assert collect.maybe_refresh(cfg, now=NOW, log=quiet) == "refreshed"
        assert collect.maybe_refresh(cfg, now=NOW + 60, log=quiet) == "fresh"
        assert collect.maybe_refresh(cfg, now=NOW + 3600, log=quiet) == "refreshed"

    def test_can_be_disabled(self, cfg, stubs, monkeypatch):
        assert collect.maybe_refresh(cfg, enabled=False, now=NOW) == "disabled"
        monkeypatch.setenv("HPCSIZER_NO_REFRESH", "1")
        assert collect.maybe_refresh(cfg, now=NOW) == "disabled"
        assert stubs.sacct_calls() == []

    def test_reader_without_write_access_just_reads(self, cfg, stubs, monkeypatch):
        monkeypatch.setattr(collect, "can_write", lambda _cfg: False)
        assert collect.maybe_refresh(cfg, now=NOW) == "read-only"
        assert stubs.sacct_calls() == []

    def test_a_running_harvest_is_not_waited_for(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        cfg.first_days = 1
        run(cfg)
        with db.SnapshotWriter(cfg.db_path):
            assert collect.maybe_refresh(cfg, now=NOW + 3600, log=quiet) == "busy"

    def test_a_failed_refresh_never_breaks_the_report(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        cfg.first_days = 1
        run(cfg)
        stubs.sacct_plan("fail", "fail", "fail")
        messages = []
        assert collect.maybe_refresh(cfg, now=NOW + 3600, log=messages.append) == "failed"
        assert "using stored data" in messages[0]

    def test_unavailable_without_sacct_or_on_a_v2_database(self, cfg, stubs):
        stubs.remove("sacct")
        assert collect.maybe_refresh(cfg, now=NOW) == "unavailable"


COVERAGE_FIELDS = ("JobIDRaw", "Submit", "End", "State", "JobName")


def coverage_stream(rows):
    return "".join(SEP.join(str(r[f]) for f in COVERAGE_FIELDS) + SEP + "\n" for r in rows)


class TestCoverage:
    def test_everything_sacct_has_is_stored(self, cfg, stubs):
        rows = simple_job(1, submit=NOW - DAY) + simple_job(2, submit=NOW - 2 * DAY)
        stubs.sacct_default(rows)
        cfg.first_days = 5
        run(cfg)
        job_lines = [r for r in rows if "." not in r["JobIDRaw"]]
        stubs.sacct_file("cov", text=coverage_stream(job_lines))
        stubs.sacct_plan("cov")
        result = collect.coverage(cfg, days=30, now=NOW)
        assert result == {
            "available": True,
            "days": 30,
            "expected": 2,
            "stored": 2,
            "missing": 0,
            "missing_examples": [],
        }
        assert " -X " in stubs.sacct_calls()[-1]

    def test_a_job_missing_from_the_database_is_reported(self, cfg, stubs):
        stubs.sacct_default(simple_job(1, submit=NOW - DAY))
        cfg.first_days = 5
        run(cfg)
        lines = [
            job_row(1, submit=NOW - DAY),
            job_row(2, submit=NOW - 2 * DAY, state="CANCELLED by 6084"),
            job_row(3, submit=NOW - DAY, name="hpcsizer-harvest"),  # never expected
            job_row(4, submit=NOW - 3600, state="RUNNING", ended=False),  # not finished
        ]
        stubs.sacct_file("cov", text=coverage_stream(lines))
        stubs.sacct_plan("cov")
        result = collect.coverage(cfg, days=30, now=NOW)
        assert (result["expected"], result["stored"], result["missing"]) == (2, 1, 1)
        assert result["missing_examples"] == [2]

    def test_unavailable_before_the_first_harvest(self, cfg):
        assert collect.coverage(cfg, now=NOW)["available"] is False


def test_field_list_matches_the_fixture():
    assert len(job_row(1)) == len(FIELDS)
