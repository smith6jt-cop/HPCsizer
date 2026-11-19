"""Tests for lib/recommender.py and lib/history.py: evidence selection and sizing rules."""

import pytest
from rows import DAY, GIB, NOW, run

from lib import history
from lib import recommender as R
from lib.analyzer import Request, request_from_options
from lib.config import load_config


@pytest.fixture
def conf(tmp_path):
    return load_config({"HPCSIZER_ROOT": str(tmp_path)})


def hist(conf, rows):
    return history.History(history.annotate(list(rows), conf))


def named(conf, name="mplx12k-spleen", user="alice", **options):
    """A target by job name; ``options`` are sbatch options (``mem="64G"``)."""
    request = request_from_options({k.replace("_", "-"): str(v) for k, v in options.items()})
    return R.target_from_name(name, user, conf, request)


def submitted(conf, name, submit_line, user="alice", gpus=0, **options):
    request = request_from_options({k.replace("_", "-"): str(v) for k, v in options.items()})
    cls = R._classified(user, name, submit_line, gpus, conf)
    return R.Target(cls=cls, request=request, label=name, explicit=True)


def rec_for(conf, rows, target=None, **kwargs):
    return R.recommend(target or named(conf), hist(conf, rows), conf, NOW, **kwargs)


class TestSettingsThatMakeNoSense:
    """The configuration refuses these; the sizing code must not crash on them either."""

    @pytest.mark.parametrize("setting", [{"last_n": 0}, {"min_runs": 0}, {"min_runs_pooled": 0}])
    def test_no_runs_is_no_evidence(self, conf, setting):
        for key, value in setting.items():
            setattr(conf, key, value)
        rec = rec_for(conf, [run(i) for i in range(5)])
        assert rec["source"] in (R.SOURCE_NONE, R.SOURCE_HISTORY)
        assert rec_for(conf, [], named(conf, name="never-ran"))["source"] == R.SOURCE_NONE


class TestEvidenceLevels:
    def test_exact_name_is_preferred(self, conf):
        rows = [run(i) for i in range(3)]
        rows += [run(10 + i, job_name="mplx30k-spleen", peak_mem_bytes=40 * GIB) for i in range(5)]
        rec = rec_for(conf, rows)
        assert rec["level"] == "exact" and rec["evidence"]["n"] == 3
        assert rec["mem"]["used"]["max"] == 10

    def test_falls_back_to_the_name_apart_from_numbers(self, conf):
        rows = [run(i) for i in range(2)]
        rows += [run(10 + i, job_name="mplx30k-spleen") for i in range(5)]
        rec = rec_for(conf, rows)
        assert rec["level"] == "family" and rec["evidence"]["n"] == 7
        assert "named like mplx#k-spleen" in rec["evidence"]["description"]

    def test_falls_back_to_the_same_script_under_another_name(self, conf):
        rows = [
            run(i, job_name="sampleA", submit_line="sbatch -J sampleA pipeline.sh")
            for i in range(4)
        ]
        target = submitted(conf, "donor-b", "sbatch -J donor-b pipeline.sh")
        rec = rec_for(conf, rows, target)
        assert rec["level"] == "signature" and rec["evidence"]["n"] == 4
        assert [t["level"] for t in rec["tried"]] == ["exact", "family", "signature"]

    def test_wrap_jobs_are_matched_by_program_not_by_the_name_wrap(self, conf):
        """Every --wrap job is named 'wrap'; matching on the name would pool unrelated work."""
        rows = [
            run(i, job_name="wrap", submit_line="sbatch --wrap python seg.py %d" % i)
            for i in range(4)
        ]
        rows += [
            run(
                10 + i,
                job_name="wrap",
                submit_line="sbatch --wrap python archive.py",
                peak_mem_bytes=50 * GIB,
            )
            for i in range(6)
        ]
        target = submitted(conf, "wrap", "sbatch --wrap python seg.py 99")
        rec = rec_for(conf, rows, target)
        assert rec["evidence"]["n"] == 4 and rec["mem"]["used"]["max"] == 10
        assert [t["level"] for t in rec["tried"]][0] == "family", (
            "no 'exact' level for a generic name"
        )

    def test_other_users_batch_jobs_are_never_used(self, conf):
        rows = [run(i, user="bob") for i in range(8)]
        rec = rec_for(conf, rows)
        assert rec["source"] == "none" and rec["level"] is None

    def test_cpu_and_gpu_variants_of_one_name_are_separate(self, conf):
        rows = [run(i, req_gpus=1, alloc_gpus=1, peak_mem_bytes=40 * GIB) for i in range(5)]
        assert rec_for(conf, rows)["source"] == "none"
        gpu = rec_for(conf, rows, named(conf, gpus=1))
        assert gpu["source"] == "history" and gpu["target"]["family"].endswith("+gpu")

    def test_runs_that_say_nothing_do_not_count(self, conf):
        rows = [run(i) for i in range(2)]
        rows.append(run(5, state="CANCELLED", elapsed_sec=0, peak_mem_bytes=None, cpu_sec=None))
        rows.append(run(6, state="FAILED", elapsed_sec=4, peak_mem_bytes=1000, cpu_sec=1.0))
        rec = rec_for(conf, rows)
        assert rec["source"] == "none"
        assert rec["tried"][0] == {"level": "exact", "found": 2, "needed": 3}

    def test_lookback_window(self, conf):
        rows = [run(i, age_days=200 + i) for i in range(5)]
        assert rec_for(conf, rows)["source"] == "none"
        conf.lookback_days = 365
        assert rec_for(conf, rows)["source"] == "history"

    def test_only_runs_that_ended_before_the_cutoff(self, conf):
        """Replay: a job is sized from what was known when it was submitted."""
        rows = [run(i, age_days=30 - i) for i in range(10)]
        rec = rec_for(conf, rows, before_ts=NOW - 25 * DAY)
        assert rec["evidence"]["n"] == 5

    def test_only_the_most_recent_runs_are_used(self, conf):
        rows = [
            run(i, age_days=59 - i, peak_mem_bytes=(50 if i < 5 else 10) * GIB) for i in range(30)
        ]
        rec = rec_for(conf, rows)
        assert rec["evidence"]["n"] == 20 and rec["evidence"]["available"] == 30
        assert rec["mem"]["used"]["max"] == 10, "the old 50 GB runs have aged out of the last 20"

    def test_sessions_use_own_history_first(self, conf):
        ood = "sys/dashboard/sys/rstudio"
        rows = [run(i, job_name=ood, state="TIMEOUT", cpu_src="polled") for i in range(3)]
        rows += [run(10 + i, user="bob", job_name=ood, peak_mem_bytes=90 * GIB) for i in range(9)]
        rec = rec_for(conf, rows, R.target_from_kind("ood:rstudio", "alice", conf))
        assert rec["level"] == "family" and rec["evidence"]["n"] == 3

    def test_sessions_fall_back_to_the_kind_across_users(self, conf):
        ood = "sys/dashboard/sys/rstudio"
        rows = [run(i, user="bob", job_name=ood) for i in range(4)]
        target = R.target_from_kind("ood:rstudio", "alice", conf)
        assert rec_for(conf, rows, target)["source"] == "none", "four is below min_runs_pooled"
        rows.append(run(9, user="carol", job_name=ood))
        rec = rec_for(conf, rows, target)
        assert rec["level"] == "kind" and rec["confidence"]["level"] == "low"
        assert "sessions of all users" in rec["evidence"]["description"]


class TestMemory:
    def test_largest_peak_times_headroom_rounded_up(self, conf):
        rows = [run(i, peak_mem_bytes=int((8 + i * 0.5) * GIB)) for i in range(9)]  # up to 12.0
        mem = rec_for(conf, rows, named(conf, mem="64G"))["mem"]
        assert mem["suggested"] == 15 * 1024  # 12.0 x 1.2 = 14.4 -> 15 GB
        assert mem["action"] == "lower" and mem["change"] == pytest.approx(15 / 64 - 1)
        assert mem["used"]["n"] == 9 and "largest peak 12 GB (job 5000008) x 1.2" in mem["basis"]

    def test_never_below_the_largest_peak_of_any_run(self, conf):
        rows = [run(i) for i in range(5)]
        rows.append(run(7, state="FAILED", peak_mem_bytes=30 * GIB))
        mem = rec_for(conf, rows, named(conf, mem="64G"))["mem"]
        assert mem["suggested"] == 30 * 1024 and "FAILED" in mem["basis"]

    def test_never_above_the_largest_request_that_completed(self, conf):
        rows = [run(i, req_mem_mb=16 * 1024, peak_mem_bytes=int(15.5 * GIB)) for i in range(5)]
        mem = rec_for(conf, rows, named(conf, mem="16G"))["mem"]
        assert mem["suggested"] == 16 * 1024 and mem["action"] == "keep"

    def test_out_of_memory_raises_above_that_request_and_names_the_job(self, conf):
        rows = [run(i, req_mem_mb=8 * 1024, peak_mem_bytes=6 * GIB) for i in range(5)]
        rows.append(
            run(9, req_mem_mb=8 * 1024, state="OUT_OF_MEMORY", oom=1, peak_mem_bytes=8 * GIB)
        )
        mem = rec_for(conf, rows, named(conf, mem="8G"))["mem"]
        assert mem["suggested"] == 12 * 1024 and mem["action"] == "raise"
        assert any("5000009 ran out of memory" in note for note in mem["notes"])

    def test_out_of_memory_alone_is_enough_for_a_figure(self, conf):
        rows = [
            run(
                i,
                req_mem_mb=8 * 1024,
                state="OUT_OF_MEMORY",
                oom=1,
                elapsed_sec=50,
                peak_mem_bytes=0,
            )
            for i in range(3)
        ]
        mem = rec_for(conf, rows, named(conf, mem="8G"))["mem"]
        assert mem["suggested"] == 12 * 1024 and "ran out of memory" in mem["basis"]

    def test_request_below_past_peaks_is_raised(self, conf):
        rows = [run(i, req_mem_mb=32 * 1024, peak_mem_bytes=20 * GIB) for i in range(5)]
        mem = rec_for(conf, rows, named(conf, mem="8G"))["mem"]
        assert mem["suggested"] == 24 * 1024 and mem["action"] == "raise"

    def test_runs_too_short_to_measure_give_no_figure(self, conf):
        rows = [run(i, elapsed_sec=40, peak_mem_bytes=2000, cpu_sec=30.0) for i in range(6)]
        rec = rec_for(conf, rows, named(conf, mem="64G"))
        assert rec["source"] == "history", "the runs completed, so they still say how long it takes"
        assert rec["mem"]["suggested"] is None and rec["mem"]["action"] is None
        assert any("not measured" in note for note in rec["mem"]["notes"])
        assert not any(opt.startswith("--mem") for opt in rec["options"])

    def test_request_close_to_the_suggestion_is_left_alone(self, conf):
        rows = [run(i, peak_mem_bytes=12 * GIB) for i in range(5)]
        mem = rec_for(conf, rows, named(conf, mem="16G"))["mem"]  # suggestion 15 GB, -6%
        assert mem["action"] == "keep" and mem["suggested"] == 16 * 1024

    def test_rounding_up_never_turns_enough_into_a_raise(self, conf):
        rows = [run(i, req_mem_mb=16000, peak_mem_bytes=int(15.5 * GIB)) for i in range(5)]
        mem = rec_for(conf, rows, named(conf, mem="16000"))["mem"]
        assert mem["action"] == "keep" and mem["suggested"] == 16000

    def test_small_jobs_get_a_floor_never_zero(self, conf):
        """Version 2 formatted 0.3 GB with %.0f and wrote --mem=0G: the whole node."""
        rows = [run(i, req_mem_mb=4096, peak_mem_bytes=20 * 1024**2) for i in range(5)]
        rec = rec_for(conf, rows, named(conf, mem="4G"))
        assert rec["mem"]["suggested"] == 512 and "--mem=512M" in rec["options"]

    def test_steps_below_two_gigabytes(self, conf):
        assert R.round_mem_mb(1100) == 1280 and R.round_mem_mb(2049) == 3072
        assert R.round_mem_mb(1) == 512 and R.round_mem_mb(15 * 1024) == 15 * 1024
        assert R.round_mem_mb(100, per_unit=True) == 128

    def test_profile_peak_is_used_when_there_is_one(self, conf):
        rows = [run(i, peak_mem_bytes=10 * GIB, mon_peak_mem_bytes=20 * GIB) for i in range(5)]
        mem = rec_for(conf, rows, named(conf, mem="64G"))["mem"]
        assert mem["suggested"] == 24 * 1024 and mem["profiled"] == 5

    def test_batch_runs_that_were_cancelled_only_set_a_floor(self, conf):
        rows = [run(i, state="CANCELLED", cpu_src="polled") for i in range(5)]
        mem = rec_for(conf, rows, named(conf, mem="64G"))["mem"]
        assert mem["suggested"] is None, "no run ended normally, so there is no basis"


class TestCpus:
    def test_lowered_to_the_highest_average_use_with_headroom(self, conf):
        rows = [run(i, cpu_sec=(2 + i * 0.25) * 3600.0) for i in range(5)]  # up to 3.0 cores
        cpus = rec_for(conf, rows, named(conf, cpus_per_task=16))["cpus"]
        assert cpus["suggested"] == 4 and cpus["action"] == "lower"  # 3.0 x 1.25 = 3.75
        assert cpus["change"] == pytest.approx(-0.75)

    def test_never_raised(self, conf):
        rows = [run(i, cpu_sec=6 * 3600.0) for i in range(5)]
        cpus = rec_for(conf, rows, named(conf, cpus_per_task=4))["cpus"]
        assert cpus["suggested"] == 4 and cpus["action"] == "keep"

    def test_a_run_that_used_every_core_blocks_a_cut(self, conf):
        """4 busy cores of 4 say the job can use at least 4 -- not that 16 are too many."""
        rows = [run(i, alloc_cpus=4, req_cpus=4, cpu_sec=3.9 * 3600.0) for i in range(5)]
        cpus = rec_for(conf, rows, named(conf, cpus_per_task=16))["cpus"]
        assert cpus["action"] == "keep" and cpus["suggested"] == 16
        assert any("kept all 4 of its cores busy" in note for note in cpus["notes"])

    def test_killed_runs_without_sampled_cpu_time_are_not_evidence(self, conf):
        rows = [run(i, state="TIMEOUT", cpu_sec=0.01, cpu_src="rusage") for i in range(5)]
        cpus = rec_for(conf, rows, named(conf, cpus_per_task=16))["cpus"]
        assert cpus["action"] == "keep" and cpus["n"] == 0

    def test_sessions_are_not_cut_on_their_average(self, conf):
        """An editor session idles between commands; 0.2 cores on average proves little."""
        rows = [run(i, job_name="vscode", cpu_sec=0.2 * 3600.0) for i in range(6)]
        target = R.target_from_kind("vscode", "alice", conf, Request(cpus_per_task=16))
        cpus = rec_for(conf, rows, target)["cpus"]
        assert cpus["action"] == "keep" and any("hpg submit" in note for note in cpus["notes"])

    def session_rows(self, **profile):
        return [
            run(
                i,
                job_name="vscode",
                cpu_sec=0.2 * 3600.0,
                mon_p95_cores=2.2,
                mon_max_cores=15.0,
                mon_n_samples=900,
                **profile,
            )
            for i in range(6)
        ]

    def test_sessions_are_cut_from_a_profile(self, conf):
        rows = self.session_rows(mon_source="cgroup-v2", mon_coverage=0.99)
        target = R.target_from_kind("vscode", "alice", conf, Request(cpus_per_task=16))
        rec = rec_for(conf, rows, target)
        assert rec["cpus"]["suggested"] == 3 and rec["cpus"]["peak"] == 15.0
        assert rec["evidence"]["profiled"] == 6

    @pytest.mark.parametrize(
        "profile",
        [
            {"mon_source": "proc", "mon_coverage": 1.0},  # the batch shell's tree, not srun's
            {"mon_source": "cgroup-v2", "mon_coverage": 0.5},  # half the run, or 1 of 2 nodes
            {},
        ],
    )
    def test_a_profile_that_saw_part_of_the_job_cuts_nothing(self, conf, profile):
        rows = self.session_rows(**profile)
        target = R.target_from_kind("vscode", "alice", conf, Request(cpus_per_task=16))
        cpus = rec_for(conf, rows, target)["cpus"]
        assert cpus["action"] == "keep" and cpus["suggested"] == 16

    def test_batch_job_whose_profile_missed_the_work_is_sized_from_accounting(self, conf):
        """Without a cgroup the sampler follows the batch shell and sees an idle srun."""
        rows = [
            run(i, cpu_sec=6 * 3600.0, mon_source="proc", mon_coverage=1.0, mon_p95_cores=0.01)
            for i in range(5)
        ]
        cpus = rec_for(conf, rows, named(conf, cpus_per_task=16))["cpus"]
        assert cpus["suggested"] == 8 and "average cores in use" in cpus["basis"]

    def test_sustained_cores_never_undercut_the_average(self, conf):
        rows = [
            run(i, cpu_sec=6 * 3600.0, mon_source="cgroup-v2", mon_coverage=1.0, mon_p95_cores=2.0)
            for i in range(5)
        ]
        cpus = rec_for(conf, rows, named(conf, cpus_per_task=16))["cpus"]
        assert cpus["suggested"] == 8, "6 cores on average x 1.25, not 2 x 1.25"

    def test_multi_task_jobs_are_not_resized(self, conf):
        rows = [run(i) for i in range(5)]
        rec = rec_for(conf, rows, named(conf, ntasks=4, cpus_per_task=4))
        assert rec["cpus"]["action"] == "keep" and rec["cpus"]["suggested"] == 16
        assert rec["can_apply"] is False and "more than one task or node" in rec["apply_blockers"]

    def test_script_without_a_cpu_option_asks_for_one(self, conf):
        rows = [run(i) for i in range(5)]
        rec = rec_for(conf, rows, submitted(conf, "mplx12k-spleen", "sbatch run_mplx.sh"))
        assert rec["cpus"]["requested"] == 1 and rec["cpus"]["requested_from"] == "default"
        assert rec["cpus"]["action"] == "keep"


class TestWhatASuggestionMayRestOn:
    """Findings of the review of the sizing rules."""

    def test_cpus_are_not_lowered_from_runs_that_crashed(self, conf):
        """Three start-ups that failed after two minutes kept one core busy: no evidence."""
        rows = [
            run(i, state="FAILED", elapsed_sec=120, cpu_sec=100.0, peak_mem_bytes=GIB)
            for i in range(4)
        ]
        cpus = rec_for(conf, rows, named(conf, cpus_per_task=16))["cpus"]
        assert cpus["action"] == "keep" and cpus["suggested"] == 16
        assert any("ended normally" in note for note in cpus["notes"])

    def test_a_job_that_ran_into_its_limit_still_counts_for_cpus(self, conf):
        rows = [run(i, state="TIMEOUT", cpu_src="polled") for i in range(4)]
        cpus = rec_for(conf, rows, named(conf, cpus_per_task=16))["cpus"]
        assert cpus["action"] == "lower" and cpus["suggested"] == 4

    def test_other_members_sessions_are_advice_never_applied(self, conf):
        rows = [
            run(i, user="alice", job_name="vscode", peak_mem_bytes=20 * GIB, state="CANCELLED")
            for i in range(6)
        ]
        target = R.target_from_kind(
            "vscode", "bob", conf, Request(mem_form="mem", mem_value_mb=256 * 1024)
        )
        rec = rec_for(conf, rows, target)
        assert rec["level"] == "kind" and rec["mem"]["action"] == "lower"
        assert rec["can_apply"] is False
        assert any("other members" in reason for reason in rec["apply_blockers"])

    def test_session_limit_is_not_lowered_to_the_limit_that_cut_every_session_off(self, conf):
        rows = [
            run(i, job_name="vscode", state="TIMEOUT", elapsed_sec=12 * 3600, req_time_min=720)
            for i in range(6)
        ]
        target = R.target_from_kind("vscode", "alice", conf, Request(time_min=1440))
        time_ = rec_for(conf, rows, target)["time"]
        assert time_["action"] == "keep" and time_["suggested"] == 1440
        assert any("ran until their limit" in note for note in time_["notes"])

    def test_session_limit_is_lowered_from_sessions_that_were_closed_earlier(self, conf):
        rows = [
            run(i, job_name="vscode", state="CANCELLED", elapsed_sec=3 * 3600, req_time_min=1440)
            for i in range(6)
        ]
        target = R.target_from_kind("vscode", "alice", conf, Request(time_min=1440))
        time_ = rec_for(conf, rows, target)["time"]
        assert time_["action"] == "lower" and time_["suggested"] == 270

    def test_time_is_not_shortened_from_runs_that_had_more_cores(self, conf):
        """The CPU cut was applied last time: the request now has 4 cores, the runs had 16."""
        rows = [run(i) for i in range(10)]
        rec = rec_for(conf, rows, named(conf, cpus_per_task=4, time="2-0"))
        assert rec["cpus"]["action"] == "keep"
        assert rec["time"]["action"] == "keep" and rec["time"]["suggested"] == 2880
        assert any("had more cores" in note for note in rec["time"]["notes"])

    def test_time_is_shortened_once_runs_with_the_same_cores_exist(self, conf):
        rows = [run(i, req_cpus=4, alloc_cpus=4) for i in range(10)]
        rec = rec_for(conf, rows, named(conf, cpus_per_task=4, time="2-0"))
        assert rec["time"]["action"] == "lower"

    def test_per_cpu_memory_is_not_rounded_far_above_the_request(self, conf):
        rows = [
            run(i, req_mem_mb=33600, peak_mem_bytes=int(27.5 * GIB), cpu_sec=15 * 3600.0)
            for i in range(5)
        ]
        rec = rec_for(conf, rows, named(conf, cpus_per_task=16, mem_per_cpu="2100M"))
        assert rec["mem"]["action"] == "keep" and rec["cpus"]["action"] == "keep"
        assert "--mem-per-cpu=2100M" in rec["options"], rec["options"]

    def test_lowered_per_cpu_memory_adds_up_to_about_the_suggested_total(self, conf):
        rows = [run(i, req_mem_mb=64 * 1024, peak_mem_bytes=27 * GIB) for i in range(5)]
        rec = rec_for(conf, rows, named(conf, cpus_per_task=16, mem_per_cpu="4G"))
        assert rec["cpus"]["action"] == "lower", "which pins the total"
        option = next(o for o in rec["options"] if o.startswith("--mem-per-cpu="))
        share_mb = R.sbatch_opts.parse_mem(option.split("=")[1])
        total_mb = share_mb * rec["cpus"]["suggested"]
        assert rec["mem"]["suggested"] <= total_mb <= rec["mem"]["suggested"] * 1.05

    def test_per_unit_rounding_steps(self):
        assert R.round_mem_mb(100, per_unit=True) == 128
        assert R.round_mem_mb(2062.5, per_unit=True) == 2176
        assert R.round_mem_mb(9000, per_unit=True) == 9088

    def test_value_the_submission_does_not_state_is_passed_on_when_applied(self, conf):
        """The last run asked for 64 GB on its command line; this submission states nothing,
        so sbatch would fall back to the cluster's default."""
        rows = [
            run(i, peak_mem_bytes=52 * GIB, elapsed_sec=40 * 3600, cpu_sec=15 * 40 * 3600.0)
            for i in range(5)
        ]
        target = submitted(conf, "mplx12k-spleen", "sbatch run_mplx.sh", cpus_per_task=16)
        rec = rec_for(conf, rows, target)
        assert rec["cpus"]["action"] == "keep"
        assert rec["mem"]["requested_from"] == "last run" and rec["mem"]["action"] == "keep"
        assert rec["changes"].get("mem") == "--mem=64G"
        assert rec["changes"].get("time") == "--time=2-00:00:00"

    def test_stored_multi_task_job_is_not_sized_as_one_task(self, conf):
        pending = run(
            99,
            state="PENDING",
            submit_line="sbatch -n 16 --mem=64G mpi_job.sh",
            job_name="mpi_job.sh",
            req_cpus=16,
            alloc_cpus=None,
            n_steps=0,
            n_tasks=None,
        )
        req = R.request_of_job(pending)
        assert req.ntasks == 16 and req.cpus_per_task == 1 and req.multi

    def pending(self, **kwargs):
        options = dict(
            state="PENDING",
            job_name="mpi_job.sh",
            submit_line="sbatch mpi_job.sh",
            alloc_cpus=None,
            n_steps=0,
            n_tasks=None,
        )
        options.update(kwargs)
        return run(99, **options)

    def test_stored_job_whose_task_layout_is_unknown(self, conf):
        """A pending job has no steps; 16 CPUs may be 16 tasks or one task with 16 threads,
        and the earlier runs of its family, which asked for 8, do not say."""
        rows = [
            run(i, job_name="mpi_job.sh", submit_line="sbatch mpi_job.sh", req_cpus=8, alloc_cpus=8)
            for i in range(5)
        ]
        target = R.target_from_job(history.annotate([self.pending()], conf)[0], conf)
        cpus = rec_for(conf, rows, target)["cpus"]
        assert cpus["action"] == "keep" and cpus["suggested"] == 16
        assert any("how its CPUs are divided into tasks" in note for note in cpus["notes"])

    def test_pending_job_borrows_the_layout_of_its_familys_last_run(self, conf):
        rows = [run(i, job_name="mpi_job.sh", submit_line="sbatch mpi_job.sh") for i in range(5)]
        target = R.target_from_job(history.annotate([self.pending()], conf)[0], conf)
        assert rec_for(conf, rows, target)["cpus"]["suggested"] == 4, (
            "one task of 16 CPUs, like them"
        )
        rows = [
            run(i, job_name="mpi_job.sh", submit_line="sbatch mpi_job.sh", n_tasks=16)
            for i in range(5)
        ]
        cpus = rec_for(conf, rows, target)["cpus"]
        assert cpus["action"] == "keep" and cpus["kept_because"] == "multi"


# Rows whose jobs had the two cores the time tests ask for: a limit is not
# shortened from runs that had more cores than the request.
TWO_CORES = {"req_cpus": 2, "alloc_cpus": 2}


class TestTime:
    def test_high_percentile_times_headroom(self, conf):
        rows = [run(i, elapsed_sec=1800 + 60 * i, **TWO_CORES) for i in range(10)]  # 30..39 minutes
        time_ = rec_for(conf, rows, named(conf, time="1-0", cpus_per_task=2))["time"]
        assert time_["suggested"] == 60  # 39 x 1.5 = 58.5 -> next 5 minutes
        assert time_["action"] == "lower"

    def test_at_least_the_longest_completed_run(self, conf):
        rows = [run(i, elapsed_sec=600, **TWO_CORES) for i in range(20)]
        rows.append(run(40, elapsed_sec=6000, **TWO_CORES))  # 100 min; p95 of the last 20 is 10
        time_ = rec_for(conf, rows, named(conf, time="1-0", cpus_per_task=2))["time"]
        assert time_["suggested"] == 120  # 100 x 1.1 = 110 -> next 15 minutes
        assert "longest of 20" in time_["basis"]

    def test_timeout_raises_above_that_limit_and_names_the_job(self, conf):
        rows = [run(i, req_time_min=60, elapsed_sec=1200) for i in range(4)]
        rows.append(run(9, req_time_min=60, state="TIMEOUT", elapsed_sec=3600, cpu_src="polled"))
        time_ = rec_for(conf, rows, named(conf, time="60", cpus_per_task=2))["time"]
        assert time_["suggested"] == 90 and time_["action"] == "raise"
        assert any("5000009 hit its 01:00:00 limit" in note for note in time_["notes"])

    def test_not_above_the_longest_limit_that_sufficed(self, conf):
        rows = [run(i, req_time_min=60, elapsed_sec=3300) for i in range(5)]  # 55 of 60 minutes
        time_ = rec_for(conf, rows, named(conf, time="60", cpus_per_task=2))["time"]
        assert time_["suggested"] == 60 and time_["action"] == "keep"

    def test_not_shortened_together_with_a_cpu_cut(self, conf):
        rows = [run(i) for i in range(5)]  # 1 h runs, 3 of 16 cores, 2-day limit
        rec = rec_for(conf, rows, named(conf, time="2-0", cpus_per_task=16))
        assert rec["cpus"]["action"] == "lower"
        assert rec["time"]["action"] == "keep" and rec["time"]["suggested"] == 2880
        assert any("not shortened together" in note for note in rec["time"]["notes"])
        assert "time" not in rec["changes"]

    def test_minimum(self, conf):
        rows = [run(i, elapsed_sec=130, req_time_min=60, **TWO_CORES) for i in range(5)]
        time_ = rec_for(conf, rows, named(conf, time="60", cpus_per_task=2))["time"]
        assert time_["suggested"] == conf.min_time_min

    def test_rounding_steps(self):
        assert R.round_time_min(58.5) == 60 and R.round_time_min(61) == 75
        assert R.round_time_min(361) == 420 and R.round_time_min(0.2, 15) == 15

    def test_for_sessions_a_timeout_is_a_normal_ending(self, conf):
        rows = [
            run(
                i,
                job_name="vscode",
                state="TIMEOUT",
                req_time_min=480,
                elapsed_sec=480 * 60,
                cpu_src="polled",
            )
            for i in range(6)
        ]
        target = R.target_from_kind("vscode", "alice", conf, Request(time_min=480))
        time_ = rec_for(conf, rows, target)["time"]
        assert time_["action"] == "keep" and time_["suggested"] == 480 and not time_["notes"]

    def test_sessions_can_be_shortened(self, conf):
        rows = [
            run(
                i,
                job_name="vscode",
                state="CANCELLED",
                req_time_min=2880,
                elapsed_sec=3 * 3600,
                cpu_src="polled",
            )
            for i in range(6)
        ]
        target = R.target_from_kind("vscode", "alice", conf, Request(time_min=2880))
        time_ = rec_for(conf, rows, target)["time"]
        assert time_["action"] == "lower" and time_["suggested"] == 270  # 180 x 1.5

    def test_unlimited_request_gets_a_finite_suggestion(self, conf):
        rows = [run(i) for i in range(5)]
        time_ = rec_for(conf, rows, named(conf, time="UNLIMITED", cpus_per_task=2))["time"]
        assert time_["requested"] is None and time_["action"] == "set" and time_["suggested"] == 90


class TestGpus:
    def gpu_run(self, i, util):
        return run(i, req_gpus=1, alloc_gpus=1, gpu_util_max=util)

    def test_kept_when_used(self, conf):
        rows = [self.gpu_run(i, 40.0 + i) for i in range(5)]
        gpus = rec_for(conf, rows, named(conf, gpus=1))["gpus"]
        assert gpus["action"] == "keep" and gpus["suggested"] == 1 and gpus["used"]["max"] == 44

    def test_dropped_when_no_run_used_it(self, conf):
        rows = [self.gpu_run(i, 0.0) for i in range(5)]
        gpus = rec_for(conf, rows, named(conf, gpus=1))["gpus"]
        assert gpus["action"] == "lower" and gpus["suggested"] == 0

    def test_kept_when_one_run_used_it(self, conf):
        rows = [self.gpu_run(i, 0.0) for i in range(5)] + [self.gpu_run(9, 70.0)]
        assert rec_for(conf, rows, named(conf, gpus=1))["gpus"]["action"] == "keep"

    def test_kept_when_use_was_not_recorded(self, conf):
        rows = [self.gpu_run(i, None) for i in range(5)]
        gpus = rec_for(conf, rows, named(conf, gpus=1))["gpus"]
        assert gpus["action"] == "keep" and gpus["n"] == 0


class TestOptions:
    def test_whole_request_in_the_order_people_write_it(self, conf):
        rows = [run(i) for i in range(5)]
        rec = rec_for(conf, rows, named(conf, mem="64G", cpus_per_task=16, time="2-0"))
        assert rec["options"] == ["--mem=12G", "--cpus-per-task=4", "--time=2-00:00:00"]
        assert rec["changes"] == {"mem": "--mem=12G", "cpus": "--cpus-per-task=4"}
        assert rec["can_apply"] is True and rec["apply_blockers"] == []

    def test_memory_per_cpu_form_is_kept_and_recomputed_for_the_new_cpu_count(self, conf):
        rows = [run(i) for i in range(5)]
        rec = rec_for(conf, rows, named(conf, mem_per_cpu="4G", cpus_per_task=16))
        assert rec["mem"]["requested"] == 64 * 1024 and rec["mem"]["suggested"] == 12 * 1024
        assert rec["changes"]["mem"] == "--mem-per-cpu=3G"  # 12 GB over 4 CPUs

    def test_total_memory_is_pinned_when_only_the_cpus_drop(self, conf):
        """With --mem-per-cpu, or a per-CPU cluster default, fewer CPUs would mean less memory."""
        rows = [run(i, elapsed_sec=3600, peak_mem_bytes=60 * GIB) for i in range(5)]
        rec = rec_for(conf, rows, named(conf, mem_per_cpu="4G", cpus_per_task=16))
        assert rec["mem"]["action"] == "keep" and rec["cpus"]["suggested"] == 4
        assert rec["changes"]["mem"] == "--mem-per-cpu=16G"

    def test_memory_from_the_last_run_is_spelled_out_when_cpus_drop(self, conf):
        rows = [run(i, peak_mem_bytes=60 * GIB) for i in range(5)]
        rec = rec_for(
            conf, rows, submitted(conf, "mplx12k-spleen", "sbatch run.sh", cpus_per_task=16)
        )
        assert rec["mem"]["requested_from"] == "last run" and rec["mem"]["action"] == "keep"
        assert rec["changes"] == {
            "cpus": "--cpus-per-task=4",
            "mem": "--mem=64G",
            "time": "--time=2-00:00:00",  # not stated either: passed on, or the default applies
        }

    def test_whole_node_memory_is_never_applied(self, conf):
        rows = [run(i) for i in range(5)]
        rec = rec_for(conf, rows, named(conf, mem="0", cpus_per_task=2))
        assert rec["mem"]["requested"] is None
        assert not rec["can_apply"] and "--mem=0 asks for the whole node" in rec["apply_blockers"]
        assert not any(opt.startswith("--mem") for opt in rec["options"])

    def test_heterogeneous_job(self, conf):
        rows = [run(i) for i in range(5)]
        rec = rec_for(conf, rows, named(conf, hetjob=""))
        assert "heterogeneous job" in rec["apply_blockers"] and not rec["can_apply"]

    def test_nothing_to_apply_when_the_request_already_fits(self, conf):
        rows = [
            run(i, req_mem_mb=12 * 1024, cpu_sec=3.5 * 3600.0, req_time_min=90) for i in range(5)
        ]
        rec = rec_for(conf, rows, named(conf, mem="12G", cpus_per_task=4, time="90"))
        assert rec["changes"] == {} and rec["can_apply"] is False and rec["apply_blockers"] == []


class TestBaseline:
    def test_without_a_request_the_last_run_is_the_reference(self, conf):
        rows = [run(i) for i in range(5)]
        rec = rec_for(conf, rows)
        for name, value in (("mem", 64 * 1024), ("cpus", 16), ("time", 2880)):
            assert rec[name]["requested"] == value and rec[name]["requested_from"] == "last run"
        assert rec["mem"]["action"] == "lower"

    def test_like_a_stored_job(self, conf):
        rows = history.annotate([run(i) for i in range(5)], conf)
        target = R.target_from_job(rows[2], conf)
        assert target.cls["family"] == "alice/batch/mplx#k-spleen"
        assert target.request.mem_mb == 64 * 1024 and target.request.cpus == 16
        rec = R.recommend(target, history.History(rows), conf, NOW)
        assert rec["mem"]["requested_from"] == "request" and rec["level"] == "exact"

    def test_like_a_stored_job_with_an_override(self, conf):
        rows = history.annotate([run(i) for i in range(5)], conf)
        target = R.target_from_job(rows[0], conf, request_from_options({"mem": "8G"}))
        assert target.request.mem_mb == 8 * 1024 and target.request.cpus == 16

    def test_request_of_a_multi_node_job(self, conf):
        job = run(1, alloc_nodes=2, n_tasks=8, req_cpus=32, req_mem_mb=200 * 1024)
        request = R.request_of_job(job)
        assert request.ntasks == 8 and request.cpus_per_task == 4 and request.nodes == 2
        assert request.mem_mb == 200 * 1024 and request.multi

    def test_kind_must_be_a_session_kind(self, conf):
        with pytest.raises(ValueError, match="not a session kind"):
            R.target_from_kind("batch", "alice", conf)
        assert R.target_from_kind("VSCode", "alice", conf).cls["family"] == "alice/vscode"
        gpu = R.target_from_kind("ood:jupyter", "alice", conf, Request(gpus=1))
        assert gpu.cls["family"] == "alice/ood:jupyter+gpu"


class TestConfidence:
    def test_grows_with_the_number_of_own_runs(self, conf):
        assert rec_for(conf, [run(i) for i in range(4)])["confidence"]["level"] == "medium"
        assert rec_for(conf, [run(i) for i in range(12)])["confidence"]["level"] == "high"

    def test_lower_when_memory_varies_widely(self, conf):
        rows = [run(i, peak_mem_bytes=(2 if i < 11 else 40) * GIB) for i in range(12)]
        conf_ = rec_for(conf, rows)["confidence"]
        assert conf_["level"] == "medium" and "varies 20-fold" in conf_["reason"]


def context_run(i, input_gb, peak_gb, tools='["scanpy"]', **kwargs):
    return run(
        i,
        user="bob",
        job_name="other%d" % i,
        peak_mem_bytes=int(peak_gb * GIB),
        ctx_input_gb=input_gb,
        ctx_tools_json=tools,
        **kwargs,
    )


class TestModel:
    def test_fit_line(self):
        fit = R.fit_line([(1, 5), (2, 7), (3, 9), (4, 11)])
        assert fit["slope"] == pytest.approx(2) and fit["intercept"] == pytest.approx(3)
        assert fit["r2"] == pytest.approx(1) and fit["max_residual"] == pytest.approx(0, abs=1e-9)
        assert (fit["x_min"], fit["x_max"], fit["n"]) == (1, 4, 4)

    def test_fit_needs_different_input_sizes(self):
        assert R.fit_line([(2, 5), (2, 7), (2, 9)]) is None
        assert R.fit_line([(1, 1)]) is None

    def test_used_when_the_job_has_no_history(self, conf):
        """Memory = 2 GB per GB of input + 3 GB, measured on someone else's scanpy jobs."""
        rows = [context_run(i, input_gb=i + 1, peak_gb=2 * (i + 1) + 3) for i in range(12)]
        target = named(conf, name="brand-new", mem="100G")
        target.tools, target.input_gb = ["scanpy", "numpy"], 10.0
        rec = rec_for(conf, rows, target)
        assert rec["source"] == "model" and rec["model"]["tool"] == "scanpy"
        assert rec["mem"]["suggested"] == 28 * 1024  # (2 x 10 + 3) x 1.2 = 27.6
        assert rec["confidence"]["level"] == "low"
        assert rec["can_apply"] is False, "a figure from other jobs is shown, not applied"
        assert rec["cpus"]["suggested"] is None and rec["time"]["suggested"] is None

    def test_the_largest_miss_above_the_line_is_added(self, conf):
        rows = [context_run(i, input_gb=i + 1, peak_gb=2 * (i + 1) + 3) for i in range(12)]
        rows.append(context_run(20, input_gb=6.5, peak_gb=26))  # 10 GB above the line
        target = named(conf, name="brand-new")
        target.tools, target.input_gb = ["scanpy"], 10.0
        rec = rec_for(conf, rows, target)
        fit = rec["model"]
        expected = (fit["slope"] * 10 + fit["intercept"] + fit["max_residual"]) * 1.2
        assert rec["mem"]["suggested"] == R.round_mem_mb(expected * 1024)
        assert rec["mem"]["suggested"] > 28 * 1024

    def test_runs_that_ran_out_of_memory_are_not_fitted(self, conf):
        rows = [context_run(i, input_gb=i + 1, peak_gb=2 * (i + 1) + 3) for i in range(9)]
        rows.append(context_run(15, 30, 4, state="OUT_OF_MEMORY", oom=1))
        target = named(conf, name="brand-new")
        target.tools, target.input_gb = ["scanpy"], 10.0
        assert rec_for(conf, rows, target)["source"] != "model", "9 usable points are too few"

    def test_poor_fit_is_not_used(self, conf):
        peaks = [5, 40, 8, 33, 6, 45, 9, 30, 7, 41, 5, 38]
        rows = [context_run(i, input_gb=i + 1, peak_gb=peaks[i]) for i in range(12)]
        target = named(conf, name="brand-new")
        target.tools, target.input_gb = ["scanpy"], 10.0
        rec = rec_for(conf, rows, target)
        assert rec["source"] == "cold-start"

    def test_outside_the_fitted_range_is_flagged(self, conf):
        rows = [context_run(i, input_gb=i + 1, peak_gb=2 * (i + 1) + 3) for i in range(12)]
        target = named(conf, name="brand-new")
        target.tools, target.input_gb = ["scanpy"], 100.0
        rec = rec_for(conf, rows, target)
        assert any("outside the range" in note for note in rec["notes"])

    def test_history_wins_over_the_model(self, conf):
        rows = [context_run(i, input_gb=i + 1, peak_gb=2 * (i + 1) + 3) for i in range(12)]
        rows += [run(30 + i) for i in range(4)]
        target = named(conf, mem="64G")
        target.tools, target.input_gb = ["scanpy"], 10.0
        assert rec_for(conf, rows, target)["source"] == "history"

    def test_larger_input_than_any_past_run_is_pointed_out(self, conf):
        rows = [run(i, ctx_input_gb=5.0) for i in range(4)]
        target = named(conf, mem="64G")
        target.input_gb = 50.0
        rec = rec_for(conf, rows, target)
        assert any("larger than in any of these runs" in note for note in rec["notes"])


class TestColdStartAndNone:
    def test_rule_of_thumb_is_shown_but_never_applied(self, conf):
        target = named(conf, name="brand-new", mem="8G", cpus_per_task=1)
        target.tools, target.input_gb = ["Seurat", "SCTransform"], 10.0
        rec = rec_for(conf, [], target)
        assert rec["source"] == "cold-start" and rec["confidence"]["level"] == "guess"
        assert rec["mem"]["suggested"] == 45 * 1024 and rec["options"] == ["--mem=45G"]
        assert rec["can_apply"] is False
        assert any("usually run with 4 CPUs" in note for note in rec["cpus"]["notes"])

    def test_no_figure_without_history_tools_or_inputs(self, conf):
        rec = rec_for(conf, [], named(conf, name="brand-new", mem="8G"))
        assert rec["source"] == "none" and rec["options"] == [] and rec["changes"] == {}
        assert rec["mem"]["suggested"] is None and rec["mem"]["requested"] == 8 * 1024
        assert any("no usable history" in note for note in rec["notes"])


class TestColdStartHeuristics:
    def test_seurat_memory(self):
        assert R._cold_start_mem(["Seurat"], total_input_gb=10.0) == pytest.approx(30.0)

    def test_sctransform_memory(self):
        assert R._cold_start_mem(["SCTransform"], total_input_gb=10.0) == pytest.approx(45.0)

    def test_most_demanding_tool_wins_not_alphabetical(self):
        """sorted() put FindMarkers (2.5x) ahead of SCTransform (4.5x); take the max."""
        tools = sorted(["Seurat", "SCTransform", "FindMarkers", "readRDS"])
        assert tools[0] == "FindMarkers"
        assert R._cold_start_mem(tools, total_input_gb=10.0) == pytest.approx(45.0)
        assert R._cold_start_cpus(tools) == 4

    def test_no_estimate_without_input_or_history(self):
        """A multiplier-driven tool with nothing to multiply must not invent a number."""
        assert R._cold_start_mem(["Seurat"], total_input_gb=0.0) is None
        assert R._cold_start_mem(["scanpy"], total_input_gb=0.0) is None

    def test_scimap_has_a_real_baseline(self):
        assert R._cold_start_mem(["scimap"], total_input_gb=0.0) == pytest.approx(16.0)

    def test_unknown_tool_without_input_declines_too(self):
        assert R._cold_start_mem([], total_input_gb=0.0) is None
        assert R._cold_start_mem(["totally_unknown"], total_input_gb=0.0) is None

    def test_input_independent_tools_still_answer_without_input(self):
        assert R._cold_start_mem(["cellranger"], total_input_gb=0.0) == pytest.approx(32.0)
        assert R._cold_start_mem(["STAR"], total_input_gb=0.0) == pytest.approx(32.0)
        assert R._cold_start_mem(["salmon"], total_input_gb=0.0) == pytest.approx(16.0)

    def test_generic_fallback(self):
        assert R._cold_start_mem([], total_input_gb=5.0) == pytest.approx(26.0)

    def test_qupath_per_core(self):
        assert R._cold_start_mem(["QuPath"], total_input_gb=0.0, req_cpus=8) == pytest.approx(16.0)

    def test_cpu_hints(self):
        assert R._cold_start_cpus(["Seurat"]) == 4 and R._cold_start_cpus(["cellranger"]) == 8
        assert R._cold_start_cpus(["unknown"], req_cpus=2) == 2


class TestText:
    def test_report_shows_requested_used_and_suggested(self, conf):
        rows = [run(i, peak_mem_bytes=int((8 + i * 0.5) * GIB)) for i in range(9)]
        rows.append(run(20, state="TIMEOUT", req_time_min=60, elapsed_sec=3600, cpu_src="polled"))
        text = R.render_text(
            rec_for(conf, rows, named(conf, mem="64G", cpus_per_task=16, time="2-0"))
        )
        assert "Family      alice/batch/mplx#k-spleen" in text
        assert "10 runs named mplx12k-spleen by alice" in text and "9 COMPLETED, 1 TIMEOUT" in text
        memory = next(line for line in text.splitlines() if line.startswith("Memory"))
        assert (
            "64 GB" in memory
            and "10 / 12 / 12 GB" in memory
            and "15 GB" in memory
            and "-77%" in memory
        )
        assert "#SBATCH --mem=15G" in text and "#SBATCH --cpus-per-task=4" in text
        assert "hit its 01:00:00 limit" in text and "Confidence  high" in text

    def test_report_without_data_has_no_request_block(self, conf):
        text = R.render_text(rec_for(conf, [], named(conf, name="brand-new", mem="8G")))
        assert "no comparable runs are stored" in text and "#SBATCH" not in text

    def test_megabyte_sized_jobs(self, conf):
        rows = [run(i, req_mem_mb=4096, peak_mem_bytes=300 * 1024**2) for i in range(5)]
        text = R.render_text(rec_for(conf, rows, named(conf, mem="4G")))
        assert "300 / 300 / 300 MB" in text and "512 MB" in text
