"""Tests for lib/slurm.py against the sacct output forms seen in production."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from sacct_fixture import BASE_TS, job_row, simple_job, step_row, stream  # noqa: E402

from lib.slurm import (  # noqa: E402
    FIELDS,
    SEP,
    SacctFormatError,
    TruncatedOutput,
    build_sacct_cmd,
    group_records,
    is_open,
    merge_job,
    parse_duration,
    parse_jobid,
    parse_mem,
    parse_state,
    parse_tres,
    parse_tres_usage,
    parse_ts,
    records_to_dicts,
    sacct_env,
    split_records,
    tres_gpus,
    tres_mem_bytes,
)

GIB = 1024**3


class TestParseDuration:
    @pytest.mark.parametrize(
        "text,seconds",
        [
            ("00:04.231", 4.231),  # TotalCPU below one hour: the form that was read as 0
            ("41:07.250", 2467.25),
            ("59:59.999", 3599.999),
            ("01:58:35", 7115),
            ("00:05:23", 323),
            ("2-02:29:43", 181783),
            ("1-02:03:04.500", 93784.5),
            ("1-02", 93600),  # D-HH and D-HH:MM are valid time limits
            ("1-02:30", 95400),
        ],
    )
    def test_every_form_sacct_prints(self, text, seconds):
        assert parse_duration(text) == pytest.approx(seconds)

    @pytest.mark.parametrize("text", ["", "UNLIMITED", "Partition_Limit", "INVALID", "abc", None])
    def test_unreadable_is_none_not_zero(self, text):
        assert parse_duration(text) is None


class TestParseMem:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("86984640K", 86984640 * 1024),
            ("20480K", 20480 * 1024),
            ("64G", 64 * GIB),
            ("4000M", 4000 * 1024**2),
            ("1T", 1024 * GIB),
            ("2.50G", int(2.5 * GIB)),
            ("1144847403527", 1144847403527),  # bare usage counters are bytes
            ("0", 0),
        ],
    )
    def test_sizes(self, text, expected):
        assert parse_mem(text) == expected

    def test_bare_number_uses_the_given_unit(self):
        assert parse_mem("512", default_unit="M") == 512 * 1024**2

    @pytest.mark.parametrize("text", ["64Gn", "64Gc"])
    def test_legacy_per_node_and_per_cpu_suffix_is_accepted(self, text):
        assert parse_mem(text) == 64 * GIB

    @pytest.mark.parametrize("text", ["", None, "lots", "G"])
    def test_unreadable_is_none(self, text):
        assert parse_mem(text) is None


class TestTres:
    def test_requested_tres(self):
        tres = parse_tres("billing=8,cpu=8,gres/gpu=2,gres/gpu:a100=2,mem=64G,node=1")
        assert tres["cpu"] == "8"
        assert tres_mem_bytes(tres) == 64 * GIB
        assert tres_gpus(tres) == (2, "a100")

    def test_no_gpu(self):
        assert tres_gpus(parse_tres("billing=1,cpu=1,mem=4000M,node=1")) == (0, None)

    def test_typed_gpu_without_generic_count(self):
        assert tres_gpus(parse_tres("cpu=4,gres/gpu:l4=1,mem=8G")) == (1, "l4")

    def test_empty(self):
        assert parse_tres("") == {}
        assert tres_mem_bytes({}) is None

    def test_usage_string_from_the_cluster(self):
        usage = parse_tres_usage(
            "cpu=02:58:45,fs/disk=1144847403527,gres/gpumem=1000M,gres/gpuutil=38,"
            "mem=86984640K,pages=101246,vmem=1772107760K"
        )
        assert usage["cpu_sec"] == 10725
        assert usage["disk_bytes"] == 1144847403527
        assert usage["gpu_mem_bytes"] == 1000 * 1024**2
        assert usage["gpu_util"] == 38
        assert usage["mem_bytes"] == 86984640 * 1024


class TestParseState:
    def test_cancelled_by_uid_is_cancelled(self):
        assert parse_state("CANCELLED by 6084") == ("CANCELLED", "CANCELLED by 6084", "6084")

    @pytest.mark.parametrize(
        "raw",
        ["COMPLETED", "FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL"],
    )
    def test_plain_states(self, raw):
        assert parse_state(raw) == (raw, raw, None)

    def test_empty(self):
        assert parse_state("")[0] == "UNKNOWN"


class TestParseJobId:
    def test_plain(self):
        ids = parse_jobid("123", "123")
        assert (ids["job_id_raw"], ids["job_id"], ids["step"]) == (123, "123", None)

    def test_array_task(self):
        ids = parse_jobid("500_7", "507")
        assert (ids["job_id_raw"], ids["job_id"]) == (507, "500_7")
        assert (ids["array_job_id"], ids["array_task_id"]) == (500, 7)

    def test_array_tasks_that_never_started(self):
        ids = parse_jobid("500_[5-9]", "500")
        assert ids["array_job_id"] == 500 and ids["array_task_id"] is None

    def test_heterogeneous_component(self):
        ids = parse_jobid("900+1", "901")
        assert ids["job_id_raw"] == 901 and ids["het_offset"] == 1

    @pytest.mark.parametrize("step", ["batch", "extern", "0", "interactive"])
    def test_steps(self, step):
        ids = parse_jobid(f"500_7.{step}", f"507.{step}")
        assert ids["step"] == step and ids["job_id"] == "500_7"


class TestParseTs:
    def test_epoch(self):
        assert parse_ts("1794000000") == 1794000000

    @pytest.mark.parametrize("text", ["Unknown", "None", "", "0", None])
    def test_unset(self, text):
        assert parse_ts(text) is None

    def test_iso_is_read_as_utc(self):
        assert parse_ts("2026-11-06T21:20:00") == 1794000000


class TestBuildCommand:
    def test_all_users_of_the_accounts(self):
        cmd = build_sacct_cmd(["maigan", "clive"], 1794000000)
        assert "-a" in cmd, "without -a sacct returns only the caller's jobs"
        assert cmd[cmd.index("-A") + 1] == "maigan,clive"
        assert cmd[cmd.index("-S") + 1] == "2026-11-06T21:20:00"
        assert "-p" in cmd and "-n" in cmd and "-X" not in cmd
        assert "--delimiter=" + SEP in cmd
        assert cmd[cmd.index("-o") + 1].split(",") == list(FIELDS)

    def test_end_time(self):
        cmd = build_sacct_cmd(["maigan"], 1794000000, 1794003600)
        assert cmd[cmd.index("-E") + 1] == "2026-11-06T22:20:00"

    def test_refuses_to_query_the_whole_cluster(self):
        with pytest.raises(ValueError):
            build_sacct_cmd([], 1794000000)

    def test_environment_requests_epochs_in_utc(self):
        env = sacct_env({"TZ": "America/New_York", "PATH": "/bin"})
        assert env["TZ"] == "UTC" and env["SLURM_TIME_FORMAT"] == "%s" and env["PATH"] == "/bin"


class TestSplitRecords:
    def test_round_trip(self):
        rows = simple_job(101)
        recs = split_records(stream(rows))
        assert len(recs) == 3 and all(len(r) == len(FIELDS) for r in recs)
        assert records_to_dicts(recs)[0]["JobIDRaw"] == "101"

    def test_empty_output(self):
        assert split_records("") == []
        assert split_records("\n") == []

    def test_pipe_and_newline_inside_free_text(self):
        row = job_row(
            102,
            name="a|b",
            submit_line='sbatch --wrap "echo one\necho two | sort"',
        )
        recs = records_to_dicts(split_records(stream([row, job_row(103)])))
        assert len(recs) == 2
        assert recs[0]["JobName"] == "a|b"
        assert recs[0]["SubmitLine"] == 'sbatch --wrap "echo one\necho two | sort"'
        assert recs[1]["JobIDRaw"] == "103"

    def test_separator_inside_submit_line_does_not_shift_fields(self):
        row = job_row(104, submit_line="sbatch --wrap x" + SEP + "y")
        recs = records_to_dicts(split_records(stream([row, job_row(105)])))
        assert [r["JobIDRaw"] for r in recs] == ["104", "105"]
        assert recs[0]["SubmitLine"] == "sbatch --wrap x" + SEP + "y"

    def test_output_cut_mid_record_is_rejected(self):
        text = stream(simple_job(106))
        with pytest.raises(TruncatedOutput):
            split_records(text[: len(text) - 40])

    def test_output_cut_between_fields_is_rejected(self):
        tokens = stream(simple_job(107)).split(SEP)
        with pytest.raises(TruncatedOutput):
            split_records(SEP.join(tokens[: len(FIELDS) + 5]) + SEP)

    def test_garbage_is_rejected(self):
        with pytest.raises(SacctFormatError):
            split_records(SEP.join(["sacct: error"] + ["x"] * (len(FIELDS) - 1)) + SEP + "\n")


class TestGroupRecords:
    def test_steps_follow_their_job(self):
        rows = simple_job(201) + simple_job(202)
        groups, orphans = group_records(rows)
        assert orphans == 0
        assert [(g[0]["JobIDRaw"], len(g[1])) for g in groups] == [("201", 2), ("202", 2)]

    def test_reused_job_id_stays_two_jobs(self):
        first = simple_job(300, submit=BASE_TS, name="old")
        second = simple_job(300, submit=BASE_TS + 90 * 86400, name="new")
        groups, _ = group_records(first + second)
        assert [g[0]["JobName"] for g in groups] == ["old", "new"]
        assert all(len(g[1]) == 2 for g in groups)

    def test_step_without_a_job_line_is_counted(self):
        _, orphans = group_records([step_row(999, "batch")])
        assert orphans == 1


class TestMergeJob:
    def test_batch_job(self):
        rows = simple_job(
            400,
            cpus=4,
            mem="16G",
            elapsed=600,
            timelimit=120,
            total_cpu="02:07.732",
            batch_total_cpu="02:07.732",
            batch_max_rss="2097152K",
            name="read-xenium",
        )
        job, steps = merge_job(rows[0], rows[1:])
        assert job["job_id_raw"] == 400 and job["job_id"] == "400"
        assert job["submit_ts"] == BASE_TS and job["end_ts"] == BASE_TS + 630
        assert job["state"] == "COMPLETED"
        assert job["req_time_min"] == 120
        assert job["req_mem_mb"] == 16384 and job["alloc_mem_mb"] == 16384
        assert job["req_cpus"] == 4 and job["alloc_cpus"] == 4
        assert job["cpu_sec"] == pytest.approx(127.733)  # batch + extern
        assert job["cpu_src"] == "rusage"
        assert job["peak_mem_bytes"] == (2097152 + 12288) * 1024
        assert job["mem_src"] == "step"
        assert job["n_steps"] == 2 and len(steps) == 2
        assert {s["step_id"] for s in steps} == {"batch", "extern"}

    def test_cancelled_job_uses_polled_cpu(self):
        rows = simple_job(
            401,
            state="CANCELLED by 6084",
            total_cpu="00:00.004",
            batch_state="CANCELLED",
            batch_total_cpu="00:00.003",
            batch_polled_cpu="00:31:40",
        )
        job, _ = merge_job(rows[0], rows[1:])
        assert job["state"] == "CANCELLED" and job["state_raw"] == "CANCELLED by 6084"
        assert job["cpu_sec"] == pytest.approx(1900.001)
        assert job["cpu_src"] in ("polled", "mixed")

    def test_parallel_steps_are_summed(self):
        job_line = job_row(402, mem="64G", elapsed=1000)
        start = BASE_TS + 30
        steps = [
            step_row(402, "batch", start=start, elapsed=1000, max_rss="102400K"),
            step_row(402, "0", start=start + 10, elapsed=500, max_rss="10485760K"),
            step_row(402, "1", start=start + 20, elapsed=500, max_rss="10485760K"),
        ]
        job, _ = merge_job(job_line, steps)
        assert job["peak_mem_bytes"] == (102400 + 2 * 10485760) * 1024
        assert job["mem_src"] == "overlap_sum"

    def test_sequential_steps_are_not_summed(self):
        job_line = job_row(403, mem="64G", elapsed=1000)
        start = BASE_TS + 30
        steps = [
            step_row(403, "0", start=start, elapsed=400, max_rss="10485760K"),
            step_row(403, "1", start=start + 400, elapsed=400, max_rss="8388608K"),
        ]
        job, _ = merge_job(job_line, steps)
        assert job["peak_mem_bytes"] == 10485760 * 1024
        assert job["mem_src"] == "step"

    def test_multi_task_step_is_estimated_and_capped(self):
        job_line = job_row(404, mem="16G", cpus=8)
        steps = [step_row(404, "0", ntasks=8, max_rss="4194304K", tot_mem="33554432K")]
        job, _ = merge_job(job_line, steps)
        assert job["peak_mem_bytes"] == 16 * GIB  # capped at the allocation
        assert job["mem_src"] == "ntasks_est"

    def test_gpu_job(self):
        rows = simple_job(
            405,
            gpus=1,
            gpu_type="a100",
            batch_gpu_util=38,
            batch_gpu_mem="1000M",
            batch_disk_read=1144847403527,
            batch_disk_write=5000,
        )
        job, _ = merge_job(rows[0], rows[1:])
        assert (job["req_gpus"], job["alloc_gpus"], job["gpu_type"]) == (1, 1, "a100")
        assert job["gpu_util_max"] == 38 and job["gpu_util_ave"] == 38
        assert job["gpu_mem_max_bytes"] == 1000 * 1024**2
        assert job["disk_read_bytes"] == 1144847403527 and job["disk_write_bytes"] == 5000

    def test_out_of_memory_step_marks_the_job(self):
        rows = simple_job(406, state="OUT_OF_MEMORY", batch_state="OUT_OF_MEMORY")
        job, _ = merge_job(rows[0], rows[1:])
        assert job["oom"] == 1 and job["state"] == "OUT_OF_MEMORY"

    def test_job_cancelled_before_it_started(self):
        rows = simple_job(407, state="CANCELLED by 6084", started=False)
        job, steps = merge_job(rows[0], rows[1:])
        assert steps == []
        assert job["start_ts"] is None and job["end_ts"] is not None
        assert job["elapsed_sec"] == 0
        assert job["peak_mem_bytes"] is None and job["cpu_sec"] is None
        assert job["req_mem_mb"] == 16384 and job["alloc_mem_mb"] is None

    def test_array_task(self):
        rows = simple_job(507, display="500_7")
        job, steps = merge_job(rows[0], rows[1:])
        assert (job["job_id"], job["job_id_raw"]) == ("500_7", 507)
        assert (job["array_job_id"], job["array_task_id"]) == (500, 7)
        assert len(steps) == 2

    def test_unlimited_time(self):
        job, _ = merge_job(job_row(408, timelimit="UNLIMITED"), [])
        assert job["req_time_min"] is None

    def test_interactive_step_is_recorded(self):
        job_line = job_row(409)
        job, _ = merge_job(job_line, [step_row(409, "interactive"), step_row(409, "extern")])
        assert job["has_interactive_step"] == 1

    def test_raw_record_is_kept(self):
        job, steps = merge_job(*_split(simple_job(410, name="keep-me")))
        assert '"JobName": "keep-me"' in job["raw_json"]
        assert all(s["raw_json"] for s in steps)


def _split(rows):
    return rows[0], rows[1:]


class TestIsOpen:
    def test_running(self):
        assert is_open(job_row(600, state="RUNNING", ended=False))

    def test_pending(self):
        assert is_open(job_row(601, state="PENDING", started=False, ended=False))

    def test_finished(self):
        assert not is_open(job_row(602))
        assert not is_open(job_row(603, state="CANCELLED by 6084", started=False))

    def test_final_state_without_an_end_time_waits(self):
        """slurmdbd can report the state before the end time; wait for both."""
        assert is_open(job_row(604, state="COMPLETED", ended=False))
