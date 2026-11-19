"""Tests for lib/jobview.py: single jobs, the runs of a family, flags and export."""

import pytest
from rows import GIB, run

from lib import history, jobview
from lib.config import load_config


@pytest.fixture
def conf(tmp_path):
    return load_config({"HPCSIZER_ROOT": str(tmp_path)})


def annotated(conf, rows):
    return history.annotate(list(rows), conf)


class TestFlags:
    def test_every_flag_comes_with_what_was_measured(self, conf):
        job = annotated(conf, [run(1, req_time_min=2880)])[0]
        assert job["m"]["flags"] == ["mem_overrequest", "cpu_overrequest", "time_overrequest"]
        assert jobview.flag_detail(job, "mem_overrequest") == (
            "peak 10 GB of 64 GB requested (16%) for 1 h: 54.0 GB-h unused"
        )
        assert jobview.flag_detail(job, "cpu_overrequest") == (
            "3 of 16 cores busy on average (19%) for 1 h: 13.0 core-h unused"
        )
        assert jobview.flag_detail(job, "time_overrequest") == (
            "finished in 01:00:00 of a 2-00:00:00 limit (2%)"
        )

    def test_failure_flags(self, conf):
        oom, timed_out, idle = annotated(
            conf,
            [
                run(1, state="OUT_OF_MEMORY", oom=1, req_mem_mb=8192, elapsed_sec=1200),
                run(2, state="TIMEOUT", req_time_min=60, cpu_src="polled"),
                run(3, req_gpus=2, alloc_gpus=2, gpu_util_max=1.0),
            ],
        )
        assert (
            jobview.flag_detail(oom, "oom")
            == "ran out of memory with 8 GB requested after 00:20:00"
        )
        assert jobview.flag_detail(timed_out, "timeout") == "hit its 01:00:00 limit"
        assert jobview.flag_detail(idle, "gpu_idle") == (
            "2 GPU(s) for 1 h, busiest moment 1%: 2.0 GPU-h idle"
        )

    def test_listing_is_bounded_and_ordered_by_what_is_at_stake(self, conf):
        rows = [run(i, elapsed_sec=3600 * (i + 1)) for i in range(40)]
        jobs = annotated(conf, rows)
        flags = sum(len(job["m"]["flags"]) for job in jobs)
        text = jobview.render_flags(jobs, days=30, limit=5, per_job=True)
        assert "Jobs, largest first (5 of %d)" % flags in text
        listed = [ln.split()[0] for ln in text.splitlines() if ln.strip().startswith("50000")]
        assert listed[0] == "5000039" and len(listed) == 5, "the longest run wasted the most"

    def test_grouped_by_family(self, conf):
        rows = [run(i) for i in range(6)]
        rows += [run(10 + i, job_name="other", req_mem_mb=640 * 1024) for i in range(2)]
        jobs = annotated(conf, rows)
        counts, items = jobview.flagged(jobs)
        assert counts["mem_overrequest"] == 8
        groups = jobview.flag_groups(items)
        assert groups[0]["family"] == "alice/batch/other" and groups[0]["jobs"] == 2
        assert groups[0]["detail"] == "typical peak 10 GB of 640 GB requested; 1,260 GB-h unused"
        text = jobview.render_flags(jobs, days=30)
        assert "By job family, largest first (6 of 6" in text

    def test_unmeasured_is_counted_but_listed_only_on_request(self, conf):
        jobs = annotated(
            conf, [run(1, elapsed_sec=30, peak_mem_bytes=10, cpu_sec=1.0, req_time_min=30)]
        )
        counts, items = jobview.flagged(jobs)
        assert counts == {"unmeasured": 1} and items == []
        _counts, items = jobview.flagged(jobs, only="unmeasured")
        assert len(items) == 1
        assert "too short" in jobview.flag_detail(jobs[0], "unmeasured")

    def test_no_flags(self, conf):
        jobs = annotated(conf, [run(1, req_mem_mb=12 * 1024, cpu_sec=14 * 3600.0, req_time_min=90)])
        assert "No job was flagged." in jobview.render_flags(jobs, days=30)


class TestMatchFamilies:
    @pytest.fixture
    def jobs(self, conf):
        return annotated(
            conf,
            [
                run(1),
                run(2, job_name="mplx30k-spleen"),
                run(3, user="bob", job_name="align"),
                run(4, job_name="align"),
                run(5, job_name="wrap", submit_line="sbatch --wrap python seg.py 4"),
            ],
        )

    def test_exact_family(self, jobs):
        assert jobview.match_families(jobs, "bob/batch/align") == ["bob/batch/align"]

    def test_job_name_prefers_the_callers_own_family(self, jobs):
        assert jobview.match_families(jobs, "align", user="alice") == ["alice/batch/align"]
        assert jobview.match_families(jobs, "align", user="carol") == [
            "alice/batch/align",
            "bob/batch/align",
        ]

    def test_numbered_name(self, jobs):
        assert jobview.match_families(jobs, "mplx99k-spleen") == ["alice/batch/mplx#k-spleen"]

    def test_program_of_a_wrap_job(self, jobs):
        assert jobview.match_families(jobs, "python:seg.py") == ["alice/wrap/cmd:python:seg.py"]

    def test_fragment(self, jobs):
        assert jobview.match_families(jobs, "spleen") == ["alice/batch/mplx#k-spleen"]
        assert jobview.match_families(jobs, "nothing-like-it") == []


class TestHistory:
    def test_runs_and_spread(self, conf):
        rows = [run(i, peak_mem_bytes=(8 + i) * GIB) for i in range(5)]
        rows.append(run(9, state="TIMEOUT", req_time_min=60, cpu_src="polled"))
        text = jobview.render_history(annotated(conf, rows), "alice/batch/mplx#k-spleen", limit=3)
        assert "6 run(s) -- 5 COMPLETED, 1 TIMEOUT" in text
        assert "(the latest 3 are listed; the spread below covers all)" in text
        last = next(line for line in text.splitlines() if line.startswith("5000009"))
        assert (
            "TIMEOUT" in last
            and "01:00:00 / 01:00:00" in last
            and last.rstrip().endswith("TIMEOUT mem cpu")
        )
        spread = next(line for line in text.splitlines() if line.startswith("Memory used"))
        assert spread.split()[2:] == ["6", "8", "GB", "10", "GB", "12", "GB", "12", "GB"]
        assert jobview.FLAG_LEGEND in text

    def test_gpu_column_only_when_there_are_gpus(self, conf):
        plain = jobview.render_history(annotated(conf, [run(1)]), "x")
        assert "GPU busiest" not in plain
        gpu = jobview.render_history(
            annotated(conf, [run(1, alloc_gpus=1, gpu_util_max=42.0)]), "x"
        )
        assert "GPU busiest" in gpu and "42%" in gpu

    def test_empty(self, conf):
        assert jobview.render_history([], "nobody") == "nobody: 0 run(s) -- none"


class TestJob:
    def test_requested_and_used(self, conf):
        job = annotated(
            conf,
            [
                run(
                    1,
                    account="mygroup",
                    partition="hpg-default",
                    qos="mygroup",
                    exit_code="0:0",
                    mem_src="overlap_sum",
                    disk_read_bytes=3 * GIB,
                    disk_write_bytes=GIB,
                    start_ts=run(1)["submit_ts"] + 90,
                    work_dir="/blue/mygroup/alice",
                )
            ],
        )[0]
        steps = [
            {
                "step_id": "batch",
                "state": "COMPLETED",
                "elapsed_sec": 3600,
                "ntasks": 1,
                "alloc_cpus": 16,
                "cpu_sec": 10800.0,
                "cpu_src": "rusage",
                "max_rss_bytes": 10 * GIB,
                "gpu_util_max": None,
            },
        ]
        text = jobview.render_job(job, steps, others=1)
        assert text.splitlines()[0].startswith(
            "Job 5000001  mplx12k-spleen  [batch]  user alice, account mygroup"
        )
        assert "waited 00:01:30, ran 01:00:00" in text
        memory = next(line for line in text.splitlines() if line.startswith("Memory"))
        assert (
            "64 GB" in memory
            and "10 GB (16%)" in memory
            and "sum of the steps that ran together" in memory
        )
        cpus = next(line for line in text.splitlines() if line.startswith("CPUs"))
        assert "3 cores (19%)" in cpus and "CPU time from rusage" in cpus
        assert "read 3.0 GB, written 1.0 GB" in text
        assert "this job id was used 1 more time(s)" in text
        assert "mem_overrequest   peak 10 GB of 64 GB" in text
        assert (
            "hpg recommend --like 5000001" in text
            and "hpg history 'alice/batch/mplx#k-spleen'" in text
        )

    def test_unmeasured_values_say_so(self, conf):
        job = annotated(
            conf, [run(1, state="TIMEOUT", elapsed_sec=40, peak_mem_bytes=0, cpu_sec=0.01)]
        )[0]
        text = jobview.render_job(job, [])
        assert text.count("not measured") == 2 and "Steps" not in text

    def test_profile(self, conf):
        job = annotated(
            conf,
            [
                run(
                    1,
                    mon_n_samples=360,
                    mon_interval_sec=10.0,
                    mon_source="cgroup-v2",
                    mon_peak_mem_bytes=11 * GIB,
                    mon_avg_cores=3.0,
                    mon_p95_cores=7.5,
                    mon_max_cores=15.8,
                    mon_cpu_sec=10800.0,
                    mon_top_comm="python R",
                    mon_series_path="timeseries/5000001.tsv.gz",
                )
            ],
        )[0]
        text = jobview.render_job(job, [], profile={"notes": ["memory grew steadily"]})
        assert "Profile (in-job sampler, cgroup-v2)" in text
        assert (
            "360 samples every 10 s; peak memory 11.0 GB; cores average 3, sustained 7.5, busiest 15.8"
            in text
        )
        assert "largest processes: python R" in text and "- memory grew steadily" in text
        assert "11 GB (17%)" in text and "peak; profile" in text
        assert "hpg plot 5000001" in text


class TestExport:
    def test_stored_and_derived_columns(self, conf):
        jobs = annotated(
            conf, [run(1), run(2, elapsed_sec=20, peak_mem_bytes=5, cpu_sec=1.0, req_time_min=30)]
        )
        record = jobview.export_record(jobs[0])
        assert list(record) == list(jobview.EXPORT_COLUMNS)
        assert record["family"] == "alice/batch/mplx#k-spleen" and record["mem_used_gb"] == 10.0
        assert record["flags"] == "mem_overrequest cpu_overrequest time_overrequest"
        rows = jobview.export_rows(jobs)
        short = dict(zip(jobview.EXPORT_COLUMNS, rows[1]))
        assert short["mem_used_gb"] == "" and short["flags"] == "unmeasured"
        assert jobview.render_date_range(jobs) == jobview.render_date_range(jobs[::-1])
        assert jobview.render_date_range([]) == "no jobs"
