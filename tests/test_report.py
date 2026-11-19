"""Tests for lib/report.py: resource-hour-weighted totals and the group report."""

import csv
import io
import json

import pytest
from rows import DAY, GIB, NOW, run

from lib import history, report
from lib.config import load_config


@pytest.fixture
def conf(tmp_path):
    return load_config({"HPCSIZER_ROOT": str(tmp_path), "HPCSIZER_ACCT": "mygroup"})


def annotated(conf, rows):
    return history.annotate(list(rows), conf)


def build(conf, rows, **kwargs):
    jobs = annotated(conf, rows)
    kwargs.setdefault("days", 90)
    return report.build(jobs, history.History(jobs), conf, NOW, **kwargs)


def mixed():
    """alice: a pipeline that over-asks. bob: sessions, a GPU job, a failure."""
    rows = [run(i) for i in range(6)]  # 64 GB / 16 CPUs asked, 10 GB / 3 cores used, 1 h
    rows += [
        run(
            20 + i,
            user="bob",
            job_name="vscode",
            state="TIMEOUT",
            elapsed_sec=8 * 3600,
            req_time_min=480,
            req_mem_mb=128 * 1024,
            req_cpus=8,
            alloc_cpus=8,
            peak_mem_bytes=16 * GIB,
            cpu_sec=4 * 3600.0,
            cpu_src="polled",
        )
        for i in range(3)
    ]
    rows += [
        run(
            30 + i,
            user="bob",
            job_name="train",
            req_gpus=1,
            alloc_gpus=1,
            gpu_util_max=0.0,
            req_cpus=4,
            alloc_cpus=4,
            cpu_sec=3600.0,
        )
        for i in range(3)
    ]
    rows.append(
        run(
            40,
            user="bob",
            job_name="align",
            state="OUT_OF_MEMORY",
            oom=1,
            req_mem_mb=8192,
            peak_mem_bytes=8 * GIB,
        )
    )
    rows.append(
        run(41, user="bob", job_name="quick", elapsed_sec=20, peak_mem_bytes=1000, cpu_sec=5.0)
    )
    rows.append(
        run(
            42,
            user="bob",
            job_name="never",
            state="CANCELLED",
            elapsed_sec=0,
            peak_mem_bytes=None,
            cpu_sec=None,
        )
    )
    return rows


class TestTotals:
    def test_ratios_are_weighted_by_resource_hours(self, conf):
        """One 10-hour job at 10% outweighs ten 6-minute jobs at 100%."""
        rows = [run(0, elapsed_sec=36000, cpu_sec=16 * 3600.0)]  # 16 of 160 core-hours
        rows += [run(1 + i, elapsed_sec=360, cpu_sec=16 * 360.0) for i in range(10)]  # all busy
        tot = report.summarize(annotated(conf, rows))
        assert tot["core_h_alloc"] == pytest.approx(176)
        assert tot["cpu_used_frac"] == pytest.approx(32 / 176)  # not the 0.92 a plain mean gives

    def test_unmeasured_jobs_are_reserved_but_not_in_the_ratio(self, conf):
        """Charging a 20-second job's whole request as waste inflated every total."""
        tot = report.summarize(
            annotated(conf, [run(0), run(1, elapsed_sec=20, peak_mem_bytes=500)])
        )
        assert tot["ran"] == 2 and tot["mem_n"] == 1
        assert tot["mem_gbh_req"] == 64 and tot["mem_gbh_used"] == 10
        assert tot["mem_gbh_reserved"] == pytest.approx(64 + 64 * 20 / 3600)
        assert tot["mem_used_frac"] == pytest.approx(10 / 64)

    def test_jobs_that_never_ran_count_as_jobs_only(self, conf):
        rows = [run(0, state="CANCELLED", elapsed_sec=0, peak_mem_bytes=None, cpu_sec=None)]
        tot = report.summarize(annotated(conf, rows))
        assert tot["jobs"] == 1 and tot["ran"] == 0 and tot["core_h_reserved"] == 0
        assert tot["mem_used_frac"] is None and tot["cpu_used_frac"] is None

    def test_use_above_the_request_does_not_exceed_100_percent(self, conf):
        tot = report.summarize(annotated(conf, [run(0, req_mem_mb=8192, peak_mem_bytes=9 * GIB)]))
        assert tot["mem_used_frac"] == 1.0 and tot["mem_gbh_unused"] == 0

    def test_gpu_hours(self, conf):
        rows = [
            run(i, req_gpus=1, alloc_gpus=1, gpu_util_max=u)
            for i, u in enumerate([0.0, 80.0, None])
        ]
        tot = report.summarize(annotated(conf, rows))
        assert tot["gpu_h"] == 3 and tot["gpu_h_measured"] == 2 and tot["gpu_h_idle"] == 1
        assert tot["gpu_idle_frac"] == 0.5


class TestBuild:
    def test_users_kinds_and_families(self, conf):
        rep = build(conf, mixed())
        assert [u["user"] for u in rep["users"]] == ["bob", "alice"], "most core-hours first"
        assert {k["kind"] for k in rep["kinds"]} == {"batch", "vscode"}
        assert rep["n_families"] == 6 and rep["totals"]["jobs"] == 15
        assert rep["unmeasured"] == {"memory": 1, "cpu": 2}

    def test_top_lists_rank_by_unused_resource_hours(self, conf):
        rep = build(conf, mixed())
        assert rep["top_memory"][0] == "bob/vscode"  # 3 x 8 h x 112 GB
        assert rep["top_memory"][1] == "alice/batch/mplx#k-spleen"
        assert rep["top_gpu"] == ["bob/batch/train+gpu"]
        assert rep["sessions"] == ["bob/vscode"]
        assert "bob/batch/never" not in rep["top_memory"]

    def test_each_family_carries_the_suggestion_its_next_run_would_get(self, conf):
        rep = build(conf, mixed())
        by = {r["family"]: r for r in rep["families"]}
        pipeline = by["alice/batch/mplx#k-spleen"]["suggestion"]
        assert pipeline["mem_mb"] == 12 * 1024 and pipeline["cpus"] == 4
        assert pipeline["options"][0] == "--mem=12G" and pipeline["source"] == "history"
        session = by["bob/vscode"]["suggestion"]
        assert (
            session["mem_mb"] == 20 * 1024 and session["cpus_kept_because"] == "unprofiled-session"
        )
        assert by["bob/batch/train+gpu"]["suggestion"]["gpus_action"] == "lower"
        assert by["bob/batch/quick"]["suggestion"]["source"] == "none"

    def test_failures(self, conf):
        rep = build(conf, mixed())
        states = {f["state"]: f for f in rep["failures"]}
        assert set(states) == {"OUT_OF_MEMORY"}, "a session that hit its limit did not fail"
        assert states["OUT_OF_MEMORY"]["families"][0] == {
            "family": "bob/batch/align",
            "jobs": 1,
            "examples": ["5000040"],
        }

    def test_suggestions_use_history_older_than_the_window(self, conf):
        """A 7-day report still sizes a family from its 180 days of runs."""
        everything = annotated(
            conf, [run(i, age_days=40 - i) for i in range(10)] + [run(50, age_days=1)]
        )
        window = [j for j in everything if j["end_ts"] >= NOW - 7 * DAY]
        rep = report.build(window, history.History(everything), conf, NOW, days=7)
        assert rep["totals"]["jobs"] == 1
        assert rep["families"][0]["suggestion"]["source"] == "history"

    def test_empty(self, conf):
        rep = build(conf, [])
        assert rep["totals"]["jobs"] == 0 and rep["families"] == []
        assert "No finished jobs in this window." in report.render_text(rep)
        assert "No finished jobs in this window." in report.render_markdown(rep)


class TestRender:
    def test_text(self, conf):
        text = report.render_text(build(conf, mixed(), watermark_ts=NOW))
        assert text.startswith("HPCsizer report: account mygroup, ")
        assert "15 finished jobs of 2 user(s) in 6 job families" in text
        total = next(line for line in text.splitlines() if line.strip().startswith("Total"))
        assert total.split()[1] == "15"
        assert "Memory requested and not used" in text and "Interactive sessions" in text
        vscode = next(line for line in text.splitlines() if line.strip().startswith("bob/vscode"))
        assert "128 GB" in vscode and "16 / 16 GB" in vscode and "20 GB" in vscode
        cpu_section = text[text.index("Cores allocated and not used") :]
        assert "profile it first" in cpu_section
        assert "drop the GPU" in text and "OUT_OF_MEMORY" in text
        assert "1 of the 14 jobs that ran have no memory measurement" in " ".join(text.split())

    def test_filters_are_named_in_the_heading(self, conf):
        rep = build(conf, [run(i) for i in range(3)], user="alice", kind="batch")
        assert "account mygroup, user alice, kind batch" in report.render_text(rep).splitlines()[0]
        one = build(conf, [run(i) for i in range(3)], account="otherlab")
        assert "account otherlab," in report.render_text(one).splitlines()[0]

    def test_markdown(self, conf):
        text = report.render_markdown(build(conf, mixed()))
        assert text.startswith("# HPCsizer report")
        assert "## Requested and used, by user" in text
        assert "| User | Jobs |" in text and "|---|---:|" in text

    def test_json_round_trips(self, conf):
        rep = build(conf, mixed())
        again = json.loads(json.dumps(rep))
        assert again["totals"]["jobs"] == 15 and len(again["families"]) == 6

    def test_csv_has_one_line_per_family(self, conf):
        rows = list(csv.DictReader(io.StringIO(report.render_csv(build(conf, mixed())))))
        assert len(rows) == 6 and set(rows[0]) == set(report.FAMILY_CSV)
        pipeline = next(r for r in rows if r["family"] == "alice/batch/mplx#k-spleen")
        assert pipeline["jobs"] == "6" and pipeline["suggested_mem_mb"] == "12288"
        assert float(pipeline["mem_gbh_measured"]) == 384 and float(pipeline["mem_gbh_used"]) == 60
        never = next(r for r in rows if r["family"] == "bob/batch/never")
        assert never["suggested_mem_mb"] == "" and never["mem_used_gb_p50"] == ""


class TestFamilies:
    def test_table_and_sorting(self, conf):
        rows = report.family_rows(annotated(conf, mixed()))
        by_memory = sorted(rows, key=report.FAMILY_SORTS["memory"])
        assert by_memory[0]["family"] == "bob/vscode"
        by_name = sorted(rows, key=report.FAMILY_SORTS["name"])
        assert by_name[0]["family"] == "alice/batch/mplx#k-spleen"
        text = report.families_table(by_memory)
        assert "bob/vscode" in text.splitlines()[1] and "16 / 16 GB" in text


def test_a_family_row_is_sized_from_the_whole_family(conf):
    """Not from the runs sharing its latest member's exact name."""
    rows = [run(i, job_name="mplx45k-spleen", peak_mem_bytes=30 * GIB) for i in range(4)]
    rows += [run(10 + i, job_name="mplx12k-spleen", peak_mem_bytes=8 * GIB) for i in range(4)]
    rep = build(conf, rows)
    (family,) = rep["families"]
    assert family["family"] == "alice/batch/mplx#k-spleen"
    assert family["suggestion"]["mem_mb"] == 36 * 1024
