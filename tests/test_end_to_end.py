"""The whole path on a synthetic account: sacct -> harvest -> every report.

The account (tests/synthetic.py) has four users and every kind of job the
collector must cope with. These tests assert the facts a reader of the reports
relies on, not their layout.
"""

import csv
import io
import json
import time

import pytest
from synthetic import account_rows

from lib import cli, db


def hpg(*argv):
    return cli.main(list(argv))


@pytest.fixture
def account(root, stubs, monkeypatch, capsys):
    monkeypatch.setenv("USER", "alice")
    stubs.sacct_default(account_rows(int(time.time())))
    assert hpg("harvest") == 0
    capsys.readouterr()
    return root


def out_of(capsys, *argv, status=0):
    assert hpg(*argv) == status
    return capsys.readouterr().out


class TestCollection:
    def test_every_member_and_every_final_state_is_stored(self, account):
        with db.reader(str(account / "profiles.db")) as conn:
            jobs = db.fetch_jobs(conn)
        assert {j["user"] for j in jobs} == {"alice", "bjones", "cchen", "dpatel"}
        assert {j["state"] for j in jobs} == {
            "COMPLETED",
            "CANCELLED",
            "TIMEOUT",
            "FAILED",
            "OUT_OF_MEMORY",
            "NODE_FAIL",
        }
        assert not any((j["job_name"] or "").startswith("hpcsizer-") for j in jobs)
        assert not any(j["state"] in ("RUNNING", "PENDING") for j in jobs)

    def test_a_reused_job_id_is_two_jobs(self, account):
        with db.reader(str(account / "profiles.db")) as conn:
            both = db.find_jobs(conn, "4200001")
        assert [j["job_name"] for j in both] == ["new-incarnation", "old-incarnation"]

    def test_killed_sessions_still_have_cpu_time(self, account, capsys):
        """TotalCPU is ~0 for a cancelled job; the sampled value is what counts."""
        data = json.loads(out_of(capsys, "history", "--no-refresh", "alice/vscode", "--json"))
        assert len(data) == 32 and all(r["cpu_source"] in ("polled", "mixed") for r in data)
        assert all(0.5 < r["cores_used"] < 1.3 for r in data)

    def test_second_harvest_adds_nothing(self, account, capsys):
        assert hpg("harvest") == 0
        assert "0 new" in capsys.readouterr().err


class TestReport:
    def test_who_reserved_what_and_how_much_of_it_was_used(self, account, capsys):
        rep = json.loads(out_of(capsys, "report", "--no-refresh", "--format", "json"))
        users = {u["user"]: u for u in rep["users"]}
        assert list(users) == ["alice", "bjones", "cchen", "dpatel"], (
            "ordered by core-hours reserved"
        )
        assert users["alice"]["cpu_used_frac"] < 0.10, "sessions holding 16 cores, about 1 busy"
        assert users["cchen"]["cpu_used_frac"] > 0.6
        kinds = {k["kind"]: k for k in rep["kinds"]}
        assert kinds["vscode"]["core_h_reserved"] > 3 * kinds["batch"]["core_h_reserved"]
        assert rep["top_memory"][0] == "alice/vscode" and rep["top_gpu"] == ["alice/vscode+gpu"]
        assert {f["state"] for f in rep["failures"]} == {
            "OUT_OF_MEMORY",
            "TIMEOUT",
            "FAILED",
            "NODE_FAIL",
        }
        assert rep["totals"]["flags"]["oom"] == 1 and rep["totals"]["flags"]["timeout"] == 1

    def test_suggestions_in_the_report_follow_the_policy(self, account, capsys):
        rep = json.loads(out_of(capsys, "report", "--no-refresh", "--format", "json"))
        sug = {r["family"]: r["suggestion"] for r in rep["families"]}
        assert sug["alice/batch/mplx#k-spleen"]["options"][:2] == ["--mem=15G", "--cpus-per-task=4"]
        assert sug["alice/vscode"]["cpus_kept_because"] == "unprofiled-session"
        assert sug["alice/vscode+gpu"]["gpus_action"] == "lower"
        assert sug["cchen/batch/train-unet+gpu"]["gpus_action"] == "keep"
        align = sug["bjones/batch/align"]
        assert align["mem_mb"] == 24 * 1024, "one run ran out of memory at 16 GB"
        assert align["cpus_kept_because"] == "saturated"
        wrap = {f: s for f, s in sug.items() if "/wrap/" in f}
        assert set(wrap) == {
            "bjones/wrap/cmd:python:read_xenium.py",
            "bjones/wrap/cmd:python:archive.py",
        }, "two programs, both named 'wrap' by Slurm"
        assert wrap["bjones/wrap/cmd:python:archive.py"]["mem_mb"] == 2048
        assert wrap["bjones/wrap/cmd:python:read_xenium.py"]["mem_action"] == "keep"

    def test_formats_and_filters(self, account, capsys, tmp_path):
        text = out_of(capsys, "report", "--no-refresh")
        assert "Requested and used, by user" in text and text.rstrip().splitlines()[-1].startswith(
            "Data as of"
        )
        md = out_of(capsys, "report", "--no-refresh", "--format", "md")
        assert md.startswith("# HPCsizer report") and "| alice |" in md
        target = tmp_path / "families.csv"
        assert hpg("report", "--no-refresh", "--format", "csv", "--out", str(target)) == 0
        assert "wrote %s" % target in capsys.readouterr().err
        rows = list(csv.DictReader(io.StringIO(target.read_text())))
        assert len(rows) == 13 and rows[0]["family"] == "alice/vscode"
        one = out_of(capsys, "report", "--no-refresh", "--user", "cchen", "--kind", "batch")
        assert "user cchen, kind batch" in one and "alice" not in one
        assert hpg("report", "--no-refresh", "--user", "nobody") == 3


class TestViews:
    def test_families(self, account, capsys):
        text = out_of(capsys, "families", "--no-refresh", "--sort", "jobs", "-n", "3")
        assert "14 job families" in text and "(the first 3 by jobs)" in text
        data = json.loads(out_of(capsys, "families", "--no-refresh", "--user", "dpatel", "--json"))
        assert {r["family"] for r in data} == {
            "dpatel/batch/cmd:test.sh",
            "dpatel/batch/bigjob",
            "dpatel/interactive",
        }

    def test_history_of_a_family_by_name(self, account, capsys):
        text = out_of(capsys, "history", "--no-refresh", "align", "--user", "bjones")
        assert "bjones/batch/align in the last 180 days: 14 run(s)" in text
        assert "12 COMPLETED" in text and "1 TIMEOUT" in text and "1 OUT_OF_MEMORY" in text
        assert "What to request next: hpg recommend --like" in text
        own = out_of(capsys, "history", "--no-refresh", "-n", "5")
        assert own.startswith("Jobs of alice in the last 180 days: 71 run(s)")
        assert hpg("history", "--no-refresh", "no-such-thing") == 3
        assert hpg("history", "--no-refresh", "python") == 2  # two wrap families match
        err = capsys.readouterr().err
        assert "matches 2 families" in err and "cmd:python:archive.py" in err

    def test_flags(self, account, capsys):
        text = out_of(capsys, "flags", "--no-refresh")
        assert "By job family, largest first" in text
        first = [ln for ln in text.splitlines() if ln.strip().startswith("alice/vscode ")][0]
        assert "mem_overrequest" in first and "of 256 GB requested" in first
        only = out_of(capsys, "flags", "--no-refresh", "--flag", "oom", "--jobs")
        assert "ran out of memory with 16 GB requested" in only
        data = json.loads(out_of(capsys, "flags", "--no-refresh", "--json", "--flag", "gpu_idle"))
        assert (
            data["counts"]["gpu_idle"] == 6 and data["families"][0]["family"] == "alice/vscode+gpu"
        )

    def test_one_job(self, account, capsys):
        data = json.loads(
            out_of(capsys, "history", "--no-refresh", "train-unet", "--user", "cchen", "--json")
        )
        job_id = data[-1]["job_id"]
        text = out_of(capsys, "job", "--no-refresh", job_id)
        assert (
            "train-unet  [batch]  user cchen" in text
            and "Family    cchen/batch/train-unet+gpu" in text
        )
        gpus = next(line for line in text.splitlines() if line.startswith("GPUs"))
        assert "1 a100" in gpus and "busiest moment" in gpus and "GPU memory up to" in gpus
        assert "Steps" in text and "batch" in text and "extern" in text
        full = json.loads(out_of(capsys, "job", "--no-refresh", job_id, "--json"))
        assert full["metrics"]["gpus"] == 1 and full["sacct"]["JobName"] == "train-unet"
        assert len(full["steps"]) == 2
        assert hpg("job", "--no-refresh", "1") == 3
        assert "hpg queue" in capsys.readouterr().err

    def test_export(self, account, capsys):
        text = out_of(capsys, "export", "--no-refresh", "--user", "cchen")
        rows = list(csv.DictReader(io.StringIO(text)))
        assert len(rows) == 29 and {r["user"] for r in rows} == {"cchen"}
        arrays = [r for r in rows if r["job_name"] == "xen_raw_evidence"]
        assert len(arrays) == 20 and all("_" in r["job_id"] for r in arrays)
        data = json.loads(
            out_of(capsys, "export", "--no-refresh", "--days", "3", "--format", "json")
        )
        assert data and all(r["end_ts"] >= time.time() - 3 * 86400 - 5 for r in data)

    def test_queue(self, account, capsys):
        text = out_of(capsys, "queue")
        assert "2 pending, 1 running" in text.splitlines()[0]
        assert "scontrol update JobId=" in text and "MinMemoryNode=" in text
        assert "resubmit with --mem=24G" in text, "the pending align array asks for what failed"
        mine = out_of(capsys, "queue", "--user", "bjones")
        assert "vscode" not in mine and "align" in mine


class TestPreJob:
    def test_recommend_for_each_kind_of_target(self, account, capsys, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "run_mplx.sh").write_text(
            "#!/bin/bash\n#SBATCH -J mplx45k-spleen\n#SBATCH --mem=64G -c 16 -t 2-0\npython run.py\n"
        )
        script = out_of(capsys, "recommend", "--no-refresh", "run_mplx.sh")
        assert "10 runs named mplx45k-spleen by alice" in script and "#SBATCH --mem=15G" in script
        session = out_of(capsys, "recommend", "--no-refresh", "--kind", "vscode")
        assert "20 vscode sessions of alice (the latest of 32)" in session
        assert "#SBATCH --mem=37G" in session and "hpg submit" in session
        wrap = out_of(
            capsys,
            "recommend",
            "--no-refresh",
            "--user",
            "bjones",
            "--mem=32G",
            "-c",
            "4",
            "--wrap",
            "python archive.py --sample S999",
        )
        assert "12 runs of python:archive.py by bjones" in wrap and "#SBATCH --mem=2G" in wrap
        assert out_of(
            capsys, "recommend", "--no-refresh", "--user", "dpatel", "--name", "bigjob", status=3
        )

    def test_backtest(self, account, capsys):
        data = json.loads(out_of(capsys, "backtest", "--no-refresh", "--json"))
        by = {p["name"]: p for p in data["policies"]}
        asked, configured = by["as requested"]["mem"], by["configured"]["mem"]
        assert asked["short"] == 1, "the one job that ran out of memory"
        assert configured["sized"] > 100
        assert configured["gbh_suggested"] < 0.4 * asked["gbh_requested"]
        assert configured["short"] <= 3
