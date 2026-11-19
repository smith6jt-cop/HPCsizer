"""Tests for the hpg command line (lib/cli.py), run in-process."""

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import SCRONTAB_STUB
from sacct_fixture import simple_job

from lib import cli, collect, db

REPO = Path(__file__).resolve().parent.parent

DAY = 86400


def hpg(*argv):
    return cli.main(list(argv))


@pytest.fixture
def env(root, stubs, monkeypatch):
    """Recent jobs in sacct, short first window, no network of real Slurm commands."""
    (root / "hpcsizer.ini").write_text("[collector]\nfirst_days = 5\n")
    return stubs


def recent(jid, **kwargs):
    import time

    kwargs.setdefault("submit", int(time.time()) - 3600)
    return simple_job(jid, **kwargs)


class TestHarvest:
    def test_harvest_creates_the_database(self, env, root, capsys):
        env.sacct_default(recent(1) + recent(2, user="bjones"))
        assert hpg("harvest") == 0
        err = capsys.readouterr().err
        assert "2 new" in err and "2 user(s)" in err
        with db.reader(str(root / "profiles.db")) as conn:
            assert len(db.fetch_jobs(conn)) == 2

    def test_since(self, env, capsys):
        env.sacct_default(recent(1))
        assert hpg("harvest", "--since", "70d") == 0
        assert len(env.sacct_calls()) == 3
        assert hpg("harvest", "--since", "whenever") == 2

    def test_manual_failure_exits_non_zero(self, env, capsys):
        env.sacct_plan("fail", "fail", "fail")
        assert hpg("harvest") == 1
        assert "nothing was published" in capsys.readouterr().err

    def test_scheduled_failure_is_tolerated_while_data_is_fresh(self, env, capsys):
        """One stalled run must not fail the Slurm job and send mail."""
        env.sacct_default(recent(1))
        assert hpg("harvest") == 0
        env.sacct_plan("fail", "fail", "fail")
        assert hpg("harvest", "--trigger", "schedule") == 0
        assert "the next run resumes" in capsys.readouterr().err

    def test_scheduled_failure_fails_the_job_once_stale(self, env, root, capsys):
        env.sacct_default(recent(1))
        assert hpg("harvest") == 0
        path = str(root / "profiles.db")
        conn = sqlite3.connect(path)
        conn.execute("UPDATE meta SET value = value - 30*3600 WHERE key='watermark_ts'")
        conn.commit()
        conn.close()
        env.sacct_plan("fail", "fail", "fail")
        assert hpg("harvest", "--trigger", "schedule") == 1
        assert "failing the job" in capsys.readouterr().err

    def test_scheduled_failure_with_no_data_at_all_fails(self, env):
        env.sacct_plan("fail", "fail", "fail")
        assert hpg("harvest", "--trigger", "schedule") == 1

    def test_scheduled_run_yields_to_a_harvest_in_progress(self, env, root, capsys):
        env.sacct_default(recent(1))
        with db.SnapshotWriter(str(root / "profiles.db")):
            assert hpg("harvest", "--trigger", "schedule") == 0
            assert hpg("harvest") == 1
        assert "skipped" in capsys.readouterr().err


class TestStatus:
    def test_missing_then_ok(self, env, capsys):
        assert hpg("status", "--no-sacct") == 1
        assert "STATUS: MISSING" in capsys.readouterr().out
        env.sacct_default(recent(1) + recent(2, user="bjones"))
        hpg("harvest")
        capsys.readouterr()
        assert hpg("status", "--no-sacct") == 0
        out = capsys.readouterr().out
        assert "STATUS: OK" in out and "Last harvest" in out
        assert "smith6jt 1" in out and "bjones 1" in out

    def test_json(self, env, capsys):
        env.sacct_default(recent(1))
        hpg("harvest")
        capsys.readouterr()
        assert hpg("status", "--no-sacct", "--json") == 0
        data = json.loads(capsys.readouterr().out)
        assert data["state"] == "ok" and data["rows"]["total"] == 1

    def test_version_2_database_is_explained(self, env, root, capsys):
        conn = sqlite3.connect(str(root / "profiles.db"))
        conn.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY)")
        conn.close()
        assert hpg("status", "--no-sacct") == 1
        assert "hpg db migrate" in capsys.readouterr().out
        assert hpg("harvest") == 1


class TestDb:
    def make_v2(self, root):
        import v2_schema

        path = str(root / "profiles.db")
        v2_schema.create(path)
        v2_schema.insert_job(
            path, {"job_id": "1", "user": "u", "job_name": "j", "state": "COMPLETED", "flags": "[]"}
        )
        return path

    def test_migrate_and_check(self, env, root, capsys):
        self.make_v2(root)
        assert hpg("db", "check") == 1
        assert hpg("db", "migrate") == 0
        out = capsys.readouterr().out
        assert "migrated                : 1" in out and "backup" in out
        assert os.listdir(root / "backup")
        assert hpg("db", "check") == 0
        assert "integrity ok" in capsys.readouterr().out
        assert hpg("db", "migrate") == 1  # already current

    def test_migrate_refuses_while_the_old_chain_is_queued(self, env, root, capsys):
        self.make_v2(root)
        env.install("squeue")
        env.output("squeue", "51598968\n51598969\n")
        assert hpg("db", "migrate") == 1
        assert "scancel --name=hpcsizer-scheduler" in capsys.readouterr().err
        assert hpg("db", "migrate", "--force") == 0


class TestReparse:
    def test_stored_rows_are_rebuilt_from_their_sacct_records(self, env, root, capsys):
        """A corrected parser reaches jobs Slurm has long deleted."""
        env.sacct_default(recent(1, batch_total_cpu="02:30.500") + recent(2))
        assert hpg("harvest") == 0
        path = str(root / "profiles.db")
        conn = sqlite3.connect(path)
        before = conn.execute(
            "SELECT cpu_sec, peak_mem_bytes FROM jobs WHERE job_id='1'"
        ).fetchone()
        # What an older, wrong parser would have left behind:
        conn.execute("UPDATE jobs SET cpu_sec=0, peak_mem_bytes=NULL, parser_version=0, state='?'")
        conn.execute("UPDATE steps SET cpu_sec=0")
        conn.commit()
        conn.close()
        env.remove("sacct")  # Slurm no longer has these jobs
        capsys.readouterr()
        assert hpg("db", "reparse") == 0
        assert "Re-derived 2 of 2 stored jobs" in capsys.readouterr().out
        with db.reader(path) as conn:
            job = db.find_jobs(conn, "1")[0]
            steps = db.fetch_steps(conn, job["id"])
        assert (job["cpu_sec"], job["peak_mem_bytes"]) == tuple(before)
        assert job["cpu_sec"] == pytest.approx(150.501) and job["state"] == "COMPLETED"
        assert job["parser_version"] == collect.slurm.PARSER_VERSION
        assert {s["step_id"] for s in steps} == {"batch", "extern"}
        assert sum(s["cpu_sec"] for s in steps) == pytest.approx(150.501)

    def test_migrated_rows_are_left_alone(self, env, root, capsys):
        import v2_schema

        path = str(root / "profiles.db")
        v2_schema.create(path)
        v2_schema.insert_job(
            path,
            {"job_id": "9", "user": "u", "job_name": "old", "state": "COMPLETED", "flags": "[]",
             "end_time": "2026-05-01T08:10:00", "sacct_elapsed_sec": 600},
        )  # fmt: skip
        assert hpg("db", "migrate") == 0
        capsys.readouterr()
        assert hpg("db", "reparse") == 0
        assert "Re-derived 0 of 1 stored jobs" in capsys.readouterr().out
        with db.reader(path) as conn:
            assert db.find_jobs(conn, "9")[0]["source"] == "legacy"

    def test_needs_a_database(self, env, capsys):
        assert hpg("db", "reparse") == 1


class TestSchedule:
    @pytest.fixture
    def sched(self, env, root):
        (root / "bin").mkdir()
        (root / "bin" / "harvest.sh").write_text("#!/bin/bash\n")
        env.install("scrontab", SCRONTAB_STUB)
        env.install("squeue")
        return env

    def test_dry_run_install_show_remove(self, sched, capsys):
        assert hpg("schedule", "install", "--dry-run") == 0
        assert "Would install" in capsys.readouterr().out and sched.scrontab_table() == ""
        assert hpg("schedule", "show") == 1
        assert hpg("schedule", "install", "--every", "day") == 0
        assert "17 3 * * *" in sched.scrontab_table()
        capsys.readouterr()
        assert hpg("schedule", "show") == 0
        assert "scrontab: 17 3 * * *" in capsys.readouterr().out
        assert hpg("schedule", "remove") == 0
        assert sched.scrontab_table() == ""

    def test_error_is_reported(self, env, root, capsys):
        assert hpg("schedule", "install") == 1
        assert "launcher not found" in capsys.readouterr().err


class TestRecommend:
    @pytest.fixture
    def stored(self, env, monkeypatch, tmp_path, capsys):
        """Six finished runs of one job: 64 GB and 16 CPUs requested, 10 GB and 3 cores used.

        The peak is the batch step plus the extern step, a little over 10 GB,
        so 1.2 times it rounds up to 13 GB.
        """
        import time

        now = int(time.time())
        rows = []
        for i in range(6):
            rows += simple_job(
                100 + i,
                name="mplx12k-spleen",
                submit_line="sbatch run_mplx.sh",
                mem="64G",
                cpus=16,
                timelimit=2880,
                elapsed=3600,
                submit=now - (7 - i) * 12 * 3600,
                batch_max_rss="10485760K",
                batch_total_cpu="03:00:00",
            )
        env.sacct_default(rows)
        assert hpg("harvest") == 0
        monkeypatch.setenv("USER", "smith6jt")
        work = tmp_path / "work"
        work.mkdir()
        (work / "run_mplx.sh").write_text(
            "#!/bin/bash\n#SBATCH --job-name=mplx12k-spleen\n#SBATCH --mem=64G -c 16 -t 2-0\n"
            "python run.py\n"
        )
        monkeypatch.chdir(work)
        capsys.readouterr()
        return env

    def test_script(self, stored, capsys):
        assert hpg("recommend", "run_mplx.sh") == 0
        out = capsys.readouterr().out
        assert "6 runs named mplx12k-spleen by smith6jt" in out
        memory = next(line for line in out.splitlines() if line.startswith("Memory"))
        assert "64 GB" in memory and "13 GB" in memory and "-80%" in memory
        assert "#SBATCH --mem=13G" in out and "#SBATCH --cpus-per-task=4" in out
        assert out.rstrip().splitlines()[-1].startswith("Data as of ")

    def test_sbatch_options_are_read_as_sbatch_reads_them(self, stored, capsys):
        """Options before the script override it; what follows the script is the script's."""
        assert hpg("recommend", "--json", "--mem=8G", "-c", "2", "run_mplx.sh", "--json") == 0
        rec = json.loads(capsys.readouterr().out)
        assert rec["mem"]["requested"] == 8192 and rec["cpus"]["requested"] == 2
        assert rec["mem"]["action"] == "raise" and rec["mem"]["suggested"] == 13 * 1024
        assert rec["target"]["request"]["time_min"] == 2880
        assert rec["data"]["as_of_ts"] and rec["source"] == "history"

    def test_name(self, stored, capsys):
        assert hpg("recommend", "--name", "mplx12k-spleen") == 0
        out = capsys.readouterr().out
        assert "64 GB (last run)" in out and "#SBATCH --mem=13G" in out

    def test_other_user(self, stored, capsys):
        assert hpg("recommend", "--user", "bjones", "--name", "mplx12k-spleen") == 3
        assert "no comparable runs are stored" in capsys.readouterr().out

    def test_like_a_stored_job(self, stored, capsys):
        assert hpg("recommend", "--like", "103") == 0
        assert "like job 103" in capsys.readouterr().out
        assert hpg("recommend", "--like", "999") == 3
        assert "no stored job 999" in capsys.readouterr().err

    def test_wrap_without_history_exits_3(self, stored, capsys):
        assert hpg("recommend", "--wrap", "python never_run.py") == 3
        out = capsys.readouterr().out
        assert "smith6jt/wrap/cmd:python:never_run.py" in out and "#SBATCH" not in out

    def test_days_narrows_the_window(self, stored, capsys):
        assert hpg("recommend", "--days", "1", "--name", "mplx12k-spleen") == 3

    @pytest.mark.parametrize(
        "argv",
        [
            [],
            ["--name", "a", "run_mplx.sh"],
            ["--name", "a", "--kind", "vscode"],
            ["--kind", "batch"],
            ["missing.sh"],
            ["--mem=4G"],
        ],
    )
    def test_usage_errors(self, stored, capsys, argv):
        assert hpg("recommend", *argv) == 2
        assert "hpg recommend:" in capsys.readouterr().err

    def test_refreshes_stale_data_unless_told_not_to(self, stored, root, capsys):
        conn = sqlite3.connect(str(root / "profiles.db"))
        conn.execute("UPDATE meta SET value = value - 3600 WHERE key='watermark_ts'")
        conn.commit()
        conn.close()
        calls = len(stored.sacct_calls())
        assert hpg("recommend", "--no-refresh", "--name", "mplx12k-spleen") == 0
        assert len(stored.sacct_calls()) == calls
        assert hpg("recommend", "--name", "mplx12k-spleen") == 0
        refreshed = len(stored.sacct_calls())
        assert refreshed > calls
        assert hpg("recommend", "--name", "mplx12k-spleen") == 0
        assert len(stored.sacct_calls()) == refreshed, "fresh again: sacct is not asked twice"

    def test_no_database(self, env, capsys):
        env.remove("sacct")
        assert hpg("recommend", "--name", "x") == 1
        assert "no database" in capsys.readouterr().err

    def test_help_does_not_need_a_job(self, capsys):
        with pytest.raises(SystemExit) as exit_info:
            hpg("recommend", "--help")
        assert exit_info.value.code == 0
        assert "hpg recommend --like 51598968" in capsys.readouterr().out


class TestPassThrough:
    def test_hpg_options_come_first(self):
        own, rest = cli.split_passthrough(
            "recommend", ["--json", "--user", "u", "--days=30", "-c", "4", "job.sh", "--json"]
        )
        assert own == ["--json", "--user", "u", "--days=30"]
        assert rest == ["-c", "4", "job.sh", "--json"]

    def test_explicit_separator(self):
        own, rest = cli.split_passthrough("recommend", ["--json", "--", "--name", "x.sh"])
        assert own == ["--json"] and rest == ["--name", "x.sh"]

    def test_nothing_to_pass(self):
        assert cli.split_passthrough("recommend", ["--name", "x"]) == (["--name", "x"], [])


class TestBacktest:
    def test_report_and_json(self, env, capsys):
        import time

        now = int(time.time())
        rows = []
        for i in range(6):
            rows += simple_job(
                200 + i,
                name="align",
                mem="64G",
                elapsed=3600,
                submit=now - (7 - i) * 12 * 3600,
                batch_max_rss="10485760K",
            )
        env.sacct_default(rows)
        assert hpg("harvest") == 0
        capsys.readouterr()
        assert hpg("backtest", "--no-refresh", "--set", "mem_headroom=1.5") == 0
        out = capsys.readouterr().out
        assert "Backtest of 6 jobs" in out and "custom" in out and "Data as of" in out
        assert hpg("backtest", "--no-refresh", "--json") == 0
        data = json.loads(capsys.readouterr().out)
        configured = next(p for p in data["policies"] if p["name"] == "configured")
        assert configured["mem"]["sized"] == 3 and configured["mem"]["short"] == 0
        assert hpg("backtest", "--no-refresh", "--user", "nobody") == 3
        assert hpg("backtest", "--no-refresh", "--set", "bogus=1") == 2
        assert "--set takes KEY=VALUE" in capsys.readouterr().err


def test_version(capsys):
    with pytest.raises(SystemExit) as exit_info:
        hpg("--version")
    assert exit_info.value.code == 0
    assert capsys.readouterr().out.startswith("hpg 3.")


def test_no_command_prints_help(capsys):
    assert hpg() == 2
    assert "COMMAND" in capsys.readouterr().out


def test_a_harvest_also_ingests_profiles():
    from lib import profile

    assert cli._ingest_hook() is profile.ingest_spool
    assert collect.DAY == DAY


class TestShim:
    """bin/hpg: what a user actually runs."""

    SHIM = REPO / "bin" / "hpg"

    def test_stays_valid_for_old_interpreters(self):
        """So that it can say "needs Python 3.9" instead of dying on a syntax error."""
        import ast

        ast.parse(self.SHIM.read_text(), feature_version=(3, 6))

    def test_runs_without_any_third_party_package(self, root):
        done = subprocess.run(
            [sys.executable, "-I", "-S", str(self.SHIM), "--version"],
            stdout=subprocess.PIPE,
            universal_newlines=True,
            timeout=60,
        )
        assert done.returncode == 0 and done.stdout.startswith("hpg 3.")

    def test_found_through_a_symlink(self, root, tmp_path):
        link = tmp_path / "hpg"
        link.symlink_to(self.SHIM)
        done = subprocess.run(
            [sys.executable, str(link), "status", "--no-sacct"],
            stdout=subprocess.PIPE,
            universal_newlines=True,
            timeout=60,
        )
        assert done.returncode == 1 and "STATUS: MISSING" in done.stdout
