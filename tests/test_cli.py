"""Tests for the hpg command line (lib/cli.py), run in-process."""

import json
import os
import sqlite3

import pytest
from conftest import SCRONTAB_STUB
from sacct_fixture import simple_job

from lib import cli, collect, db

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
        env.sacct_default(recent(1) + recent(2, user="ebutterworth"))
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
        env.sacct_default(recent(1) + recent(2, user="ebutterworth"))
        hpg("harvest")
        capsys.readouterr()
        assert hpg("status", "--no-sacct") == 0
        out = capsys.readouterr().out
        assert "STATUS: OK" in out and "Last harvest" in out
        assert "smith6jt 1" in out and "ebutterworth 1" in out

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
        from lib import legacy_db

        path = str(root / "profiles.db")
        legacy_db.init_db(path)
        legacy_db.insert_job(
            {"job_id": "1", "user": "u", "job_name": "j", "state": "COMPLETED", "flags": "[]"},
            db_path=path,
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


def test_version(capsys):
    with pytest.raises(SystemExit) as exit_info:
        hpg("--version")
    assert exit_info.value.code == 0
    assert capsys.readouterr().out.startswith("hpg 3.")


def test_no_command_prints_help(capsys):
    assert hpg() == 2
    assert "COMMAND" in capsys.readouterr().out


def test_refresh_hook_is_optional():
    assert cli._ingest_hook() is None or callable(cli._ingest_hook())
    assert collect.DAY == DAY
