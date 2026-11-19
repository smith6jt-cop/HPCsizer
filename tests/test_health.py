"""Tests for hpg status and hpg preflight."""

import os
import sqlite3

from conftest import SCRONTAB_STUB
from sacct_fixture import BASE_TS, job_row, simple_job

from lib import collect, health

DAY = 86400
NOW = BASE_TS + 10 * DAY


def harvest(cfg, now=NOW):
    return collect.harvest(cfg, now=now, log=lambda _m: None, sleep=lambda _s: None)


class TestStatus:
    def test_missing_database(self, cfg, stubs):
        st = health.status(cfg, now=NOW, with_sacct=False)
        assert st["state"] == "missing" and st["ok"] is False
        assert any("hpg harvest" in p for p in st["problems"])

    def test_version_2_database(self, cfg, stubs):
        conn = sqlite3.connect(cfg.db_path)
        conn.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY)")
        conn.close()
        st = health.status(cfg, now=NOW, with_sacct=False)
        assert st["state"] == "needs-migration"
        assert any("hpg db migrate" in p for p in st["problems"])

    def test_ok_then_stale(self, cfg, stubs):
        stubs.sacct_default(
            simple_job(1, submit=NOW - DAY) + simple_job(2, submit=NOW - DAY, user="bjones")
        )
        cfg.first_days = 5
        harvest(cfg)
        fresh = health.status(cfg, now=NOW + 600, with_sacct=False)
        assert fresh["state"] == "ok" and fresh["ok"] is True
        assert fresh["age_sec"] == 600 and fresh["rows"]["total"] == 2
        assert dict(fresh["recent"]["users"]) == {"smith6jt": 1, "bjones": 1}

        stale = health.status(cfg, now=NOW + 27 * 3600, with_sacct=False)
        assert stale["state"] == "stale" and stale["ok"] is False
        assert any("no successful harvest for 27.0 h" in p for p in stale["problems"])

    def test_single_user_is_called_out(self, cfg, stubs):
        """What five months of collecting only the owner's jobs would have shown."""
        rows = []
        for i in range(25):
            rows += simple_job(100 + i, submit=NOW - DAY)
        stubs.sacct_default(rows)
        cfg.first_days = 5
        harvest(cfg)
        st = health.status(cfg, now=NOW + 60, with_sacct=False)
        assert any("only smith6jt has jobs" in p for p in st["problems"])

    def test_missing_jobs_fail_the_status(self, cfg, stubs, monkeypatch):
        stubs.sacct_default(simple_job(1, submit=NOW - DAY))
        cfg.first_days = 5
        harvest(cfg)
        monkeypatch.setattr(
            collect,
            "coverage",
            lambda *_a, **_k: {
                "available": True,
                "days": 30,
                "expected": 3,
                "stored": 1,
                "missing": 2,
                "missing_examples": [7, 8],
            },
        )
        st = health.status(cfg, now=NOW + 60)
        assert st["ok"] is False and any("2 finished job(s) in sacct" in p for p in st["problems"])

    def test_schedule_and_spool_are_reported(self, cfg, stubs, root):
        stubs.install("scrontab", SCRONTAB_STUB)
        stubs.install("squeue")
        st = health.status(cfg, now=NOW, with_sacct=False)
        assert st["schedule"]["mode"] == "none"
        assert any("hpg schedule install" in p for p in st["problems"])
        assert st["spool"]["exists"] is False
        (root / "spool").mkdir()
        (root / "spool").chmod(0o3770)
        spool = health.status(cfg, now=NOW, with_sacct=False)["spool"]
        assert spool["shared"] is True and spool["mode"] == "drwxrws--T"


class TestPreflight:
    def by_name(self, checks):
        return {c["check"]: c for c in checks}

    def test_all_pass_with_working_slurm(self, cfg, stubs, root):
        rows = simple_job(1, submit=NOW - 3600) + simple_job(
            2, submit=NOW - 7200, user="bjones", submit_line="sbatch --wrap python x.py"
        )
        rows.append(job_row(3, state="PENDING", started=False, ended=False, submit=NOW - 60))
        stubs.sacct_default(rows)
        stubs.install("scrontab", SCRONTAB_STUB)
        stubs.install("scontrol")
        stubs.output(
            "scontrol",
            "MaxJobId               = 67043328\nFirstJobId             = 1\n"
            "ScronParameters        = enable\nMaxMemPerCPU           = UNLIMITED\n",
        )
        checks = self.by_name(health.preflight(cfg, now=NOW))
        assert checks["interpreter"]["status"] == "PASS"
        assert checks["sacct query"]["status"] == "PASS"
        assert checks["epoch timestamps"]["status"] == "PASS"
        assert checks["other members visible"]["status"] == "PASS"
        assert "bjones" in checks["other members visible"]["detail"]
        assert checks["value forms"]["status"] == "PASS"
        assert "--wrap" in checks["SubmitLine form"]["detail"]
        assert checks["full-retention query"]["status"] == "PASS"
        assert checks["unfinished jobs query"]["status"] == "PASS"
        assert checks["unfinished jobs query"]["detail"].startswith("1 pending or running job(s)")
        assert "MaxJobId=67043328" in checks["cluster limits"]["detail"]
        assert checks["scrontab enabled"]["status"] == "PASS"
        assert checks["scrontab answers"]["status"] == "PASS"
        assert checks["compute node"]["status"] == "SKIP"
        assert not [c for c in checks.values() if c["status"] == "FAIL"]

    def test_rejected_field_list_fails(self, cfg, stubs):
        stubs.sacct_plan("fail", "fail")
        checks = self.by_name(health.preflight(cfg, now=NOW))
        assert checks["sacct query"]["status"] == "FAIL"
        assert "connection refused" in checks["sacct query"]["detail"]

    def test_only_own_jobs_visible_is_a_warning(self, cfg, stubs):
        stubs.sacct_default(simple_job(1, submit=NOW - 3600))
        checks = self.by_name(health.preflight(cfg, now=NOW))
        assert checks["other members visible"]["status"] == "WARN"

    def test_no_account_stops_early(self, cfg, stubs):
        cfg.accounts = []
        checks = self.by_name(health.preflight(cfg, now=NOW))
        assert checks["accounts"]["status"] == "FAIL" and stubs.sacct_calls() == []

    def test_compute_probe(self, cfg, stubs):
        stubs.sacct_default(simple_job(1, submit=NOW - 3600))
        stubs.install("srun")
        stubs.output(
            "srun",
            "0::/system.slice/slurmstepd.scope/job_1/step_0\npython (3, 9, 18) sqlite 3.34.1\n",
        )
        checks = self.by_name(health.preflight(cfg, compute=True, now=NOW))
        assert checks["compute node"]["status"] == "PASS"
        assert "cgroup v2" in checks["compute node"]["detail"]
        assert "--account=maigan" in stubs.calls("srun")[0]


class TestCodePermissions:
    def test_code_that_others_can_change_is_called_out(self, cfg, stubs, root):
        """A profiled job runs bin/monitor.py as its submitter: nobody else may edit it."""
        (root / "bin").mkdir()
        sampler = root / "bin" / "monitor.py"
        sampler.write_text("# sampler\n")
        os.chmod(str(root), 0o750)
        os.chmod(str(root / "bin"), 0o750)
        sampler.chmod(0o640)
        assert health.writable_by_others(cfg) == []
        sampler.chmod(0o664)
        os.chmod(str(root / "bin"), 0o770)
        assert health.writable_by_others(cfg) == [str(root / "bin"), str(sampler)]
        os.makedirs(cfg.spool_dir)
        st = health.status(cfg, with_sacct=False)
        assert any("writable by others" in problem for problem in st["problems"])


class TestWhoCanReadTheDatabase:
    """Job names, command lines and working directories of every member are in it."""

    def test_open_all_the_way_up_is_called_out(self, cfg, stubs, tmp_path):
        harvest(cfg)
        os.chmod(cfg.root, 0o755)
        assert health.readable_by_others(cfg, top=str(tmp_path)) is True
        report = health.status(cfg, now=NOW, with_sacct=False)
        text = " ".join(report["problems"])
        assert ("can be read by every user" in text) == health.readable_by_others(cfg)

    def test_a_directory_others_cannot_enter(self, cfg, stubs, tmp_path):
        os.chmod(cfg.root, 0o750)
        try:
            harvest(cfg)
            assert health.readable_by_others(cfg, top=str(tmp_path)) is False
        finally:
            os.chmod(cfg.root, 0o755)

    def test_a_closed_directory_further_up_is_enough(self, cfg, stubs, tmp_path):
        harvest(cfg)
        os.chmod(cfg.root, 0o755)
        outer = os.path.dirname(cfg.root)
        os.chmod(outer, 0o700)
        try:
            assert health.readable_by_others(cfg, top=os.path.dirname(outer)) is False
        finally:
            os.chmod(outer, 0o755)

    def test_preflight_has_a_line_for_it(self, cfg, stubs):
        harvest(cfg)
        checks = {c["check"]: c for c in health.preflight(cfg, now=NOW)}
        assert checks["database privacy"]["status"] in ("PASS", "WARN")
