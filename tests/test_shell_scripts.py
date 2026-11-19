"""Tests for bin/harvest.sh, the launcher run by scrontab (or as a chain job).

It is bash, so it is driven through subprocess with stub Slurm commands on
PATH that record how they were called.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sacct_fixture import simple_job

REPO = Path(__file__).resolve().parent.parent
LAUNCHER = REPO / "bin" / "harvest.sh"


@pytest.fixture
def site(root, stubs, monkeypatch):
    """A root whose bin/ and lib/ point at the real code, with stub Slurm tools."""
    (root / "bin").symlink_to(REPO / "bin")
    (root / "lib").symlink_to(REPO / "lib")
    (root / "hpcsizer.ini").write_text("[collector]\nfirst_days = 5\n")
    monkeypatch.setenv("HPCSIZER_PYTHON", sys.executable)
    stubs.install("squeue")
    stubs.install("sbatch")
    return root


def launch(*args, env=None):
    return subprocess.run(
        ["bash", str(LAUNCHER), *args],
        env=env or dict(os.environ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=120,
    )


def recent(jid):
    import time

    return simple_job(jid, submit=int(time.time()) - 3600)


class TestScheduledRun:
    def test_runs_the_collector_and_logs(self, site, stubs):
        stubs.sacct_default(recent(1))
        result = launch()
        assert result.returncode == 0, result.stderr
        log = (site / "logs" / "harvest.log").read_text()
        assert "[harvest]" in log and "1 new" in log
        assert (site / "profiles.db").exists()
        assert result.stdout == "" and result.stderr == "", "everything goes to the log"
        assert stubs.calls("sbatch") == [], "plain mode never submits anything"

    def test_works_from_an_unrelated_directory_without_the_environment(self, site, stubs, tmp_path):
        """scrontab starts the job in $HOME with none of the user's variables."""
        stubs.sacct_default(recent(1))
        (site / "hpcsizer.ini").write_text(
            "[slurm]\naccounts = maigan\n[collector]\nfirst_days = 5\n"
        )
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(tmp_path),
            "STUB_DIR": os.environ["STUB_DIR"],
            "HPCSIZER_PYTHON": sys.executable,
            "HPCSIZER_TMPDIR": str(tmp_path),
        }
        result = subprocess.run(
            ["bash", str(site / "bin" / "harvest.sh")],
            env=env,
            cwd=str(tmp_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        # The account came from hpcsizer.ini beside the code, not from the shell.
        assert "-A maigan" in stubs.sacct_calls()[0]
        assert (site / "profiles.db").exists()
        assert not list(tmp_path.glob("core*")) and not list(tmp_path.glob("slurm-*.out"))

    def test_failure_with_no_data_fails_the_job(self, site, stubs):
        stubs.sacct_plan("fail", "fail", "fail")
        result = launch()
        assert result.returncode == 1
        log = (site / "logs" / "harvest.log").read_text()
        assert "failing the job" in log and "exit status 1" in log

    def test_log_is_rotated(self, site, stubs):
        stubs.sacct_default(recent(1))
        (site / "logs").mkdir()
        (site / "logs" / "harvest.log").write_text("x" * 5_000_100)
        assert launch().returncode == 0
        assert (site / "logs" / "harvest.log.1").stat().st_size == 5_000_100
        assert (site / "logs" / "harvest.log").stat().st_size < 10_000

    def test_core_dumps_are_disabled(self):
        assert "ulimit -c 0" in LAUNCHER.read_text()

    def test_unusable_interpreter_falls_back(self, site, stubs, monkeypatch):
        stubs.sacct_default(recent(1))
        monkeypatch.setenv("HPCSIZER_PYTHON", "/nonexistent/python")
        assert launch().returncode == 0


class TestChainMode:
    def test_queues_one_successor_before_touching_shared_storage(self, site, stubs):
        stubs.install(
            "sbatch",
            '#!/usr/bin/env bash\necho "sbatch $*" >> "$STUB_DIR/calls"\n'
            'if [[ ! -d "$HPCSIZER_ROOT/logs" ]]; then echo "BEFORE_LOGDIR" >> "$STUB_DIR/calls"; fi\n'
            "echo 999\n",
        )
        stubs.sacct_default(recent(1))
        assert launch("--chain", "1hour").returncode == 0
        calls = stubs.calls()
        submits = [c for c in calls if c.startswith("sbatch ")]
        assert len(submits) == 1
        assert "--dependency=singleton" in submits[0]
        assert "--begin=now+1hour" in submits[0]
        assert "--job-name=hpcsizer-harvest" in submits[0]
        assert submits[0].endswith("--chain 1hour")
        assert "BEFORE_LOGDIR" in calls, "the successor must be queued before logs/ is created"
        assert not any("watchdog" in c for c in calls)

    def test_does_not_queue_a_second_successor(self, site, stubs):
        """A duplicate chain collapses: the extra run finds a successor and adds none."""
        stubs.output("squeue", "4242\n")
        stubs.sacct_default(recent(1))
        assert launch("--chain", "1hour").returncode == 0
        assert stubs.calls("sbatch") == []
        assert "-t PENDING" in stubs.calls("squeue")[0]

    def test_resubmits_when_squeue_cannot_be_asked(self, site, stubs):
        stubs.returncode("squeue", 1)
        stubs.sacct_default(recent(1))
        assert launch("--chain", "1day").returncode == 0
        assert len(stubs.calls("sbatch")) == 1 and "--begin=now+1day" in stubs.calls("sbatch")[0]

    def test_harvest_still_runs_when_resubmission_fails(self, site, stubs):
        stubs.returncode("sbatch", 1)
        stubs.sacct_default(recent(1))
        assert launch("--chain", "1hour").returncode == 0
        assert (site / "profiles.db").exists()


def test_syntax():
    assert subprocess.run(["bash", "-n", str(LAUNCHER)]).returncode == 0
