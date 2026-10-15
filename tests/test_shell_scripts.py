"""Tests for bin/scheduler.sh and bin/harvest.sh.

Both are bash, so they are driven through subprocess with stubbed `sbatch`,
`squeue` and `sacct` on PATH. Each stub records its invocations to a file so the
tests can assert on what the script actually submitted.
"""

import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

REPO = Path(__file__).parent.parent
SCHEDULER = REPO / "bin" / "scheduler.sh"
HARVEST = REPO / "bin" / "harvest.sh"


def _stub(bindir: Path, name: str, body: str) -> None:
    path = bindir / name
    path.write_text("#!/usr/bin/env bash\n" + body + "\n")
    path.chmod(0o755)


@pytest.fixture
def env(tmp_path):
    """A fake repo root plus a bin dir prepended to PATH for stubs."""
    root = tmp_path / "repo"
    (root / "bin").mkdir(parents=True)
    shutil.copy(SCHEDULER, root / "bin" / "scheduler.sh")
    shutil.copy(HARVEST, root / "bin" / "harvest.sh")
    # harvest.sh imports lib.db via $PYTHONPATH
    (root / "lib").mkdir()
    for f in ("__init__.py", "db.py"):
        src = REPO / "lib" / f
        if src.exists():
            shutil.copy(src, root / "lib" / f)

    bindir = tmp_path / "stubbin"
    bindir.mkdir()
    calls = tmp_path / "calls.txt"

    environ = dict(os.environ)
    environ["PATH"] = f"{bindir}:{environ['PATH']}"
    environ["HPCSIZER_ROOT"] = str(root)
    environ["HPCSIZER_DB"] = str(tmp_path / "test.db")
    environ["STUB_CALLS"] = str(calls)
    # Keep the real retry count but drop the backoff, so the suite stays fast.
    environ["HPCSIZER_HARVEST_RETRY_DELAY"] = "0"
    environ.pop("HPCSIZER_ACCT", None)
    environ.pop("SLURM_JOB_ID", None)
    return {
        "root": root,
        "bindir": bindir,
        "calls": calls,
        "environ": environ,
        "log": root / "logs" / "scheduler.log",
    }


def _run(script: Path, env, *args, mode=None):
    cmd = ["bash", str(script)]
    if mode:
        cmd.append(mode)
    cmd.extend(args)
    return subprocess.run(cmd, env=env["environ"], capture_output=True, text=True, timeout=120)


def _calls(env):
    if not env["calls"].exists():
        return []
    return [ln for ln in env["calls"].read_text().splitlines() if ln.strip()]


# ---------------------------------------------------------------------------
# scheduler.sh — watchdog mode
# ---------------------------------------------------------------------------


class TestWatchdogHealthCheck:
    def test_stands_down_when_squeue_fails(self, env):
        """squeue failing must not read as "no primaries"; that would restart
        the chain on no information."""
        _stub(env["bindir"], "squeue", 'echo "squeue: error: unavailable" >&2; exit 1')
        _stub(env["bindir"], "sbatch", 'echo "sbatch $*" >> "$STUB_CALLS"; echo 999')
        _run(SCHEDULER, env, mode="watchdog")
        log = env["log"].read_text()
        assert "chain state unknown, standing down" in log
        assert "Chain appears broken" not in log
        assert _calls(env) == []

    def test_quiet_when_a_primary_is_queued(self, env):
        _stub(env["bindir"], "squeue", "echo 12345")
        _stub(env["bindir"], "sbatch", 'echo "sbatch $*" >> "$STUB_CALLS"; echo 999')
        _run(SCHEDULER, env, mode="watchdog")
        log = env["log"].read_text()
        assert "Chain is healthy" in log
        assert _calls(env) == []

    def test_restarts_when_no_primary_is_queued(self, env):
        _stub(env["bindir"], "squeue", "exit 0")  # success, no output
        _stub(env["bindir"], "sbatch", 'echo "sbatch $*" >> "$STUB_CALLS"; echo 999')
        _run(SCHEDULER, env, mode="watchdog")
        log = env["log"].read_text()
        assert "Chain appears broken" in log
        calls = _calls(env)
        # a primary, a short watchdog, and a long backstop
        assert len(calls) == 3
        assert sum("hpcsizer-scheduler" in c for c in calls) == 1
        assert sum("hpcsizer-watchdog" in c for c in calls) == 2
        assert any("now+6hours" in c for c in calls)

    def test_own_job_id_excluded_from_the_count(self, env):
        """A watchdog overlapping a running primary must not count that primary
        as proof of health if it is the watchdog's own allocation."""
        _stub(env["bindir"], "squeue", "echo 777")
        _stub(env["bindir"], "sbatch", 'echo "sbatch $*" >> "$STUB_CALLS"; echo 999')
        env["environ"]["SLURM_JOB_ID"] = "777"
        _run(SCHEDULER, env, mode="watchdog")
        log = env["log"].read_text()
        assert "Chain appears broken" in log, "own job id should not count"


# ---------------------------------------------------------------------------
# scheduler.sh — primary mode
# ---------------------------------------------------------------------------


class TestPrimaryResubmit:
    def test_resubmit_precedes_shared_filesystem_io(self, env):
        """The resubmit must happen before the log directory is touched: a stall
        there is what killed the chain on 2026-10-14."""
        _stub(env["bindir"], "squeue", "exit 0")
        _stub(
            env["bindir"],
            "sbatch",
            'echo "sbatch $*" >> "$STUB_CALLS"\n'
            'if [[ ! -d "$HPCSIZER_ROOT/logs" ]]; then echo "BEFORE_LOGDIR" >> "$STUB_CALLS"; fi\n'
            "echo 999",
        )
        _stub(env["bindir"], "sacct", "exit 1")
        _run(SCHEDULER, env)
        calls = _calls(env)
        assert "BEFORE_LOGDIR" in calls, "first submission ran after logs/ existed"
        # and the buffered message still reaches the log
        assert "Submitted primary job 999" in env["log"].read_text()

    def test_submits_successor_and_watchdog(self, env):
        _stub(env["bindir"], "squeue", "exit 0")
        _stub(env["bindir"], "sbatch", 'echo "sbatch $*" >> "$STUB_CALLS"; echo 999')
        _stub(env["bindir"], "sacct", "exit 1")
        _run(SCHEDULER, env)
        submits = [c for c in _calls(env) if c.startswith("sbatch")]
        assert sum("hpcsizer-scheduler" in c for c in submits) == 1
        assert sum("hpcsizer-watchdog" in c for c in submits) >= 1
        assert any("now+15minutes" in c for c in submits)
        assert any("now+18minutes" in c for c in submits)


# ---------------------------------------------------------------------------
# harvest.sh — retry and resumable lookback
# ---------------------------------------------------------------------------

_SACCT_ROW = (
    "60000001|smith6jt|seurat_run|maigan|maigan|COMPLETED|"
    "2026-10-01T10:00:00|2026-10-01T10:01:00|2026-10-01T11:00:00|"
    "64Gn|8|12:00:00|billing=8,cpu=8|20480K|01:00:00|04:00:00"
)


class TestHarvestRetry:
    def test_succeeds_first_try(self, env):
        _stub(env["bindir"], "sacct", f'echo "{_SACCT_ROW}"')
        r = _run(HARVEST, env)
        assert r.returncode == 0, r.stderr
        assert "Upserted 1 jobs" in r.stdout
        assert (env["root"] / "logs" / ".harvest_state").exists()

    def test_retries_then_succeeds(self, env):
        _stub(
            env["bindir"],
            "sacct",
            'n=$(cat "$STUB_CALLS" 2>/dev/null | wc -l); echo x >> "$STUB_CALLS"\n'
            f'if (( n < 2 )); then echo "sacct: error: transient" >&2; exit 1; fi\n'
            f'echo "{_SACCT_ROW}"',
        )
        r = _run(HARVEST, env)
        assert r.returncode == 0, r.stderr
        assert "attempt 1/3 failed" in r.stderr
        assert "attempt 2/3 failed" in r.stderr
        assert "Upserted 1 jobs" in r.stdout

    def test_exhausted_retries_do_not_advance_the_watermark(self, env):
        _stub(env["bindir"], "sacct", 'echo "sacct: error: down" >&2; exit 1')
        r = _run(HARVEST, env)
        assert r.returncode == 1
        assert "all 3 attempts failed" in r.stderr
        assert not (env["root"] / "logs" / ".harvest_state").exists()


class TestHarvestResumableLookback:
    def test_resumes_from_the_last_success(self, env):
        # Relative to now: a fixed date would drift past the 7-day cap over time.
        recent = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S")
        (env["root"] / "logs").mkdir(parents=True, exist_ok=True)
        (env["root"] / "logs" / ".harvest_state").write_text(recent + "\n")
        _stub(env["bindir"], "sacct", 'echo "SINCE $*" >> "$STUB_CALLS"; exit 1')
        _run(HARVEST, env)
        assert any(recent in c for c in _calls(env)), (
            "should query from the recorded watermark, not a fixed 20 minutes"
        )

    def test_clamps_an_ancient_watermark(self, env):
        (env["root"] / "logs").mkdir(parents=True, exist_ok=True)
        (env["root"] / "logs" / ".harvest_state").write_text("2020-01-01T00:00:00\n")
        _stub(env["bindir"], "sacct", 'echo "SINCE $*" >> "$STUB_CALLS"; exit 1')
        r = _run(HARVEST, env)
        assert "predates the 7-day cap" in r.stdout
        assert not any("2020-01-01" in c for c in _calls(env))

    def test_ignores_a_corrupt_watermark(self, env):
        (env["root"] / "logs").mkdir(parents=True, exist_ok=True)
        (env["root"] / "logs" / ".harvest_state").write_text("not-a-timestamp")
        _stub(env["bindir"], "sacct", 'echo "SINCE $*" >> "$STUB_CALLS"; exit 1')
        r = _run(HARVEST, env)
        assert r.returncode == 1  # sacct stub fails, but it must not crash earlier
        assert any("SINCE" in c for c in _calls(env))
