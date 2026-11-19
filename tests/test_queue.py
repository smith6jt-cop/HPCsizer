"""Tests for lib/queue.py: unfinished jobs next to the history of their family."""

import time

import pytest
from sacct_fixture import job_row, simple_job

from lib import cli, collect, queue
from lib.history import History, load

HOUR = 3600


def pending(jid, **kwargs):
    kwargs.setdefault("state", "PENDING")
    return [job_row(jid, started=False, ended=False, **kwargs)]


@pytest.fixture
def now():
    return int(time.time())


def finished(now, n=6, **kwargs):
    """``n`` finished runs: 64 GB, 16 CPUs and 2 days asked for; 10 GB, 3 cores and 1 h used."""
    rows = []
    for i in range(n):
        options = dict(
            name="mplx12k-spleen",
            submit_line="sbatch run_mplx.sh",
            mem="64G",
            cpus=16,
            timelimit=2880,
            elapsed=3600,
            submit=now - (n - i) * 12 * HOUR,
            batch_max_rss="10485760K",
            batch_total_cpu="03:00:00",
        )
        options.update(kwargs)
        rows += simple_job(100 + i, **options)
    return rows


def view(cfg, now):
    """Harvest what the stub serves, then build the queue view from the same output."""
    collect.harvest(cfg, now=now, log=lambda _m: None)
    jobs, _info = load(cfg)
    return queue.build(cfg, History(jobs), now)


class TestQuery:
    def test_asks_sacct_for_unfinished_jobs_of_every_user(self, cfg):
        cmd = queue.open_jobs_cmd(cfg)
        assert cmd[:4] == ["sacct", "-a", "-A", "maigan"]
        assert cmd[cmd.index("-s") + 1].startswith("PENDING,RUNNING")
        assert "-S" not in cmd, "with -s and no window sacct reports the present"

    def test_needs_an_account(self, cfg):
        cfg.accounts = []
        with pytest.raises(ValueError):
            queue.open_jobs_cmd(cfg)

    def test_only_unfinished_jobs_and_never_hpcsizers_own(self, cfg, stubs, now):
        rows = finished(now, 2)
        rows += pending(200, name="mplx12k-spleen", submit=now - 60)
        rows += pending(201, name="hpcsizer-harvest", submit=now - 60)
        rows.append(
            job_row(202, state="RUNNING", ended=False, submit=now - HOUR, elapsed=HOUR - 30)
        )
        stubs.sacct_default(rows)
        jobs = queue.fetch_open(cfg)
        assert sorted(j["job_id"] for j in jobs) == ["200", "202"]
        assert {j["family"] for j in jobs} == {
            "smith6jt/batch/mplx#k-spleen",
            "smith6jt/batch/cmd:job.sh",
        }

    def test_sacct_failure(self, cfg, stubs):
        stubs.sacct_plan("fail")
        with pytest.raises(collect.HarvestError):
            queue.fetch_open(cfg)


class TestAdvice:
    def test_pending_job_asking_for_more_than_its_family_used(self, cfg, stubs, now):
        rows = finished(now)
        rows += pending(
            200,
            name="mplx12k-spleen",
            submit_line="sbatch run_mplx.sh",
            mem="64G",
            cpus=16,
            timelimit=2880,
            submit=now - 60,
        )
        stubs.sacct_default(rows)
        result = view(cfg, now)
        (row,) = result["jobs"]
        assert row["state"] == "PENDING" and row["history"].startswith("6 runs, memory up to 10 GB")
        assert row["changes"] == {"cpus": "--cpus-per-task=4", "mem": "--mem=13G"}
        assert row["scontrol"] == "scontrol update JobId=200 MinMemoryNode=13312"
        text = queue.render_text(result)
        assert "1 pending" in text.splitlines()[0]
        assert (
            "scontrol update JobId=200 MinMemoryNode=13312   # mplx12k-spleen: memory 64 GB -> 13 GB (6 runs)"
            in text
        )
        assert "A different CPU count needs a resubmission." in text

    def test_time_limit_is_lowered_when_the_cpu_count_stays(self, cfg, stubs, now):
        rows = finished(now, cpus=4, batch_total_cpu="03:55:00")  # all four cores busy
        rows += pending(
            200, name="mplx12k-spleen", mem="64G", cpus=4, timelimit=2880, submit=now - 60
        )
        stubs.sacct_default(rows)
        (row,) = view(cfg, now)["jobs"]
        assert row["scontrol"] == "scontrol update JobId=200 MinMemoryNode=13312 TimeLimit=01:30:00"

    def test_pending_array_is_addressed_without_its_throttle(self, cfg, stubs, now):
        rows = finished(now)
        rows += pending(
            300,
            display="300_[4-50%5]",
            name="mplx12k-spleen",
            mem="64G",
            cpus=16,
            timelimit=2880,
            submit=now - 60,
        )
        stubs.sacct_default(rows)
        (row,) = view(cfg, now)["jobs"]
        assert row["job_id"] == "300_[4-50%5]"
        assert row["scontrol"].startswith("scontrol update JobId=300_[4-50] ")

    def test_pending_job_asking_for_less_than_its_family_needed(self, cfg, stubs, now):
        rows = finished(now, mem="8G", batch_max_rss="6291456K")
        rows += simple_job(
            150,
            name="mplx12k-spleen",
            mem="8G",
            cpus=16,
            timelimit=2880,
            state="OUT_OF_MEMORY",
            exit_code="0:125",
            submit=now - 2 * HOUR,
            elapsed=1200,
            batch_state="OUT_OF_MEMORY",
            batch_max_rss="8388608K",
        )
        rows += pending(
            200, name="mplx12k-spleen", mem="8G", cpus=16, timelimit=2880, submit=now - 60
        )
        stubs.sacct_default(rows)
        result = view(cfg, now)
        (row,) = result["jobs"]
        assert row["scontrol"] is None
        assert row["risks"] == [
            "asks for 8 GB; job 150 ran out of memory on %s with 8 GB requested -- resubmit with --mem=12G"
            % time.strftime("%Y-%m-%d", time.localtime(now - 2 * HOUR + 30 + 1200))
        ]
        assert "Pending jobs that ask for less than their family needed:" in queue.render_text(
            result
        )

    def test_running_jobs_get_no_command(self, cfg, stubs, now):
        rows = finished(now)
        rows.append(
            job_row(
                200,
                name="mplx12k-spleen",
                mem="64G",
                cpus=16,
                timelimit=2880,
                state="RUNNING",
                ended=False,
                submit=now - HOUR,
                elapsed=HOUR - 30,
            )
        )
        stubs.sacct_default(rows)
        result = view(cfg, now)
        (row,) = result["jobs"]
        assert row["scontrol"] is None and row["elapsed_sec"] == HOUR - 30
        assert "resized in place" not in queue.render_text(result)

    def test_job_without_history(self, cfg, stubs, now):
        stubs.sacct_default(finished(now, 1) + pending(200, name="brand-new", submit=now - 60))
        result = view(cfg, now)
        (row,) = result["jobs"]
        assert row["history"] == "no finished runs yet" and row["changes"] == {}
        assert queue.render_text(result, users=["nobody"]).count("\n") == 0

    def test_empty_queue(self, cfg, stubs, now):
        stubs.sacct_default(finished(now, 1))
        assert "no unfinished jobs" in queue.render_text(view(cfg, now))


class TestCommand:
    def test_works_before_anything_was_harvested(self, cfg, stubs, now, capsys):
        stubs.sacct_default(pending(200, name="brand-new", submit=now - 60))
        assert cli.main(["queue"]) == 0
        assert "no finished runs yet" in capsys.readouterr().out

    def test_json_and_failure(self, cfg, stubs, now, capsys):
        import json

        stubs.sacct_default(pending(200, name="brand-new", submit=now - 60))
        assert cli.main(["queue", "--json"]) == 0
        assert json.loads(capsys.readouterr().out)["states"] == {"PENDING": 1}
        stubs.sacct_plan("fail")
        assert cli.main(["queue"]) == 1
        assert "hpg queue:" in capsys.readouterr().err
