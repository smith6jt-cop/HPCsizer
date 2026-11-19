"""Tests for lib/schedule.py: the scrontab entry and the single-job chain."""

import pytest
from conftest import SCRONTAB_STUB

from lib import schedule


@pytest.fixture
def sched(cfg, stubs, root):
    (root / "bin").mkdir()
    (root / "bin" / "harvest.sh").write_text("#!/bin/bash\n")
    stubs.install("scrontab", SCRONTAB_STUB)
    stubs.install("squeue")
    stubs.install("sbatch")
    stubs.install("scancel")
    cfg.sched_qos = "maigan"
    cfg.sched_partition = "hpg-default"
    cfg.mail_user = "owner@example.edu"
    return cfg


class TestScronBlock:
    def test_entry_content(self, sched):
        block = schedule.scron_block(sched, "hour")
        lines = block.splitlines()
        assert lines[0] == schedule.BEGIN and lines[-1] == schedule.END
        assert "#SCRON --job-name=hpcsizer-harvest" in lines
        assert "#SCRON --account=maigan" in lines  # from the harvested account
        assert "#SCRON --qos=maigan" in lines and "#SCRON --partition=hpg-default" in lines
        assert "#SCRON --time=00:20:00" in lines and "#SCRON --mail-type=FAIL" in lines
        assert "#SCRON --mail-user=owner@example.edu" in lines
        assert lines[-2] == "17 * * * * %s/bin/harvest.sh" % sched.root

    def test_job_name_keeps_the_internal_prefix(self):
        """The harvest job is excluded from the data by its name prefix."""
        from lib.db import is_internal_job

        assert is_internal_job(schedule.JOB_NAME)

    def test_no_cluster_specific_option_is_hard_coded(self, sched):
        sched.sched_qos = sched.sched_partition = sched.sched_account = ""
        block = schedule.scron_block(sched)
        assert "--qos" not in block and "--partition" not in block and "--account" not in block

    @pytest.mark.parametrize("every,cron", [("15min", "*/15 * * * *"), ("day", "17 3 * * *")])
    def test_intervals(self, sched, every, cron):
        assert cron in schedule.scron_block(sched, every)

    def test_unknown_interval(self, sched):
        with pytest.raises(schedule.ScheduleError):
            schedule.scron_block(sched, "fortnight")


class TestScrontabInstall:
    def test_installs_into_an_empty_scrontab(self, sched, stubs):
        schedule.install(sched, "hour")
        table = stubs.scrontab_table()
        assert table.count(schedule.BEGIN) == 1 and "17 * * * *" in table
        assert schedule.describe(sched)["mode"] == "scrontab"

    def test_other_entries_are_preserved_and_reinstall_replaces(self, sched, stubs):
        (stubs.dir / "scrontab.table").write_text("#SCRON -t 1:00\n@daily /home/u/mine.sh\n")
        schedule.install(sched, "hour")
        schedule.install(sched, "day")
        table = stubs.scrontab_table()
        assert "@daily /home/u/mine.sh" in table
        assert table.count(schedule.BEGIN) == 1
        assert "17 3 * * *" in table and "17 * * * *" not in table

    def test_dry_run_changes_nothing(self, sched, stubs):
        text = schedule.install(sched, "hour", dry_run=True)
        assert schedule.BEGIN in text and stubs.scrontab_table() == ""
        assert stubs.calls("scrontab") == []

    def test_remove_leaves_the_rest(self, sched, stubs):
        (stubs.dir / "scrontab.table").write_text("@daily /home/u/mine.sh\n")
        schedule.install(sched, "hour")
        assert schedule.remove(sched) == ["scrontab entry removed"]
        assert stubs.scrontab_table() == "@daily /home/u/mine.sh\n"
        assert schedule.describe(sched)["mode"] == "none"

    def test_rejected_entry_is_an_error(self, sched, stubs):
        stubs.install("scrontab", '#!/bin/bash\nif [[ "$1" == "-l" ]]; then exit 0; fi\n'
                      'echo "scrontab: error: Invalid qos" >&2; exit 1\n')  # fmt: skip
        with pytest.raises(schedule.ScheduleError) as err:
            schedule.install(sched, "hour")
        assert "Invalid qos" in str(err.value)

    def test_missing_launcher(self, sched, root):
        (root / "bin" / "harvest.sh").unlink()
        with pytest.raises(schedule.ScheduleError):
            schedule.install(sched)


class TestChain:
    def test_one_job_with_singleton_dependency(self, sched, stubs):
        stubs.output("sbatch", "4242\n")
        assert "4242" in schedule.install(sched, "hour", mode="chain")
        (call,) = stubs.calls("sbatch")
        assert "--dependency=singleton" in call and "--job-name=hpcsizer-harvest" in call
        assert "HPCSIZER_ROOT=%s" % sched.root in call
        assert call.endswith("bin/harvest.sh --chain 1hour")
        assert "watchdog" not in call

    def test_refuses_a_second_chain(self, sched, stubs):
        stubs.output("squeue", "4242 PENDING\n")
        with pytest.raises(schedule.ScheduleError):
            schedule.install(sched, mode="chain")
        assert stubs.calls("sbatch") == []
        assert schedule.describe(sched) == {"mode": "chain", "detail": "4242 PENDING"}

    def test_remove_cancels_chain_jobs(self, sched, stubs):
        stubs.output("squeue", "4242 PENDING\n")
        assert schedule.remove(sched) == ["chain job(s) cancelled"]
        assert any("--name=hpcsizer-harvest" in c for c in stubs.calls("scancel"))


def test_describe_without_slurm(cfg, stubs):
    cfg.scrontab = "no-such-scrontab"
    cfg.squeue = "no-such-squeue"
    assert schedule.describe(cfg)["mode"] == "unknown"
