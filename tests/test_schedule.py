"""Tests for lib/schedule.py: the scrontab entry and the single-job chain."""

import os

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


ENV_RECORDER = r"""#!/usr/bin/env bash
printf '%s %s\n' "$(basename "$0")" "$*" >> "$STUB_DIR/calls"
printf 'ROOT=%s\nOPTS=%s\n' "${HPCSIZER_ROOT:-}" "${HPCSIZER_SBATCH_OPTS:-}" > "$STUB_DIR/sbatch.env"
echo 4242
"""


class TestChain:
    def test_one_job_with_singleton_dependency(self, sched, stubs):
        stubs.output("sbatch", "4242\n")
        assert "4242" in schedule.install(sched, "hour", mode="chain")
        (call,) = stubs.calls("sbatch")
        assert "--dependency=singleton" in call and "--job-name=hpcsizer-harvest" in call
        assert "--export=ALL" in call
        assert call.endswith("bin/harvest.sh --chain 1hour")
        assert "watchdog" not in call

    def test_successors_inherit_the_root_and_the_sbatch_options(self, sched, stubs):
        """Each run queues the next from the node, without reading the configuration.

        Without this the second job of a chain had no account, time limit or
        output setting: only the first was submitted with them.
        """
        stubs.install("sbatch", ENV_RECORDER)
        schedule.install(sched, "hour", mode="chain")
        env = dict(
            line.split("=", 1) for line in (stubs.dir / "sbatch.env").read_text().splitlines()
        )
        assert env["ROOT"] == sched.root
        for option in ("--account=maigan", "--qos=maigan", "--time=00:20:00", "--mem=1gb",
                       "--output=/dev/null", "--mail-type=FAIL"):  # fmt: skip
            assert option in env["OPTS"].split()
        assert schedule.chain_environment(sched)["PATH"] == os.environ["PATH"]

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


class TestRemoveReportsFailures:
    def test_failed_cancellation_is_an_error_not_a_success(self, sched, stubs):
        """Saying "removed" while the chain keeps resubmitting itself would be worse than failing."""
        (stubs.dir / "scrontab.table").write_text("@daily /home/u/mine.sh\n")
        schedule.install(sched, "hour")
        stubs.output("squeue", "4242 PENDING\n")
        stubs.install("scancel", '#!/bin/bash\necho "scancel: error: Invalid job id" >&2\nexit 1\n')
        with pytest.raises(schedule.ScheduleError) as err:
            schedule.remove(sched)
        assert "chain: scancel: error: Invalid job id" in str(err.value)
        assert "done before that: scrontab entry removed" in str(err.value), "both are attempted"
        assert schedule.BEGIN not in stubs.scrontab_table()

    def test_failed_scrontab_update_is_an_error_and_the_chain_is_still_cancelled(
        self, sched, stubs
    ):
        schedule.install(sched, "hour")
        table = stubs.scrontab_table()
        stubs.install(
            "scrontab",
            '#!/bin/bash\nif [[ "$1" == "-l" ]]; then cat "$STUB_DIR/scrontab.table"; exit 0; fi\n'
            'echo "scrontab: error: slurmctld is down" >&2; exit 1\n',
        )
        stubs.output("squeue", "4242 RUNNING\n")
        with pytest.raises(schedule.ScheduleError) as err:
            schedule.remove(sched)
        assert "scrontab: scrontab: error: slurmctld is down" in str(err.value)
        assert "chain job(s) cancelled" in str(err.value)
        assert stubs.scrontab_table() == table and stubs.calls("scancel")

    def test_a_cluster_without_scrontab_is_not_a_failure(self, sched, stubs):
        stubs.remove("scrontab")
        stubs.output("squeue", "4242 PENDING\n")
        assert schedule.remove(sched) == ["chain job(s) cancelled"]

    def test_nothing_scheduled(self, sched, stubs):
        assert schedule.remove(sched) == []

    def test_no_slurm_at_all(self, cfg, stubs):
        cfg.scrontab = "no-such-scrontab"
        cfg.squeue = "no-such-squeue"
        with pytest.raises(schedule.ScheduleError, match="neither scrontab nor squeue"):
            schedule.remove(cfg)

    def test_command_line_exit_status(self, sched, stubs, capsys):
        from lib import cli

        stubs.output("squeue", "4242 PENDING\n")
        stubs.returncode("scancel", 1)
        assert cli.main(["schedule", "remove"]) == 1
        assert "hpg schedule:" in capsys.readouterr().err


def test_describe_without_slurm(cfg, stubs):
    cfg.scrontab = "no-such-scrontab"
    cfg.squeue = "no-such-squeue"
    assert schedule.describe(cfg)["mode"] == "unknown"
