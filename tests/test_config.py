"""Tests for lib/config.py."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.config import load_config  # noqa: E402


def test_defaults_are_relative_to_the_root(tmp_path):
    cfg = load_config({"HPCSIZER_ROOT": str(tmp_path)})
    assert cfg.db_path == str(tmp_path / "profiles.db")
    assert cfg.spool_dir == str(tmp_path / "spool")
    assert cfg.lock_path == str(tmp_path / "profiles.db.lock")
    assert cfg.accounts == []
    assert cfg.session_names == ["vscode"]


def test_environment_variables_from_the_previous_version_still_work(tmp_path):
    cfg = load_config(
        {
            "HPCSIZER_ROOT": str(tmp_path),
            "HPCSIZER_DB": str(tmp_path / "x" / "p.db"),
            "HPCSIZER_ACCT": "maigan, clive",
            "HPCSIZER_HARVEST_RETRIES": "5",
            "HPCSIZER_HARVEST_RETRY_DELAY": "0",
        }
    )
    assert cfg.db_path == str(tmp_path / "x" / "p.db")
    assert cfg.accounts == ["maigan", "clive"]
    assert cfg.retries == 5 and cfg.retry_delay == 0


def test_ini_file_supplies_what_a_scrontab_job_has_no_environment_for(tmp_path):
    (tmp_path / "hpcsizer.ini").write_text(
        "[slurm]\naccounts = maigan\nsacct = /opt/slurm/bin/sacct\n"
        "[schedule]\nqos = maigan\npartition = hpg-default\nmail_user = a@b.c\n"
        "[paths]\nspool = /blue/maigan/hpcsizer-spool\n"
        "[policy]\nmem_headroom = 1.3\nmin_runs = 4\n"
        "[classify]\nsession_names = vscode, jupyter\n"
    )
    cfg = load_config({"HPCSIZER_ROOT": str(tmp_path)})
    assert cfg.accounts == ["maigan"]
    assert cfg.sacct == "/opt/slurm/bin/sacct"
    assert cfg.sched_account == "maigan"  # defaults to the single harvested account
    assert (cfg.sched_qos, cfg.sched_partition, cfg.mail_user) == ("maigan", "hpg-default", "a@b.c")
    assert cfg.spool_dir == "/blue/maigan/hpcsizer-spool"
    assert cfg.mem_headroom == 1.3 and cfg.min_runs == 4
    assert cfg.session_names == ["vscode", "jupyter"]


def test_environment_overrides_the_ini_file(tmp_path):
    (tmp_path / "hpcsizer.ini").write_text("[slurm]\naccounts = maigan\n")
    cfg = load_config({"HPCSIZER_ROOT": str(tmp_path), "HPCSIZER_ACCT": "brusko"})
    assert cfg.accounts == ["brusko"]


def test_explicit_config_path(tmp_path):
    ini = tmp_path / "elsewhere.ini"
    ini.write_text("[slurm]\naccounts = clive\n")
    cfg = load_config({"HPCSIZER_ROOT": str(tmp_path), "HPCSIZER_CONFIG": str(ini)})
    assert cfg.accounts == ["clive"]
