"""Tests for lib/config.py."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.config import ConfigError, load_config  # noqa: E402


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
            "HPCSIZER_ACCT": "maigan, otherlab",
            "HPCSIZER_HARVEST_RETRIES": "5",
            "HPCSIZER_HARVEST_RETRY_DELAY": "0",
        }
    )
    assert cfg.db_path == str(tmp_path / "x" / "p.db")
    assert cfg.accounts == ["maigan", "otherlab"]
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
    cfg = load_config({"HPCSIZER_ROOT": str(tmp_path), "HPCSIZER_ACCT": "thirdlab"})
    assert cfg.accounts == ["thirdlab"]


def test_explicit_config_path(tmp_path):
    ini = tmp_path / "elsewhere.ini"
    ini.write_text("[slurm]\naccounts = otherlab\n")
    cfg = load_config({"HPCSIZER_ROOT": str(tmp_path), "HPCSIZER_CONFIG": str(ini)})
    assert cfg.accounts == ["otherlab"]


def test_data_roots_and_model_settings(tmp_path):
    assert "/blue" in load_config({"HPCSIZER_ROOT": str(tmp_path)}).data_roots
    (tmp_path / "hpcsizer.ini").write_text(
        "[paths]\ndata_roots = /data, /scratch/local\n"
        "[policy]\nmodel_min_points = 6\nmodel_min_r2 = 0.8\n"
    )
    cfg = load_config({"HPCSIZER_ROOT": str(tmp_path)})
    assert cfg.data_roots == ["/data", "/scratch/local"]
    assert cfg.model_min_points == 6 and cfg.model_min_r2 == 0.8


class TestValuesThatMakeNoSense:
    """A value outside what the code can work with is not used; the default is, and it is said."""

    def load(self, tmp_path, text):
        (tmp_path / "hpcsizer.ini").write_text(text)
        return load_config({"HPCSIZER_ROOT": str(tmp_path)})

    @pytest.mark.parametrize(
        "section, key, raw",
        [
            ("policy", "last_n", "0"),
            ("policy", "min_runs", "0"),
            ("policy", "min_runs_pooled", "-2"),
            ("policy", "lookback_days", "0"),
            ("policy", "mem_headroom", "0.5"),
            ("policy", "cpu_headroom", "nan"),
            ("policy", "time_headroom", "inf"),
            ("policy", "time_percentile", "7"),
            ("policy", "time_percentile", "0"),
            ("policy", "model_min_r2", "1.5"),
            ("policy", "last_n", "many"),
            ("measurement", "acct_sample_sec", "0"),
            ("collector", "slice_days", "0"),
            ("collector", "stale_hours", "-1"),
            ("collector", "retries", "0"),
        ],
    )
    def test_default_is_kept_and_the_problem_is_named(self, tmp_path, section, key, raw):
        default = getattr(load_config({"HPCSIZER_ROOT": str(tmp_path)}), key)
        cfg = self.load(tmp_path, "[%s]\n%s = %s\n" % (section, key, raw))
        assert getattr(cfg, key) == default
        assert len(cfg.problems) == 1 and key in cfg.problems[0] and raw in cfg.problems[0]

    def test_sensible_values_are_taken(self, tmp_path):
        cfg = self.load(
            tmp_path,
            "[policy]\nlast_n = 5\nmem_headroom = 1.5\ntime_percentile = 1\n"
            "[collector]\noverlap_days = 0\nretry_delay = 0\n",
        )
        assert (cfg.last_n, cfg.mem_headroom, cfg.time_percentile) == (5, 1.5, 1.0)
        assert cfg.overlap_days == 0 and cfg.problems == []

    def test_a_file_that_is_not_an_ini_file(self, tmp_path):
        with pytest.raises(ConfigError, match="hpcsizer.ini"):
            self.load(tmp_path, "this is not an ini file\n[[[\n")
