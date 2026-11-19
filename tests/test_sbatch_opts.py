"""Tests for lib/sbatch_opts.py: sbatch's option grammar and option values."""

import pytest

from lib import sbatch_opts


class TestSbatchArgv:
    def test_script_and_its_arguments(self):
        p = sbatch_opts.parse_argv("--mem=8G -c 4 -t 1:00:00 run.sh --mem=1G input".split())
        assert p.script == "run.sh" and p.script_index == 5
        assert p.script_args == ["--mem=1G", "input"], "options after the script belong to it"
        assert (p.get("mem"), p.get("cpus-per-task"), p.get("time")) == ("8G", "4", "1:00:00")
        assert not p.uncertain

    def test_separate_and_attached_forms(self):
        p = sbatch_opts.parse_argv(["--mem", "8G", "-c8", "-J", "x", "--time=30", "s.sh"])
        assert (p.get("mem"), p.get("cpus-per-task"), p.get("job-name"), p.get("time")) == (
            "8G",
            "8",
            "x",
            "30",
        )
        assert [(o.name, o.start, o.end) for o in p.options][:2] == [
            ("mem", 0, 2),
            ("cpus-per-task", 2, 3),
        ]

    def test_flags_and_bundled_short_options(self):
        p = sbatch_opts.parse_argv(["-H", "--parsable", "-Qc2", "--exclusive", "s.sh"])
        assert p.has("hold") and p.has("parsable") and p.has("quiet") and p.has("exclusive")
        assert p.get("cpus-per-task") == "2" and p.script == "s.sh"

    def test_wrap_has_no_script(self):
        p = sbatch_opts.parse_argv(["-c", "2", "--wrap", "python a.py"])
        assert p.wrap == "python a.py" and p.script is None

    def test_last_occurrence_wins(self):
        assert sbatch_opts.parse_argv(["--mem=1G", "--mem=2G", "s.sh"]).get("mem") == "2G"

    def test_unknown_option_without_a_value_is_uncertain(self):
        p = sbatch_opts.parse_argv(["--frobnicate", "8", "s.sh"])
        assert p.uncertain and p.unknown == ["--frobnicate"]

    def test_unknown_option_with_a_value_is_safe(self):
        p = sbatch_opts.parse_argv(["--frobnicate=8", "s.sh"])
        assert not p.uncertain and p.script == "s.sh"

    def test_double_dash_ends_options(self):
        p = sbatch_opts.parse_argv(["-c", "2", "--", "-odd-name.sh", "x"])
        assert p.script == "-odd-name.sh" and p.script_args == ["x"]

    def test_stdin_script(self):
        assert sbatch_opts.parse_argv(["--mem=1G"]).script is None

    def test_strip_sbatch_word(self):
        assert sbatch_opts.strip_sbatch_word(["/opt/slurm/bin/sbatch", "a.sh"]) == ["a.sh"]
        assert sbatch_opts.strip_sbatch_word(["a.sh"]) == ["a.sh"]

    def test_heterogeneous_job(self):
        """`sbatch -c 4 : -c 1 job.sh` -- the colon is not the script."""
        p = sbatch_opts.parse_argv(["-c", "4", ":", "-c", "1", "job.sh"])
        assert p.het and p.script == "job.sh"
        assert not sbatch_opts.parse_argv(["-c", "4", "job.sh"]).het


class TestTime:
    @pytest.mark.parametrize(
        "spec,minutes",
        [
            ("90", 90),  # minutes
            ("90:30", 91),  # minutes:seconds, rounded up as Slurm does
            ("1:30:00", 90),  # hours:minutes:seconds
            ("00:10:00", 10),
            ("2-0", 2880),  # days-hours
            ("2-12", 3600),
            ("2-12:30", 3630),  # days-hours:minutes
            ("1-00:00:01", 1441),  # days-hours:minutes:seconds
            ("7-00:00:00", 10080),
        ],
    )
    def test_every_documented_form(self, spec, minutes):
        assert sbatch_opts.parse_time(spec) == (minutes, False)

    @pytest.mark.parametrize("spec", ["UNLIMITED", "unlimited", "INFINITE", "0", "-1"])
    def test_unlimited(self, spec):
        assert sbatch_opts.parse_time(spec) == (None, True)

    @pytest.mark.parametrize("spec", ["", "soon", "1-2-3", "1:2:3:4", "1.5", None])
    def test_unreadable_is_none_not_a_crash(self, spec):
        """The previous parser raised ValueError on D-HH and on UNLIMITED."""
        assert sbatch_opts.parse_time(spec) == (None, False)

    @pytest.mark.parametrize(
        "minutes,text",
        [
            (5, "00:05:00"),
            (90, "01:30:00"),
            (1440, "1-00:00:00"),
            (2941, "2-01:01:00"),
            (0, "00:01:00"),
            (0.2, "00:01:00"),
        ],
    )
    def test_format(self, minutes, text):
        assert sbatch_opts.format_time(minutes) == text

    def test_format_round_trips(self):
        for minutes in (1, 59, 60, 61, 1439, 1440, 4321, 20160):
            assert sbatch_opts.parse_time(sbatch_opts.format_time(minutes)) == (minutes, False)


class TestMemory:
    @pytest.mark.parametrize(
        "spec,mb",
        [
            ("8G", 8192),
            ("8g", 8192),
            ("16GB", 16384),
            ("4000", 4000),  # the default unit is megabytes
            ("4000M", 4000),
            ("1.5G", 1536),
            ("1T", 1048576),
            ("524288K", 512),
            ("0", 0),
        ],
    )
    def test_parse(self, spec, mb):
        assert sbatch_opts.parse_mem(spec) == mb

    def test_unreadable(self):
        assert sbatch_opts.parse_mem("lots") is None and sbatch_opts.parse_mem(None) is None

    @pytest.mark.parametrize(
        "mb,text",
        [(13312, "13G"), (1024, "1G"), (1536, "1536M"), (768, "768M"), (13000.2, "13001M")],
    )
    def test_format(self, mb, text):
        assert sbatch_opts.format_mem(mb) == text

    def test_format_never_writes_zero(self):
        """`--mem=0G` asks for all memory of the node; version 2 wrote it for small jobs."""
        assert sbatch_opts.format_mem(0.3) == "1M"
        assert sbatch_opts.format_mem(0) == "1M"


class TestCounts:
    def test_gpus(self):
        assert sbatch_opts.parse_gpus("2") == (2, None)
        assert sbatch_opts.parse_gpus("a100:2") == (2, "a100")
        assert sbatch_opts.parse_gpus("a100") == (1, "a100")
        assert sbatch_opts.parse_gpus(None) == (0, None)

    def test_gres(self):
        assert sbatch_opts.parse_gres_gpus("gpu") == (1, None)
        assert sbatch_opts.parse_gres_gpus("gpu:2") == (2, None)
        assert sbatch_opts.parse_gres_gpus("gpu:a100:2,lustre:1") == (2, "a100")
        assert sbatch_opts.parse_gres_gpus("lustre:1") == (0, None)

    def test_nodes_and_tasks(self):
        assert sbatch_opts.parse_count("3") == 3
        assert sbatch_opts.parse_count("2-4") == 2  # a range asks for at least the minimum
        assert sbatch_opts.parse_count("2k") == 2048
        assert sbatch_opts.parse_count("many") is None
