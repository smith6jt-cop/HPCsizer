"""Tests for lib/sbatch_opts.py and lib/classify.py."""

import pytest

from lib import sbatch_opts
from lib.classify import (
    classify,
    collapse_digits,
    command_signature,
    is_session_kind,
    job_kind,
    submit_signature,
)
from lib.config import load_config


@pytest.fixture
def conf(tmp_path):
    return load_config({"HPCSIZER_ROOT": str(tmp_path)})


def job(**kwargs):
    base = {"user": "smith6jt", "job_name": "job", "submit_line": "sbatch job.sh"}
    base.update(kwargs)
    return base


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


class TestSignature:
    @pytest.mark.parametrize(
        "line,expected",
        [
            ("sbatch --wrap python /blue/g/x/read_xenium.py --sample 12", "python:read_xenium.py"),
            ("sbatch -c 4 --wrap=module load R && Rscript fit.R --seed 3", "Rscript:fit.R"),
            ("sbatch --wrap conda run -n scimap python -m scimap.cli run", "python:scimap.cli"),
            (
                "sbatch --wrap apptainer exec --nv -B /blue img.sif python3.11 train.py",
                "python:train.py",
            ),
            ("sbatch --wrap srun -n 4 ./solver input.dat", "solver"),
            ("sbatch --wrap=STEP=3 python -u driver.py", "python:driver.py"),
            ("sbatch --wrap bash -c cd /x; python a.py", "python:a.py"),
            ("sbatch --wrap sleep 150", "sleep"),
            ("sbatch --time 1:00:00 run_step07.sh arg1", "run_step07.sh"),
            ("/opt/slurm/bin/sbatch vscode_gpu.sh", "vscode_gpu.sh"),
            ("sbatch -J name --array=1-10%2 pipeline.sbatch", "pipeline.sbatch"),
            ("sbatch --wrap java -Xmx8g -jar /apps/QuPath.jar script x", "java:QuPath.jar"),
        ],
    )
    def test_submit_lines(self, line, expected):
        assert submit_signature(line) == expected

    def test_arguments_never_enter_the_signature(self):
        a = submit_signature("sbatch --wrap python seg.py --donor 6539 --tile 4")
        b = submit_signature("sbatch --wrap python seg.py --donor 6541 --tile 9")
        assert a == b == "python:seg.py"

    def test_no_submit_line(self):
        assert submit_signature(None) is None and submit_signature("") is None

    def test_only_setup_commands(self):
        assert command_signature("module load R".split()) is None

    def test_collapse_digits(self):
        assert collapse_digits("mplx12k-spleen") == "mplx#k-spleen"
        assert collapse_digits("v3-dg") == collapse_digits("v14-dg") == "v#-dg"


class TestKind:
    def test_open_ondemand(self, conf):
        assert job_kind(job(job_name="ondemand/sys/dashboard/sys/rstudio"), conf) == "ood:rstudio"
        assert job_kind(job(job_name="sys/dashboard/sys/hpg_desktop"), conf) == "ood:hpg_desktop"

    def test_configured_session_name(self, conf):
        assert (
            job_kind(job(job_name="vscode", submit_line="sbatch vscode_gpu.sh"), conf) == "vscode"
        )
        conf.session_names = ["vscode", "jupyter"]
        assert job_kind(job(job_name="Jupyter"), conf) == "jupyter"

    def test_interactive(self, conf):
        assert job_kind(job(has_interactive_step=1), conf) == "interactive"
        assert job_kind(job(submit_line="salloc -c 4"), conf) == "interactive"
        assert job_kind(job(job_name="hpg-interactive-bash"), conf) == "interactive"

    def test_wrap_and_batch(self, conf):
        assert (
            job_kind(job(job_name="wrap", submit_line="sbatch --wrap python a.py"), conf) == "wrap"
        )
        assert (
            job_kind(job(job_name="named", submit_line="sbatch --wrap=python a.py"), conf) == "wrap"
        )
        assert job_kind(job(), conf) == "batch"
        assert job_kind(job(submit_line=None), conf) == "batch"  # migrated rows

    def test_session_kinds(self, conf):
        assert is_session_kind("ood:rstudio", conf) and is_session_kind("vscode", conf)
        assert is_session_kind("interactive", conf)
        assert not is_session_kind("wrap", conf) and not is_session_kind("batch", conf)


class TestFamily:
    def test_wrap_jobs_are_separated_by_program(self, conf):
        """All 875 monthly --wrap jobs are named 'wrap'; the name cannot tell them apart."""
        a = classify(
            job(job_name="wrap", submit_line="sbatch --wrap python read_xenium.py 1"), conf
        )
        b = classify(job(job_name="wrap", submit_line="sbatch --wrap python archive.py 1"), conf)
        assert a["family"] == "smith6jt/wrap/cmd:python:read_xenium.py"
        assert b["family"] == "smith6jt/wrap/cmd:python:archive.py"
        assert a["kind"] == b["kind"] == "wrap"

    def test_named_batch_jobs_group_by_normalized_name(self, conf):
        a = classify(job(job_name="mplx12k-spleen", submit_line="sbatch a.sh"), conf)
        b = classify(job(job_name="mplx30k-spleen", submit_line="sbatch b.sh"), conf)
        assert a["family"] == b["family"] == "smith6jt/batch/mplx#k-spleen"
        assert a["name_exact"] == "mplx12k-spleen" and a["signature"] == "a.sh"

    def test_generic_name_falls_back_to_the_script(self, conf):
        c = classify(job(job_name="job", submit_line="sbatch run_step07.sh"), conf)
        assert c["family"] == "smith6jt/batch/cmd:run_step#.sh"

    def test_sessions_are_one_family_per_user_and_kind(self, conf):
        c = classify(job(user="ebutterworth", job_name="ondemand/sys/dashboard/sys/rstudio"), conf)
        assert c["family"] == "ebutterworth/ood:rstudio" and c["session"] is True

    def test_gpu_separates_two_scripts_with_one_name(self, conf):
        cpu = classify(job(job_name="vscode", submit_line="sbatch vscode_cpu.sh"), conf)
        gpu = classify(
            job(job_name="vscode", submit_line="sbatch vscode_gpu.sh", alloc_gpus=1), conf
        )
        assert cpu["family"] == "smith6jt/vscode" and gpu["family"] == "smith6jt/vscode+gpu"

    def test_array_is_an_attribute_not_a_kind(self, conf):
        c = classify(job(job_name="xen_raw_evidence", array_job_id=500, array_task_id=7), conf)
        assert c["kind"] == "batch" and c["array"] is True

    def test_migrated_row_without_submit_line(self, conf):
        c = classify(job(job_name="wrap", submit_line=None), conf)
        assert c["family"] == "smith6jt/batch/wrap" and c["signature"] is None

    def test_users_are_never_pooled_in_a_family(self, conf):
        a = classify(job(user="a", job_name="align"), conf)
        b = classify(job(user="b", job_name="align"), conf)
        assert a["family"] != b["family"]
