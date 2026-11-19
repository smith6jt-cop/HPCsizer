"""Tests for lib/classify.py: kinds, program signatures and job families."""

import pytest

from lib.classify import (
    classify,
    collapse_digits,
    command_signature,
    is_session_kind,
    job_kind,
    shown_command,
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


class TestProgramsThatAreNotAFileName:
    """Different programs of one user must not share a family: one would be sized from the other."""

    def wrap(self, command):
        return submit_signature("sbatch --wrap " + command)

    @pytest.mark.parametrize(
        "one, other",
        [
            ("python -c import a; a.run(1)", "python -c import b; b.train()"),
            ("python -c import numpy as np; small()", "python -c import numpy as np; huge()"),
            ("Rscript -e rmarkdown::render('a.Rmd')", "Rscript -e source('fit.R')"),
            ("matlab -nodisplay -batch run_a", "matlab -nodisplay -batch run_b"),
            ("matlab -r segment_all; exit", "matlab -r stitch; exit"),
            ("perl -e print 1", "perl -e print 2 if 0"),
            ("julia -e using A; A.go()", "julia -e using B; B.go()"),
            (
                "for f in *.tif; do python seg.py $f; done",
                "for f in *.csv; do Rscript fit.R $f; done",
            ),
            (
                "while read s; do ./align $s; done < samples.txt",
                "while read s; do ./sort $s; done < s.txt",
            ),
            ("if [ -f x ]; then ./a; fi", "if [ -f y ]; then ./b; fi"),
        ],
    )
    def test_inline_code_and_shell_constructs_are_told_apart(self, one, other):
        a, b = self.wrap(one), self.wrap(other)
        assert a and b and a != b
        assert self.wrap(one) == a, "the same command is the same program"

    def test_the_same_one_liner_with_another_number_is_one_family(self, conf):
        a = classify(job(job_name="wrap", submit_line="sbatch --wrap python -c run(12, 3)"), conf)
        b = classify(
            job(job_name="wrap", submit_line="sbatch --mem=8G --wrap python -c run(13, 4)"), conf
        )
        c = classify(job(job_name="wrap", submit_line="sbatch --wrap python -c other(12)"), conf)
        assert a["family"] == b["family"] != c["family"]
        assert a["family"].startswith("smith6jt/wrap/cmd:python:-c:")

    @pytest.mark.parametrize(
        "command, expected",
        [
            ("R CMD BATCH --no-save fit.R out.txt", "Rscript:fit.R"),
            ("R --no-save < fit.R", "Rscript:fit.R"),
            ("python -W ignore a.py", "python:a.py"),
            ("python -X dev -u b.py --n 3", "python:b.py"),
            ("java -cp lib.jar Main in.txt", "java:Main"),
            ("julia -t 4 sim.jl", "julia:sim.jl"),
            ("mpirun -np 4 ./solver in.dat", "solver"),
            ("mpiexec --mca btl self -n 2 ./a.out", "a.out"),
            ("mpirun -n 8 python sim.py", "python:sim.py"),
            ("(python a.py)", "python:a.py"),
            ("( cd /x && python a.py )", "python:a.py"),
            ("{ module load R; Rscript fit.R; }", "Rscript:fit.R"),
            ("python tool --input data.py", "python:tool"),
        ],
    )
    def test_the_program_is_not_an_option_value_or_a_launcher(self, command, expected):
        assert self.wrap(command) == expected

    def test_wrap_as_an_argument_of_a_script_is_not_a_wrap_job(self, conf):
        c = classify(job(job_name="run.sh", submit_line="sbatch -c 2 run.sh --wrap tight"), conf)
        assert c["kind"] == "batch" and c["signature"] == "run.sh"
        w = classify(job(job_name="wrap", submit_line="sbatch -c 2 --wrap run.sh --wrap"), conf)
        assert w["kind"] == "wrap"


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
        c = classify(job(user="bjones", job_name="ondemand/sys/dashboard/sys/rstudio"), conf)
        assert c["family"] == "bjones/ood:rstudio" and c["session"] is True

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


class TestShownCommand:
    """What 'hpg job' prints as the command: what was typed, not what hpg submit made of it."""

    SAMPLER = (
        "( HPCSIZER=sampler exec python3 -S /blue/mygroup/hpc-sizer/bin/monitor.py "
        "--spool /blue/mygroup/hpc-sizer/spool --pid $$ </dev/null >/dev/null 2>&1 & )"
    )

    def test_sampler_line_in_front_of_a_wrapped_command(self):
        line = "sbatch --parsable --wrap %s; sleep 150" % self.SAMPLER
        assert shown_command(line) == "sbatch --parsable --wrap sleep 150"
        line = "sbatch --wrap=%s; python a.py && echo done" % self.SAMPLER
        assert shown_command(line) == "sbatch --wrap=python a.py && echo done"

    def test_temporary_copy_of_a_script(self):
        line = "sbatch --mem=4G /tmp/hpg-ab12_d3x/run_mplx.sh 45 /blue/lab/data"
        assert shown_command(line) == "sbatch --mem=4G run_mplx.sh 45 /blue/lab/data"
        line = "sbatch /scratch/local/1234/hpg-zzzzzzzz/job.sh"
        assert shown_command(line) == "sbatch job.sh"

    def test_everything_else_is_left_as_it_is(self):
        for line in (
            "sbatch run_mplx.sh 45",
            "sbatch /home/alice/hpg-notes/run.sh",
            "sbatch --wrap ( cd /tmp && ls ); echo HPCSIZER",
            "srun --pty bash",
            "",
        ):
            assert shown_command(line) == line

    def test_family_and_shown_command_agree(self, conf):
        job = {
            "user": "alice",
            "job_name": "wrap",
            "submit_line": "sbatch --wrap %s; python fit.py --n 3" % self.SAMPLER,
        }
        assert classify(job, conf)["name_key"] == "cmd:python:fit.py"
