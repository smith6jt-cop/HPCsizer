"""Tests for lib/submit.py and ``hpg submit``: what reaches sbatch, and what never changes."""

import json
import os
import pwd
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
from sacct_fixture import simple_job

from lib import classify, cli, sbatch_opts, submit
from lib.config import load_config

REPO = Path(__file__).resolve().parent.parent
ME = pwd.getpwuid(os.getuid()).pw_name

# Records its arguments (one per line) and the script it was given, then answers like sbatch.
SBATCH_STUB = r"""#!/usr/bin/env bash
printf '%s\n' "$@" > "$STUB_DIR/sbatch.args"
for arg in "$@"; do
    if [[ -f "$arg" ]]; then cp "$arg" "$STUB_DIR/sbatch.script"; printf '%s' "$arg" > "$STUB_DIR/sbatch.path"; fi
done
if [[ -f "$STUB_DIR/sbatch.rc" ]]; then echo "sbatch: error: Batch job submission failed" >&2; exit "$(cat "$STUB_DIR/sbatch.rc")"; fi
if [[ -n "${STUB_RUN:-}" && -f "$STUB_DIR/sbatch.script" ]]; then
    SLURM_JOB_ID=777 SLURMD_NODENAME=c1 SLURM_CPUS_ON_NODE=2 bash "$STUB_DIR/sbatch.script" > "$STUB_DIR/job.out" 2>&1
fi
if printf '%s\n' "$@" | grep -qx -- '--parsable'; then echo "777"; else echo "Submitted batch job 777"; fi
"""

SCRIPT = """#!/bin/bash
#SBATCH --job-name=mplx12k-spleen
#SBATCH --mem=64G -c 16
#SBATCH -t 2-0

python run.py "$@"
"""


@pytest.fixture
def conf(root, stubs, monkeypatch):
    """A root with the sampler in place, a spool, a stub sbatch, and USER set."""
    (root / "bin").mkdir()
    shutil.copy(str(REPO / "bin" / "monitor.py"), str(root / "bin" / "monitor.py"))
    (root / "spool").mkdir()
    (root / "hpcsizer.ini").write_text("[slurm]\ncompute_python = %s\n" % sys.executable)
    stubs.install("sbatch", SBATCH_STUB)
    monkeypatch.setenv("USER", ME)
    return load_config()


@pytest.fixture
def work(tmp_path, monkeypatch):
    folder = tmp_path / "work"
    folder.mkdir()
    (folder / "run_mplx.sh").write_text(SCRIPT)
    monkeypatch.chdir(folder)
    return folder


def sent(stubs):
    return (stubs.dir / "sbatch.args").read_text().splitlines()


def sent_script(stubs):
    return (stubs.dir / "sbatch.script").read_text()


def parse(*args):
    return sbatch_opts.parse_argv(list(args))


class TestSamplerOptions:
    def test_other_nodes_are_sampled_unless_turned_off(self, conf):
        assert "fan-out" not in submit.sampler_line(conf), "the sampler does it by default"
        conf.profile_multi_node = False
        assert " --no-fan-out </dev/null" in submit.sampler_line(conf)

    def test_perf_only_when_configured(self, conf):
        assert "--perf" not in submit.sampler_line(conf)
        conf.profile_perf = True
        assert " --perf </dev/null" in submit.sampler_line(conf)

    def test_ini_setting(self, root, stubs):
        assert load_config().profile_multi_node is True
        (root / "hpcsizer.ini").write_text("[profile]\nmulti_node = no\n")
        assert load_config().profile_multi_node is False


class TestSamplerLine:
    def test_is_valid_shell_and_names_everything_by_full_path(self, conf):
        line = submit.sampler_line(conf)
        assert line.startswith("( HPCSIZER=sampler exec ") and line.endswith(
            "</dev/null >/dev/null 2>&1 & )"
        )
        assert conf.spool_dir in line and submit.sampler_path(conf) in line and "--pid $$" in line
        assert subprocess.run(["bash", "-n", "-c", line]).returncode == 0
        assert subprocess.run(["sh", "-n", "-c", line]).returncode == 0

    def test_perf_is_opt_in(self, conf):
        assert "--perf" not in submit.sampler_line(conf)
        conf.profile_perf = True
        assert " --perf " in submit.sampler_line(conf)

    def test_goes_after_the_last_directive_sbatch_reads(self):
        text = "#!/bin/bash\n#SBATCH --mem=8G\n#SBATCH -c 2\n\nmodule load R\n#SBATCH --mem=64G\nRscript a.R\n"
        lines = submit.inject_script(text, "LINE").splitlines()
        assert lines[3].startswith("LINE  # added by hpg submit")
        assert lines[:3] == text.splitlines()[:3] and lines[4:] == text.splitlines()[3:]

    def test_script_without_directives_or_shebang(self):
        assert (
            submit.inject_script("#!/bin/sh\necho hi\n", "LINE").splitlines()[1].startswith("LINE")
        )
        assert submit.inject_script("echo hi\n", "LINE").startswith("LINE")
        assert (
            submit.inject_script("#!/bin/sh\n#SBATCH -c 2", "LINE")
            .splitlines()[2]
            .startswith("LINE")
        )

    def test_wrap_prefix_does_not_change_what_the_job_is_classified_as(self, conf):
        """Slurm stores the submit line; the family must still be the user's program."""
        wrapped = submit.inject_wrap("python seg.py --tile 4", submit.sampler_line(conf))
        assert classify.submit_signature("sbatch --wrap " + wrapped) == "python:seg.py"
        assert classify.submit_signature("sbatch -c 2 --wrap=" + wrapped) == "python:seg.py"


class TestEditArgs:
    def changes(self, **kwargs):
        return kwargs

    def test_options_go_before_the_script_token(self):
        """After it they would be arguments of the script: `sbatch job.sh --mem=8G`."""
        args = ["job.sh", "input.txt"]
        out = submit.edit_args(
            args, parse(*args), {"mem": "--mem=13G", "cpus": "--cpus-per-task=4"}
        )
        assert out == ["--mem=13G", "--cpus-per-task=4", "job.sh", "input.txt"]

    def test_callers_own_options_are_replaced_in_every_spelling(self):
        args = ["--mem", "64G", "-c16", "-t", "2-0", "--qos=x", "job.sh", "--mem=1G"]
        out = submit.edit_args(
            args,
            parse(*args),
            {"mem": "--mem=13G", "cpus": "--cpus-per-task=4", "time": "--time=01:30:00"},
        )
        assert out == [
            "--qos=x",
            "--mem=13G",
            "--cpus-per-task=4",
            "--time=01:30:00",
            "job.sh",
            "--mem=1G",
        ]

    def test_only_what_changes_is_touched(self):
        args = ["--mem=64G", "-c", "16", "-t", "60", "job.sh"]
        out = submit.edit_args(args, parse(*args), {"mem": "--mem=13G"})
        assert out == ["-c", "16", "-t", "60", "--mem=13G", "job.sh"]

    def test_memory_form_is_whatever_the_change_says(self):
        args = ["--mem-per-cpu=4G", "-c", "16", "job.sh"]
        out = submit.edit_args(
            args, parse(*args), {"mem": "--mem-per-cpu=3G", "cpus": "--cpus-per-task=4"}
        )
        assert out == ["--mem-per-cpu=3G", "--cpus-per-task=4", "job.sh"]

    def test_bundled_short_options_are_left_and_overridden(self):
        args = ["-Qc16", "job.sh"]
        out = submit.edit_args(args, parse(*args), {"cpus": "--cpus-per-task=4"})
        assert out == ["-Qc16", "--cpus-per-task=4", "job.sh"], "-Q must survive; the later -c wins"

    def test_wrap_has_no_script_token(self):
        args = ["--mem=64G", "--wrap", "python a.py"]
        out = submit.edit_args(args, parse(*args), {"mem": "--mem=13G"})
        assert out == ["--wrap", "python a.py", "--mem=13G"]

    def test_double_dash(self):
        args = ["-c", "16", "--", "-odd.sh"]
        out = submit.edit_args(args, parse(*args), {"cpus": "--cpus-per-task=4"})
        assert out == ["--cpus-per-task=4", "--", "-odd.sh"]

    def test_no_changes(self):
        args = ["--mem=64G", "job.sh"]
        assert submit.edit_args(args, parse(*args), {}) == args

    def test_replace_wrap_keeps_its_form(self):
        args = ["-c", "2", "--wrap=old", "--mem=1G"]
        assert submit.replace_wrap(args, parse(*args), "new") == [
            "-c",
            "2",
            "--wrap=new",
            "--mem=1G",
        ]
        args = ["--wrap", "old"]
        assert submit.replace_wrap(args, parse(*args), "new") == ["--wrap", "new"]


class TestHelpers:
    def test_job_id(self):
        assert submit.job_id_from("Submitted batch job 51598968\n") == "51598968"
        assert submit.job_id_from("51598968\n") == "51598968"
        assert submit.job_id_from("51598968;hipergator\n") == "51598968"
        assert submit.job_id_from("Submitted batch job 12 on cluster x\n") == "12"
        assert submit.job_id_from("sbatch: error\n") is None and submit.job_id_from("") is None

    @pytest.mark.parametrize(
        "args,reason",
        [
            (["--help"], "informational"),
            (["--mem=4G"], "standard input"),
            (["-c", "4", ":", "-c", "1", "job.sh"], "heterogeneous"),
            (["--frobnicate", "x", "job.sh"], "not known to hpg"),
        ],
    )
    def test_what_is_handed_to_sbatch_untouched(self, args, reason):
        assert reason in submit.passes_straight_through(parse(*args))

    def test_ordinary_submissions_are_handled(self):
        assert submit.passes_straight_through(parse("-c", "4", "job.sh")) is None
        assert submit.passes_straight_through(parse("--wrap", "x")) is None

    def test_spool_problems(self, conf):
        assert submit.spool_problem(conf) is None
        os.unlink(submit.sampler_path(conf))
        assert "sampler" in submit.spool_problem(conf)
        shutil.rmtree(conf.spool_dir)
        assert "does not exist" in submit.spool_problem(conf)


class TestPrepare:
    def test_script_is_profiled_through_a_copy_with_the_same_name(self, conf, work):
        plan = submit.prepare(["-c", "4", "run_mplx.sh", "s1"], conf)
        try:
            assert plan.profiled and plan.final[:2] == ["-c", "4"] and plan.final[3] == "s1"
            copy = plan.final[2]
            assert os.path.basename(copy) == "run_mplx.sh" and os.path.dirname(copy) == plan.tmpdir
            text = open(copy).read()
            assert text.count("HPCSIZER=sampler") == 1
            assert text.replace(text.splitlines(keepends=True)[4], "") == SCRIPT, (
                "nothing else differs"
            )
            assert (work / "run_mplx.sh").read_text() == SCRIPT, "the user's file is never edited"
            assert classify.submit_signature("sbatch " + " ".join(plan.final)) == "run_mplx.sh"
        finally:
            plan.cleanup()
        assert not os.path.exists(copy)

    def test_changes_and_profiling_together(self, conf, work):
        plan = submit.prepare(["--mem=64G", "run_mplx.sh"], conf, changes={"mem": "--mem=13G"})
        try:
            assert plan.final[0] == "--mem=13G" and plan.applied == {"mem": "--mem=13G"}
            assert os.path.basename(plan.final[1]) == "run_mplx.sh"
        finally:
            plan.cleanup()

    def test_wrap(self, conf, work):
        plan = submit.prepare(["-c", "2", "--wrap", "python a.py"], conf)
        assert plan.profiled and plan.tmpdir is None
        assert plan.final[:2] == ["-c", "2"] and plan.final[2] == "--wrap"
        assert plan.final[3].startswith("( HPCSIZER=sampler exec ") and plan.final[3].endswith(
            "); python a.py"
        )

    def test_script_in_another_language_is_submitted_unprofiled(self, conf, work):
        (work / "job.py").write_text("#!/usr/bin/env python3\n#SBATCH -c 2\nprint(1)\n")
        plan = submit.prepare(["job.py"], conf)
        assert not plan.profiled and plan.final == ["job.py"]
        assert any("not run by sh or bash" in note for note in plan.notes)

    def test_script_that_looks_up_its_own_path_is_submitted_unprofiled(self, conf, work):
        """Slurm records the temporary copy as the job's Command; such a script would be
        sent to a folder that no longer exists."""
        (work / "self.sh").write_text(
            "#!/bin/bash\n#SBATCH -c 2\n"
            "HERE=$(dirname $(scontrol show job $SLURM_JOB_ID | awk -F= '/Command=/{print $2}'))\n"
            "cd $HERE && ./run\n"
        )
        plan = submit.prepare(["self.sh"], conf)
        assert not plan.profiled and plan.final == ["self.sh"] and plan.tmpdir is None
        assert any("reads its own path from 'scontrol show job'" in note for note in plan.notes)

    def test_script_that_only_prints_its_job_record_is_profiled(self, conf, work):
        (work / "log.sh").write_text("#!/bin/bash\nscontrol show job $SLURM_JOB_ID\n./run\n")
        plan = submit.prepare(["log.sh"], conf)
        try:
            assert plan.profiled
        finally:
            plan.cleanup()

    def test_without_a_spool_the_job_is_submitted_unchanged(self, conf, work):
        shutil.rmtree(conf.spool_dir)
        plan = submit.prepare(["run_mplx.sh"], conf)
        assert plan.final == ["run_mplx.sh"] and any("not profiled" in note for note in plan.notes)

    def test_no_profile_and_test_only(self, conf, work):
        assert submit.prepare(["run_mplx.sh"], conf, profile=False).final == ["run_mplx.sh"]
        assert submit.prepare(["--test-only", "run_mplx.sh"], conf).final == [
            "--test-only",
            "run_mplx.sh",
        ]

    def test_anything_unusual_goes_to_sbatch_exactly_as_given(self, conf, work):
        for args in (
            ["--frobnicate", "8", "run_mplx.sh"],
            ["-c", "4", ":", "-c", "1", "run_mplx.sh"],
            ["missing.sh"],
        ):
            plan = submit.prepare(args, conf, changes={"mem": "--mem=1G"})
            assert plan.final == args and not plan.profiled and plan.applied == {}
            assert plan.notes and "exactly as given" in plan.notes[0]


def history_rows(now, n=6):
    rows = []
    for i in range(n):
        rows += simple_job(
            100 + i,
            user=ME,
            name="mplx12k-spleen",
            submit_line="sbatch run_mplx.sh",
            mem="64G",
            cpus=16,
            timelimit=2880,
            elapsed=3600,
            submit=now - (n + 1 - i) * 12 * 3600,
            batch_max_rss="10485760K",
            batch_total_cpu="03:00:00",
        )
    return rows


@pytest.fixture
def stored(conf, stubs, work, capsys):
    """Six finished runs of the job in ``run_mplx.sh``: 64 GB and 16 CPUs asked, 10 GB and 3 used."""
    stubs.sacct_default(history_rows(int(time.time())))
    assert cli.main(["harvest"]) == 0
    capsys.readouterr()
    return conf


class TestCommand:
    def test_default_is_advice_then_the_request_as_written(self, stored, stubs, capsys):
        assert cli.main(["submit", "run_mplx.sh", "s1"]) == 0
        out, err = capsys.readouterr()
        assert out == "Submitted batch job 777\n", "stdout is sbatch's and nothing else"
        assert "Suggested request" in err and "#SBATCH --mem=13G" in err
        assert "Submitting as requested. To use the suggested request: hpg submit --apply" in err
        args = sent(stubs)
        assert len(args) == 2 and os.path.basename(args[0]) == "run_mplx.sh" and args[1] == "s1"
        assert "HPCSIZER=sampler" in sent_script(stubs)
        assert not os.path.exists((stubs.dir / "sbatch.path").read_text()), "the copy is removed"

    def test_apply_puts_the_history_backed_request_on_the_command_line(self, stored, stubs, capsys):
        assert cli.main(["submit", "--apply", "--yes", "--mem=64G", "run_mplx.sh"]) == 0
        _out, err = capsys.readouterr()
        assert "Submitting with: --mem=13G --cpus-per-task=4" in err
        args = sent(stubs)
        assert (
            args[:2] == ["--mem=13G", "--cpus-per-task=4"]
            and os.path.basename(args[2]) == "run_mplx.sh"
        )
        assert "#SBATCH --mem=64G -c 16" in sent_script(stubs), (
            "directives stay; the command line overrides"
        )

    def test_context_is_recorded_for_the_harvester(self, stored, stubs, capsys):
        assert cli.main(["submit", "--apply", "-y", "run_mplx.sh"]) == 0
        path = os.path.join(stored.spool_dir, "submit", "777.json")
        ctx = json.load(open(path))
        assert ctx["job_id"] == "777" and ctx["user"] == ME and ctx["profiled"] is True
        assert ctx["applied"] == {"cpus": "--cpus-per-task=4", "mem": "--mem=13G"}
        assert ctx["suggested"]["source"] == "history" and ctx["request"]["mem_mb"] == 65536
        assert ctx["script_hash"] and abs(ctx["submitted_ts"] - time.time()) < 60
        assert oct(os.stat(path).st_mode & 0o777) == "0o640"

    def test_apply_with_nothing_to_apply(self, conf, stubs, work, capsys):
        stubs.sacct_default(simple_job(1, user=ME))
        cli.main(["harvest"])
        capsys.readouterr()
        assert cli.main(["submit", "--apply", "--yes", "run_mplx.sh"]) == 0
        err = capsys.readouterr().err
        assert "no finished runs of %s/batch/mplx#k-spleen yet" % ME in err
        assert "nothing to apply" in err and len(sent(stubs)) == 1

    def test_as_is_needs_no_database(self, conf, stubs, work, capsys):
        assert cli.main(["submit", "--as-is", "run_mplx.sh"]) == 0
        out, err = capsys.readouterr()
        assert out == "Submitted batch job 777\n" and "Suggested" not in err
        assert "The job is profiled" in err

    def test_missing_database_does_not_stop_a_submission(self, conf, stubs, work, capsys):
        assert cli.main(["submit", "run_mplx.sh"]) == 0
        out, err = capsys.readouterr()
        assert out == "Submitted batch job 777\n" and "no report (no database" in err

    def test_parsable_output_and_exit_status_are_sbatchs(self, stored, stubs, capsys):
        assert cli.main(["sbatch", "--as-is", "--parsable", "run_mplx.sh"]) == 0
        assert capsys.readouterr().out == "777\n"
        assert os.path.exists(os.path.join(stored.spool_dir, "submit", "777.json"))
        os.unlink(os.path.join(stored.spool_dir, "submit", "777.json"))
        (stubs.dir / "sbatch.rc").write_text("1")
        assert cli.main(["submit", "--as-is", "run_mplx.sh"]) == 1
        assert capsys.readouterr().out == ""
        assert not os.path.exists(os.path.join(stored.spool_dir, "submit", "777.json"))

    def test_no_profile(self, stored, stubs, capsys):
        assert cli.main(["submit", "--as-is", "--no-profile", "run_mplx.sh"]) == 0
        assert sent(stubs) == ["run_mplx.sh"] and "HPCSIZER" not in sent_script(stubs)

    def test_wrap(self, stored, stubs, capsys):
        assert cli.main(["submit", "--as-is", "-c", "2", "--wrap", "python a.py"]) == 0
        args = sent(stubs)
        assert args[:3] == ["-c", "2", "--wrap"] and args[3].endswith("); python a.py")

    def test_usage_errors(self, conf, stubs, work, capsys):
        assert cli.main(["submit"]) == 2
        assert cli.main(["submit", "--apply", "--as-is", "run_mplx.sh"]) == 2
        assert not (stubs.dir / "sbatch.args").exists()

    def test_sbatch_missing(self, conf, stubs, work, capsys):
        stubs.remove("sbatch")
        assert cli.main(["submit", "--as-is", "run_mplx.sh"]) in (127, 1)

    @pytest.mark.parametrize(
        "answer,code,applied", [("", 0, True), ("y", 0, True), ("n", 0, False), ("q", 1, None)]
    )
    def test_asked_only_at_a_terminal(
        self, stored, stubs, capsys, monkeypatch, answer, code, applied
    ):
        monkeypatch.setattr(submit, "interactive", lambda: True)
        prompts = []
        monkeypatch.setattr(submit, "ask", lambda prompt: prompts.append(prompt) or answer)
        assert cli.main(["submit", "--apply", "run_mplx.sh"]) == code
        assert len(prompts) == 1 and "--mem=13G" in prompts[0]
        if applied is None:
            assert not (stubs.dir / "sbatch.args").exists()
        else:
            assert ("--mem=13G" in sent(stubs)) is applied

    def test_never_asks_without_a_terminal(self, stored, stubs, capsys, monkeypatch):
        monkeypatch.setattr(
            submit, "ask", lambda prompt: pytest.fail("prompted without a terminal")
        )
        assert cli.main(["submit", "--apply", "run_mplx.sh"]) == 0
        assert "--mem=13G" in sent(stubs)


class TestWholeLoop:
    def test_submit_run_harvest_and_see_the_profile(self, stored, stubs, work, capsys, monkeypatch):
        """sbatch is stubbed to run the script it is handed, as a node would."""
        (work / "busy.sh").write_text(
            "#!/bin/bash\n#SBATCH -J busy -c 2 --mem=1G -t 10\n"
            '%s -c "\nimport time\nx = bytearray(50 * 1024 * 1024)\nt = time.time()\n'
            'while time.time() - t < 3.2: sum(range(20000))\n"\necho finished\n' % sys.executable
        )
        monkeypatch.setenv("STUB_RUN", "1")
        assert cli.main(["submit", "--as-is", "busy.sh"]) == 0
        capsys.readouterr()
        assert (stubs.dir / "job.out").read_text() == "finished\n", (
            "the job's own output is untouched"
        )
        series = os.path.join(stored.spool_dir, "777.c1.tsv")
        deadline = time.time() + 10
        while time.time() < deadline and not os.path.exists(series):
            time.sleep(0.1)
        time.sleep(0.5)  # the sampler takes a last sample when the script's shell is gone
        assert os.path.exists(series)

        now = int(time.time())
        rows = history_rows(now) + simple_job(
            777, user=ME, name="busy", submit_line="sbatch busy.sh", submit=now - 60, wait=1, elapsed=4,
            cpus=2, mem="1G", timelimit=10, batch_max_rss="1000K", batch_total_cpu="00:00.900",
        )  # fmt: skip
        stubs.sacct_default(rows)
        assert cli.main(["harvest"]) == 0
        assert "1 profile" in capsys.readouterr().err
        assert cli.main(["job", "--no-refresh", "777", "--json"]) == 0
        job = json.loads(capsys.readouterr().out)
        assert job["mon_source"] == "proc" and job["mon_n_samples"] >= 1
        assert job["mon_peak_mem_bytes"] > 50 * 1024 * 1024
        assert job["metrics"]["mem_source"] == "profile", "a 4-second job accounting cannot measure"
        assert json.loads(job["ctx_tools_json"]) == ["time"] and job["ctx_script_hash"]


class TestContextFile:
    """What 'hpg submit' leaves for the harvester, in a folder the whole group can write to."""

    @pytest.fixture
    def folder(self, conf):
        path = os.path.join(conf.spool_dir, "submit")
        os.mkdir(path)
        return path

    def test_written_group_readable_whatever_the_umask(self, conf, folder):
        before = os.umask(0o077)
        try:
            path = submit.write_context(conf, "777", {"tools": ["Seurat"]})
        finally:
            os.umask(before)
        assert path == os.path.join(folder, "777.json")
        assert json.load(open(path)) == {"tools": ["Seurat"]}
        assert oct(os.stat(path).st_mode & 0o777) == "0o640"
        assert os.listdir(folder) == ["777.json"], "no temporary file is left"

    def test_link_under_the_jobs_name_is_never_written_through(self, conf, folder, tmp_path):
        """A member plants links for the next job ids, pointing at a file of the submitter."""
        victim = tmp_path / "thesis.tex"
        victim.write_text("three years of work\n")
        victim.chmod(0o600)
        os.symlink(str(victim), os.path.join(folder, "777.json"))
        path = submit.write_context(conf, "777", {"tools": []})
        assert victim.read_text() == "three years of work\n"
        assert oct(victim.stat().st_mode & 0o777) == "0o600"
        # Here the folder is not sticky, so the link itself is replaced by the record.
        assert path and not os.path.islink(path) and json.load(open(path)) == {"tools": []}

    def test_dangling_link_does_not_create_its_target(self, conf, folder, tmp_path):
        target = tmp_path / "appears-in-your-home"
        os.symlink(str(target), os.path.join(folder, "777.json"))
        submit.write_context(conf, "777", {})
        assert not target.exists()

    def test_record_that_cannot_be_put_in_place_leaves_nothing_behind(
        self, conf, folder, monkeypatch
    ):
        def refuse(*_args, **_kwargs):
            raise PermissionError("sticky folder: the name belongs to somebody else")

        monkeypatch.setattr(submit.os, "replace", refuse)
        assert submit.write_context(conf, "777", {}) is None
        assert os.listdir(folder) == []

    def test_folder_is_only_ever_created_by_the_harvest(self, conf):
        assert submit.write_context(conf, "777", {}) is None
        assert not os.path.exists(os.path.join(conf.spool_dir, "submit"))

    def test_folder_that_is_a_link_is_not_used(self, conf, tmp_path):
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        os.symlink(str(elsewhere), os.path.join(conf.spool_dir, "submit"))
        assert submit.write_context(conf, "777", {}) is None
        assert os.listdir(str(elsewhere)) == []

    @pytest.mark.skipif(os.getuid() != 0, reason="needs root to give the folder to another user")
    def test_folder_of_somebody_other_than_the_spools_owner_is_not_used(self, conf, folder):
        os.chown(folder, 65534, -1)
        assert submit.write_context(conf, "777", {}) is None
        assert os.listdir(folder) == []

    def test_job_id_must_be_a_number(self, conf, folder):
        assert submit.write_context(conf, "../777", {}) is None
        assert os.listdir(folder) == [] and os.listdir(conf.spool_dir) == ["submit"]
