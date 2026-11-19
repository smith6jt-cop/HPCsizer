"""Tests for lib/analyzer.py: reading a request the way sbatch reads it."""

import os

import pytest

from lib.analyzer import (
    Request,
    analyze_script,
    analyze_submission,
    detect_input_files,
    detect_language,
    detect_tools,
    directive_lines,
    ignored_directives,
    inputs_from_arguments,
    is_shell_script,
    parse_sbatch_directives,
    referenced_scripts,
    request_from_options,
    script_hash,
)

R_SCRIPT = """#!/bin/bash
#SBATCH --job-name=seurat_analysis
#SBATCH --mem=128G
#SBATCH --cpus-per-task=8
#SBATCH --time=12:00:00

module load R/4.3.1
Rscript -e "
library(Seurat)
library(ggplot2)
obj <- readRDS('/blue/maigan/user/data/sample.rds')
obj <- SCTransform(obj)
"
"""

PYTHON_SCRIPT = """#!/bin/bash
#SBATCH --job-name=scanpy_run
#SBATCH --mem=64G
#SBATCH --cpus-per-task=4
#SBATCH --gres=gpu:a100:1
#SBATCH --time=4:00:00

conda activate scanpy
python3 -c "
import scanpy as sc
import scvi
"
"""


def request(text, **cli):
    return request_from_options(parse_sbatch_directives(text), cli)


class TestDirectives:
    def test_long_options_with_equals(self):
        req = request(R_SCRIPT)
        assert req.job_name == "seurat_analysis"
        assert req.mem_mb == 128 * 1024 and req.cpus == 8 and req.time_min == 720

    def test_short_and_space_separated_options(self):
        """Version 2 only understood `--name=value`; `-c 8` and `--mem 16G` were not seen."""
        req = request("#!/bin/bash\n#SBATCH -c 8 -t 2-12 -J fit\n#SBATCH --mem 16G\n")
        assert (req.cpus, req.time_min, req.job_name, req.mem_mb) == (8, 3600, "fit", 16384)

    def test_attached_short_option(self):
        assert request("#SBATCH -c16 -t90\n").cpus == 16

    def test_memory_per_cpu_is_multiplied_by_the_cpus(self):
        """Version 2 stored `--mem-per-cpu=4G` with 8 CPUs as a 4 GB request."""
        req = request("#SBATCH --mem-per-cpu=4G\n#SBATCH --cpus-per-task=8\n")
        assert req.mem_form == "mem-per-cpu" and req.mem_mb == 32 * 1024

    def test_memory_per_cpu_counts_every_task(self):
        req = request("#SBATCH --mem-per-cpu=2G --ntasks=4 --cpus-per-task=3\n")
        assert req.cpus == 12 and req.mem_mb == 24 * 1024 and req.multi

    def test_memory_per_gpu(self):
        req = request("#SBATCH --mem-per-gpu=24G --gpus=2\n")
        assert req.mem_mb == 48 * 1024
        assert request("#SBATCH --mem-per-gpu=24G\n").mem_mb is None  # no GPU count to multiply

    def test_memory_per_node_times_nodes(self):
        req = request("#SBATCH --mem=10G --nodes=3\n")
        assert req.mem_mb == 30 * 1024 and req.multi

    def test_mem_zero_is_the_whole_node_not_zero(self):
        req = request("#SBATCH --mem=0\n")
        assert req.mem_all and req.mem_mb is None

    def test_default_memory_unit_is_megabytes(self):
        assert request("#SBATCH --mem=4000\n").mem_mb == 4000

    @pytest.mark.parametrize(
        "line,count,kind",
        [
            ("--gres=gpu:a100:2", 2, "a100"),
            ("--gres=gpu:1", 1, None),
            ("--gres=gpu", 1, None),
            ("--gpus=3", 3, None),
            ("-G a100:2", 2, "a100"),
            ("--gpus-per-node=2 --nodes=2", 4, None),
            ("--gpus-per-task=1 --ntasks=4", 4, None),
            ("--gres=lustre:1", 0, None),
        ],
    )
    def test_every_way_to_ask_for_gpus(self, line, count, kind):
        req = request("#SBATCH %s\n" % line)
        assert (req.gpus, req.gpu_type) == (count, kind)

    def test_array_and_tasks(self):
        req = request("#SBATCH --array=1-100%10\n#SBATCH -n 4\n#SBATCH --ntasks-per-node=2\n")
        assert req.array == "1-100%10" and req.ntasks == 4

    def test_tasks_per_node(self):
        assert request("#SBATCH --ntasks-per-node=8 -N 2\n").ntasks == 16

    @pytest.mark.parametrize("value", ["UNLIMITED", "INFINITE", "0"])
    def test_unlimited_time(self, value):
        req = request("#SBATCH --time=%s\n" % value)
        assert req.time_min is None and req.time_unlimited and not req.problems

    def test_unreadable_values_are_reported_not_raised(self):
        req = request("#SBATCH --time=soon --mem=lots -c many\n")
        assert req.time_min is None and req.mem_mb is None and req.cpus == 1
        assert len(req.problems) == 3

    def test_directives_stop_at_the_first_command(self):
        """sbatch ignores #SBATCH lines below the first command; so must the comparison."""
        text = "#!/bin/bash\n#SBATCH --mem=8G\n\n# a comment\nmodule load R\n#SBATCH --mem=64G\n"
        assert request(text).mem_mb == 8 * 1024
        assert ignored_directives(text) == [6]
        assert [index for index, _rest in directive_lines(text)] == [1]

    def test_comment_after_a_directive(self):
        req = request("#SBATCH --mem=8G   # was 64G\n#SBATCH -J 'my job' # name\n")
        assert req.mem_mb == 8 * 1024 and req.job_name == "my job"

    def test_similar_prefix_and_indented_lines_are_not_directives(self):
        text = "#SBATCHX --mem=1G\n  #SBATCH --mem=2G\n##SBATCH --mem=3G\n#SBATCH --mem=4G\n"
        assert request(text).mem_mb == 4 * 1024

    def test_last_occurrence_wins(self):
        assert request("#SBATCH --mem=1G\n#SBATCH --mem=2G\n").mem_mb == 2048

    def test_heterogeneous_job(self):
        req = request("#SBATCH -c 4\n#SBATCH hetjob\n#SBATCH -c 1\n")
        assert req.het and req.multi

    def test_no_directives(self):
        req = request("#!/bin/bash\necho hi\n")
        assert req.mem_mb is None and req.cpus == 1 and req.time_min is None and req.gpus == 0


class TestPrecedence:
    def test_command_line_overrides_the_script(self):
        req = request("#SBATCH --mem=8G -c 4 -t 60\n", **{"mem": "64G", "time": "2-0"})
        assert req.mem_mb == 64 * 1024 and req.cpus == 4 and req.time_min == 2880

    def test_a_memory_option_replaces_the_other_forms(self):
        """`sbatch --mem=64G` on a script that says --mem-per-cpu asks for 64 GB, not both."""
        req = request("#SBATCH --mem-per-cpu=4G -c 8\n", **{"mem": "64G"})
        assert req.mem_form == "mem" and req.mem_mb == 64 * 1024
        back = request("#SBATCH --mem=64G -c 8\n", **{"mem-per-cpu": "2G"})
        assert back.mem_form == "mem-per-cpu" and back.mem_mb == 16 * 1024

    def test_a_gpu_option_replaces_the_other_forms(self):
        req = request("#SBATCH --gres=gpu:4\n", **{"gpus": "1"})
        assert req.gpus == 1

    def test_to_dict_carries_the_totals(self):
        data = Request(cpus_per_task=4, ntasks=2, mem_form="mem", mem_value_mb=1024.0).to_dict()
        assert data["cpus"] == 8 and data["mem_mb"] == 1024.0 and data["multi"] is True


class TestLanguageDetection:
    def test_r_module(self):
        assert detect_language(R_SCRIPT) == "R"

    def test_python_import(self):
        assert detect_language(PYTHON_SCRIPT) == "python"

    def test_rscript_shebang(self):
        assert detect_language("#!/usr/bin/env Rscript\nlibrary(Seurat)") == "R"

    def test_bash_default(self):
        assert detect_language("#!/bin/bash\necho hello") == "bash"

    def test_cellranger(self):
        assert detect_language("#!/bin/bash\ncellranger count --id=sample") == "bash"

    def test_shell_scripts(self):
        assert is_shell_script("#!/bin/bash\n") and is_shell_script("#!/usr/bin/env bash\n")
        assert is_shell_script("#!/bin/sh -l\n") and is_shell_script("echo no shebang\n")
        assert not is_shell_script("#!/usr/bin/env python3\n")
        assert not is_shell_script("#!/usr/bin/env -S\n")


class TestToolDetection:
    def test_r_libraries(self):
        tools = detect_tools(R_SCRIPT, "R")
        assert "Seurat" in tools and "ggplot2" in tools

    def test_python_imports(self):
        tools = detect_tools(PYTHON_SCRIPT, "python")
        assert "scanpy" in tools and "scvi" in tools

    def test_cli_tool(self):
        assert "cellranger" in detect_tools("#!/bin/bash\ncellranger count --id=s", "bash")

    def test_seurat_ns_operator(self):
        assert "Seurat" in detect_tools("Seurat::SCTransform(obj)", "R")


@pytest.fixture
def data(tmp_path):
    """A stand-in data file system with files of known size."""
    folder = tmp_path / "blue"
    (folder / "raw").mkdir(parents=True)
    (folder / "out").mkdir()
    sizes = {
        "raw/s1.h5ad": 3_000_000,
        "raw/s2.h5ad": 1_000_000,
        "raw/reads_1.fastq.gz": 2_000_000,
        "raw/reads_2.fastq.gz": 2_000_000,
        "out/result.rds": 50_000_000,
        "image.sif": 90_000_000,
        "run.py": 2_000,
    }
    for name, size in sizes.items():
        with open(folder / name, "wb") as handle:
            handle.truncate(size)
    return folder


def found(text, data):
    return {
        os.path.relpath(f["path"], str(data)): round(f["size_gb"] * 1024**3)
        for f in detect_input_files(text, roots=[str(data)])
    }


class TestInputFileDetection:
    def test_existing_files_with_their_sizes(self, data):
        text = 'x <- readRDS("%s/raw/s1.h5ad")\npython a.py %s/raw/s2.h5ad\n' % (data, data)
        assert found(text, data) == {"raw/s1.h5ad": 3_000_000, "raw/s2.h5ad": 1_000_000}

    def test_missing_paths_and_directories_are_not_inputs(self, data):
        text = "python a.py %s/raw/missing.h5ad %s/raw %s/raw/\n" % (data, data, data)
        assert found(text, data) == {}

    def test_paths_outside_the_data_roots_are_ignored(self, data, tmp_path):
        other = tmp_path / "elsewhere.bin"
        other.write_bytes(b"x" * 10)
        assert found("cat %s\n" % other, data) == {}

    @pytest.mark.parametrize(
        "line",
        [
            "python a.py > {d}/out/result.rds",
            "python a.py 2>> {d}/out/result.rds",
            "python a.py --output {d}/out/result.rds",
            "python a.py --out={d}/out/result.rds",
            "python a.py -o {d}/out/result.rds",
            'saveRDS(obj, file = "{d}/out/result.rds")',
            'write.csv(df, "{d}/out/result.rds")',
            'adata.write_h5ad("{d}/out/result.rds")',
            'df.to_csv("{d}/out/result.rds")',
            'handle = open("{d}/out/result.rds", "w")',
            "rm -f {d}/out/result.rds",
            "#SBATCH --output={d}/out/result.rds",
            "# cat {d}/out/result.rds",
        ],
    )
    def test_outputs_are_not_inputs(self, data, line):
        """A 50 MB result of the last run must not be added to the input of the next."""
        assert found(line.format(d=data) + "\n", data) == {}

    @pytest.mark.parametrize(
        "line",
        [
            'obj <- readRDS("{d}/out/result.rds")',
            'adata = sc.read_h5ad("{d}/out/result.rds")',
            "python a.py --input {d}/out/result.rds",
            'handle = open("{d}/out/result.rds")',
            "cat {d}/out/result.rds",
        ],
    )
    def test_the_same_file_read_is_an_input(self, data, line):
        assert found(line.format(d=data) + "\n", data) == {"out/result.rds": 50_000_000}

    def test_programs_and_container_images_are_not_data(self, data):
        text = "apptainer exec %s/image.sif python %s/run.py\n" % (data, data)
        assert found(text, data) == {}

    def test_glob_patterns(self, data):
        assert found("zcat %s/raw/reads_*.fastq.gz\n" % data, data) == {
            "raw/reads_*.fastq.gz": 4_000_000
        }

    def test_a_file_is_counted_once(self, data):
        text = "cat %s/raw/s1.h5ad %s/raw/*.h5ad %s/raw/s1.h5ad\n" % (data, data, data)
        assert sum(found(text, data).values()) == 4_000_000

    def test_shell_variables_end_a_path(self, data):
        assert found("cat %s/raw/$SAMPLE.h5ad\n" % data, data) == {}

    def test_command_line_arguments(self, data, tmp_path):
        local = tmp_path / "local.h5ad"
        local.write_bytes(b"x" * 1000)
        files = inputs_from_arguments(
            [
                "--sample",
                "%s/raw/s1.h5ad" % data,
                "--out",
                "%s/raw/s2.h5ad" % data,
                "--ref=local.h5ad",
                "--log=%s/out/result.rds" % data,
                "plainword",
            ],
            base_dir=str(tmp_path),
        )
        assert sorted(os.path.basename(f["path"]) for f in files) == ["local.h5ad", "s1.h5ad"]


class TestScriptHash:
    def test_deterministic(self):
        assert script_hash(R_SCRIPT) == script_hash(R_SCRIPT)

    def test_different_for_different_content(self):
        assert script_hash(R_SCRIPT) != script_hash(PYTHON_SCRIPT)

    def test_returns_hex_string(self):
        digest = script_hash("hello")
        assert len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)


class TestAnalyzeScript:
    def test_follows_the_program_a_shell_script_runs(self, data, tmp_path):
        """The packages and the data paths are in fit.R, not in the three lines of bash."""
        (tmp_path / "fit.R").write_text(
            'library(Seurat)\nobj <- readRDS("%s/raw/s1.h5ad")\nsaveRDS(obj, "%s/out/result.rds")\n'
            % (data, data)
        )
        script = tmp_path / "job.sh"
        script.write_text("#!/bin/bash\n#SBATCH --mem=8G\nmodule load R\nRscript fit.R\n")
        info = analyze_script(str(script), cwd=str(tmp_path), roots=[str(data)])
        assert info["language"] == "R" and "Seurat" in info["tools"] and "readRDS" in info["tools"]
        assert info["followed"] == [str(tmp_path / "fit.R")]
        assert round(info["total_input_gb"] * 1024**3) == 3_000_000
        assert info["shell"] and info["options"] == {"mem": "8G"}
        assert referenced_scripts("Rscript $HOME/fit.R\n", [str(tmp_path)]) == []

    def test_unreadable_script(self, tmp_path):
        with pytest.raises(OSError):
            analyze_script(str(tmp_path / "missing.sh"))


class TestAnalyzeSubmission:
    def test_script_with_command_line_options(self, data, tmp_path):
        (tmp_path / "job.sh").write_text(
            "#!/bin/bash\n#SBATCH -J fit --mem=8G -c 4 -t 30\npython run.py\n"
        )
        sub = analyze_submission(
            ["--mem=16G", "job.sh", "%s/raw/s2.h5ad" % data], cwd=str(tmp_path), roots=[str(data)]
        )
        assert sub.job_name == "fit"
        assert sub.submit_line == "sbatch --mem=16G job.sh %s/raw/s2.h5ad" % data
        assert sub.request.mem_mb == 16384 and sub.request.cpus == 4 and sub.request.time_min == 30
        assert round(sub.input_gb * 1024**3) == 1_000_000, "the script's argument is an input"
        assert sub.shell and sub.script_hash and sub.wrap is None

    def test_job_name_defaults_to_the_script_name(self, tmp_path):
        (tmp_path / "align_reads.sh").write_text("#!/bin/bash\necho\n")
        assert (
            analyze_submission(["align_reads.sh"], cwd=str(tmp_path)).job_name == "align_reads.sh"
        )

    def test_wrap(self, data, tmp_path):
        (tmp_path / "seg.py").write_text("import scanpy\n")
        sub = analyze_submission(
            ["-c", "2", "--wrap", "python seg.py %s/raw/s1.h5ad" % data],
            cwd=str(tmp_path),
            roots=[str(data)],
        )
        assert sub.job_name == "wrap" and sub.wrap and sub.script_path is None
        assert sub.language == "python" and sub.tools == ["scanpy"]
        assert round(sub.input_gb * 1024**3) == 3_000_000
        assert sub.request.cpus == 2 and sub.shell

    def test_context_is_what_gets_recorded(self, data, tmp_path):
        sub = analyze_submission(
            ["--wrap", "cat %s/raw/s1.h5ad" % data], cwd=str(tmp_path), roots=[str(data)]
        )
        ctx = sub.context()
        assert ctx["input_gb"] == round(3_000_000 / 1024**3, 6)
        assert ctx["inputs"][0]["path"].endswith("raw/s1.h5ad") and ctx["script_hash"]

    def test_ignored_directive_is_reported(self, tmp_path):
        (tmp_path / "job.sh").write_text("#!/bin/bash\necho start\n#SBATCH --mem=64G\n")
        sub = analyze_submission(["job.sh"], cwd=str(tmp_path))
        assert sub.request.mem_mb is None
        assert any("line 3" in note and "ignored by sbatch" in note for note in sub.notes)

    def test_unknown_option_is_reported(self, tmp_path):
        (tmp_path / "job.sh").write_text("#!/bin/bash\n")
        sub = analyze_submission(["--frobnicate", "job.sh"], cwd=str(tmp_path))
        assert sub.parsed.uncertain and any("--frobnicate" in note for note in sub.notes)

    def test_missing_script(self, tmp_path):
        with pytest.raises(OSError):
            analyze_submission(["nope.sh"], cwd=str(tmp_path))

    def test_script_on_standard_input(self):
        sub = analyze_submission(["--mem=4G"])
        assert sub.job_name == "sbatch" and sub.script_path is None and sub.request.mem_mb == 4096
