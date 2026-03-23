"""Tests for lib/analyzer.py"""

import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.analyzer import (
    parse_sbatch_directives,
    detect_language,
    detect_tools,
    detect_input_files,
    script_hash,
    analyze_script,
)


SAMPLE_R_SCRIPT = """#!/bin/bash
#SBATCH --job-name=seurat_analysis
#SBATCH --mem=128G
#SBATCH --cpus-per-task=8
#SBATCH --time=12:00:00
#SBATCH --gres=gpu:0

module load R/4.3

Rscript - <<'EOF'
library(Seurat)
library(dplyr)
obj <- readRDS("/blue/group/data/cells.rds")
obj <- SCTransform(obj)
obj <- FindMarkers(obj, ident.1="A", ident.2="B")
saveRDS(obj, "/blue/group/results/out.rds")
EOF
"""

SAMPLE_PYTHON_SCRIPT = """#!/usr/bin/env python3
#SBATCH --mem=64G
#SBATCH --cpus-per-task=4
#SBATCH --time=4:00:00

import scanpy as sc
import scvi

adata = sc.read_h5ad("/blue/group/data/cells.h5ad")
scvi.model.SCVI.setup_anndata(adata)
"""

SAMPLE_CELLRANGER_SCRIPT = """#!/bin/bash
#SBATCH --mem=256G
#SBATCH --cpus-per-task=16
#SBATCH --time=24:00:00

cellranger count --id=sample1 --fastqs=/blue/group/raw/ --sample=sample1
"""


class TestSbatchDirectives:
    def test_mem_G(self):
        d = parse_sbatch_directives("#SBATCH --mem=128G\n")
        assert d["req_mem_gb"] == pytest.approx(128.0)

    def test_mem_M(self):
        d = parse_sbatch_directives("#SBATCH --mem=4096M\n")
        assert d["req_mem_gb"] == pytest.approx(4.0)

    def test_mem_per_cpu(self):
        d = parse_sbatch_directives("#SBATCH --mem-per-cpu=8G\n")
        assert d["req_mem_gb"] == pytest.approx(8.0)

    def test_cpus(self):
        d = parse_sbatch_directives("#SBATCH --cpus-per-task=8\n")
        assert d["req_cpus"] == 8

    def test_time_hms(self):
        d = parse_sbatch_directives("#SBATCH --time=12:00:00\n")
        assert d["req_time_min"] == 720

    def test_time_dhms(self):
        d = parse_sbatch_directives("#SBATCH --time=1-06:00:00\n")
        assert d["req_time_min"] == 1 * 1440 + 360

    def test_gpus(self):
        d = parse_sbatch_directives("#SBATCH --gres=gpu:2\n")
        assert d["req_gpus"] == 2

    def test_gpus_with_type(self):
        d = parse_sbatch_directives("#SBATCH --gres=gpu:a100:4\n")
        assert d["req_gpus"] == 4

    def test_full_r_script(self):
        d = parse_sbatch_directives(SAMPLE_R_SCRIPT)
        assert d["req_mem_gb"] == pytest.approx(128.0)
        assert d["req_cpus"] == 8
        assert d["req_time_min"] == 720
        assert d["req_gpus"] == 0
        assert d["job_name"] == "seurat_analysis"


class TestLanguageDetection:
    def test_r_module(self):
        assert detect_language(SAMPLE_R_SCRIPT) == "R"

    def test_python_import(self):
        assert detect_language(SAMPLE_PYTHON_SCRIPT) == "python"

    def test_rscript_shebang(self):
        assert detect_language("#!/usr/bin/env Rscript\nlibrary(ggplot2)") == "R"

    def test_bash_default(self):
        assert detect_language("#!/bin/bash\necho hello") == "bash"

    def test_cellranger(self):
        assert detect_language(SAMPLE_CELLRANGER_SCRIPT) == "bash"


class TestToolDetection:
    def test_r_libraries(self):
        tools = detect_tools(SAMPLE_R_SCRIPT, "R")
        assert "Seurat" in tools
        assert "dplyr" in tools

    def test_python_imports(self):
        tools = detect_tools(SAMPLE_PYTHON_SCRIPT, "python")
        assert "scanpy" in tools
        assert "scvi" in tools

    def test_cli_tool(self):
        tools = detect_tools(SAMPLE_CELLRANGER_SCRIPT, "bash")
        assert "cellranger" in tools

    def test_seurat_ns_operator(self):
        text = "Seurat::FindVariableFeatures(obj)"
        tools = detect_tools(text, "R")
        assert "Seurat" in tools


class TestInputFileDetection:
    def test_finds_blue_paths(self):
        text = 'readRDS("/blue/group/data/cells.rds")'
        files = detect_input_files(text)
        assert any(f["path"].endswith("cells.rds") for f in files)

    def test_finds_orange_paths(self):
        text = 'read.csv("/orange/group/data/meta.csv")'
        files = detect_input_files(text)
        assert any(f["path"].endswith("meta.csv") for f in files)

    def test_finds_home_paths(self):
        text = 'read.csv("/home/user/data/meta.csv")'
        files = detect_input_files(text)
        assert any(f["path"].endswith("meta.csv") for f in files)

    def test_finds_red_paths(self):
        text = 'readRDS("/red/group/data/cells.rds")'
        files = detect_input_files(text)
        assert any(f["path"].endswith("cells.rds") for f in files)

    def test_no_size_for_nonexistent(self):
        text = 'readRDS("/blue/nonexistent/fake.rds")'
        files = detect_input_files(text)
        assert files[0]["size_gb"] is None

    def test_deduplication(self):
        text = 'f("/blue/a/b.rds") g("/blue/a/b.rds")'
        files = detect_input_files(text)
        paths = [f["path"] for f in files]
        assert len(paths) == len(set(paths))


class TestScriptHash:
    def test_deterministic(self):
        text = "hello world"
        assert script_hash(text) == script_hash(text)

    def test_different_for_different_content(self):
        assert script_hash("abc") != script_hash("xyz")

    def test_returns_hex_string(self):
        h = script_hash("test")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


class TestAnalyzeScript:
    def test_analyze_r_script(self, tmp_path):
        p = tmp_path / "test.sbatch"
        p.write_text(SAMPLE_R_SCRIPT)
        result = analyze_script(str(p))
        assert result["language"] == "R"
        assert "Seurat" in result["static_tools"]
        assert result["req_mem_gb"] == pytest.approx(128.0)
        assert result["req_cpus"] == 8
        assert isinstance(result["script_hash"], str)

    def test_analyze_python_script(self, tmp_path):
        p = tmp_path / "test.sbatch"
        p.write_text(SAMPLE_PYTHON_SCRIPT)
        result = analyze_script(str(p))
        assert result["language"] == "python"
        assert "scanpy" in result["static_tools"]
