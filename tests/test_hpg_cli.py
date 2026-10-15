"""Tests for bin/hpg helpers that rewrite sbatch directives.

bin/hpg has no .py extension (it is installed as a console script), so it is
loaded by path rather than imported normally.
"""

import importlib.machinery
import importlib.util
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

pytest.importorskip("click")

_HPG_PATH = Path(__file__).parent.parent / "bin" / "hpg"
_spec = importlib.util.spec_from_loader(
    "hpg_cli", importlib.machinery.SourceFileLoader("hpg_cli", str(_HPG_PATH))
)
hpg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hpg)


SCRIPT_WITH_DIRECTIVES = """#!/bin/bash
#SBATCH --job-name=test
#SBATCH --mem=256G
#SBATCH --cpus-per-task=8
#SBATCH --time=12:00:00

Rscript analysis.R
"""

SCRIPT_WITHOUT_DIRECTIVES = """#!/bin/bash
#SBATCH --job-name=test

Rscript analysis.R
"""


class TestFormatWalltime:
    def test_renders_hh_mm_ss(self):
        assert hpg._format_walltime(90) == "01:30:00"
        assert hpg._format_walltime(15) == "00:15:00"
        assert hpg._format_walltime(1500) == "25:00:00"

    def test_none_is_unknown(self):
        assert hpg._format_walltime(None) == "unknown"


class TestApplyRecommendations:
    def test_overwrites_existing_directives(self):
        rec = {"mem_gb": 64.0, "cpus": 4, "time_min": 90}
        out = hpg._apply_recommendations(SCRIPT_WITH_DIRECTIVES, rec)
        assert "#SBATCH --mem=64G" in out
        assert "#SBATCH --cpus-per-task=4" in out
        assert "#SBATCH --time=01:30:00" in out
        assert "256G" not in out

    def test_inserts_missing_directives(self):
        """Plain re.sub left these out, so accepting a recommendation did nothing."""
        rec = {"mem_gb": 64.0, "cpus": 4, "time_min": 90}
        out = hpg._apply_recommendations(SCRIPT_WITHOUT_DIRECTIVES, rec)
        assert "#SBATCH --mem=64G" in out
        assert "#SBATCH --cpus-per-task=4" in out
        assert "#SBATCH --time=01:30:00" in out

    def test_inserted_directives_stay_in_header(self):
        out = hpg._apply_recommendations(SCRIPT_WITHOUT_DIRECTIVES, {"mem_gb": 8.0, "cpus": 2})
        lines = out.splitlines()
        last_sbatch = max(i for i, ln in enumerate(lines) if ln.startswith("#SBATCH"))
        payload = min(i for i, ln in enumerate(lines) if ln.startswith("Rscript"))
        assert last_sbatch < payload

    def test_preserves_mem_per_cpu_form(self):
        script = SCRIPT_WITH_DIRECTIVES.replace("--mem=256G", "--mem-per-cpu=32G")
        out = hpg._apply_recommendations(script, {"mem_gb": 64.0, "cpus": 4})
        assert "#SBATCH --mem-per-cpu=16G" in out  # 64 / 4
        assert "--mem=" not in out.replace("--mem-per-cpu=", "")

    def test_no_memory_directive_when_recommendation_is_none(self):
        out = hpg._apply_recommendations(SCRIPT_WITH_DIRECTIVES, {"mem_gb": None, "cpus": 4})
        assert "#SBATCH --mem=256G" in out  # left untouched
        assert "#SBATCH --cpus-per-task=4" in out

    def test_walltime_untouched_when_not_recommended(self):
        out = hpg._apply_recommendations(SCRIPT_WITH_DIRECTIVES, {"mem_gb": 8.0, "cpus": 2})
        assert "#SBATCH --time=12:00:00" in out
