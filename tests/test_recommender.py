"""Tests for lib/recommender.py"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.db import get_connection, init_db
from lib.recommender import _cold_start_cpus, _cold_start_mem, recommend


class TestColdStartHeuristics:
    def test_seurat_memory(self):
        mem = _cold_start_mem(["Seurat"], total_input_gb=10.0)
        assert mem == pytest.approx(30.0)  # 3.0 × 10 GB

    def test_sctransform_memory(self):
        # detect_tools emits bare names, so this is the key that can actually match
        mem = _cold_start_mem(["SCTransform"], total_input_gb=10.0)
        assert mem == pytest.approx(45.0)  # 4.5 × 10 GB

    def test_most_demanding_tool_wins_not_alphabetical(self):
        """sorted() put FindMarkers (2.5x) ahead of SCTransform (4.5x); take the max."""
        tools = sorted(["Seurat", "SCTransform", "FindMarkers", "readRDS"])
        assert tools[0] == "FindMarkers"
        assert _cold_start_mem(tools, total_input_gb=10.0) == pytest.approx(45.0)
        assert _cold_start_cpus(tools) == 4

    def test_no_estimate_without_input_or_history(self):
        """A multiplier-driven tool with nothing to multiply must not invent a number."""
        assert _cold_start_mem(["Seurat"], total_input_gb=0.0) is None
        assert _cold_start_mem(["scanpy"], total_input_gb=0.0) is None

    def test_scimap_has_a_real_baseline(self):
        mem = _cold_start_mem(["scimap"], total_input_gb=0.0)
        assert mem == pytest.approx(16.0)  # was 1.0 GB from max(0, 0, 1.0)

    def test_unknown_tool_without_input_declines_too(self):
        """An unrecognised tool is less informative than a recognised one, so it
        must not answer where a recognised one declines."""
        assert _cold_start_mem([], total_input_gb=0.0) is None
        assert _cold_start_mem(["totally_unknown"], total_input_gb=0.0) is None

    def test_input_independent_tools_still_answer_without_input(self):
        """mult == 0, baseline > 0 entries are not input-driven; keep them working."""
        assert _cold_start_mem(["cellranger"], total_input_gb=0.0) == pytest.approx(32.0)
        assert _cold_start_mem(["STAR"], total_input_gb=0.0) == pytest.approx(32.0)
        assert _cold_start_mem(["salmon"], total_input_gb=0.0) == pytest.approx(16.0)

    def test_scanpy_memory(self):
        mem = _cold_start_mem(["scanpy"], total_input_gb=8.0)
        assert mem == pytest.approx(16.0)  # 2.0 × 8 GB

    def test_cellranger_baseline(self):
        mem = _cold_start_mem(["cellranger"], total_input_gb=0.0)
        assert mem == pytest.approx(32.0)

    def test_generic_fallback(self):
        mem = _cold_start_mem([], total_input_gb=5.0)
        assert mem == pytest.approx(max(2.0 * 5.0 + 16.0, 16.0))

    def test_qupath_per_core(self):
        mem = _cold_start_mem(["QuPath"], total_input_gb=0.0, req_cpus=8)
        assert mem == pytest.approx(16.0)  # 2 GB × 8 cores

    def test_seurat_cpus(self):
        cpus = _cold_start_cpus(["Seurat"])
        assert cpus == 4

    def test_cellranger_cpus(self):
        cpus = _cold_start_cpus(["cellranger"])
        assert cpus == 8


class TestRecommend:
    def test_returns_dict_with_required_keys(self, tmp_path):
        db = str(tmp_path / "test.db")
        from lib.db import init_db

        init_db(db)
        rec = recommend(["Seurat"], total_input_gb=10.0, db_path=db)
        assert "mem_gb" in rec
        assert "cpus" in rec
        assert "source" in rec

    def test_heuristic_source_when_no_db_data(self, tmp_path):
        db = str(tmp_path / "empty.db")
        from lib.db import init_db

        init_db(db)
        rec = recommend(["Seurat"], total_input_gb=5.0, db_path=db)
        assert rec["source"] == "heuristic"

    def test_db_source_when_sufficient_data(self, tmp_path):
        db = str(tmp_path / "full.db")
        from datetime import datetime

        from lib.db import init_db, insert_job

        init_db(db)
        recent_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        for i in range(6):
            insert_job(
                {
                    "job_id": str(i),
                    "user": "alice",
                    "state": "COMPLETED",
                    "static_tools": '["Seurat"]',
                    "sidecar_peak_gb": 20.0 + i,
                    "req_mem_gb": 128.0,
                    "req_cpus": 4,
                    "end_time": recent_time,
                    "flags": "[]",
                },
                db_path=db,
            )
        rec = recommend(["Seurat"], total_input_gb=10.0, db_path=db)
        assert rec["source"] == "database"

    def test_mem_positive(self, tmp_path):
        db = str(tmp_path / "test2.db")

        init_db(db)
        rec = recommend(["scanpy"], total_input_gb=8.0, db_path=db)
        assert rec["mem_gb"] > 0

    def test_declines_to_guess_without_input_or_history(self, tmp_path):
        db = str(tmp_path / "noguess.db")
        init_db(db)
        rec = recommend(["scanpy"], total_input_gb=0.0, db_path=db)
        assert rec["mem_gb"] is None
        assert rec["source"] == "insufficient-data"
        assert any("hpg history" in w for w in rec["warnings"])

    def test_model_source_when_model_exists(self, tmp_path):
        db = str(tmp_path / "model.db")
        from lib.db import init_db, upsert_tool_model

        init_db(db)
        upsert_tool_model(
            {
                "tool": "Seurat",
                "mem_per_input_gb": 3.0,
                "baseline_gb": 2.0,
                "optimal_cpus": 4,
                "r_squared": 0.9,
                "sample_count": 15,
            },
            db_path=db,
        )
        rec = recommend(["Seurat"], total_input_gb=10.0, db_path=db)
        assert rec["source"] == "model"
        assert rec["mem_gb"] == pytest.approx((3.0 * 10.0 + 2.0) * 1.10)


def _seed(
    db, n, tool="Seurat", *, sidecar=False, peak=10.0, elapsed=3600, state="COMPLETED", user="alice"
):
    """Insert jobs directly, bypassing insert_job's JSON encoding."""
    conn = get_connection(db)
    with conn:
        for i in range(n):
            conn.execute(
                "INSERT INTO jobs (job_id, user, job_name, state, end_time, static_tools,"
                " req_cpus, sidecar_peak_gb, sacct_peak_rss_gb, sacct_elapsed_sec)"
                " VALUES (?,?,?,?,datetime('now','-1 day'),?,?,?,?,?)",
                (
                    f"{state}-{user}-{tool}-{i}",
                    user,
                    f"{tool}_job",
                    state,
                    f'["{tool}"]',
                    4,
                    peak if sidecar else None,
                    peak,
                    elapsed,
                ),
            )
    conn.close()


class TestDatabaseTier:
    def test_sacct_only_history_still_counts(self, tmp_path):
        """Backfilled jobs have no sidecar_peak_gb; requiring it hid the whole tier."""
        db = str(tmp_path / "sacct.db")
        init_db(db)
        _seed(db, 6, sidecar=False, peak=20.0)
        rec = recommend(["Seurat"], total_input_gb=0.0, db_path=db)
        assert rec["source"] == "database"
        assert rec["samples"] == 6
        assert rec["sidecar_samples"] == 0
        assert rec["mem_gb"] == pytest.approx(22.0)  # p90 20 GB x 1.10

    def test_user_filter_narrows_samples(self, tmp_path):
        db = str(tmp_path / "users.db")
        init_db(db)
        _seed(db, 6, user="alice")
        _seed(db, 6, user="bob")
        assert recommend(["Seurat"], 0.0, db_path=db)["samples"] == 12
        assert recommend(["Seurat"], 0.0, db_path=db, user="alice")["samples"] == 6


class TestWalltime:
    def test_walltime_from_history(self, tmp_path):
        db = str(tmp_path / "wt.db")
        init_db(db)
        _seed(db, 6, elapsed=3600)  # 60 min
        rec = recommend(["Seurat"], total_input_gb=0.0, db_path=db)
        assert rec["time_min"] == 90  # 60 x 1.5 headroom
        assert rec["time_samples"] == 6

    def test_timeout_jobs_excluded_from_walltime(self, tmp_path):
        """TIMEOUT elapsed is the old limit, not the real need -- it must not anchor."""
        db = str(tmp_path / "to.db")
        init_db(db)
        _seed(db, 6, elapsed=3600, state="COMPLETED")
        _seed(db, 6, elapsed=86400, state="TIMEOUT")
        rec = recommend(["Seurat"], total_input_gb=0.0, db_path=db)
        assert rec["time_min"] == 90

    def test_no_walltime_below_threshold(self, tmp_path):
        db = str(tmp_path / "few.db")
        init_db(db)
        _seed(db, 2)
        assert recommend(["Seurat"], 0.0, db_path=db).get("time_min") is None


class TestModelTierGuards:
    def test_zero_model_result_is_not_a_recommendation(self, tmp_path):
        """A fit with a negative intercept stores baseline_gb=0; with no input that
        yields 0, which would be written as '#SBATCH --mem=0G' -- all node memory."""
        from lib.db import upsert_tool_model

        db = str(tmp_path / "zero.db")
        init_db(db)
        upsert_tool_model(
            {
                "tool": "Seurat",
                "mem_per_input_gb": 1.5,
                "baseline_gb": 0.0,
                "optimal_cpus": 8,
                "r_squared": 0.9,
                "sample_count": 30,
            },
            db_path=db,
        )
        rec = recommend(["Seurat"], total_input_gb=0.0, db_path=db)
        assert rec["mem_gb"] is None
        assert rec["source"] == "insufficient-data"

    def test_model_still_used_when_it_yields_a_positive_number(self, tmp_path):
        from lib.db import upsert_tool_model

        db = str(tmp_path / "model_ok.db")
        init_db(db)
        upsert_tool_model(
            {
                "tool": "Seurat",
                "mem_per_input_gb": 1.5,
                "baseline_gb": 0.0,
                "optimal_cpus": 8,
                "r_squared": 0.9,
                "sample_count": 30,
            },
            db_path=db,
        )
        rec = recommend(["Seurat"], total_input_gb=10.0, db_path=db)
        assert rec["source"] == "model"
        assert rec["mem_gb"] == pytest.approx(15.0 * 1.10)

    def test_user_filter_warns_that_model_tier_ignores_it(self, tmp_path):
        from lib.db import upsert_tool_model

        db = str(tmp_path / "warn.db")
        init_db(db)
        upsert_tool_model(
            {
                "tool": "Seurat",
                "mem_per_input_gb": 1.5,
                "baseline_gb": 4.0,
                "optimal_cpus": 8,
                "r_squared": 0.9,
                "sample_count": 30,
            },
            db_path=db,
        )
        rec = recommend(["Seurat"], total_input_gb=10.0, db_path=db, user="alice")
        assert rec["source"] == "model"
        assert any("do not apply to the fitted-model tier" in w for w in rec["warnings"])

    def test_no_filter_means_no_such_warning(self, tmp_path):
        from lib.db import upsert_tool_model

        db = str(tmp_path / "nowarn.db")
        init_db(db)
        upsert_tool_model(
            {
                "tool": "Seurat",
                "mem_per_input_gb": 1.5,
                "baseline_gb": 4.0,
                "optimal_cpus": 8,
                "r_squared": 0.9,
                "sample_count": 30,
            },
            db_path=db,
        )
        rec = recommend(["Seurat"], total_input_gb=10.0, db_path=db)
        assert not any("fitted-model tier" in w for w in rec["warnings"])
