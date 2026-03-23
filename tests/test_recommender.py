"""Tests for lib/recommender.py"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.recommender import _cold_start_cpus, _cold_start_mem, recommend


class TestColdStartHeuristics:
    def test_seurat_memory(self):
        mem = _cold_start_mem(["Seurat"], total_input_gb=10.0)
        assert mem == pytest.approx(30.0)  # 3.0 × 10 GB

    def test_seurat_sctransform_memory(self):
        mem = _cold_start_mem(["Seurat_SCTransform"], total_input_gb=10.0)
        assert mem == pytest.approx(45.0)  # 4.5 × 10 GB

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
        from lib.db import init_db, insert_job

        init_db(db)
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
                    "end_time": "2026-01-01T12:00:00",
                    "flags": "[]",
                },
                db_path=db,
            )
        rec = recommend(["Seurat"], total_input_gb=10.0, db_path=db)
        assert rec["source"] == "database"

    def test_mem_positive(self, tmp_path):
        db = str(tmp_path / "test2.db")
        from lib.db import init_db

        init_db(db)
        rec = recommend(["scanpy"], total_input_gb=0.0, db_path=db)
        assert rec["mem_gb"] > 0

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
