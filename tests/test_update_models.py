"""Tests for bin/update_models.py"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from bin.update_models import fit_model, update_models
from lib.db import get_tool_model, init_db, insert_job


def _make_jobs_with_model(n=15, slope=3.0, baseline=2.0, noise=0.5):
    """Generate synthetic jobs for model fitting."""
    import random

    random.seed(42)
    jobs = []
    for i in range(n):
        input_gb = (i + 1) * 2.0
        peak_gb = slope * input_gb + baseline + random.uniform(-noise, noise)
        jobs.append(
            {
                "job_id": str(i),
                "state": "COMPLETED",
                "static_tools": json.dumps(["Seurat"]),
                "input_files": json.dumps([{"path": "/blue/data.rds", "size_gb": input_gb}]),
                "sidecar_peak_gb": peak_gb,
                "req_cpus": 4,
                "end_time": "2026-01-01T12:00:00",
                "flags": "[]",
            }
        )
    return jobs


class TestFitModel:
    def test_returns_none_when_too_few_samples(self):
        jobs = _make_jobs_with_model(n=5)
        assert fit_model(jobs) is None

    def test_fits_with_sufficient_data(self):
        jobs = _make_jobs_with_model(n=15, slope=3.0, baseline=2.0, noise=0.1)
        model = fit_model(jobs)
        assert model is not None
        assert model["mem_per_input_gb"] == pytest.approx(3.0, abs=0.2)
        assert model["baseline_gb"] == pytest.approx(2.0, abs=0.5)
        assert model["r_squared"] > 0.95

    def test_model_keys(self):
        jobs = _make_jobs_with_model(n=15)
        model = fit_model(jobs)
        assert model is not None
        for key in ("mem_per_input_gb", "baseline_gb", "optimal_cpus", "r_squared", "sample_count"):
            assert key in model

    def test_non_negative_params(self):
        jobs = _make_jobs_with_model(n=15, slope=0.1, baseline=0.0)
        model = fit_model(jobs)
        if model:
            assert model["mem_per_input_gb"] >= 0
            assert model["baseline_gb"] >= 0


class TestUpdateModels:
    def test_update_stores_model(self, tmp_path):
        db = str(tmp_path / "test.db")
        init_db(db)
        for job in _make_jobs_with_model(n=15):
            insert_job(job, db_path=db)
        update_models(db_path=db, days=3650)
        model = get_tool_model("Seurat", db_path=db)
        assert model is not None
        assert model["sample_count"] >= 10

    def test_update_skips_tool_with_few_samples(self, tmp_path):
        db = str(tmp_path / "sparse.db")
        init_db(db)
        for i in range(3):
            insert_job(
                {
                    "job_id": str(i),
                    "state": "COMPLETED",
                    "static_tools": json.dumps(["rare_tool"]),
                    "input_files": json.dumps([{"path": "/blue/x.rds", "size_gb": float(i + 1)}]),
                    "sidecar_peak_gb": float((i + 1) * 2),
                    "req_cpus": 4,
                    "end_time": "2026-01-01T12:00:00",
                    "flags": "[]",
                },
                db_path=db,
            )
        update_models(db_path=db, days=3650)
        model = get_tool_model("rare_tool", db_path=db)
        assert model is None
