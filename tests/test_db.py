"""Tests for lib/db.py"""

# Use a temp DB for every test
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.db import get_job, get_tool_model, init_db, insert_job, query_jobs, upsert_tool_model


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    init_db(db)
    return db


def test_init_db_creates_tables(tmp_db):
    import sqlite3

    conn = sqlite3.connect(tmp_db)
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    conn.close()
    assert "jobs" in tables
    assert "tool_models" in tables
    assert "daily_summary" in tables


def test_migrate_adds_new_columns(tmp_db):
    from lib.db import migrate_db

    migrate_db(tmp_db)
    import sqlite3

    conn = sqlite3.connect(tmp_db)
    cursor = conn.execute("PRAGMA table_info(jobs)")
    columns = {row[1] for row in cursor.fetchall()}
    conn.close()
    assert "lustre_peak_read_mb_s" in columns
    assert "num_nodes" in columns
    assert "node_imbalance_cv" in columns


def test_insert_and_get_job(tmp_db):
    job = {
        "job_id": "12345",
        "user": "alice",
        "job_name": "seurat_test",
        "state": "COMPLETED",
        "req_mem_gb": 64.0,
        "req_cpus": 4,
        "sacct_peak_rss_gb": 20.0,
        "sacct_elapsed_sec": 3600,
        "sacct_cpu_time_sec": 14400,
        "static_tools": ["Seurat"],
        "flags": [],
    }
    insert_job(job, db_path=tmp_db)
    result = get_job("12345", db_path=tmp_db)
    assert result is not None
    assert result["user"] == "alice"
    assert result["req_mem_gb"] == 64.0


def test_insert_job_replaces_existing(tmp_db):
    job = {"job_id": "99", "user": "bob", "state": "COMPLETED", "flags": []}
    insert_job(job, db_path=tmp_db)
    job2 = {"job_id": "99", "user": "carol", "state": "FAILED", "flags": []}
    insert_job(job2, db_path=tmp_db)
    result = get_job("99", db_path=tmp_db)
    assert result["user"] == "carol"


def test_query_jobs_user_filter(tmp_db):
    for uid, user in [("1", "alice"), ("2", "bob"), ("3", "alice")]:
        insert_job(
            {
                "job_id": uid,
                "user": user,
                "state": "COMPLETED",
                "end_time": "2026-01-01T12:00:00",
                "flags": [],
            },
            db_path=tmp_db,
        )
    alice_jobs = query_jobs(user="alice", days=365 * 10, db_path=tmp_db)
    assert all(j["user"] == "alice" for j in alice_jobs)
    assert len(alice_jobs) == 2


def test_upsert_and_get_tool_model(tmp_db):
    model = {
        "tool": "Seurat",
        "mem_per_input_gb": 3.0,
        "baseline_gb": 2.0,
        "optimal_cpus": 4,
        "r_squared": 0.95,
        "sample_count": 25,
    }
    upsert_tool_model(model, db_path=tmp_db)
    result = get_tool_model("Seurat", db_path=tmp_db)
    assert result is not None
    assert result["mem_per_input_gb"] == 3.0
    assert result["sample_count"] == 25


def test_tool_model_upsert_updates(tmp_db):
    model = {
        "tool": "scanpy",
        "mem_per_input_gb": 2.0,
        "baseline_gb": 1.0,
        "optimal_cpus": 4,
        "r_squared": 0.8,
        "sample_count": 10,
    }
    upsert_tool_model(model, db_path=tmp_db)
    model2 = {**model, "sample_count": 20, "r_squared": 0.92}
    upsert_tool_model(model2, db_path=tmp_db)
    result = get_tool_model("scanpy", db_path=tmp_db)
    assert result["sample_count"] == 20
    assert result["r_squared"] == 0.92
