"""Tests for lib/db.py"""

# Use a temp DB for every test
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.db import (
    get_connection,
    get_job,
    get_tool_model,
    init_db,
    insert_job,
    is_internal_job,
    query_jobs,
    upsert_tool_model,
)


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


# --- HPCsizer internal-job exclusion ---


@pytest.mark.parametrize(
    "job_name",
    ["hpcsizer-scheduler", "hpcsizer-watchdog", "hpcsizer-anything"],
)
def test_is_internal_job_true(job_name):
    assert is_internal_job(job_name) is True


@pytest.mark.parametrize(
    "job_name",
    ["seurat_test", "hpg-interactive-jupyter", "my-hpcsizer-job", "", None],
)
def test_is_internal_job_false(job_name):
    assert is_internal_job(job_name) is False


def test_insert_job_skips_internal(tmp_db):
    insert_job(
        {"job_id": "sched1", "user": "smith6jt", "job_name": "hpcsizer-scheduler", "flags": []},
        db_path=tmp_db,
    )
    insert_job(
        {"job_id": "wd1", "user": "smith6jt", "job_name": "hpcsizer-watchdog", "flags": []},
        db_path=tmp_db,
    )
    assert get_job("sched1", db_path=tmp_db) is None
    assert get_job("wd1", db_path=tmp_db) is None


def test_insert_job_still_stores_real_jobs(tmp_db):
    """Guard against an over-broad predicate swallowing real work."""
    insert_job(
        {"job_id": "real1", "user": "smith6jt", "job_name": "seurat_run", "flags": []},
        db_path=tmp_db,
    )
    insert_job(
        {"job_id": "real2", "user": "smith6jt", "job_name": "hpg-interactive-jupyter", "flags": []},
        db_path=tmp_db,
    )
    assert get_job("real1", db_path=tmp_db) is not None
    assert get_job("real2", db_path=tmp_db) is not None


def _insert_bypassing_guard(db_path, job_id, job_name):
    """Write a row directly, as pre-purge rows were written before the guard."""
    conn = get_connection(db_path)
    with conn:
        conn.execute(
            "INSERT INTO jobs (job_id, user, job_name, state, end_time) "
            "VALUES (?, 'smith6jt', ?, 'COMPLETED', '2026-01-01T12:00:00')",
            (job_id, job_name),
        )
    conn.close()


def test_query_jobs_excludes_preexisting_internal_rows(tmp_db):
    _insert_bypassing_guard(tmp_db, "old_sched", "hpcsizer-scheduler")
    _insert_bypassing_guard(tmp_db, "old_real", "seurat_run")
    ids = {j["job_id"] for j in query_jobs(days=365 * 10, db_path=tmp_db)}
    assert ids == {"old_real"}


def test_query_jobs_keeps_null_job_name(tmp_db):
    """COALESCE keeps rows with a NULL job_name; a bare NOT LIKE would drop them."""
    insert_job(
        {
            "job_id": "noname",
            "user": "smith6jt",
            "state": "COMPLETED",
            "end_time": "2026-01-01T12:00:00",
            "flags": [],
        },
        db_path=tmp_db,
    )
    ids = {j["job_id"] for j in query_jobs(days=365 * 10, db_path=tmp_db)}
    assert "noname" in ids


def test_get_job_still_returns_internal_rows(tmp_db):
    """Direct lookup by job ID is deliberately unfiltered."""
    _insert_bypassing_guard(tmp_db, "old_sched", "hpcsizer-scheduler")
    result = get_job("old_sched", db_path=tmp_db)
    assert result is not None
    assert result["job_name"] == "hpcsizer-scheduler"
