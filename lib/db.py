"""Database access layer for HPCsizer.

Uses SQLite with WAL mode stored on shared group storage.
"""

import sqlite3
import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List

DEFAULT_DB_PATH = os.environ.get(
    "HPCSIZER_DB",
    str(Path(__file__).parent.parent / "profiles.db"),
)


def get_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Return a SQLite connection with WAL mode and row_factory set."""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Create all tables if they do not exist."""
    conn = get_connection(db_path)
    with conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                -- identification
                job_id          TEXT PRIMARY KEY,
                user            TEXT,
                job_name        TEXT,
                account         TEXT,
                qos             TEXT,
                state           TEXT,
                submit_time     TEXT,
                start_time      TEXT,
                end_time        TEXT,
                -- resource requests
                req_mem_gb      REAL,
                req_cpus        INTEGER,
                req_time_min    INTEGER,
                req_gpus        INTEGER DEFAULT 0,
                -- actual usage from sacct
                sacct_peak_rss_gb   REAL,
                sacct_elapsed_sec   INTEGER,
                sacct_cpu_time_sec  INTEGER,
                -- actual usage from sidecar
                sidecar_peak_gb     REAL,
                sidecar_p95_gb      REAL,
                sidecar_median_gb   REAL,
                sidecar_peak_read_mb_s  REAL,
                sidecar_peak_write_mb_s REAL,
                sidecar_avg_threads     REAL,
                sidecar_numa_miss_rate  REAL,
                -- detected context
                static_tools    TEXT,   -- JSON list
                runtime_tools   TEXT,   -- JSON list
                input_files     TEXT,   -- JSON list of {path, size_gb}
                conda_env       TEXT,
                script_hash     TEXT,
                -- computed efficiency
                mem_efficiency  REAL,
                cpu_efficiency  REAL,
                waste_gb        REAL,
                -- anomaly flags
                flags           TEXT,   -- JSON list of flag names
                -- optional perf counters
                cpi             REAL,
                cache_miss_rate REAL,
                -- source tracking
                has_sidecar     INTEGER DEFAULT 0,
                created_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS tool_models (
                tool            TEXT PRIMARY KEY,
                mem_per_input_gb    REAL,
                baseline_gb         REAL,
                optimal_cpus        INTEGER,
                r_squared           REAL,
                sample_count        INTEGER,
                updated_at          TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS daily_summary (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                summary_date    TEXT,
                user            TEXT,
                job_count       INTEGER DEFAULT 0,
                total_waste_gb  REAL DEFAULT 0,
                avg_mem_efficiency  REAL,
                avg_cpu_efficiency  REAL,
                flag_count      INTEGER DEFAULT 0,
                UNIQUE(summary_date, user)
            );
            """
        )
    conn.close()


def insert_job(job: Dict[str, Any], db_path: str = DEFAULT_DB_PATH) -> None:
    """Insert or replace a job record."""
    for field in ("static_tools", "runtime_tools", "input_files", "flags"):
        if field in job and isinstance(job[field], (list, dict)):
            job[field] = json.dumps(job[field])
    conn = get_connection(db_path)
    columns = ", ".join(job.keys())
    placeholders = ", ".join("?" * len(job))
    sql = f"INSERT OR REPLACE INTO jobs ({columns}) VALUES ({placeholders})"
    with conn:
        conn.execute(sql, list(job.values()))
    conn.close()


def get_job(job_id: str, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    """Fetch a single job by ID."""
    conn = get_connection(db_path)
    row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    conn.close()
    if row is None:
        return None
    return dict(row)


def query_jobs(
    user: Optional[str] = None,
    tool: Optional[str] = None,
    days: int = 30,
    db_path: str = DEFAULT_DB_PATH,
) -> List[Dict[str, Any]]:
    """Query jobs with optional filters."""
    clauses = ["datetime(end_time) >= datetime('now', ?)"]
    params: list = [f"-{days} days"]
    if user:
        clauses.append("user = ?")
        params.append(user)
    if tool:
        clauses.append("(static_tools LIKE ? OR runtime_tools LIKE ?)")
        params.extend([f"%{tool}%", f"%{tool}%"])
    where = " AND ".join(clauses)
    conn = get_connection(db_path)
    rows = conn.execute(
        f"SELECT * FROM jobs WHERE {where} ORDER BY end_time DESC", params
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_tool_model(
    tool: str, db_path: str = DEFAULT_DB_PATH
) -> Optional[Dict[str, Any]]:
    """Fetch regression model for a tool."""
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT * FROM tool_models WHERE tool = ?", (tool,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_tool_model(model: Dict[str, Any], db_path: str = DEFAULT_DB_PATH) -> None:
    """Insert or replace a tool model."""
    conn = get_connection(db_path)
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO tool_models
                (tool, mem_per_input_gb, baseline_gb, optimal_cpus,
                 r_squared, sample_count, updated_at)
            VALUES (:tool, :mem_per_input_gb, :baseline_gb, :optimal_cpus,
                    :r_squared, :sample_count, datetime('now'))
            """,
            model,
        )
    conn.close()


def upsert_daily_summary(
    summary_date: str,
    user: str,
    data: Dict[str, Any],
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    """Upsert a daily per-user summary row."""
    conn = get_connection(db_path)
    with conn:
        conn.execute(
            """
            INSERT INTO daily_summary
                (summary_date, user, job_count, total_waste_gb,
                 avg_mem_efficiency, avg_cpu_efficiency, flag_count)
            VALUES (:summary_date, :user, :job_count, :total_waste_gb,
                    :avg_mem_efficiency, :avg_cpu_efficiency, :flag_count)
            ON CONFLICT(summary_date, user) DO UPDATE SET
                job_count           = excluded.job_count,
                total_waste_gb      = excluded.total_waste_gb,
                avg_mem_efficiency  = excluded.avg_mem_efficiency,
                avg_cpu_efficiency  = excluded.avg_cpu_efficiency,
                flag_count          = excluded.flag_count
            """,
            {"summary_date": summary_date, "user": user, **data},
        )
    conn.close()
