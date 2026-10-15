"""Database access layer for HPCsizer.

Uses SQLite with WAL mode stored on shared group storage.
"""

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_DB_PATH = os.environ.get(
    "HPCSIZER_DB",
    str(Path(__file__).resolve().parent.parent / "profiles.db"),
)

# Job names belonging to HPCsizer's own infrastructure. These are excluded
# from ingest and from all reporting queries: the scheduler chain submits
# ~192 jobs/day at 512MB/1CPU for ~6s each (a successor and a watchdog every
# 15 minutes), which would otherwise dominate
# every efficiency statistic.
INTERNAL_JOB_NAME_PREFIXES = ("hpcsizer-",)


def is_internal_job(job_name: Optional[str]) -> bool:
    """True if job_name belongs to HPCsizer's own infrastructure."""
    if not job_name:
        return False
    return job_name.startswith(INTERNAL_JOB_NAME_PREFIXES)


_INTERNAL_SQL_FILTER = " AND ".join(
    f"COALESCE(job_name,'') NOT LIKE '{p}%'" for p in INTERNAL_JOB_NAME_PREFIXES
)


def input_gb_from_job(job: Dict[str, Any]) -> Optional[float]:
    """Total input size in GB for a stored job, parsed from the input_files JSON.

    There is no total_input_gb column: analyze_script() produces that key for a
    script about to be submitted, but what lands in the jobs table is the
    input_files list. Readers must derive the total from it.
    """
    raw = job.get("input_files")
    if not raw:
        return None
    if isinstance(raw, str):
        try:
            files = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return None
    else:
        files = raw
    total = sum(f.get("size_gb") or 0 for f in files if isinstance(f, dict))
    return total if total > 0 else None


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
                -- lustre filesystem metrics
                lustre_peak_read_mb_s   REAL,
                lustre_peak_write_mb_s  REAL,
                lustre_avg_metadata_ops_s REAL,
                lustre_total_read_gb    REAL,
                lustre_total_write_gb   REAL,
                -- multi-node
                num_nodes           INTEGER DEFAULT 1,
                node_imbalance_cv   REAL,
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
    migrate_db(db_path)


def migrate_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Add columns introduced in later versions. Safe to call repeatedly."""
    conn = get_connection(db_path)
    new_columns = [
        # Lustre metrics (Feature 1)
        ("lustre_peak_read_mb_s", "REAL"),
        ("lustre_peak_write_mb_s", "REAL"),
        ("lustre_avg_metadata_ops_s", "REAL"),
        ("lustre_total_read_gb", "REAL"),
        ("lustre_total_write_gb", "REAL"),
        # Hardware counters (Feature 2)
        # (cpi and cache_miss_rate already exist in schema)
        # Multi-node (Feature 3)
        ("num_nodes", "INTEGER DEFAULT 1"),
        ("node_imbalance_cv", "REAL"),
    ]
    with conn:
        for col, typedef in new_columns:
            try:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {typedef}")
            except sqlite3.OperationalError:
                pass  # column already exists
    conn.close()


def insert_job(job: Dict[str, Any], db_path: str = DEFAULT_DB_PATH) -> None:
    """Insert or replace a job record.

    HPCsizer's own infrastructure jobs are silently dropped here so that every
    ingest path (harvest.sh, backfill.py, finalize.py) is covered by one guard.
    """
    if is_internal_job(job.get("job_name")):
        return
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
    # exclude HPCsizer's own scheduler/harvest jobs
    clauses.append(_INTERNAL_SQL_FILTER)
    where = " AND ".join(clauses)
    conn = get_connection(db_path)
    rows = conn.execute(
        f"SELECT * FROM jobs WHERE {where} ORDER BY end_time DESC", params
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_tool_model(tool: str, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    """Fetch regression model for a tool."""
    conn = get_connection(db_path)
    row = conn.execute("SELECT * FROM tool_models WHERE tool = ?", (tool,)).fetchone()
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
