"""The version 2 database, frozen: the schema its last release created, as SQL.

The version 2 code is gone; a production database written by it still has to
migrate. This is that schema verbatim, opened the way version 2 opened it (WAL
mode), so the migration is tested against the real thing rather than a sketch.
"""

import sqlite3
from typing import Any, Dict

V2_DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    user            TEXT,
    job_name        TEXT,
    account         TEXT,
    qos             TEXT,
    state           TEXT,
    submit_time     TEXT,
    start_time      TEXT,
    end_time        TEXT,
    req_mem_gb      REAL,
    req_cpus        INTEGER,
    req_time_min    INTEGER,
    req_gpus        INTEGER DEFAULT 0,
    sacct_peak_rss_gb   REAL,
    sacct_elapsed_sec   INTEGER,
    sacct_cpu_time_sec  INTEGER,
    sidecar_peak_gb     REAL,
    sidecar_p95_gb      REAL,
    sidecar_median_gb   REAL,
    sidecar_peak_read_mb_s  REAL,
    sidecar_peak_write_mb_s REAL,
    sidecar_avg_threads     REAL,
    sidecar_numa_miss_rate  REAL,
    static_tools    TEXT,
    runtime_tools   TEXT,
    input_files     TEXT,
    conda_env       TEXT,
    script_hash     TEXT,
    mem_efficiency  REAL,
    cpu_efficiency  REAL,
    waste_gb        REAL,
    flags           TEXT,
    cpi             REAL,
    cache_miss_rate REAL,
    lustre_peak_read_mb_s   REAL,
    lustre_peak_write_mb_s  REAL,
    lustre_avg_metadata_ops_s REAL,
    lustre_total_read_gb    REAL,
    lustre_total_write_gb   REAL,
    num_nodes           INTEGER DEFAULT 1,
    node_imbalance_cv   REAL,
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


def connect(path: str) -> sqlite3.Connection:
    """A connection opened as version 2 opened every one of its connections."""
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def create(path: str) -> None:
    conn = connect(path)
    with conn:
        conn.executescript(V2_DDL)
    conn.close()


def insert_job(path: str, job: Dict[str, Any]) -> None:
    """``INSERT OR REPLACE`` of the given columns, on a fresh connection, as version 2 did."""
    conn = connect(path)
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO jobs (%s) VALUES (%s)"
            % (", ".join(job), ", ".join("?" * len(job))),
            list(job.values()),
        )
    conn.close()
