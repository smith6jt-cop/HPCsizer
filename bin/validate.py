#!/usr/bin/env python3
"""Validate HPCsizer setup.

Run without arguments after install to check that runtime directories,
the database, and the Python environment are ready.  Run with ``--db``
after backfilling to verify database contents.

Usage
-----
  python bin/validate.py          # post-install checks
  python bin/validate.py --db     # database content checks
"""

import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.db import (
    _INTERNAL_SQL_FILTER,
    DEFAULT_DB_PATH,
    INTERNAL_JOB_NAME_PREFIXES,
    init_db,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
REQUIRED_DIRS = ["timeseries", "plots", "logs"]
EXPECTED_TABLES = {"jobs", "daily_summary", "tool_models"}


def check_dirs():
    ok = True
    for d in REQUIRED_DIRS:
        p = REPO_ROOT / d
        if p.is_dir():
            print(f"  [OK]   {d}/")
        else:
            print(f"  [MISS] {d}/ — creating")
            p.mkdir(parents=True, exist_ok=True)
            ok = False
    return ok


def check_db(db_path):
    print(f"\n  Database: {db_path}")
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    missing = EXPECTED_TABLES - tables
    if missing:
        print(f"  [FAIL] Missing tables: {missing}")
        conn.close()
        return False
    for t in sorted(EXPECTED_TABLES):
        print(f"  [OK]   table '{t}' exists")
    conn.close()
    return True


def check_db_contents(db_path):
    """Report database contents.

    The raw row count describes the table itself and stays unfiltered, so that
    internal rows reappearing is visible rather than hidden.  Everything below
    the leak check draws a conclusion about real work and therefore excludes
    HPCsizer's own infrastructure jobs.
    """
    print(f"\n  Database: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ok = True

    total = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    print(f"\n  Total jobs (all rows): {total}")

    # Leak check: insert_job drops internal jobs, so any present here mean an
    # ingest path is bypassing the guard (renamed --job-name in scheduler.sh,
    # a new internal job, or a stale checkout of backfill.py).
    internal_pattern_sql = " OR ".join(
        f"COALESCE(job_name,'') LIKE '{p}%'" for p in INTERNAL_JOB_NAME_PREFIXES
    )
    internal_sql = f"SELECT COUNT(*) FROM jobs WHERE {internal_pattern_sql}"
    n_internal = conn.execute(internal_sql).fetchone()[0]
    if n_internal:
        print(
            f"  [FAIL] {n_internal} HPCsizer-internal jobs present — "
            f"insert_job filter is not being applied on some ingest path"
        )
        ok = False
    else:
        print("  [OK]   no HPCsizer-internal jobs in database")

    n_real = conn.execute(f"SELECT count(*) FROM jobs WHERE {_INTERNAL_SQL_FILTER}").fetchone()[0]
    if n_real == 0:
        print("  [WARN] No real jobs — run bin/backfill.py first")
        conn.close()
        return False

    print("\n  Real jobs only (HPCsizer infrastructure excluded):")

    # Per-user summary
    print("\n  Per-user summary:")
    print(f"  {'user':<16} {'count':>6} {'avg_mem_eff':>12} {'has_rss':>8}")
    print(f"  {'-' * 16} {'-' * 6} {'-' * 12} {'-' * 8}")
    for row in conn.execute(
        f"""SELECT user, count(*) as cnt,
                   round(avg(mem_efficiency), 3) as avg_mem_eff,
                   sum(sacct_peak_rss_gb IS NOT NULL) as has_rss
            FROM jobs WHERE {_INTERNAL_SQL_FILTER} GROUP BY user"""
    ):
        print(
            f"  {row['user']:<16} {row['cnt']:>6} "
            f"{row['avg_mem_eff'] or 'N/A':>12} {row['has_rss']:>8}"
        )

    # RSS coverage
    row = conn.execute(
        f"""SELECT count(*) as total,
                   sum(sacct_peak_rss_gb IS NOT NULL) as with_rss
            FROM jobs WHERE {_INTERNAL_SQL_FILTER}"""
    ).fetchone()
    with_rss = row["with_rss"] or 0
    pct = (with_rss / row["total"] * 100) if row["total"] else 0
    print(f"\n  MaxRSS coverage: {with_rss}/{row['total']} ({pct:.0f}%)")

    # Anomaly flags
    flagged = conn.execute(
        f"""SELECT flags, count(*) as cnt
            FROM jobs WHERE flags != '[]' AND {_INTERNAL_SQL_FILTER}
            GROUP BY flags ORDER BY cnt DESC"""
    ).fetchall()
    if flagged:
        print("\n  Anomaly flags:")
        for row in flagged:
            print(f"    {row['flags']:<40} {row['cnt']:>5}")
    else:
        print("\n  No anomaly flags found.")

    conn.close()
    return ok


def check_scheduler_alive():
    """Report whether the self-resubmitting scheduler chain is still queued.

    Excluding internal jobs makes the database structurally blind to the
    chain's health, so this is the only place that signal remains visible.
    Returns None when squeue is unavailable (CI, a workstation), so callers
    can leave the overall result untouched.
    """
    if shutil.which("squeue") is None:
        print("  [SKIP] squeue not available — cannot check scheduler chain")
        return None
    try:
        proc = subprocess.run(
            [
                "squeue",
                "-h",
                "-u",
                os.environ.get("USER", ""),
                "--name=hpcsizer-scheduler",
                "-t",
                "PENDING,RUNNING",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"  [SKIP] squeue failed: {exc}")
        return None
    if proc.returncode != 0:
        print(f"  [SKIP] squeue exited {proc.returncode}: {proc.stderr.strip()}")
        return None
    queued = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    if queued:
        print(f"  [OK]   scheduler chain queued ({len(queued)} job(s) PENDING/RUNNING)")
        return True
    print(
        "  [WARN] no hpcsizer-scheduler job queued — the chain may have stopped; "
        "check logs/scheduler.log"
    )
    return False


def main():
    db_mode = "--db" in sys.argv
    db_path = os.environ.get("HPCSIZER_DB", DEFAULT_DB_PATH)

    if db_mode:
        print("=== HPCsizer database validation ===")
        ok = check_db_contents(db_path)
        print("\n  Scheduler chain:")
        if check_scheduler_alive() is False:
            ok = False
    else:
        print("=== HPCsizer setup validation ===")
        print("\nRuntime directories:")
        ok = check_dirs()
        print("\nDatabase:")
        ok = check_db(db_path) and ok
        print("\nTests:")
        print("  Run:  python -m pytest tests/ -v")

    print()
    if ok:
        print("All checks passed.")
    else:
        print("Some checks had warnings — see above.")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
