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
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.db import DEFAULT_DB_PATH, init_db

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
    print(f"\n  Database: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    total = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    print(f"\n  Total jobs: {total}")
    if total == 0:
        print("  [WARN] No jobs — run bin/backfill.py first")
        conn.close()
        return False

    # Per-user summary
    print("\n  Per-user summary:")
    print(f"  {'user':<16} {'count':>6} {'avg_mem_eff':>12} {'has_rss':>8}")
    print(f"  {'-' * 16} {'-' * 6} {'-' * 12} {'-' * 8}")
    for row in conn.execute(
        """SELECT user, count(*) as cnt,
                  round(avg(mem_efficiency), 3) as avg_mem_eff,
                  sum(sacct_peak_rss_gb IS NOT NULL) as has_rss
           FROM jobs GROUP BY user"""
    ):
        print(
            f"  {row['user']:<16} {row['cnt']:>6} "
            f"{row['avg_mem_eff'] or 'N/A':>12} {row['has_rss']:>8}"
        )

    # RSS coverage
    row = conn.execute(
        """SELECT count(*) as total,
                  sum(sacct_peak_rss_gb IS NOT NULL) as with_rss
           FROM jobs"""
    ).fetchone()
    pct = (row["with_rss"] / row["total"] * 100) if row["total"] else 0
    print(f"\n  MaxRSS coverage: {row['with_rss']}/{row['total']} ({pct:.0f}%)")

    # Anomaly flags
    flagged = conn.execute(
        """SELECT flags, count(*) as cnt
           FROM jobs WHERE flags != '[]'
           GROUP BY flags ORDER BY cnt DESC"""
    ).fetchall()
    if flagged:
        print("\n  Anomaly flags:")
        for row in flagged:
            print(f"    {row['flags']:<40} {row['cnt']:>5}")
    else:
        print("\n  No anomaly flags found.")

    conn.close()
    return True


def main():
    db_mode = "--db" in sys.argv
    db_path = os.environ.get("HPCSIZER_DB", DEFAULT_DB_PATH)

    if db_mode:
        print("=== HPCsizer database validation ===")
        ok = check_db_contents(db_path)
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
