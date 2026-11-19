"""hpg -- command-line interface for HPCsizer.

Standard library only, so it runs with the system ``python3`` for any group
member: no conda environment and no install step.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, Optional, Sequence

from lib import __version__, collect, db, health, schedule
from lib.config import Config, load_config

# Commands still served by the version 2 implementation until they are replaced.
LEGACY_COMMANDS = ("submit", "recommend", "interactive", "report", "history", "plot", "flags")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_NO_DATA = 3


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def fmt_ts(ts: Optional[float]) -> str:
    if not ts:
        return "never"
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))


def fmt_age(seconds: Optional[float]) -> str:
    if seconds is None:
        return "unknown"
    if seconds < 90:
        return "%d s" % seconds
    if seconds < 5400:
        return "%.0f min" % (seconds / 60.0)
    if seconds < 2 * 86400:
        return "%.1f h" % (seconds / 3600.0)
    return "%.1f d" % (seconds / 86400.0)


def fmt_int(value: Optional[float]) -> str:
    return "-" if value is None else "{:,}".format(int(value))


def fmt_bytes(value: Optional[float]) -> str:
    if value is None:
        return "-"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return ("%.0f %s" if unit == "B" else "%.1f %s") % (size, unit)
        size /= 1024.0
    return "%d B" % value


def _err(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _ingest_hook():
    """The spool ingester, when the profiler module is present."""
    try:
        from lib import profile
    except ImportError:
        return None
    return getattr(profile, "ingest_spool", None)


# ---------------------------------------------------------------------------
# harvest
# ---------------------------------------------------------------------------


def cmd_harvest(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    try:
        since = collect.parse_since(args.since, now) if args.since else None
    except ValueError as exc:
        _err("hpg harvest: %s" % exc)
        return EXIT_USAGE
    log = _err
    try:
        collect.harvest(
            cfg,
            since_ts=since,
            deep=args.deep,
            trigger=args.trigger,
            now=now,
            log=log,
            ingest=_ingest_hook(),
        )
        return EXIT_OK
    except db.LockBusy as exc:
        # Another harvest is in progress; it will publish the same records.
        log("[harvest] %s; skipped" % exc)
        return EXIT_OK if args.trigger != "manual" else EXIT_ERROR
    except (collect.HarvestError, db.DatabaseError, OSError) as exc:
        log("[harvest] %s ERROR: %s; nothing was published" % (fmt_ts(now), exc))
        if args.trigger != "schedule":
            return EXIT_ERROR
        # Scheduled run: fail the Slurm job (and so trigger its failure mail)
        # only when collection has really stopped, not for one stalled attempt.
        mark = collect.watermark(cfg)
        if mark is not None and now - mark < cfg.stale_hours * 3600:
            log(
                "[harvest] last success %s ago (limit %g h); the next run resumes from it"
                % (fmt_age(now - mark), cfg.stale_hours)
            )
            return EXIT_OK
        log("[harvest] no successful harvest within %g h; failing the job" % cfg.stale_hours)
        return EXIT_ERROR


# ---------------------------------------------------------------------------
# status / preflight
# ---------------------------------------------------------------------------


def render_status(st: Dict[str, Any]) -> str:
    lines = ["HPCsizer %s" % st["version"]]

    def row(label: str, value: str) -> None:
        lines.append("  %-16s%s %s" % (label, ":" if label else " ", value))

    schema = st.get("schema")
    if schema is None:
        row("Database", "%s (not created yet)" % st["db_path"])
    else:
        size = ", " + fmt_bytes(st["db_bytes"]) if "db_bytes" in st else ""
        row("Database", "%s (schema %s%s)" % (st["db_path"], schema if schema else "2", size))
    row("Accounts", ", ".join(st["accounts"]) or "(none configured)")

    if st.get("watermark_ts"):
        last = st.get("last_harvest") or {}
        row(
            "Last harvest",
            "%s (%s ago)%s"
            % (
                fmt_ts(st["watermark_ts"]),
                fmt_age(st.get("age_sec")),
                " on %s, %s" % (last.get("host"), last.get("trigger")) if last else "",
            ),
        )
        if last:
            row(
                "",
                "%s records: %s new, %s updated, %s still open, %s internal, %s unparsed, %s late"
                % tuple(
                    fmt_int(last.get(k))
                    for k in (
                        "n_records",
                        "n_inserted",
                        "n_updated",
                        "n_open",
                        "n_internal",
                        "n_unparsed",
                        "n_late",
                    )
                ),
            )
        row("Deep pass", fmt_ts(st.get("last_deep_ts")))
    elif schema == db.SCHEMA_VERSION:
        row("Last harvest", "never")

    if "rows" in st:
        sources = st["rows"]["sources"]
        row(
            "Rows",
            "%s (%s)"
            % (
                fmt_int(st["rows"]["total"]),
                ", ".join("%s %s" % (k, fmt_int(v)) for k, v in sorted(sources.items())) or "empty",
            ),
        )
        recent = st["recent"]
        row(
            "Last 30 days",
            " | ".join("%s %s" % (u, fmt_int(n)) for u, n in recent["users"]) or "no finished jobs",
        )
        if recent["states"]:
            row("", " | ".join("%s %s" % (s, fmt_int(n)) for s, n in recent["states"]))

    cov = st.get("coverage")
    if cov:
        if cov.get("available"):
            pct = 100.0 * cov["stored"] / cov["expected"] if cov["expected"] else 100.0
            row(
                "Coverage (30 d)",
                "%s of %s finished jobs in sacct are stored (%.1f%%)"
                % (fmt_int(cov["stored"]), fmt_int(cov["expected"]), pct),
            )
        else:
            row("Coverage (30 d)", "not checked (%s)" % cov.get("reason"))

    sched = st["schedule"]
    row("Schedule", "%s: %s" % (sched["mode"], sched["detail"]))
    spool = st["spool"]
    row(
        "Spool",
        "%s %s%s"
        % (
            spool["path"],
            spool.get("mode", "(missing)"),
            " -- " + spool["note"] if spool.get("note") else "",
        ),
    )
    interp = st["interpreter"]
    row(
        "Interpreter",
        "%s, Python %s, SQLite %s" % (interp["path"], interp["python"], interp["sqlite"]),
    )

    for problem in st["problems"]:
        lines.append("  ! " + problem)
    lines.append("STATUS: %s" % ("OK" if st["ok"] else st.get("state", "unknown").upper()))
    return "\n".join(lines)


def cmd_status(args: argparse.Namespace, cfg: Config) -> int:
    st = health.status(cfg, with_sacct=not args.no_sacct)
    if args.json:
        print(json.dumps(st, indent=2, sort_keys=True, default=str))
    else:
        print(render_status(st))
    return EXIT_OK if st["ok"] else EXIT_ERROR


def cmd_preflight(args: argparse.Namespace, cfg: Config) -> int:
    checks = health.preflight(cfg, compute=args.compute)
    if args.json:
        print(json.dumps(checks, indent=2))
    else:
        width = max(len(c["check"]) for c in checks)
        for check in checks:
            print("%-4s  %-*s  %s" % (check["status"], width, check["check"], check["detail"]))
    failed = [c for c in checks if c["status"] == health.FAIL]
    if not args.json:
        print("PREFLIGHT: %s" % ("FAILED (%d)" % len(failed) if failed else "OK"))
    return EXIT_ERROR if failed else EXIT_OK


# ---------------------------------------------------------------------------
# db
# ---------------------------------------------------------------------------


def _old_chain_jobs(cfg: Config) -> Optional[int]:
    """Queued jobs of the version 2 scheduler chain, or None if squeue cannot say."""
    cmd = [
        cfg.squeue,
        "-h",
        "--name=hpcsizer-scheduler,hpcsizer-watchdog",
        "-t",
        "PENDING,RUNNING",
        "-o",
        "%i",
    ]
    user = os.environ.get("USER")
    if user:
        cmd += ["-u", user]
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=60
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return len([ln for ln in proc.stdout.splitlines() if ln.strip()])


def cmd_db(args: argparse.Namespace, cfg: Config) -> int:
    if args.db_command == "migrate":
        queued = _old_chain_jobs(cfg)
        if queued and not args.force:
            _err(
                "hpg db migrate: %d job(s) of the old scheduler chain are still queued and may "
                "write the database.\nStop them first:\n"
                "  scancel --name=hpcsizer-scheduler; scancel --name=hpcsizer-watchdog\n"
                "(or pass --force)" % queued
            )
            return EXIT_ERROR
        stats = db.migrate_v2(cfg.db_path, cfg.backup_dir)
        print("Migrated %s to schema %d" % (cfg.db_path, db.SCHEMA_VERSION))
        print("  version 2 rows          : %s" % fmt_int(stats["v2_rows"]))
        print("  migrated                : %s" % fmt_int(stats["migrated"]))
        print("  dropped (HPCsizer jobs) : %s" % fmt_int(stats["dropped_internal"]))
        print("  dropped (unfinished)    : %s" % fmt_int(stats["dropped_unfinished"]))
        print("  CPU time set to unknown : %s" % fmt_int(stats["cpu_time_unknown"]))
        print("  recovered from the WAL  : %s" % fmt_bytes(stats["wal_bytes"]))
        for path in stats["backup"]:
            print("  backup                  : %s" % path)
        print("Next: hpg harvest --since %gd" % cfg.first_days)
        return EXIT_OK

    # check
    version = db.detect_version(cfg.db_path)
    if version is None:
        print("no database at %s" % cfg.db_path)
        return EXIT_ERROR
    if version != db.SCHEMA_VERSION:
        print("%s: schema version %s -- run 'hpg db migrate'" % (cfg.db_path, version or 2))
        return EXIT_ERROR
    with db.reader(cfg.db_path) as conn:
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
        counts = db.summary_counts(conn)
        steps = conn.execute("SELECT COUNT(*) FROM steps").fetchone()[0]
        runs = conn.execute("SELECT COUNT(*) FROM harvest_runs").fetchone()[0]
        profiles = conn.execute("SELECT COUNT(*) FROM job_monitor").fetchone()[0]
    print("%s: schema %d, integrity %s" % (cfg.db_path, version, result))
    print(
        "  jobs %s, steps %s, harvests %s, profiles %s"
        % (fmt_int(counts["total"]), fmt_int(steps), fmt_int(runs), fmt_int(profiles))
    )
    return EXIT_OK if result == "ok" else EXIT_ERROR


# ---------------------------------------------------------------------------
# schedule
# ---------------------------------------------------------------------------


def cmd_schedule(args: argparse.Namespace, cfg: Config) -> int:
    try:
        if args.schedule_command == "install":
            text = schedule.install(cfg, every=args.every, mode=args.mode, dry_run=args.dry_run)
            if args.dry_run:
                print("Would install (%s mode):" % args.mode)
            else:
                print("Installed (%s mode):" % args.mode)
            print(text, end="")
            return EXIT_OK
        if args.schedule_command == "remove":
            done = schedule.remove(cfg)
            print("; ".join(done) if done else "nothing was scheduled")
            return EXIT_OK
        info = schedule.describe(cfg)
        print("%s: %s" % (info["mode"], info["detail"]))
        if info.get("block"):
            print(info["block"])
        return EXIT_OK if info["mode"] in ("scrontab", "chain") else EXIT_ERROR
    except schedule.ScheduleError as exc:
        _err("hpg schedule: %s" % exc)
        return EXIT_ERROR


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hpg",
        description="HPCsizer: what Slurm jobs requested, what they used, and what to request next.",
    )
    parser.add_argument("--version", action="version", version="hpg " + __version__)
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    p = sub.add_parser("harvest", help="collect finished jobs from sacct")
    p.add_argument("--since", help="start of the window: 182d, 36h, 2026-06-01 (default: resume)")
    p.add_argument("--deep", action="store_true", help="also re-read the long late-arrival window")
    p.add_argument(
        "--trigger",
        choices=("manual", "schedule", "refresh"),
        default="manual",
        help=argparse.SUPPRESS,
    )
    p.set_defaults(func=cmd_harvest)

    p = sub.add_parser("status", help="state of collection; non-zero exit when stale or incomplete")
    p.add_argument("--no-sacct", action="store_true", help="skip the coverage check against sacct")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("preflight", help="check what this cluster's Slurm provides")
    p.add_argument(
        "--compute", action="store_true", help="also run a 1-minute job on a compute node"
    )
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.set_defaults(func=cmd_preflight)

    p = sub.add_parser("db", help="database maintenance")
    dsub = p.add_subparsers(dest="db_command", metavar="ACTION")
    dsub.required = True
    m = dsub.add_parser("migrate", help="convert a version 2 database (once)")
    m.add_argument(
        "--force", action="store_true", help="migrate even if old scheduler jobs are queued"
    )
    dsub.add_parser("check", help="integrity check and row counts")
    p.set_defaults(func=cmd_db)

    p = sub.add_parser("schedule", help="install, show or remove the scheduled harvest")
    ssub = p.add_subparsers(dest="schedule_command", metavar="ACTION")
    ssub.required = True
    i = ssub.add_parser("install", help="write the scrontab entry (or start a chain job)")
    i.add_argument("--every", choices=sorted(schedule.CRON), default="hour")
    i.add_argument("--mode", choices=("scrontab", "chain"), default="scrontab")
    i.add_argument("--dry-run", action="store_true", help="print what would be installed")
    ssub.add_parser("show", help="what is scheduled now")
    ssub.add_parser("remove", help="remove the scrontab entry and cancel chain jobs")
    p.set_defaults(func=cmd_schedule)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in LEGACY_COMMANDS:
        from lib import legacy_cli

        try:
            legacy_cli.cli.main(args=argv, prog_name="hpg", standalone_mode=False)
        except SystemExit as exc:
            return int(exc.code or 0)
        return EXIT_OK

    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return EXIT_USAGE
    cfg = load_config()
    try:
        return int(args.func(args, cfg))
    except db.DatabaseError as exc:
        _err("hpg: %s" % exc)
        return EXIT_ERROR
    except BrokenPipeError:
        return EXIT_OK
    except KeyboardInterrupt:
        return 130


def run() -> None:
    sys.exit(main())
