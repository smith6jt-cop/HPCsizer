"""hpg -- command-line interface for HPCsizer.

Standard library only, so it runs with the system ``python3`` for any group
member: no conda environment and no install step.
"""

import argparse
import dataclasses
import getpass
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from lib import (
    __version__,
    analyzer,
    backtest,
    collect,
    db,
    health,
    history,
    jobview,
    metrics,
    plotter,
    profile,
    queue,
    recommender,
    report,
    sbatch_opts,
    schedule,
    slurm,
    submit,
)
from lib.config import Config, ConfigError, load_config
from lib.fmt import fmt_age, fmt_bytes, fmt_int, fmt_ts

# Commands whose trailing arguments are sbatch's, passed through untouched:
# hpg's own options come first, and everything from the first other word on is
# read exactly as sbatch would read it.
PASS_THROUGH: Dict[str, Tuple[frozenset, frozenset]] = {
    # command: (options without a value, options with a value)
    "recommend": (
        frozenset({"--json", "--no-refresh", "-h", "--help"}),
        frozenset({"--user", "--days", "--name", "--like", "--kind"}),
    ),
    "submit": (
        frozenset({"--apply", "--as-is", "--yes", "-y", "--no-profile", "-h", "--help"}),
        frozenset(),
    ),
}
PASS_THROUGH["sbatch"] = PASS_THROUGH["submit"]

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_NO_DATA = 3


def _err(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def current_user() -> str:
    return os.environ.get("USER") or getpass.getuser()


def split_passthrough(command: str, tokens: Sequence[str]) -> Tuple[List[str], List[str]]:
    """Split ``tokens`` into hpg's leading options and the sbatch arguments after them."""
    flags, valued = PASS_THROUGH[command]
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token == "--":
            return list(tokens[:i]), list(tokens[i + 1 :])
        name, eq, _value = token.partition("=")
        if name in flags and not eq:
            i += 1
        elif name in valued:
            i += 1 if eq else 2
        else:
            break
    return list(tokens[:i]), list(tokens[i:])


def data_line(watermark_ts: Optional[int], now: int, refresh: str = "") -> str:
    """The line every report ends with: how old its data is."""
    if not watermark_ts:
        return "Data: no harvest has been recorded yet"
    note = {
        "failed": "; a refresh from sacct failed",
        "busy": "; a harvest is running, this is the snapshot before it",
    }.get(refresh, "")
    return "Data as of %s (%s old)%s" % (fmt_ts(watermark_ts), fmt_age(now - watermark_ts), note)


def _ingest_hook():
    """What a harvest also does: store the in-job profiles of the jobs that finished."""
    return profile.ingest_spool


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
            # A row written before unchanged records were told apart counts
            # every record that was read again as updated.
            seen_again = (
                "%s already stored" % fmt_int(last.get("n_updated"))
                if last.get("n_unchanged") is None
                else "%s changed, %s unchanged"
                % (fmt_int(last.get("n_updated")), fmt_int(last.get("n_unchanged")))
            )
            row(
                "",
                "%s records: %s new, %s, %s still open, %s internal, %s unparsed, %s late"
                % (
                    fmt_int(last.get("n_records")),
                    fmt_int(last.get("n_inserted")),
                    seen_again,
                    fmt_int(last.get("n_open")),
                    fmt_int(last.get("n_internal")),
                    fmt_int(last.get("n_unparsed")),
                    fmt_int(last.get("n_late")),
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

    if args.db_command == "reparse":
        stats = collect.reparse(cfg, log=_err)
        print(
            "Re-derived %s of %s stored jobs from their sacct records (parser version %d); "
            "%s left as they are"
            % (
                fmt_int(stats["reparsed"]),
                fmt_int(stats["rows"]),
                slurm.PARSER_VERSION,
                fmt_int(stats["skipped"]),
            )
        )
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
# recommend / backtest
# ---------------------------------------------------------------------------

RECOMMEND_EPILOG = """\
examples:
  hpg recommend job.sh                       a script, read like sbatch reads it
  hpg recommend --mem=64G -c 16 job.sh       with options given on the command line
  hpg recommend --wrap "python segment.py /blue/grp/s1.tif"
  hpg recommend --name mplx12k-spleen        jobs with that name, no script needed
  hpg recommend --like 51598968              a job like a stored one
  hpg recommend --kind ood:rstudio           interactive sessions of one kind

exit status: 0 with a suggestion, 3 when there is no data to base one on.
"""


def cmd_recommend(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    if args.days:
        cfg = dataclasses.replace(cfg, lookback_days=args.days)
    selectors = [name for name in ("name", "like", "kind") if getattr(args, name)]
    parsed = sbatch_opts.parse_argv(args.sbatch_args)
    has_job = parsed.script is not None or parsed.wrap is not None
    if len(selectors) > 1 or (selectors and has_job):
        _err("hpg recommend: give a script, --wrap, --name, --like or --kind -- one of them")
        return EXIT_USAGE
    if not selectors and not has_job:
        _err("hpg recommend: nothing to size; give a script, --wrap CMD, --name, --like or --kind")
        return EXIT_USAGE

    user = args.user or current_user()
    submission = None
    if has_job:
        try:
            submission = analyzer.analyze_submission(args.sbatch_args, roots=cfg.data_roots)
        except OSError as exc:
            _err("hpg recommend: %s" % exc)
            return EXIT_USAGE
    request = analyzer.request_from_options(analyzer.options_dict(parsed))

    refresh = collect.maybe_refresh(
        cfg, enabled=not args.no_refresh, now=now, ingest=_ingest_hook()
    )
    jobs, info = history.load(cfg, since_ts=now - int(cfg.lookback_days * history.DAY))
    try:
        if args.like:
            with db.reader(cfg.db_path) as conn:
                found = db.find_jobs(conn, args.like)
            if not found:
                _err("hpg recommend: no stored job %s" % args.like)
                return EXIT_NO_DATA
            job = history.annotate(found[:1], cfg)[0]
            target = recommender.target_from_job(job, cfg, request if parsed.options else None)
        elif args.kind:
            target = recommender.target_from_kind(args.kind, user, cfg, request)
        elif args.name:
            target = recommender.target_from_name(args.name, user, cfg, request)
        else:
            target = recommender.target_from_submission(submission, user, cfg)
    except ValueError as exc:
        _err("hpg recommend: %s" % exc)
        return EXIT_USAGE

    rec = recommender.recommend(target, history.History(jobs), cfg, now)
    rec["data"] = {"as_of_ts": info["watermark_ts"], "refresh": refresh, "db": cfg.db_path}
    if args.json:
        print(json.dumps(rec, indent=2, sort_keys=True, default=str))
    else:
        print(recommender.render_text(rec))
        print(data_line(info["watermark_ts"], now, refresh))
    return EXIT_OK if rec["source"] != recommender.SOURCE_NONE else EXIT_NO_DATA


def cmd_backtest(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    try:
        custom = backtest.parse_settings(args.set, cfg) if args.set else None
    except ValueError as exc:
        _err("hpg backtest: %s" % exc)
        return EXIT_USAGE
    refresh = collect.maybe_refresh(
        cfg, enabled=not args.no_refresh, now=now, ingest=_ingest_hook()
    )
    jobs, info = history.load(cfg)
    result = backtest.backtest(
        jobs,
        cfg,
        since_ts=now - args.days * history.DAY if args.days else None,
        user=args.user,
        kind=args.kind,
        custom=custom,
        custom_label=", ".join(args.set or []),
    )
    result["data"] = {"as_of_ts": info["watermark_ts"], "refresh": refresh, "db": cfg.db_path}
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(backtest.render_text(result))
        print(data_line(info["watermark_ts"], now, refresh))
    return EXIT_OK if result["jobs"] else EXIT_NO_DATA


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------

SUBMIT_EPILOG = """\
examples:
  hpg submit job.sh                      report, then submit as written, profiled
  hpg submit --apply job.sh              submit with the request its history supports
  hpg submit --apply --yes -c 16 job.sh  the same without asking, from a script
  hpg submit --as-is --wrap "python x.py"   no report: just submit and profile
  jid=$(hpg submit --parsable job.sh)    sbatch's output and exit status are unchanged

The report is written to standard error. Nothing in your script file is changed.
"""


def cmd_submit(args: argparse.Namespace, cfg: Config) -> int:
    sbatch_args = list(args.sbatch_args)
    if not sbatch_args:
        _err("hpg submit: nothing to submit; give what you would give sbatch (see --help)")
        return EXIT_USAGE
    if args.apply and args.as_is:
        _err("hpg submit: --apply and --as-is exclude each other")
        return EXIT_USAGE
    user = current_user()
    mode = "as-is" if args.as_is else ("apply" if args.apply else "advise")
    rec: Optional[Dict[str, Any]] = None

    # Everything up to the call of sbatch is advice and convenience. Whatever
    # fails in it -- a bug included -- the job is submitted, exactly as given.
    try:
        parsed = sbatch_opts.parse_argv(sbatch_args)
        if mode != "as-is" and submit.passes_straight_through(parsed) is None:
            now = int(time.time())
            try:
                sub = analyzer.analyze_submission(sbatch_args, roots=cfg.data_roots)
                jobs, info = history.load(cfg, since_ts=now - int(cfg.lookback_days * history.DAY))
                target = recommender.target_from_submission(sub, user, cfg)
                rec = recommender.recommend(target, history.History(jobs), cfg, now)
                if rec["source"] == recommender.SOURCE_NONE:
                    _err(
                        "hpg: no finished runs of %s yet; nothing to compare with."
                        % target.cls["family"]
                    )
                else:
                    _err(recommender.render_text(rec))
                    _err(data_line(info["watermark_ts"], now))
            except OSError:
                pass  # an unreadable script: sbatch reports that better than this could
            except db.DatabaseError as exc:
                _err("hpg submit: no report (%s)" % exc)
    except Exception as exc:  # noqa: BLE001
        rec = None
        _err("hpg submit: no report (%s: %s)" % (type(exc).__name__, exc))

    changes: Dict[str, str] = {}
    if mode == "apply":
        changes, _blockers = submit.split_changes(rec)
        if changes and not args.yes and submit.interactive():
            answer = submit.ask(
                "Submit with %s? [Y]es / [n]o, as requested / [q]uit: " % " ".join(changes.values())
            )
            if answer in ("q", "quit"):
                _err("Not submitted.")
                return EXIT_ERROR
            if answer in ("n", "no"):
                changes = {}

    try:
        plan = submit.prepare(sbatch_args, cfg, profile=not args.no_profile, changes=changes)
        plan.recommendation = rec
        for line in submit.describe(plan, rec, mode):
            _err(line)
    except Exception as exc:  # noqa: BLE001
        plan = submit.Plan(sbatch_args)
        _err(
            "hpg submit: submitting exactly as given, unprofiled (%s: %s)"
            % (type(exc).__name__, exc)
        )
    return submit.run_sbatch(cfg, plan, user)


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------


def _emit(text: str, out: Optional[str] = None) -> None:
    """Print a report, or write it to ``out`` and say so on stderr."""
    text = text if text.endswith("\n") else text + "\n"
    if not out:
        sys.stdout.write(text)  # exactly one final newline: CSV output ends with its last row
        return
    with open(out, "w") as handle:
        handle.write(text)
    _err("wrote %s" % out)


def _load(
    args: argparse.Namespace,
    cfg: Config,
    now: int,
    days: float,
    user: Optional[str] = None,
    kind: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """``(window, everything, info)``: the jobs a report covers, and the history behind them.

    The window is the last ``days`` days, narrowed to ``user`` and ``kind``;
    history reaches back as far as recommendations look, whatever the window.
    """
    refresh = collect.maybe_refresh(
        cfg, enabled=not args.no_refresh, now=now, ingest=_ingest_hook()
    )
    reach = max(float(days), float(cfg.lookback_days))
    everything, info = history.load(cfg, since_ts=now - int(reach * history.DAY))
    info["refresh"] = refresh
    since = now - int(days * history.DAY)
    account = getattr(args, "account", None)
    window = [
        job
        for job in everything
        if (job.get("end_ts") or 0) >= since
        and (not user or job.get("user") == user)
        and (not kind or job.get("kind") == kind)
        and (not account or job.get("account") == account)
    ]
    return window, everything, info


def cmd_report(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    window, everything, info = _load(args, cfg, now, args.days, args.user, args.kind)
    rep = report.build(
        window,
        history.History(everything),
        cfg,
        now,
        days=args.days,
        user=args.user,
        kind=args.kind,
        account=args.account,
        top=args.top,
        watermark_ts=info["watermark_ts"],
    )
    if args.format == "json":
        text = json.dumps(rep, indent=2, sort_keys=True)
    elif args.format == "csv":
        text = report.render_csv(rep)
    elif args.format == "md":
        text = (
            report.render_markdown(rep)
            + "\n\n"
            + data_line(info["watermark_ts"], now, info["refresh"])
        )
    else:
        text = (
            report.render_text(rep) + "\n" + data_line(info["watermark_ts"], now, info["refresh"])
        )
    _emit(text, args.out)
    return EXIT_OK if window else EXIT_NO_DATA


def cmd_families(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    days = args.days or cfg.lookback_days
    window, _everything, info = _load(args, cfg, now, days, args.user, args.kind)
    rows = sorted(report.family_rows(window), key=report.FAMILY_SORTS[args.sort])
    for row in rows:
        del row["_last"]
    if args.json:
        print(json.dumps(rows[: args.limit] if args.limit else rows, indent=2, sort_keys=True))
        return EXIT_OK if rows else EXIT_NO_DATA
    shown = rows[: args.limit] if args.limit else rows
    print(
        "%s job families in the last %g days%s"
        % (
            fmt_int(len(rows)),
            days,
            "" if len(shown) == len(rows) else " (the first %d by %s)" % (len(shown), args.sort),
        )
    )
    if shown:
        print()
        print(report.families_table(shown))
    print(data_line(info["watermark_ts"], now, info["refresh"]))
    return EXIT_OK if rows else EXIT_NO_DATA


def cmd_history(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    days = args.days or cfg.lookback_days
    user = args.user or current_user()
    query = args.family
    window, _everything, info = _load(args, cfg, now, days)  # every user: a family names its own
    if not query:
        runs = [job for job in window if job.get("user") == user]
        title = "Jobs of %s in the last %g days" % (user, days)
        with_family = True
    else:
        matches = jobview.match_families(window, query, user)
        if not matches:
            _err("hpg history: no job family matches %r in the last %g days" % (query, days))
            return EXIT_NO_DATA
        if len(matches) > 1:
            _err("hpg history: %r matches %d families; name one of them:" % (query, len(matches)))
            for family in matches[:30]:
                _err("  " + family)
            return EXIT_USAGE
        runs = [job for job in window if job["family"] == matches[0]]
        title = "%s in the last %g days" % (matches[0], days)
        with_family = False
    if args.json:
        print(json.dumps([jobview.export_record(job) for job in runs], indent=2, sort_keys=True))
        return EXIT_OK if runs else EXIT_NO_DATA
    print(jobview.render_history(runs, title, limit=args.limit, with_family=with_family))
    if runs and query:
        last = max(runs, key=lambda job: job.get("end_ts") or 0)
        print("\nWhat to request next: hpg recommend --like %s" % last.get("job_id"))
    print(data_line(info["watermark_ts"], now, info["refresh"]))
    return EXIT_OK if runs else EXIT_NO_DATA


def cmd_flags(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    window, _everything, info = _load(args, cfg, now, args.days, args.user, args.kind)
    if args.json:
        counts, items = jobview.flagged(window, args.flag)
        print(
            json.dumps(
                {
                    "jobs": len(window),
                    "counts": counts,
                    "families": jobview.flag_groups(items)[: args.limit],
                    "flagged": [
                        dict(
                            jobview.export_record(job),
                            flag=flag,
                            detail=jobview.flag_detail(job, flag),
                        )
                        for _weight, job, flag in items[: args.limit]
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(
            jobview.render_flags(
                window, args.days, only=args.flag, limit=args.limit, per_job=args.jobs
            )
        )
        print(data_line(info["watermark_ts"], now, info["refresh"]))
    return EXIT_OK if window else EXIT_NO_DATA


def cmd_job(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    refresh = collect.maybe_refresh(
        cfg, enabled=not args.no_refresh, now=now, ingest=_ingest_hook()
    )
    with db.reader(cfg.db_path) as conn:
        found = db.find_jobs(conn, args.job_id)
        if not found:
            _err(
                "hpg job: no stored job %s (a job is stored once it has finished and been "
                "harvested; 'hpg queue' lists the ones still running)" % args.job_id
            )
            return EXIT_NO_DATA
        job = history.annotate(found[:1], cfg)[0]
        steps = db.fetch_steps(conn, job["id"])
        profile = db.fetch_monitor_summary(conn, job["id"])
        raw = db.fetch_raw(conn, job["id"]) if args.json else None
        mark = db.get_meta_int(conn, "watermark_ts")
    if args.json:
        record = {k: v for k, v in job.items() if k != "m"}
        record.update(metrics=job["m"], steps=steps, profile=profile, sacct=raw)
        print(json.dumps(record, indent=2, sort_keys=True))
    else:
        print(jobview.render_job(job, steps, profile, others=len(found) - 1))
        print(data_line(mark, now, refresh))
    return EXIT_OK


def cmd_plot(args: argparse.Namespace, cfg: Config) -> int:
    with db.reader(cfg.db_path) as conn:
        found = db.find_jobs(conn, args.job_id)
    if not found:
        _err("hpg plot: no stored job %s" % args.job_id)
        return EXIT_NO_DATA
    job = history.annotate(found[:1], cfg)[0]
    paths = profile.series_files(cfg, job)
    if not paths:
        _err(
            "hpg plot: job %s has no profile; jobs submitted with 'hpg submit' record one"
            % args.job_id
        )
        return EXIT_NO_DATA
    if not plotter.available():
        _err(plotter.MISSING)
        for path in paths:
            _err("  " + path)
        return EXIT_ERROR
    try:
        series = plotter.load(paths, owner=job.get("user"))
        out = plotter.plot_job(job, series, args.out or plotter.default_name(job))
    except (plotter.PlotError, OSError) as exc:
        _err("hpg plot: %s" % exc)
        return EXIT_ERROR
    print(out)
    return EXIT_OK


def cmd_export(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    days = args.days or cfg.lookback_days
    window, _everything, _info = _load(args, cfg, now, days, args.user, args.kind)
    if args.format == "json":
        text = json.dumps([jobview.export_record(job) for job in window], indent=1, sort_keys=True)
    else:
        text = report.to_csv(jobview.EXPORT_COLUMNS, jobview.export_rows(window))
    _emit(text.rstrip("\n"), args.out)
    if args.out:
        _err("%s jobs, %s" % (fmt_int(len(window)), jobview.render_date_range(window)))
    return EXIT_OK if window else EXIT_NO_DATA


def cmd_queue(args: argparse.Namespace, cfg: Config) -> int:
    now = int(time.time())
    try:
        everything, _info = history.load(cfg, since_ts=now - int(cfg.lookback_days * history.DAY))
    except db.DatabaseMissing:
        everything = []  # nothing harvested yet: the queue can still be listed
    try:
        result = queue.build(cfg, history.History(everything), now)
    except (collect.HarvestError, ValueError) as exc:
        _err("hpg queue: %s" % exc)
        return EXIT_ERROR
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(queue.render_text(result, users=[args.user] if args.user else ()))
    return EXIT_OK


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
    dsub.add_parser("reparse", help="re-derive every stored job from the sacct record kept with it")
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

    p = sub.add_parser(
        "recommend",
        help="what to request for a job, from what its earlier runs used",
        usage="hpg recommend [--user U] [--days N] [--json] [--no-refresh]\n"
        "                     (SBATCH-ARGS | --name NAME | --like JOBID | --kind KIND)",
        description="Compare a job's request with what comparable finished runs used and "
        "suggest the next request. hpg's own options come first; everything after them is "
        "read as sbatch would read it (options, then a script or --wrap).",
        epilog=RECOMMEND_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--name", help="size jobs with this job name (no script needed)")
    p.add_argument("--like", metavar="JOBID", help="size a job like this stored one")
    p.add_argument("--kind", help="size an interactive session kind, e.g. ood:rstudio or vscode")
    p.add_argument("--user", help="whose history to use (default: you)")
    p.add_argument("--days", type=int, help="how far back to look (default: policy lookback_days)")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.add_argument("--no-refresh", action="store_true", help="do not query sacct first")
    p.set_defaults(func=cmd_recommend, sbatch_args=[])

    p = sub.add_parser(
        "submit",
        aliases=["sbatch"],
        help="sbatch with a report first and a sampler inside the job",
        usage="hpg submit [--apply | --as-is] [--yes] [--no-profile] SBATCH-ARGS",
        description="Submit a job as sbatch would. Before submitting, compare the request "
        "with what earlier runs of the same job used; inside the job, record what it uses. "
        "hpg's own options come first; everything after them goes to sbatch.",
        epilog=SUBMIT_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="submit with the memory, CPUs and time its own history supports",
    )
    p.add_argument("--as-is", action="store_true", help="no report; submit as written (profiled)")
    p.add_argument("-y", "--yes", action="store_true", help="with --apply: do not ask")
    p.add_argument("--no-profile", action="store_true", help="do not add the in-job sampler")
    p.set_defaults(func=cmd_submit, sbatch_args=[])

    def reading(p: argparse.ArgumentParser, days: Optional[float]) -> None:
        """Options shared by the commands that read stored jobs."""
        p.add_argument("--user", help="only this user's jobs")
        p.add_argument("--account", help="only jobs charged to this Slurm account")
        if days is None:
            p.add_argument("--days", type=float, help="window in days (default: lookback_days)")
        else:
            p.add_argument(
                "--days", type=float, default=days, help="window in days (default: %g)" % days
            )
        p.add_argument("--no-refresh", action="store_true", help="do not query sacct first")

    p = sub.add_parser(
        "report",
        help="requested against used: by user, by kind of job, and where the unused part is",
    )
    reading(p, 30)
    p.add_argument("--kind", help="only jobs of this kind (batch, wrap, vscode, ood:<app>, ...)")
    p.add_argument("--top", type=int, default=10, help="families listed per section (default: 10)")
    p.add_argument(
        "--format",
        choices=("text", "md", "json", "csv"),
        default="text",
        help="md for pasting into a message, csv for one line per job family",
    )
    p.add_argument("--out", metavar="FILE", help="write to a file instead of the terminal")
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("families", help="job families: runs, requests and use, one line each")
    reading(p, None)
    p.add_argument("--kind", help="only families of this kind")
    p.add_argument(
        "--sort",
        choices=sorted(report.FAMILY_SORTS),
        default="cores",
        help="cores: core-hours reserved; memory, cpu: unused resource-hours (default: cores)",
    )
    p.add_argument("-n", "--limit", type=int, default=40, help="lines to print; 0 for all")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.set_defaults(func=cmd_families)

    p = sub.add_parser(
        "history",
        help="the runs of one job family (or your recent jobs), each with requested and used",
    )
    p.add_argument(
        "family",
        nargs="?",
        help="a family, a job name, or a fragment of either (default: your jobs)",
    )
    reading(p, None)
    p.add_argument("-n", "--limit", type=int, default=25, help="runs to list (default: 25)")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.set_defaults(func=cmd_history)

    p = sub.add_parser("flags", help="flagged jobs, largest first, with what was measured")
    reading(p, 30)
    p.add_argument("--kind", help="only jobs of this kind")
    p.add_argument("--flag", choices=metrics.ACCOUNTING_FLAGS, help="only this flag")
    p.add_argument("--jobs", action="store_true", help="list single jobs instead of families")
    p.add_argument("-n", "--limit", type=int, default=30, help="lines to list (default: 30)")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.set_defaults(func=cmd_flags)

    p = sub.add_parser("job", help="one finished job: requested, used, steps, flags")
    p.add_argument("job_id", help="job id as Slurm prints it: 123, 123_4")
    p.add_argument(
        "--json", action="store_true", help="everything stored, including the sacct record"
    )
    p.add_argument("--no-refresh", action="store_true", help="do not query sacct first")
    p.set_defaults(func=cmd_job)

    p = sub.add_parser("plot", help="time series of a profiled job as a PNG (needs matplotlib)")
    p.add_argument("job_id", help="job id as Slurm prints it")
    p.add_argument("--out", metavar="FILE", help="where to write (default: hpg-job-ID.png here)")
    p.set_defaults(func=cmd_plot)

    p = sub.add_parser("export", help="one line per job, stored and derived values, as CSV or JSON")
    reading(p, None)
    p.add_argument("--kind", help="only jobs of this kind")
    p.add_argument("--format", choices=("csv", "json"), default="csv")
    p.add_argument("--out", metavar="FILE", help="write to a file instead of the terminal")
    p.set_defaults(func=cmd_export)

    p = sub.add_parser(
        "queue",
        help="pending and running jobs next to what their family has used so far",
    )
    p.add_argument("--user", help="list only this user's jobs")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.set_defaults(func=cmd_queue)

    p = sub.add_parser(
        "backtest",
        help="replay stored jobs: what would a sizing policy have done?",
        description="Size every stored job from the runs that had ended before it was "
        "submitted, and compare with what it then used.",
    )
    p.add_argument(
        "--days", type=int, help="score jobs submitted in the last N days (default: all)"
    )
    p.add_argument("--user", help="score only this user's jobs")
    p.add_argument(
        "--kind", help="score only jobs of this kind (batch, wrap, vscode, ood:<app>, ...)"
    )
    p.add_argument(
        "--set",
        action="append",
        metavar="KEY=VALUE",
        help="also score the configured policy with this setting changed (repeatable), "
        "e.g. --set mem_headroom=1.1 --set last_n=10",
    )
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.add_argument("--no-refresh", action="store_true", help="do not query sacct first")
    p.set_defaults(func=cmd_backtest)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    passed_through: List[str] = []
    if argv and argv[0] in PASS_THROUGH:
        own, passed_through = split_passthrough(argv[0], argv[1:])
        argv = [argv[0]] + own

    parser = build_parser()
    args = parser.parse_args(argv)
    args.sbatch_args = passed_through
    if not getattr(args, "func", None):
        parser.print_help()
        return EXIT_USAGE
    try:
        cfg = load_config()
    except ConfigError as exc:
        _err("hpg: %s" % exc)
        if args.command in ("submit", "sbatch"):
            _err("hpg submit: submitting exactly as given, without report or profile")
            return submit.submit_as_given(passed_through)
        return EXIT_ERROR
    for problem in cfg.problems:
        _err("hpg: %s" % problem)
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
