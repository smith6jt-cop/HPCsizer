"""Replay stored jobs to see what a sizing policy would have done.

Every job is sized as ``hpg recommend`` would have sized it at the moment it
was submitted: from the runs that had already ended, and nothing later. The
suggestion is then compared with what the job went on to use.

This is how the policy constants in ``hpcsizer.ini`` are meant to be chosen:
on the group's own jobs, by looking at how many jobs a policy would have left
short against how much it would have handed back.

What "short" means, per resource:

memory  the suggestion is below the peak the job reached -- it would have been
        killed. A job that did run out of memory counts as short unless the
        suggestion is above its request, in which case the outcome is unknown.
        Accounting samples memory once a minute, so true peaks can be higher
        and the counts are a lower bound.
time    the suggested limit is shorter than the run. A job that timed out
        counts as short unless the suggestion is above its limit (unknown).
CPUs    nothing fails; a job is "slowed" when it is given fewer cores than it
        kept busy on average.
"""

import dataclasses
import statistics
from typing import Any, Callable, Dict, List, Optional, Sequence

from lib import config, recommender
from lib.config import Config
from lib.fmt import fmt_date, fmt_hours, fmt_int, fmt_pct, table
from lib.history import History

EPS = 1e-6

LEGACY_DESCRIPTION = (
    "the rule before version 3 on the same evidence: p90 of peaks x 1.10 from 5 or more runs "
    "in 90 days, CPUs = median request, time = p90 x 1.5"
)

# Settings that may be varied with ``hpg backtest --set``.
POLICY_KEYS = (
    "lookback_days",
    "last_n",
    "min_runs",
    "min_runs_pooled",
    "mem_headroom",
    "cpu_headroom",
    "time_headroom",
    "time_percentile",
    "min_time_min",
    "oom_raise",
    "timeout_raise",
)

Suggestion = Dict[str, Optional[float]]


def _history_rule(cfg: Config) -> Callable[..., Suggestion]:
    def rule(target: recommender.Target, history: History, before: int) -> Suggestion:
        rec = recommender.recommend(target, history, cfg, now=before, before_ts=before)
        if rec["source"] != recommender.SOURCE_HISTORY:
            return {}
        return {
            "mem_mb": rec["mem"]["suggested"],
            "cpus": rec["cpus"]["suggested"],
            "time_min": rec["time"]["suggested"],
        }

    return rule


def _index_percentile(values: Sequence[float], pct: float) -> float:
    ordered = sorted(values)
    return ordered[min(int(pct * len(ordered)), len(ordered) - 1)]


def _legacy_rule(cfg: Config) -> Callable[..., Suggestion]:
    legacy = dataclasses.replace(cfg, lookback_days=90, last_n=10**9, min_runs=5, min_runs_pooled=5)

    def rule(target: recommender.Target, history: History, before: int) -> Suggestion:
        _level, runs, _n, _tried = recommender.select_evidence(
            history, target.cls, legacy, before, before
        )
        out: Suggestion = {}
        sample = [
            r
            for r in runs
            if r["state"] in ("COMPLETED", "TIMEOUT") and r["m"]["mem_used_gb"] is not None
        ]
        if len(sample) >= 5:
            peaks = [r["m"]["mem_used_gb"] for r in sample]
            out["mem_mb"] = _index_percentile(peaks, 0.9) * 1.10 * 1024.0
            out["cpus"] = round(statistics.median(r.get("req_cpus") or 1 for r in sample))
        done = [
            r["elapsed_sec"] for r in runs if r["state"] == "COMPLETED" and r.get("elapsed_sec")
        ]
        if len(done) >= 5:
            out["time_min"] = max(int(round(_index_percentile(done, 0.9) / 60.0 * 1.5)), 15)
        return out

    return rule


def _unchanged(_target: recommender.Target, _history: History, _before: int) -> Suggestion:
    return {}


def parse_settings(pairs: Sequence[str], cfg: Config) -> Config:
    """A copy of ``cfg`` with ``KEY=VALUE`` policy settings applied."""
    changes: Dict[str, Any] = {}
    for pair in pairs:
        key, eq, raw = pair.partition("=")
        key = key.strip()
        if not eq or key not in POLICY_KEYS:
            raise ValueError(
                "--set takes KEY=VALUE with KEY one of: %s (got %r)"
                % (", ".join(POLICY_KEYS), pair)
            )
        try:
            changes[key] = config.convert(key, raw.strip(), isinstance(getattr(cfg, key), int))
        except ValueError as exc:
            raise ValueError("--set %s: %s" % (key, exc)) from None
    return dataclasses.replace(cfg, **changes)


def default_policies(cfg: Config, custom: Optional[Config] = None, custom_label: str = "") -> List:
    policies = [
        ("as requested", "what was actually requested", _unchanged),
        ("configured", "the policy in effect (hpcsizer.ini or defaults)", _history_rule(cfg)),
    ]
    if custom is not None:
        policies.append(("custom", custom_label, _history_rule(custom)))
    policies.append(("legacy p90", LEGACY_DESCRIPTION, _legacy_rule(cfg)))
    return policies


def _new_score(name: str, description: str) -> Dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "mem": {
            "n": 0,
            "sized": 0,
            "short": 0,
            "unknown": 0,
            "gbh_requested": 0.0,
            "gbh_suggested": 0.0,
            "gbh_used": 0.0,
        },
        "cpus": {
            "n": 0,
            "sized": 0,
            "slowed": 0,
            "coreh_requested": 0.0,
            "coreh_suggested": 0.0,
            "coreh_used": 0.0,
        },
        "time": {
            "n": 0,
            "sized": 0,
            "short": 0,
            "unknown": 0,
            "h_requested": 0.0,
            "h_suggested": 0.0,
            "h_used": 0.0,
        },
    }


def _score(score: Dict[str, Any], job: Dict[str, Any], suggestion: Suggestion) -> None:
    m = job["m"]
    hours = m["elapsed_h"]
    oom = "oom" in m["flags"]

    # -- memory --------------------------------------------------------------
    requested = m["mem_req_gb"]
    used = m["mem_used_gb"]
    if requested and (used is not None or oom):
        part = score["mem"]
        part["n"] += 1
        given = suggestion.get("mem_mb")
        if given is not None:
            part["sized"] += 1
        gb = given / 1024.0 if given is not None else requested
        if oom:
            if gb > requested + EPS:
                part["unknown"] += 1
            else:
                part["short"] += 1
        elif gb < used - EPS:
            part["short"] += 1
        part["gbh_requested"] += requested * hours
        part["gbh_suggested"] += gb * hours
        part["gbh_used"] += (used if used is not None else requested) * hours

    # -- CPUs ----------------------------------------------------------------
    alloc = m["cores_alloc"]
    busy = m["cores_used"]
    if alloc and busy is not None:
        part = score["cpus"]
        part["n"] += 1
        given = suggestion.get("cpus")
        if given is not None:
            part["sized"] += 1
        cores = given if given is not None else alloc
        if cores < busy - 0.05:
            part["slowed"] += 1
        part["coreh_requested"] += alloc * hours
        part["coreh_suggested"] += cores * hours
        part["coreh_used"] += busy * hours

    # -- time ----------------------------------------------------------------
    limit = m["time_req_min"]
    state = job.get("state")
    if limit and not job.get("session") and state in ("COMPLETED", "TIMEOUT"):
        part = score["time"]
        part["n"] += 1
        given = suggestion.get("time_min")
        if given is not None:
            part["sized"] += 1
        minutes = given if given is not None else limit
        if state == "TIMEOUT":
            if minutes > limit + EPS:
                part["unknown"] += 1
            else:
                part["short"] += 1
        elif minutes * 60.0 < (job.get("elapsed_sec") or 0) - EPS:
            part["short"] += 1
        part["h_requested"] += limit / 60.0
        part["h_suggested"] += minutes / 60.0
        part["h_used"] += hours


def backtest(
    jobs: Sequence[Dict[str, Any]],
    cfg: Config,
    since_ts: Optional[int] = None,
    user: Optional[str] = None,
    kind: Optional[str] = None,
    custom: Optional[Config] = None,
    custom_label: str = "",
) -> Dict[str, Any]:
    """Score every policy on the annotated ``jobs``.

    All of ``jobs`` serve as history; ``since_ts``, ``user`` and ``kind``
    select which of them are sized and scored.
    """
    history = History(jobs)
    policies = default_policies(cfg, custom, custom_label)
    scores = [_new_score(name, description) for name, description, _rule in policies]
    evaluated = 0
    first: Optional[int] = None
    last: Optional[int] = None
    ordered = sorted(jobs, key=lambda j: j.get("submit_ts") or j.get("start_ts") or 0)
    for job in ordered:
        before = job.get("submit_ts") or job.get("start_ts")
        if before is None or not (job.get("elapsed_sec") or 0) > 0:
            continue
        if since_ts is not None and before < since_ts:
            continue
        if user and job.get("user") != user:
            continue
        if kind and job.get("kind") != kind:
            continue
        evaluated += 1
        first = before if first is None else min(first, before)
        last = before if last is None else max(last, before)
        target = recommender.target_from_job(job, cfg)
        for (_name, _description, rule), score in zip(policies, scores):
            _score(score, job, rule(target, history, int(before)))
    return {
        "jobs": evaluated,
        "first_submit_ts": first,
        "last_submit_ts": last,
        "stored": len(jobs),
        "policies": scores,
    }


# ---------------------------------------------------------------------------
# Text
# ---------------------------------------------------------------------------


def _share(count: int, total: int) -> str:
    if not total:
        return "-"
    return "%s (%s)" % (fmt_int(count), fmt_pct(count / float(total)) if count else "0%")


def _saved(requested: float, suggested: float) -> str:
    if requested <= 0:
        return "-"
    return fmt_pct((requested - suggested) / requested)


def render_text(result: Dict[str, Any]) -> str:
    lines = [
        "Backtest of %s jobs submitted %s to %s (%s stored jobs serve as history)."
        % (
            fmt_int(result["jobs"]),
            fmt_date(result["first_submit_ts"]),
            fmt_date(result["last_submit_ts"]),
            fmt_int(result["stored"]),
        ),
        "Each job is sized only from runs that had ended before it was submitted.",
    ]
    if not result["jobs"]:
        return "\n".join(lines)
    policies = result["policies"]

    part = policies[0]["mem"]
    lines += ["", "Memory -- %s jobs with a request and a measured peak" % fmt_int(part["n"])]
    rows = []
    for policy in policies:
        p = policy["mem"]
        rows.append(
            [
                policy["name"],
                _share(p["sized"], p["n"]),
                _share(p["short"], p["n"]),
                fmt_int(p["unknown"]),
                fmt_hours(p["gbh_requested"]),
                fmt_hours(p["gbh_suggested"]),
                fmt_hours(p["gbh_used"]),
                _saved(p["gbh_requested"], p["gbh_suggested"]),
            ]
        )
    lines.append(
        table(
            ["Policy", "Sized", "Short", "Unknown", "GB-h requested", "suggested", "used", "Saved"],
            rows,
            align="lrrrrrrr",
            indent="  ",
        )
    )

    part = policies[0]["cpus"]
    lines += ["", "CPUs -- %s jobs with measured CPU time" % fmt_int(part["n"])]
    rows = []
    for policy in policies:
        p = policy["cpus"]
        rows.append(
            [
                policy["name"],
                _share(p["sized"], p["n"]),
                _share(p["slowed"], p["n"]),
                fmt_hours(p["coreh_requested"]),
                fmt_hours(p["coreh_suggested"]),
                fmt_hours(p["coreh_used"]),
                _saved(p["coreh_requested"], p["coreh_suggested"]),
            ]
        )
    lines.append(
        table(
            ["Policy", "Sized", "Slowed", "core-h requested", "suggested", "used", "Saved"],
            rows,
            align="lrrrrrr",
            indent="  ",
        )
    )

    part = policies[0]["time"]
    lines += ["", "Time limit -- %s batch jobs that completed or timed out" % fmt_int(part["n"])]
    rows = []
    for policy in policies:
        p = policy["time"]
        rows.append(
            [
                policy["name"],
                _share(p["sized"], p["n"]),
                _share(p["short"], p["n"]),
                fmt_int(p["unknown"]),
                fmt_hours(p["h_requested"]),
                fmt_hours(p["h_suggested"]),
                fmt_hours(p["h_used"]),
            ]
        )
    lines.append(
        table(
            ["Policy", "Sized", "Short", "Unknown", "limit-h requested", "suggested", "run"],
            rows,
            align="lrrrrrr",
            indent="  ",
        )
    )

    lines += ["", "Policies"]
    for policy in policies:
        lines.append("  %-13s %s" % (policy["name"], policy["description"]))
    lines += [
        "",
        "Sized: jobs the policy had enough history for. Short: jobs that would have been given",
        "less than they used (for 'as requested': the jobs that did run out). Unknown: jobs that",
        "failed as submitted and would have been given more.",
    ]
    return "\n".join(lines)
