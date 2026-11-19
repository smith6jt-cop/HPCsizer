"""Requested against used, for the group: who, which jobs, and what to ask for next.

All ratios are weighted by resource-hours. A plain average of per-job
efficiencies lets a thousand ten-second jobs outvote the one session that held
sixteen cores for two days.

Only measured jobs enter a ratio: a job too short for accounting to sample is
neither efficient nor wasteful, it is unknown, and it is counted as such.
What a ratio leaves out is always shown next to it.
"""

import csv
import io
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from lib import metrics, recommender
from lib.config import Config
from lib.fmt import (
    fmt_date,
    fmt_gb,
    fmt_hours,
    fmt_int,
    fmt_mem_mb,
    fmt_minutes,
    fmt_num,
    fmt_pct,
    fmt_ts,
    markdown_table,
    table,
    wrap,
)
from lib.history import History

FAILURE_STATES = ("OUT_OF_MEMORY", "TIMEOUT", "FAILED", "NODE_FAIL", "BOOT_FAIL", "DEADLINE")

# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def new_totals() -> Dict[str, Any]:
    return {
        "jobs": 0,
        "ran": 0,
        "elapsed_h": 0.0,
        # everything that ran, measured or not
        "core_h_reserved": 0.0,
        "mem_gbh_reserved": 0.0,
        "gpu_h": 0.0,
        # the measured part
        "mem_n": 0,
        "mem_gbh_req": 0.0,
        "mem_gbh_used": 0.0,
        "cpu_n": 0,
        "core_h_alloc": 0.0,
        "core_h_used": 0.0,
        "gpu_n": 0,
        "gpu_h_measured": 0.0,
        "gpu_h_idle": 0.0,
        "states": {},
        "flags": {},
    }


def add_job(tot: Dict[str, Any], job: Mapping[str, Any]) -> None:
    m = job["m"]
    tot["jobs"] += 1
    state = job.get("state") or "?"
    tot["states"][state] = tot["states"].get(state, 0) + 1
    for flag in m["flags"]:
        tot["flags"][flag] = tot["flags"].get(flag, 0) + 1
    hours = m["elapsed_h"]
    if hours <= 0:
        return
    tot["ran"] += 1
    tot["elapsed_h"] += hours
    tot["core_h_reserved"] += m["core_h_alloc"] or 0.0
    tot["mem_gbh_reserved"] += m["mem_gbh_req"] or 0.0
    tot["gpu_h"] += m["gpu_h"]
    if m["mem_gbh_used"] is not None and m["mem_gbh_req"] is not None:
        tot["mem_n"] += 1
        tot["mem_gbh_req"] += m["mem_gbh_req"]
        tot["mem_gbh_used"] += min(m["mem_gbh_used"], m["mem_gbh_req"])
    if m["core_h_used"] is not None and m["core_h_alloc"] is not None:
        tot["cpu_n"] += 1
        tot["core_h_alloc"] += m["core_h_alloc"]
        tot["core_h_used"] += min(m["core_h_used"], m["core_h_alloc"])
    if m["gpu_h"] and m["gpu_util"] is not None:
        tot["gpu_n"] += 1
        tot["gpu_h_measured"] += m["gpu_h"]
        tot["gpu_h_idle"] += m["gpu_h_idle"]


def finish(tot: Dict[str, Any]) -> Dict[str, Any]:
    """Add the derived ratios; None where nothing was measured."""
    tot["mem_gbh_unused"] = tot["mem_gbh_req"] - tot["mem_gbh_used"]
    tot["core_h_unused"] = tot["core_h_alloc"] - tot["core_h_used"]
    tot["mem_used_frac"] = tot["mem_gbh_used"] / tot["mem_gbh_req"] if tot["mem_gbh_req"] else None
    tot["cpu_used_frac"] = tot["core_h_used"] / tot["core_h_alloc"] if tot["core_h_alloc"] else None
    tot["gpu_idle_frac"] = (
        tot["gpu_h_idle"] / tot["gpu_h_measured"] if tot["gpu_h_measured"] else None
    )
    return tot


def summarize(jobs: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    tot = new_totals()
    for job in jobs:
        add_job(tot, job)
    return finish(tot)


def group_by(jobs: Iterable[Mapping[str, Any]], key: Callable) -> Dict[Any, Dict[str, Any]]:
    groups: Dict[Any, Dict[str, Any]] = {}
    for job in jobs:
        tot = groups.get(key(job))
        if tot is None:
            tot = groups[key(job)] = new_totals()
        add_job(tot, job)
    for tot in groups.values():
        finish(tot)
    return groups


def family_rows(jobs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One row per job family: totals plus what its runs requested and used."""
    members: Dict[str, List[Dict[str, Any]]] = {}
    for job in jobs:
        members.setdefault(job["family"], []).append(job)
    rows = []
    for family, runs in members.items():
        runs.sort(key=lambda j: j.get("end_ts") or 0)
        last = runs[-1]
        row = summarize(runs)
        row.update(
            family=family,
            user=last.get("user"),
            kind=last.get("kind"),
            session=bool(last.get("session")),
            job_name=last.get("job_name"),
            last_end_ts=last.get("end_ts"),
            last_job_id=last.get("job_id"),
            mem_req_gb=metrics.describe([r["m"]["mem_req_gb"] for r in runs]),
            mem_used_gb=metrics.describe([r["m"]["mem_used_gb"] for r in runs]),
            cores_alloc=metrics.describe([r["m"]["cores_alloc"] for r in runs]),
            cores_used=metrics.describe([r["m"]["cores_used"] for r in runs]),
            elapsed_min=metrics.describe(
                [r["elapsed_sec"] / 60.0 for r in runs if (r.get("elapsed_sec") or 0) > 0]
            ),
            time_req_min=metrics.describe([r["m"]["time_req_min"] for r in runs]),
            gpu_util=metrics.describe([r["m"]["gpu_util"] for r in runs if r["m"]["gpus"]]),
        )
        row["_last"] = last
        rows.append(row)
    return rows


def suggest_for(row: Dict[str, Any], history: History, cfg: Config, now: int) -> Dict[str, Any]:
    """What the family's runs suggest for its next run, reduced to what a table shows.

    The row covers every member of the family, so the evidence is taken at
    family level too -- not from the runs that share the latest member's exact
    name, which ``hpg recommend`` would try first.
    """
    target = recommender.target_from_job(row["_last"], cfg)
    target.cls["name_exact"] = ""
    rec = recommender.recommend(target, history, cfg, now)
    return {
        "source": rec["source"],
        "options": rec["options"],
        "changes": rec["changes"],
        "confidence": rec["confidence"]["level"],
        "mem_mb": rec["mem"]["suggested"],
        "mem_action": rec["mem"]["action"],
        "cpus": rec["cpus"]["suggested"],
        "cpus_action": rec["cpus"]["action"],
        "cpus_kept_because": rec["cpus"]["kept_because"],
        "time_min": rec["time"]["suggested"],
        "time_action": rec["time"]["action"],
        "gpus_action": rec["gpus"]["action"],
    }


def _top(rows: Sequence[Dict[str, Any]], key: str, limit: int) -> List[Dict[str, Any]]:
    ranked = sorted((r for r in rows if r[key] > 0), key=lambda r: -r[key])
    return ranked[:limit]


def _failures(jobs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for state in FAILURE_STATES:
        hit = [
            j
            for j in jobs
            if j.get("state") == state
            and not (state == "TIMEOUT" and j.get("session"))
            and (j.get("elapsed_sec") or 0) > 0
        ]
        if state == "OUT_OF_MEMORY":
            seen = {id(j) for j in hit}
            hit += [j for j in jobs if "oom" in j["m"]["flags"] and id(j) not in seen]
        if not hit:
            continue
        families: Dict[str, List[str]] = {}
        for job in hit:
            families.setdefault(job["family"], []).append(str(job.get("job_id")))
        ranked = sorted(families.items(), key=lambda kv: -len(kv[1]))
        out.append(
            {
                "state": state,
                "jobs": len(hit),
                "core_h": sum(j["m"]["core_h_alloc"] or 0.0 for j in hit),
                "families": [
                    {"family": family, "jobs": len(ids), "examples": ids[-3:]}
                    for family, ids in ranked[:5]
                ],
            }
        )
    return out


def build(
    jobs: Sequence[Dict[str, Any]],
    history: History,
    cfg: Config,
    now: int,
    days: float,
    user: Optional[str] = None,
    kind: Optional[str] = None,
    top: int = 10,
    watermark_ts: Optional[int] = None,
    account: Optional[str] = None,
) -> Dict[str, Any]:
    """The report as data. ``jobs`` are the annotated jobs of the window, already filtered."""
    totals = summarize(jobs)
    users = [
        dict(tot, user=name) for name, tot in group_by(jobs, lambda j: j.get("user") or "?").items()
    ]
    users.sort(key=lambda r: -r["core_h_reserved"])
    kinds = [dict(tot, kind=name) for name, tot in group_by(jobs, lambda j: j["kind"]).items()]
    kinds.sort(key=lambda r: -r["core_h_reserved"])
    families = family_rows(jobs)
    for row in families:
        row["suggestion"] = suggest_for(row, history, cfg, now)
        del row["_last"]
    sessions = sorted((r for r in families if r["session"]), key=lambda r: -r["core_h_reserved"])
    return {
        "accounts": list(cfg.accounts),
        "window": {"days": days, "since_ts": int(now - days * 86400), "until_ts": int(now)},
        "filters": {"user": user, "kind": kind, "account": account},
        "data": {"as_of_ts": watermark_ts},
        "totals": totals,
        "users": users,
        "kinds": kinds,
        "n_users": len(users),
        "n_families": len(families),
        "families": sorted(families, key=lambda r: -r["core_h_reserved"]),
        "top_memory": [r["family"] for r in _top(families, "mem_gbh_unused", top)],
        "top_cpu": [r["family"] for r in _top(families, "core_h_unused", top)],
        "top_gpu": [r["family"] for r in _top(families, "gpu_h_idle", top)],
        "sessions": [r["family"] for r in sessions[:top]],
        "failures": _failures(jobs),
        "unmeasured": {
            "memory": totals["ran"] - totals["mem_n"],
            "cpu": totals["ran"] - totals["cpu_n"],
        },
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _triple(d: Optional[Dict[str, float]], fmt=fmt_num) -> str:
    if not d:
        return "-"
    return "%s / %s" % (fmt(d["p50"]), fmt(d["max"]))


def _mem_pair(d: Optional[Dict[str, float]]) -> str:
    if not d:
        return "-"
    if d["max"] < 1:
        return "%.0f / %.0f MB" % (d["p50"] * 1024, d["max"] * 1024)
    return "%s / %s GB" % (fmt_num(d["p50"]), fmt_num(d["max"]))


def _suggested(sug: Mapping[str, Any], what: str) -> str:
    """The suggested value of one resource, or why there is none."""
    if sug["source"] != recommender.SOURCE_HISTORY:
        return "too few runs"
    if what == "mem":
        if sug["mem_action"] in ("lower", "raise", "set"):
            return fmt_mem_mb(sug["mem_mb"])
        return "keep" if sug["mem_action"] == "keep" else "not measured"
    if what == "cpus":
        if sug["cpus_action"] in ("lower", "set"):
            return "%d" % sug["cpus"]
        return {
            "unprofiled-session": "profile it first",
            "saturated": "keep (all busy)",
            "unmeasured": "not measured",
            "multi": "keep (multi-task)",
        }.get(sug["cpus_kept_because"], "keep")
    if what == "gpus":
        return "drop the GPU" if sug["gpus_action"] == "lower" else "keep"
    return "-"


def _sections(rep: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The report as titled tables, shared by the text and Markdown renderers."""
    by_family = {r["family"]: r for r in rep["families"]}
    tot = rep["totals"]
    sections: List[Dict[str, Any]] = []

    def usage_row(label: str, r: Mapping[str, Any]) -> List[str]:
        return [
            label,
            fmt_int(r["jobs"]),
            fmt_hours(r["core_h_reserved"]),
            fmt_pct(r["cpu_used_frac"]),
            fmt_hours(r["mem_gbh_reserved"]),
            fmt_pct(r["mem_used_frac"]),
            fmt_hours(r["gpu_h"]) if r["gpu_h"] else "-",
            fmt_pct(r["gpu_idle_frac"]) if r["gpu_h_measured"] else "-",
            fmt_int(r["flags"].get("oom", 0)),
            fmt_int(r["flags"].get("timeout", 0)),
        ]

    usage_headers = [
        "",
        "Jobs",
        "Core-h reserved",
        "busy",
        "Memory GB-h reserved",
        "used",
        "GPU-h",
        "idle",
        "OOM",
        "Timeout",
    ]
    align = "lrrrrrrrrr"

    def covered(part: float, whole: float) -> str:
        return fmt_pct(part / whole) if whole else "none"

    sections.append(
        {
            "title": "Requested and used, by user",
            "headers": ["User"] + usage_headers[1:],
            "rows": [usage_row(r["user"], r) for r in rep["users"]] + [usage_row("Total", tot)],
            "align": align,
            "note": "'busy' and 'used' are shares of what was reserved, weighted by resource-hours "
            "and taken over the jobs Slurm measured: %s of the core-hours and %s of the memory "
            "GB-hours. %s of the %s jobs that ran have no memory measurement and %s no CPU "
            "measurement (too short, or killed before CPU time was recorded)."
            % (
                covered(tot["core_h_alloc"], tot["core_h_reserved"]),
                covered(tot["mem_gbh_req"], tot["mem_gbh_reserved"]),
                fmt_int(rep["unmeasured"]["memory"]),
                fmt_int(tot["ran"]),
                fmt_int(rep["unmeasured"]["cpu"]),
            ),
        }
    )
    sections.append(
        {
            "title": "By kind of job",
            "headers": ["Kind"] + usage_headers[1:],
            "rows": [usage_row(r["kind"], r) for r in rep["kinds"]],
            "align": align,
            "note": "",
        }
    )

    def families(names: Sequence[str]) -> List[Dict[str, Any]]:
        return [by_family[name] for name in names]

    if rep["top_memory"]:
        sections.append(
            {
                "title": "Memory requested and not used: the %d largest job families"
                % len(rep["top_memory"]),
                "headers": [
                    "Family",
                    "Jobs",
                    "Requested p50",
                    "Used p50 / max",
                    "Unused GB-h",
                    "Suggested --mem",
                    "Last run",
                ],
                "rows": [
                    [
                        r["family"],
                        fmt_int(r["jobs"]),
                        fmt_gb(r["mem_req_gb"]["p50"]) if r["mem_req_gb"] else "-",
                        _mem_pair(r["mem_used_gb"]),
                        fmt_hours(r["mem_gbh_unused"]),
                        _suggested(r["suggestion"], "mem"),
                        fmt_date(r["last_end_ts"]),
                    ]
                    for r in families(rep["top_memory"])
                ],
                "align": "lrrrrrl",
                "note": "",
            }
        )
    if rep["top_cpu"]:
        sections.append(
            {
                "title": "Cores allocated and not used: the %d largest job families"
                % len(rep["top_cpu"]),
                "headers": [
                    "Family",
                    "Jobs",
                    "Allocated p50",
                    "Busy p50 / max",
                    "Unused core-h",
                    "Suggested CPUs",
                    "Last run",
                ],
                "rows": [
                    [
                        r["family"],
                        fmt_int(r["jobs"]),
                        fmt_num(r["cores_alloc"]["p50"]) if r["cores_alloc"] else "-",
                        _triple(r["cores_used"]),
                        fmt_hours(r["core_h_unused"]),
                        _suggested(r["suggestion"], "cpus"),
                        fmt_date(r["last_end_ts"]),
                    ]
                    for r in families(rep["top_cpu"])
                ],
                "align": "lrrrrrl",
                "note": "",
            }
        )
    if rep["top_gpu"]:
        sections.append(
            {
                "title": "GPUs held without being used",
                "headers": ["Family", "Jobs", "GPU-h", "Idle GPU-h", "Busiest (%)", "Suggested"],
                "rows": [
                    [
                        r["family"],
                        fmt_int(r["jobs"]),
                        fmt_hours(r["gpu_h"]),
                        fmt_hours(r["gpu_h_idle"]),
                        fmt_num(r["gpu_util"]["max"]) if r["gpu_util"] else "-",
                        _suggested(r["suggestion"], "gpus"),
                    ]
                    for r in families(rep["top_gpu"])
                ],
                "align": "lrrrrl",
                "note": "",
            }
        )
    if rep["sessions"]:
        sections.append(
            {
                "title": "Interactive sessions",
                "headers": [
                    "Family",
                    "Sessions",
                    "Hours",
                    "Core-h reserved",
                    "Cores p50",
                    "Busy p50 / max",
                    "Memory req p50",
                    "Peak p50 / max",
                    "At limit",
                ],
                "rows": [
                    [
                        r["family"],
                        fmt_int(r["jobs"]),
                        fmt_hours(r["elapsed_h"]),
                        fmt_hours(r["core_h_reserved"]),
                        fmt_num(r["cores_alloc"]["p50"]) if r["cores_alloc"] else "-",
                        _triple(r["cores_used"]),
                        fmt_gb(r["mem_req_gb"]["p50"]) if r["mem_req_gb"] else "-",
                        _mem_pair(r["mem_used_gb"]),
                        fmt_int(r["states"].get("TIMEOUT", 0)),
                    ]
                    for r in families(rep["sessions"])
                ],
                "align": "lrrrrrrrr",
                "note": "A session's cores are busy only while something runs in it; the average "
                "includes the time it waited. Sessions submitted with 'hpg submit' are profiled, "
                "which shows what they need while working.",
            }
        )
    if rep["failures"]:
        sections.append(
            {
                "title": "Jobs that did not finish",
                "headers": ["State", "Jobs", "Core-h", "Mostly"],
                "rows": [
                    [
                        f["state"],
                        fmt_int(f["jobs"]),
                        fmt_hours(f["core_h"]),
                        "; ".join(
                            "%s (%d, e.g. job %s)" % (g["family"], g["jobs"], g["examples"][-1])
                            for g in f["families"][:3]
                        ),
                    ]
                    for f in rep["failures"]
                ],
                "align": "lrrl",
                "note": "",
            }
        )
    return sections


def _heading(rep: Dict[str, Any]) -> List[str]:
    window = rep["window"]
    who = []
    if rep["filters"]["user"]:
        who.append("user %s" % rep["filters"]["user"])
    if rep["filters"]["kind"]:
        who.append("kind %s" % rep["filters"]["kind"])
    accounts = [rep["filters"]["account"]] if rep["filters"].get("account") else rep["accounts"]
    who.insert(0, "account %s" % (", ".join(accounts) or "(all stored)"))
    scope = ", ".join(who)
    tot = rep["totals"]
    return [
        "HPCsizer report: %s, %s to %s (%g days)"
        % (scope, fmt_date(window["since_ts"]), fmt_date(window["until_ts"]), window["days"]),
        "%s finished jobs of %s user(s) in %s job families; data as of %s"
        % (
            fmt_int(tot["jobs"]),
            fmt_int(rep["n_users"]),
            fmt_int(rep["n_families"]),
            fmt_ts(rep["data"]["as_of_ts"]),
        ),
    ]


def render_text(rep: Dict[str, Any]) -> str:
    lines = _heading(rep)
    if not rep["totals"]["jobs"]:
        return "\n".join(lines + ["", "No finished jobs in this window."])
    for section in _sections(rep):
        lines += ["", section["title"]]
        lines.append(
            table(section["headers"], section["rows"], align=section["align"], indent="  ")
        )
        if section["note"]:
            lines.append(wrap(section["note"], indent="  "))
    lines += [
        "",
        "Per job: hpg job ID.  Per family: hpg history FAMILY.  Before submitting: hpg recommend.",
    ]
    return "\n".join(lines)


def render_markdown(rep: Dict[str, Any]) -> str:
    head = _heading(rep)
    lines = ["# " + head[0], "", head[1]]
    if not rep["totals"]["jobs"]:
        return "\n".join(lines + ["", "No finished jobs in this window."])
    for section in _sections(rep):
        lines += ["", "## " + section["title"], ""]
        lines.append(markdown_table(section["headers"], section["rows"], align=section["align"]))
        if section["note"]:
            lines += ["", section["note"]]
    return "\n".join(lines)


FAMILY_CSV = (
    "family",
    "user",
    "kind",
    "jobs",
    "ran",
    "last_end",
    "elapsed_h",
    "core_h_reserved",
    "core_h_measured",
    "core_h_used",
    "mem_gbh_reserved",
    "mem_gbh_measured",
    "mem_gbh_used",
    "gpu_h",
    "gpu_h_idle",
    "mem_req_gb_p50",
    "mem_used_gb_p50",
    "mem_used_gb_max",
    "cores_alloc_p50",
    "cores_used_p50",
    "cores_used_max",
    "elapsed_min_p50",
    "elapsed_min_max",
    "time_req_min_p50",
    "oom",
    "timeout",
    "completed",
    "suggested_mem_mb",
    "suggested_cpus",
    "suggested_time_min",
    "suggestion_source",
    "suggestion_confidence",
)


def _stat(d: Optional[Dict[str, float]], key: str) -> Any:
    return "" if not d else round(d[key], 3)


def family_csv_rows(rep: Dict[str, Any]) -> List[List[Any]]:
    rows = []
    for r in rep["families"]:
        sug = r["suggestion"]
        rows.append(
            [
                r["family"],
                r["user"],
                r["kind"],
                r["jobs"],
                r["ran"],
                fmt_date(r["last_end_ts"]),
                round(r["elapsed_h"], 2),
                round(r["core_h_reserved"], 2),
                round(r["core_h_alloc"], 2),
                round(r["core_h_used"], 2),
                round(r["mem_gbh_reserved"], 2),
                round(r["mem_gbh_req"], 2),
                round(r["mem_gbh_used"], 2),
                round(r["gpu_h"], 2),
                round(r["gpu_h_idle"], 2),
                _stat(r["mem_req_gb"], "p50"),
                _stat(r["mem_used_gb"], "p50"),
                _stat(r["mem_used_gb"], "max"),
                _stat(r["cores_alloc"], "p50"),
                _stat(r["cores_used"], "p50"),
                _stat(r["cores_used"], "max"),
                _stat(r["elapsed_min"], "p50"),
                _stat(r["elapsed_min"], "max"),
                _stat(r["time_req_min"], "p50"),
                r["flags"].get("oom", 0),
                r["flags"].get("timeout", 0),
                r["states"].get("COMPLETED", 0),
                "" if sug["mem_mb"] is None else int(sug["mem_mb"]),
                "" if sug["cpus"] is None else int(sug["cpus"]),
                "" if sug["time_min"] is None else int(sug["time_min"]),
                sug["source"],
                sug["confidence"],
            ]
        )
    return rows


def to_csv(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(headers)
    writer.writerows(rows)
    return buffer.getvalue()


def render_csv(rep: Dict[str, Any]) -> str:
    """One line per job family -- the table to sort and filter in a spreadsheet."""
    return to_csv(FAMILY_CSV, family_csv_rows(rep))


# ---------------------------------------------------------------------------
# hpg families
# ---------------------------------------------------------------------------

FAMILY_SORTS = {
    "cores": lambda r: -r["core_h_reserved"],
    "memory": lambda r: -r["mem_gbh_unused"],
    "cpu": lambda r: -r["core_h_unused"],
    "jobs": lambda r: -r["jobs"],
    "recent": lambda r: -(r["last_end_ts"] or 0),
    "name": lambda r: r["family"],
}


def families_table(rows: Sequence[Dict[str, Any]]) -> str:
    return table(
        [
            "Family",
            "Jobs",
            "Last run",
            "Core-h",
            "Cores p50",
            "busy p50 / max",
            "Mem req p50",
            "used p50 / max",
            "Time p50 / max",
            "limit p50",
            "OOM",
            "T/O",
        ],
        [
            [
                r["family"],
                fmt_int(r["jobs"]),
                fmt_date(r["last_end_ts"]),
                fmt_hours(r["core_h_reserved"]),
                fmt_num(r["cores_alloc"]["p50"]) if r["cores_alloc"] else "-",
                _triple(r["cores_used"]),
                fmt_gb(r["mem_req_gb"]["p50"]) if r["mem_req_gb"] else "-",
                _mem_pair(r["mem_used_gb"]),
                _triple(r["elapsed_min"], fmt_minutes),
                fmt_minutes(r["time_req_min"]["p50"]) if r["time_req_min"] else "-",
                fmt_int(r["flags"].get("oom", 0)),
                fmt_int(r["flags"].get("timeout", 0)),
            ]
            for r in rows
        ],
        align="lrlrrrrrrrrr",
    )
