"""Views of single jobs and of the runs of one family: ``hpg job``, ``history``, ``flags``, ``export``."""

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from lib import metrics
from lib.classify import collapse_digits
from lib.fmt import (
    fmt_bytes,
    fmt_date,
    fmt_gb,
    fmt_hours,
    fmt_int,
    fmt_minutes,
    fmt_num,
    fmt_pct,
    fmt_seconds,
    fmt_ts,
    table,
)
from lib.profile import describe as describe_profile

MEM_SOURCE_TEXT = {
    "step": "largest step",
    "overlap_sum": "sum of the steps that ran together",
    "ntasks_est": "estimated over the tasks of a step",
    "job": "job total",
    "legacy_batch": "batch step only (migrated record)",
}

# ---------------------------------------------------------------------------
# Flags, with the numbers behind them
# ---------------------------------------------------------------------------


def flag_detail(job: Mapping[str, Any], flag: str) -> str:
    """One flag of one job, as the measurement that raised it."""
    m = job["m"]
    hours = m["elapsed_h"]
    if flag == "oom":
        return "ran out of memory with %s requested after %s" % (
            fmt_gb(m["mem_req_gb"]),
            fmt_seconds(job.get("elapsed_sec")),
        )
    if flag == "timeout":
        return "hit its %s limit" % fmt_minutes(m["time_req_min"])
    if flag == "mem_overrequest":
        return "peak %s of %s requested (%s) for %s h: %s GB-h unused" % (
            fmt_gb(m["mem_used_gb"]),
            fmt_gb(m["mem_req_gb"]),
            fmt_pct(m["mem_eff"]),
            fmt_num(hours),
            fmt_hours(m["mem_gbh_unused"]),
        )
    if flag == "cpu_overrequest":
        return "%s of %s cores busy on average (%s) for %s h: %s core-h unused" % (
            fmt_num(m["cores_used"]),
            fmt_int(m["cores_alloc"]),
            fmt_pct(m["cpu_eff"]),
            fmt_num(hours),
            fmt_hours(m["core_h_unused"]),
        )
    if flag == "time_overrequest":
        return "finished in %s of a %s limit (%s)" % (
            fmt_seconds(job.get("elapsed_sec")),
            fmt_minutes(m["time_req_min"]),
            fmt_pct(m["time_eff"]),
        )
    if flag == "gpu_idle":
        return "%d GPU(s) for %s h, busiest moment %s%%: %s GPU-h idle" % (
            m["gpus"],
            fmt_num(hours),
            fmt_num(m["gpu_util"]),
            fmt_hours(m["gpu_h_idle"]),
        )
    if flag == "unmeasured":
        return "ran %s: too short for accounting to sample its memory" % fmt_seconds(
            job.get("elapsed_sec")
        )
    return flag


def flag_weight(job: Mapping[str, Any], flag: str) -> float:
    """How much a flagged job matters, in the resource-hours behind the flag."""
    m = job["m"]
    if flag == "mem_overrequest":
        return (m["mem_gbh_unused"] or 0.0) / 4.0  # ~4 GB per core on this kind of node
    if flag == "cpu_overrequest":
        return m["core_h_unused"] or 0.0
    if flag == "gpu_idle":
        return (m["gpu_h_idle"] or 0.0) * 16.0
    if flag in ("oom", "timeout"):
        return m["core_h_alloc"] or 0.0
    if flag == "time_overrequest":
        return ((m["time_req_min"] or 0) / 60.0 - m["elapsed_h"]) / 24.0
    return 0.0


def flagged(
    jobs: Sequence[Dict[str, Any]], only: Optional[str] = None
) -> Tuple[Dict[str, int], List[Tuple[float, Dict[str, Any], str]]]:
    """``(counts, items)``: jobs per flag, and ``(weight, job, flag)`` heaviest first.

    ``unmeasured`` is counted but not listed unless asked for: it describes the
    measurement, not the job.
    """
    counts: Dict[str, int] = {}
    items: List[Tuple[float, Dict[str, Any], str]] = []
    for job in jobs:
        for flag in job["m"]["flags"]:
            counts[flag] = counts.get(flag, 0) + 1
            if only is not None and flag != only:
                continue
            if only is None and flag == "unmeasured":
                continue
            items.append((flag_weight(job, flag), job, flag))
    items.sort(key=lambda item: -item[0])
    return counts, items


FLAG_SHORT = {
    "oom": "OOM",
    "timeout": "TIMEOUT",
    "mem_overrequest": "mem",
    "cpu_overrequest": "cpu",
    "time_overrequest": "time",
    "gpu_idle": "gpu",
}
FLAG_LEGEND = "Flags: mem, cpu, time = far more requested than used; gpu = GPU idle"


def group_detail(flag: str, jobs: Sequence[Mapping[str, Any]]) -> str:
    """One flag over the jobs of one family, as typical values and a total."""

    def median(key: str) -> Optional[float]:
        d = metrics.describe([j["m"][key] for j in jobs])
        return d["p50"] if d else None

    total = lambda key: sum(j["m"][key] or 0.0 for j in jobs)  # noqa: E731
    last = max(jobs, key=lambda j: j.get("end_ts") or 0)
    if flag == "mem_overrequest":
        return "typical peak %s of %s requested; %s GB-h unused" % (
            fmt_gb(median("mem_used_gb")),
            fmt_gb(median("mem_req_gb")),
            fmt_hours(total("mem_gbh_unused")),
        )
    if flag == "cpu_overrequest":
        return "typically %s of %s cores busy; %s core-h unused" % (
            fmt_num(median("cores_used")),
            fmt_num(median("cores_alloc")),
            fmt_hours(total("core_h_unused")),
        )
    if flag == "time_overrequest":
        d = metrics.describe([j["elapsed_sec"] / 60.0 for j in jobs if j.get("elapsed_sec")])
        return "typical run %s, longest %s, of a %s limit" % (
            fmt_minutes(d["p50"]) if d else "-",
            fmt_minutes(d["max"]) if d else "-",
            fmt_minutes(median("time_req_min")),
        )
    if flag == "gpu_idle":
        return "%s GPU-h held without use" % fmt_hours(total("gpu_h_idle"))
    if flag == "oom":
        return "last: job %s with %s requested, %s" % (
            last.get("job_id"),
            fmt_gb(last["m"]["mem_req_gb"]),
            fmt_date(last.get("end_ts")),
        )
    if flag == "timeout":
        return "last: job %s at its %s limit, %s" % (
            last.get("job_id"),
            fmt_minutes(last["m"]["time_req_min"]),
            fmt_date(last.get("end_ts")),
        )
    return "%d job(s) too short to measure" % len(jobs)


def flag_groups(
    items: Sequence[Tuple[float, Dict[str, Any], str]],
) -> List[Dict[str, Any]]:
    """Flagged jobs gathered per family and flag, heaviest group first."""
    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for weight, job, flag in items:
        group = groups.setdefault(
            (job["family"], flag),
            {"family": job["family"], "flag": flag, "weight": 0.0, "runs": []},
        )
        group["weight"] += weight
        group["runs"].append(job)
    out = []
    for group in sorted(groups.values(), key=lambda g: -g["weight"]):
        runs = group.pop("runs")
        group["jobs"] = len(runs)
        group["detail"] = group_detail(group["flag"], runs)
        group["example"] = str(max(runs, key=lambda j: flag_weight(j, group["flag"])).get("job_id"))
        out.append(group)
    return out


def render_flags(
    jobs: Sequence[Dict[str, Any]],
    days: float,
    only: Optional[str] = None,
    limit: int = 30,
    per_job: bool = False,
) -> str:
    counts, items = flagged(jobs, only)
    lines = ["Flags on %s jobs that finished in the last %g days" % (fmt_int(len(jobs)), days)]
    rows = [
        [flag, fmt_int(counts.get(flag, 0)), metrics.FLAG_HELP[flag]]
        for flag in metrics.ACCOUNTING_FLAGS
        if counts.get(flag)
    ]
    if not rows:
        return "\n".join(lines + ["", "No job was flagged."])
    lines += ["", table(["Flag", "Jobs", "Meaning"], rows, align="lrl", indent="  ")]
    if not items:
        return "\n".join(lines)
    if per_job:
        shown = items[:limit]
        lines += ["", "Jobs, largest first (%d of %s):" % (len(shown), fmt_int(len(items)))]
        lines.append(
            table(
                ["Job", "User", "Family", "Flag", "What was measured"],
                [
                    [
                        job.get("job_id"),
                        job.get("user"),
                        _short_family(job),
                        flag,
                        flag_detail(job, flag),
                    ]
                    for _weight, job, flag in shown
                ],
                indent="  ",
            )
        )
    else:
        groups = flag_groups(items)
        shown_groups = groups[:limit]
        lines += [
            "",
            "By job family, largest first (%d of %s; --jobs lists single jobs):"
            % (len(shown_groups), fmt_int(len(groups))),
        ]
        lines.append(
            table(
                ["Family", "Flag", "Jobs", "What was measured", "Example"],
                [
                    [g["family"], g["flag"], fmt_int(g["jobs"]), g["detail"], "job " + g["example"]]
                    for g in shown_groups
                ],
                align="llrll",
                indent="  ",
            )
        )
    return "\n".join(lines)


def _short_family(job: Mapping[str, Any]) -> str:
    """The family without its user prefix, for tables that have a user column."""
    family = job.get("family") or ""
    user = job.get("user") or ""
    return family[len(user) + 1 :] if user and family.startswith(user + "/") else family


# ---------------------------------------------------------------------------
# hpg history
# ---------------------------------------------------------------------------


def match_families(
    jobs: Sequence[Dict[str, Any]], query: str, user: Optional[str] = None
) -> List[str]:
    """Families a query names: an exact family, a job name, or a unique fragment.

    A job name matches the caller's own families first (``user``), since a
    name such as ``align`` means different work for different people.
    """
    families: Dict[str, Dict[str, Any]] = {}
    for job in jobs:
        families.setdefault(job["family"], job)
    if query in families:
        return [query]
    key = collapse_digits(query)
    named = [
        family
        for family, job in families.items()
        if job.get("name_exact") == query
        or job.get("name_key") in (key, "cmd:" + key)
        or family.endswith("/" + query)
    ]
    if user:
        own = [f for f in named if families[f].get("user") == user]
        named = own or named
    if named:
        return sorted(named)
    low = query.lower()
    return sorted(f for f in families if low in f.lower())


def run_rows(
    runs: Sequence[Dict[str, Any]], with_family: bool = False
) -> Tuple[List[str], List[List[str]]]:
    headers = [
        "Job",
        "Ended",
        "State",
        "Elapsed / limit",
        "Memory used / req",
        "Cores busy / alloc",
    ]
    gpu = any(r["m"]["gpus"] for r in runs)
    if gpu:
        headers.append("GPU busiest")
    if with_family:
        headers.insert(1, "Family")
    headers.append("Flags")
    rows = []
    for run in runs:
        m = run["m"]
        row = [
            str(run.get("job_id")),
            fmt_ts(run.get("end_ts")),
            run.get("state") or "",
            "%s / %s" % (fmt_seconds(run.get("elapsed_sec")), fmt_minutes(m["time_req_min"])),
            "%s / %s%s"
            % (
                fmt_gb(m["mem_used_gb"]),
                fmt_gb(m["mem_req_gb"]),
                " (%s)" % fmt_pct(m["mem_eff"]) if m["mem_eff"] is not None else "",
            ),
            "%s / %s%s"
            % (
                fmt_num(m["cores_used"]),
                fmt_int(m["cores_alloc"]),
                " (%s)" % fmt_pct(m["cpu_eff"]) if m["cpu_eff"] is not None else "",
            ),
        ]
        if gpu:
            row.append("-" if m["gpu_util"] is None else "%s%%" % fmt_num(m["gpu_util"]))
        if with_family:
            row.insert(1, run.get("family") or "")
        row.append(" ".join(FLAG_SHORT[f] for f in m["flags"] if f in FLAG_SHORT))
        rows.append(row)
    return headers, rows


def render_history(
    runs: Sequence[Dict[str, Any]], title: str, limit: int = 25, with_family: bool = False
) -> str:
    """The most recent runs of a family (or of a user), newest last, and their spread."""
    runs = sorted(runs, key=lambda r: r.get("end_ts") or 0)
    states: Dict[str, int] = {}
    for run in runs:
        states[run.get("state") or "?"] = states.get(run.get("state") or "?", 0) + 1
    summary = ", ".join("%d %s" % (n, s) for s, n in sorted(states.items(), key=lambda kv: -kv[1]))
    lines = ["%s: %s run(s) -- %s" % (title, fmt_int(len(runs)), summary or "none")]
    if not runs:
        return "\n".join(lines)
    shown = runs[-limit:]
    if len(shown) < len(runs):
        lines.append("(the latest %d are listed; the spread below covers all)" % len(shown))
    headers, rows = run_rows(shown, with_family)
    lines += ["", table(headers, rows)]
    if any(row[-1] for row in rows):
        lines.append(FLAG_LEGEND)

    spread = []
    for label, values, fmt in (
        ("Memory used", [r["m"]["mem_used_gb"] for r in runs], fmt_gb),
        ("Memory requested", [r["m"]["mem_req_gb"] for r in runs], fmt_gb),
        ("Cores busy", [r["m"]["cores_used"] for r in runs], fmt_num),
        ("Cores allocated", [r["m"]["cores_alloc"] for r in runs], fmt_num),
        ("Elapsed", [r["elapsed_sec"] / 60.0 for r in runs if r.get("elapsed_sec")], fmt_minutes),
        ("Time limit", [r["m"]["time_req_min"] for r in runs], fmt_minutes),
    ):
        d = metrics.describe(values)
        if d:
            spread.append(
                [label, fmt_int(d["n"]), fmt(d["min"]), fmt(d["p50"]), fmt(d["p95"]), fmt(d["max"])]
            )
    if spread:
        lines += ["", table(["", "n", "min", "p50", "p95", "max"], spread, align="lrrrrr")]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# hpg job
# ---------------------------------------------------------------------------


def render_job(
    job: Dict[str, Any],
    steps: Sequence[Mapping[str, Any]],
    profile: Optional[Mapping[str, Any]] = None,
    others: int = 0,
) -> str:
    m = job["m"]
    lines = [
        "Job %s  %s  [%s]  user %s, account %s, partition %s, QOS %s"
        % (
            job.get("job_id"),
            job.get("job_name") or "(unnamed)",
            job.get("kind"),
            job.get("user"),
            job.get("account") or "-",
            job.get("partition") or "-",
            job.get("qos") or "-",
        ),
        "Family    %s" % job.get("family"),
        "State     %s%s, exit %s"
        % (
            job.get("state"),
            " (%s)" % job["state_raw"]
            if job.get("state_raw") not in (None, job.get("state"))
            else "",
            job.get("exit_code") or "-",
        ),
        "Times     submitted %s, waited %s, ran %s, ended %s"
        % (
            fmt_ts(job.get("submit_ts")),
            fmt_seconds(m["wait_sec"]),
            fmt_seconds(job.get("elapsed_sec")),
            fmt_ts(job.get("end_ts")),
        ),
    ]
    if job.get("submit_line"):
        lines.append("Command   %s" % job["submit_line"])
    if job.get("work_dir"):
        lines.append("Directory %s" % job["work_dir"])
    if job.get("node_list") and job.get("node_list") != "None assigned":
        lines.append("Nodes     %s" % job["node_list"])
    if others:
        lines.append(
            "Note      this job id was used %d more time(s); the most recent is shown" % others
        )

    mem_how = (
        "profile"
        if m["mem_source"] == "profile"
        else MEM_SOURCE_TEXT.get(job.get("mem_src") or "", "")
    )
    rows = [
        [
            "Memory",
            fmt_gb(m["mem_req_gb"]),
            "not measured"
            if m["mem_used_gb"] is None
            else "%s (%s)" % (fmt_gb(m["mem_used_gb"]), fmt_pct(m["mem_eff"])),
            "peak; %s" % mem_how if m["mem_used_gb"] is not None and mem_how else "",
        ],
        [
            "CPUs",
            fmt_int(m["cores_alloc"]),
            "not measured"
            if m["cores_used"] is None
            else "%s cores (%s)" % (fmt_num(m["cores_used"]), fmt_pct(m["cpu_eff"])),
            ""
            if m["cores_used"] is None
            else "average; CPU time from %s%s"
            % (
                m["cpu_source"],
                ", busiest moment %s cores" % fmt_num(m["cores_peak"])
                if m["cores_peak"] is not None
                else "",
            ),
        ],
        [
            "Time",
            "unlimited" if m["time_req_min"] is None else fmt_minutes(m["time_req_min"]),
            "%s%s"
            % (
                fmt_seconds(job.get("elapsed_sec")),
                " (%s)" % fmt_pct(m["time_eff"]) if m["time_eff"] is not None else "",
            ),
            "",
        ],
    ]
    if m["gpus"]:
        rows.append(
            [
                "GPUs",
                "%d%s" % (m["gpus"], " " + job["gpu_type"] if job.get("gpu_type") else ""),
                "not recorded"
                if m["gpu_util"] is None
                else "busiest moment %s%%" % fmt_num(m["gpu_util"]),
                "" if m["gpu_mem_gb"] is None else "GPU memory up to %s" % fmt_gb(m["gpu_mem_gb"]),
            ]
        )
    lines += ["", table(["", "Requested", "Used", ""], rows)]

    if job.get("disk_read_bytes") is not None or job.get("disk_write_bytes") is not None:
        lines.append(
            "\nDisk      read %s, written %s"
            % (fmt_bytes(job.get("disk_read_bytes")), fmt_bytes(job.get("disk_write_bytes")))
        )

    flags = m["flags"]
    if flags:
        lines += ["", "Flags"]
        lines.extend("  %-17s %s" % (flag, flag_detail(job, flag)) for flag in flags)

    if steps:
        lines += ["", "Steps"]
        lines.append(
            table(
                [
                    "Step",
                    "State",
                    "Elapsed",
                    "Tasks",
                    "CPUs",
                    "CPU time",
                    "from",
                    "Peak memory",
                    "GPU",
                ],
                [
                    [
                        s.get("step_id") or "",
                        s.get("state") or "",
                        fmt_seconds(s.get("elapsed_sec")),
                        fmt_int(s.get("ntasks")),
                        fmt_int(s.get("alloc_cpus")),
                        fmt_seconds(s.get("cpu_sec")),
                        s.get("cpu_src") or "-",
                        fmt_bytes(s.get("max_rss_bytes")),
                        "-"
                        if s.get("gpu_util_max") is None
                        else "%s%%" % fmt_num(s["gpu_util_max"]),
                    ]
                    for s in steps
                ],
                indent="  ",
            )
        )

    if job.get("mon_n_samples"):
        lines += ["", "Profile (in-job sampler, %s)" % (job.get("mon_source") or "?")]
        lines.append(
            "  %s samples every %s s; peak memory %s; cores average %s, sustained %s, busiest %s"
            % (
                fmt_int(job["mon_n_samples"]),
                fmt_num(job.get("mon_interval_sec")),
                fmt_bytes(job.get("mon_peak_mem_bytes")),
                fmt_num(job.get("mon_avg_cores")),
                fmt_num(job.get("mon_p95_cores")),
                fmt_num(job.get("mon_max_cores")),
            )
        )
        if job.get("mon_top_comm"):
            lines.append("  largest processes: %s" % job["mon_top_comm"])
        for note in describe_profile(profile or {}):
            lines.append("  - %s" % note)
        if job.get("mon_series_path"):
            lines.append("  time series: hpg plot %s" % job.get("job_id"))

    lines += [
        "",
        "Other runs of this job:  hpg history '%s'" % job.get("family"),
        "What to request next:    hpg recommend --like %s" % job.get("job_id"),
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# hpg export
# ---------------------------------------------------------------------------

EXPORT_STORED = (
    "job_id",
    "user",
    "account",
    "partition",
    "qos",
    "job_name",
    "state",
    "exit_code",
    "submit_ts",
    "start_ts",
    "end_ts",
    "elapsed_sec",
    "req_time_min",
    "req_cpus",
    "alloc_cpus",
    "alloc_nodes",
    "req_mem_mb",
    "req_gpus",
    "alloc_gpus",
    "gpu_type",
    "cpu_sec",
    "cpu_src",
    "peak_mem_bytes",
    "mem_src",
    "oom",
    "disk_read_bytes",
    "disk_write_bytes",
    "gpu_util_max",
    "gpu_mem_max_bytes",
    "n_steps",
    "source",
    "submit_line",
    "work_dir",
)
EXPORT_DERIVED = (
    "kind",
    "family",
    "mem_req_gb",
    "mem_used_gb",
    "mem_source",
    "mem_eff",
    "cores_alloc",
    "cores_used",
    "cpu_source",
    "cpu_eff",
    "time_eff",
    "gpu_util",
    "mem_gbh_req",
    "mem_gbh_unused",
    "core_h_alloc",
    "core_h_unused",
    "gpu_h",
    "gpu_h_idle",
    "wait_sec",
    "flags",
)
EXPORT_COLUMNS = EXPORT_STORED + EXPORT_DERIVED


def export_record(job: Mapping[str, Any]) -> Dict[str, Any]:
    """One job as a flat record: what Slurm reported plus what is derived from it."""
    m = job["m"]
    out: Dict[str, Any] = {name: job.get(name) for name in EXPORT_STORED}
    for name in EXPORT_DERIVED:
        if name in ("kind", "family"):
            out[name] = job.get(name)
        elif name == "flags":
            out[name] = " ".join(m["flags"])
        else:
            value = m.get(name)
            out[name] = round(value, 4) if isinstance(value, float) else value
    return out


def export_rows(jobs: Sequence[Mapping[str, Any]]) -> List[List[Any]]:
    rows = []
    for job in jobs:
        record = export_record(job)
        rows.append(["" if record[name] is None else record[name] for name in EXPORT_COLUMNS])
    return rows


def render_date_range(jobs: Sequence[Mapping[str, Any]]) -> str:
    ends = [j["end_ts"] for j in jobs if j.get("end_ts")]
    if not ends:
        return "no jobs"
    return "%s to %s" % (fmt_date(min(ends)), fmt_date(max(ends)))
