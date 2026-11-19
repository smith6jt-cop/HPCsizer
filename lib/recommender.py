"""What to request next, from what comparable jobs used.

Sources, in order of preference:

``history``     finished runs of the same job (levels in ``lib.history``)
``model``       peak memory fitted against input size for a tool the script
                uses, over runs that recorded their inputs (``hpg submit``)
``cold-start``  a per-tool rule of thumb; shown, never applied automatically
``none``        no figure is given: an invented one is worse than "unknown"

The history rules lean one way on purpose. A request that is too small kills
the job and wastes what it had computed; one that is too large costs queue
priority.

memory   largest peak of the evidence runs that ended normally, times a
         headroom; never below the largest peak of any run; never above the
         largest request that completed -- unless a run ran out of memory, in
         which case the figure is raised above that run's request
CPUs     only ever lowered: the highest number of cores a run kept busy, times
         a headroom, and not at all when that run used every core it had
time     a high percentile of the completed runs times a headroom, at least
         the longest of them; raised above the limit of any run that timed
         out; not shortened in the same suggestion that lowers the CPU count
GPUs     as requested, unless no run used the GPU it held

Interactive sessions end when their user stops or the limit cuts them off, so
for them a timeout is a normal ending, the time limit is never raised, and the
CPU count is only judged from an in-job profile (the average over hours of
waiting for input says little).
"""

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from lib import classify, metrics, sbatch_opts
from lib.analyzer import Request, Submission
from lib.config import Config
from lib.fmt import fmt_date, fmt_gb, fmt_mem_mb, fmt_minutes, fmt_num, fmt_pct, table
from lib.history import DAY, POOLED_LEVELS, History, describe_level, level_keys

SOURCE_HISTORY = "history"
SOURCE_MODEL = "model"
SOURCE_COLD = "cold-start"
SOURCE_NONE = "none"

MIN_MEM_MB = 512  # below this the job's own shell and Slurm's step daemons do not fit
MIN_CHANGE = 0.10  # a request within 10% of the suggestion is left alone
LONGEST_RUN_MARGIN = 1.1
SATURATED = 0.9  # a run that kept 90% of its cores busy would have used more

# ---------------------------------------------------------------------------
# Cold-start heuristics
# ---------------------------------------------------------------------------

# (mem_multiplier, baseline_gb, cpu_hint), keyed by the bare names detect_tools() emits.
_COLD_START: Dict[str, tuple] = {
    "readRDS": (3.0, 0.0, 1),
    "SCTransform": (4.5, 0.0, 4),  # 3.0 x file + 50%
    "FindMarkers": (2.5, 0.0, 4),
    "Seurat": (3.0, 0.0, 4),
    "scanpy": (2.0, 0.0, 4),
    "anndata": (2.0, 0.0, 4),
    "scVI": (2.0, 0.0, 4),
    "QuPath": (0.0, 2.0, None),  # 2 GB per core
    "cellranger": (0.0, 32.0, 8),
    "scimap": (2.0, 16.0, 4),
    "DESeq2": (2.0, 4.0, 4),
    "edgeR": (1.5, 2.0, 4),
    "limma": (1.5, 2.0, 4),
    "STAR": (0.0, 32.0, 8),
    "salmon": (0.0, 16.0, 8),
    "kallisto": (0.0, 12.0, 4),
}

_GENERIC_MULTIPLIER = 2.0
_GENERIC_BASELINE_GB = 16.0


def _cold_start_mem(
    tools: List[str],
    total_input_gb: float,
    req_cpus: int = 1,
) -> Optional[float]:
    """Return a cold-start memory estimate in GB, or None if none is possible.

    Takes the most demanding matching tool rather than the first one. detect_tools()
    returns a sorted list, so picking the first match meant the estimate was decided
    by alphabetical order -- FindMarkers (2.5x) beating SCTransform (4.5x) in the
    same script. Under-requesting kills the job; over-requesting only costs queue
    priority, so the maximum is the safe direction.

    Returns None when the only matching entries scale purely with input size and no
    input was detected: there is genuinely nothing to estimate from, and a confident
    small number (the old 1.0 GB floor) is actively dangerous.
    """
    estimates = []
    for tool in tools:
        if tool not in _COLD_START:
            continue
        mult, baseline, _ = _COLD_START[tool]
        if tool == "QuPath":
            estimates.append(max(baseline * req_cpus, 4.0))
        elif mult > 0 and total_input_gb <= 0 and baseline <= 0:
            continue  # multiplier-driven entry with nothing to multiply
        else:
            estimates.append(max(mult * total_input_gb + baseline, baseline))
    if estimates:
        return max(estimates)
    if total_input_gb > 0:
        return max(_GENERIC_MULTIPLIER * total_input_gb + _GENERIC_BASELINE_GB, 16.0)
    # No input size and nothing baseline-driven matched. An unrecognised tool is
    # strictly less informative than a recognised one, so it cannot be the case
    # that recognised tools decline to answer here while unknown ones return a
    # generic 16 GB.
    return None


def _cold_start_cpus(tools: List[str], req_cpus: int = 1) -> int:
    """Return cold-start CPU recommendation (most demanding matching tool)."""
    hints = [_COLD_START[t][2] for t in tools if t in _COLD_START and _COLD_START[t][2] is not None]
    if hints:
        return max(hints)
    return req_cpus


# ---------------------------------------------------------------------------
# Rounding: always up, never to zero
# ---------------------------------------------------------------------------


def round_mem_mb(mb: float, per_unit: bool = False) -> int:
    """Round a memory figure up to a value people type.

    Whole GB from 2 GB, 256 MB steps below. A job total is never below
    ``MIN_MEM_MB``; a per-CPU or per-GPU share (``per_unit``) may be, in 64 MB
    steps.
    """
    mb = float(mb)
    if per_unit:
        mb = max(mb, 64.0)
        step = 64 if mb < 512 else (256 if mb < 2048 else 1024)
    else:
        mb = max(mb, float(MIN_MEM_MB))
        step = 256 if mb < 2048 else 1024
    return int(math.ceil(mb / step - 1e-9)) * step


def round_time_min(minutes: float, minimum: int = 1) -> int:
    """Round a time limit up: 5-minute steps to an hour, 15 to six hours, then whole hours."""
    minutes = max(float(minutes), float(minimum), 1.0)
    step = 5 if minutes <= 60 else (15 if minutes <= 360 else 60)
    return int(math.ceil(minutes / step - 1e-9)) * step


def round_cpus(cores: float) -> int:
    return max(int(math.ceil(cores - 1e-9)), 1)


# ---------------------------------------------------------------------------
# The job being sized
# ---------------------------------------------------------------------------


@dataclass
class Target:
    """The job a recommendation is for.

    ``cls`` holds the same keys ``lib.classify`` gives stored jobs (plus the
    user), so a target is matched against history by exactly the rules history
    was grouped with. ``explicit`` is true for a real sbatch invocation: an
    option it leaves out then takes sbatch's default instead of "whatever the
    last run asked for".
    """

    cls: Dict[str, Any]
    request: Request
    label: str
    explicit: bool = False
    tools: List[str] = field(default_factory=list)
    input_gb: Optional[float] = None
    notes: List[str] = field(default_factory=list)


def _classified(
    user: str, job_name: str, submit_line: Optional[str], gpus: int, cfg: Config
) -> Dict[str, Any]:
    pseudo = {
        "user": user,
        "job_name": job_name,
        "submit_line": submit_line,
        "req_gpus": gpus,
        "alloc_gpus": gpus,
    }
    out = classify.classify(pseudo, cfg)
    out["user"] = user
    return out


def target_from_submission(sub: Submission, user: str, cfg: Config) -> Target:
    """Target for ``sbatch <args>``, classified as Slurm will record the job."""
    cls = _classified(user, sub.job_name, sub.submit_line, sub.request.gpus, cfg)
    label = sub.job_name
    if cls["name_key"].startswith("cmd:"):
        label = "%s (%s)" % (cls["name_key"][4:], sub.job_name)
    return Target(
        cls=cls,
        request=sub.request,
        label=label,
        explicit=True,
        tools=list(sub.tools),
        input_gb=sub.input_gb if sub.input_files else None,
        notes=list(sub.notes),
    )


def target_from_name(
    name: str, user: str, cfg: Config, request: Optional[Request] = None
) -> Target:
    """Target for "jobs called NAME" when there is no script to read."""
    request = request or Request()
    return Target(cls=_classified(user, name, None, request.gpus, cfg), request=request, label=name)


def target_from_kind(
    kind: str, user: str, cfg: Config, request: Optional[Request] = None
) -> Target:
    """Target for an interactive session kind: ``ood:rstudio``, ``vscode``, ``interactive``."""
    request = request or Request()
    kind = kind.strip()
    if not classify.is_session_kind(kind.lower() if not kind.startswith("ood:") else kind, cfg):
        raise ValueError(
            "%r is not a session kind (ood:<app>, interactive, %s); "
            "use --name or a script for batch jobs" % (kind, ", ".join(cfg.session_names))
        )
    if not kind.startswith("ood:"):
        kind = kind.lower()
    gpu = request.gpus > 0
    family = "%s/%s%s" % (user, kind, "+gpu" if gpu else "")
    cls = {
        "user": user,
        "kind": kind,
        "session": True,
        "name_key": "-",
        "name_exact": "",
        "signature": None,
        "gpu": gpu,
        "array": False,
        "family": family,
    }
    return Target(cls=cls, request=request, label="%s session" % kind)


def request_of_job(job: Mapping[str, Any]) -> Request:
    """The request of a stored job, in the shape of a parsed sbatch request."""
    req = Request(job_name=job.get("job_name"))
    cpus = job.get("req_cpus") or job.get("alloc_cpus")
    tasks = job.get("n_tasks") or 1
    nodes = job.get("alloc_nodes") or job.get("req_nodes") or 1
    if cpus:
        if tasks > 1 and cpus % tasks == 0:
            req.ntasks, req.cpus_per_task = int(tasks), int(cpus // tasks)
        else:
            req.cpus_per_task = int(cpus)
    if nodes > 1:
        req.nodes = int(nodes)
    if job.get("req_mem_mb"):
        req.mem_form = "mem"
        req.mem_value_mb = float(job["req_mem_mb"]) / max(nodes, 1)
    req.time_min = job.get("req_time_min")
    req.gpus = int(job.get("req_gpus") or job.get("alloc_gpus") or 0)
    req.gpu_type = job.get("gpu_type")
    return req


def target_from_job(
    job: Mapping[str, Any], cfg: Config, request: Optional[Request] = None
) -> Target:
    """Target for "a job like this stored one": its family and, by default, its request."""
    cls = classify.classify(job, cfg)
    cls["user"] = job.get("user") or "?"
    base = request_of_job(job)
    if request is not None:
        base = overlay_request(base, request)
    label = "%s (like job %s)" % (job.get("job_name") or cls["family"], job.get("job_id"))
    return Target(cls=cls, request=base, label=label)


def overlay_request(base: Request, over: Request) -> Request:
    """``base`` with every value that ``over`` states explicitly."""
    out = Request(**{k: v for k, v in base.__dict__.items() if k != "problems"})
    out.problems = list(base.problems) + list(over.problems)
    if over.cpus_per_task is not None or over.ntasks is not None:
        out.cpus_per_task, out.ntasks = over.cpus_per_task, over.ntasks
    if over.nodes is not None:
        out.nodes = over.nodes
    if over.mem_form is not None:
        out.mem_form, out.mem_value_mb = over.mem_form, over.mem_value_mb
    if over.time_min is not None or over.time_unlimited:
        out.time_min, out.time_unlimited = over.time_min, over.time_unlimited
    if over.gpus:
        out.gpus, out.gpu_type = over.gpus, over.gpu_type
    return out


# ---------------------------------------------------------------------------
# Evidence
# ---------------------------------------------------------------------------


def select_evidence(
    history: History,
    cls: Mapping[str, Any],
    cfg: Config,
    now: int,
    before_ts: Optional[int] = None,
) -> Tuple[Optional[str], List[Dict[str, Any]], int, List[Tuple[str, int, int]]]:
    """``(level, runs, available, tried)`` for the narrowest level with enough runs.

    ``runs`` are the most recent ``cfg.last_n`` informative runs that ended
    within the lookback window and before ``before_ts`` (default: now).
    ``tried`` lists ``(level, found, needed)`` for every level looked at.
    """
    end = int(before_ts) if before_ts is not None else int(now) + 1
    since = end - int(cfg.lookback_days * DAY)
    tried: List[Tuple[str, int, int]] = []
    for level, key in level_keys(cls):
        runs, count = history.runs(
            level, key, since, end, informative_only=True, last=int(cfg.last_n)
        )
        need = cfg.min_runs_pooled if level in POOLED_LEVELS else cfg.min_runs
        tried.append((level, count, need))
        if count >= need:
            return level, runs, count, tried
    return None, [], 0, tried


def _normal_end(run: Mapping[str, Any], session: bool) -> bool:
    state = run.get("state")
    return state == "COMPLETED" or (session and state in ("TIMEOUT", "CANCELLED"))


def _jid(run: Mapping[str, Any]) -> str:
    return str(run.get("job_id") or run.get("job_id_raw") or "?")


def _blank(requested: Optional[float], requested_from: Optional[str]) -> Dict[str, Any]:
    return {
        "requested": requested,
        "requested_from": requested_from if requested is not None else None,
        "used": None,
        "n": 0,
        "suggested": None,
        "action": None,
        "change": None,
        "basis": None,
        "kept_because": None,
        "notes": [],
    }


def _settle(
    res: Dict[str, Any], raw: float, rounded: float, lower_threshold: float = MIN_CHANGE
) -> None:
    """Fill ``suggested``, ``action`` and ``change``.

    ``raw`` is the figure the rule produced, ``rounded`` the value a person
    would type for it. Rounding up must not turn "the request is enough" into
    a raise, and a request within ``lower_threshold`` of the suggestion is not
    worth changing.
    """
    requested = res["requested"]
    if requested is None or requested <= 0:
        res.update(suggested=rounded, action="set")
        return
    if raw > requested:
        res.update(
            suggested=rounded, action="raise", change=(rounded - requested) / float(requested)
        )
    elif rounded >= requested:
        res.update(suggested=requested, action="keep", change=0.0)
    elif (requested - rounded) / float(requested) < lower_threshold:
        res.update(suggested=requested, action="keep", change=0.0)
        res["notes"].append(
            "the request is within %d%% of what history suggests" % round(lower_threshold * 100)
        )
    else:
        res.update(
            suggested=rounded, action="lower", change=(rounded - requested) / float(requested)
        )


# ---------------------------------------------------------------------------
# History rules, one resource at a time
# ---------------------------------------------------------------------------


def _memory(res: Dict[str, Any], runs: Sequence[Dict], cfg: Config, session: bool) -> None:
    measured = [r for r in runs if r["m"]["mem_used_gb"] is not None]
    normal = [r for r in measured if _normal_end(r, session)]
    ooms = [r for r in runs if "oom" in r["m"]["flags"]]
    res["used"] = metrics.describe([r["m"]["mem_used_gb"] for r in measured])
    res["n"] = len(measured)
    res["profiled"] = sum(1 for r in measured if r["m"]["mem_source"] == "profile")

    gb: Optional[float] = None
    if len(normal) >= cfg.min_runs:
        top = max(normal, key=lambda r: r["m"]["mem_used_gb"])
        gb = top["m"]["mem_used_gb"] * cfg.mem_headroom
        res["basis"] = "largest peak %s (job %s) x %s" % (
            fmt_gb(top["m"]["mem_used_gb"]),
            _jid(top),
            fmt_num(cfg.mem_headroom, 2),
        )
        if not ooms:
            sufficed = [
                r["m"]["mem_req_gb"]
                for r in runs
                if _normal_end(r, session) and r["m"]["mem_req_gb"]
            ]
            if sufficed and gb > max(sufficed):
                gb = max(sufficed)
                res["basis"] += ", limited to the largest request that completed (%s)" % fmt_gb(gb)

    if ooms:
        worst = max(ooms, key=lambda r: r["m"]["mem_req_gb"] or 0)
        for run in ooms:
            res["notes"].append(
                "job %s ran out of memory on %s with %s requested"
                % (_jid(run), fmt_date(run.get("end_ts")), fmt_gb(run["m"]["mem_req_gb"]))
            )
        if worst["m"]["mem_req_gb"]:
            raised = worst["m"]["mem_req_gb"] * cfg.oom_raise
            if gb is None or raised > gb:
                gb = raised
                res["basis"] = "%s x the request of job %s, which ran out of memory" % (
                    fmt_num(cfg.oom_raise, 2),
                    _jid(worst),
                )

    if gb is None:
        if not measured:
            res["notes"].append(
                "memory was not measured: every run was shorter than two accounting samples"
            )
        else:
            res["notes"].append(
                "memory was measured in %d run(s) that ended normally; %d are needed"
                % (len(normal), cfg.min_runs)
            )
        return

    if measured:
        floor_run = max(measured, key=lambda r: r["m"]["mem_used_gb"])
        if gb < floor_run["m"]["mem_used_gb"]:
            gb = floor_run["m"]["mem_used_gb"]
            res["basis"] = "largest peak of any run: %s (job %s, %s)" % (
                fmt_gb(gb),
                _jid(floor_run),
                floor_run.get("state"),
            )
    _settle(res, gb * 1024.0, round_mem_mb(gb * 1024.0))


def _cpus(
    res: Dict[str, Any], runs: Sequence[Dict], cfg: Config, session: bool, multi: bool
) -> None:
    averages = [r["m"]["cores_used"] for r in runs if r["m"]["cores_used"] is not None]
    res["used"] = metrics.describe(averages)
    res["n"] = len(averages)
    peaks = [r["m"]["cores_peak"] for r in runs if r["m"].get("cores_peak") is not None]
    res["peak"] = max(peaks) if peaks else None

    basis: List[Tuple[float, Dict[str, Any], str]] = []
    for run in runs:
        m = run["m"]
        if m.get("cores_p95") is not None:
            # The sustained count leaves out what was used before the first
            # sample; the average does not. Whichever is larger.
            basis.append(
                (max(m["cores_p95"], m["cores_used"] or 0.0), run, "cores kept busy (profile)")
            )
        elif not session and m["cores_used"] is not None:
            basis.append((m["cores_used"], run, "average cores in use"))
    res["profiled"] = sum(1 for _v, r, _w in basis if r["m"].get("cores_p95") is not None)

    requested = res["requested"]
    if multi:
        res.update(suggested=requested, action="keep" if requested else None, change=0.0)
        res["kept_because"] = "multi"
        res["notes"].append("more than one task or node: the CPU count is not adjusted")
        return
    if len(basis) < cfg.min_runs:
        res["kept_because"] = "unprofiled-session" if session else "unmeasured"
        if session:
            res["notes"].append(
                "a session waits for input most of the time, so its average CPU use says little; "
                "%d of these %d sessions have a profile and %d are needed to judge the CPU count "
                "(submit sessions with 'hpg submit')" % (len(basis), len(runs), cfg.min_runs)
            )
        else:
            res["notes"].append(
                "CPU time was measured in %d run(s); %d are needed" % (len(basis), cfg.min_runs)
            )
        if requested:
            res.update(suggested=requested, action="keep", change=0.0)
        return

    value, top, what = max(basis, key=lambda item: item[0])
    alloc = top["m"]["cores_alloc"]
    if alloc and value >= SATURATED * alloc:
        res["kept_because"] = "saturated"
        res["notes"].append(
            "job %s kept all %d of its cores busy, so fewer are not suggested" % (_jid(top), alloc)
        )
        if requested:
            res.update(suggested=requested, action="keep", change=0.0)
        return
    need = round_cpus(value * cfg.cpu_headroom)
    res["basis"] = "highest %s: %s (job %s) x %s" % (
        what,
        fmt_num(value),
        _jid(top),
        fmt_num(cfg.cpu_headroom, 2),
    )
    if requested is None:
        res.update(suggested=need, action="set")
    elif need < requested:
        res.update(suggested=need, action="lower", change=(need - requested) / float(requested))
    else:
        res.update(suggested=requested, action="keep", change=0.0)
        res["basis"] = None


def _time(
    res: Dict[str, Any],
    runs: Sequence[Dict],
    cfg: Config,
    session: bool,
    cpus_lowered: bool,
    unlimited: bool,
) -> None:
    done = [r for r in runs if _normal_end(r, session) and (r.get("elapsed_sec") or 0) > 0]
    elapsed = [r["elapsed_sec"] / 60.0 for r in done]
    res["used"] = metrics.describe(elapsed)
    res["n"] = len(done)
    timeouts = (
        []
        if session
        else [r for r in runs if r.get("state") == "TIMEOUT" and r.get("req_time_min")]
    )

    minutes: Optional[float] = None
    if len(done) >= cfg.min_runs:
        high = metrics.quantile(elapsed, cfg.time_percentile)
        longest = max(elapsed)
        if high * cfg.time_headroom >= longest * LONGEST_RUN_MARGIN:
            minutes = high * cfg.time_headroom
            res["basis"] = "%s of %d completed runs (%s) x %s" % (
                "longest" if high == longest else "p%d" % round(cfg.time_percentile * 100),
                len(done),
                fmt_minutes(high),
                fmt_num(cfg.time_headroom, 2),
            )
        else:
            minutes = longest * LONGEST_RUN_MARGIN
            res["basis"] = "longest of %d completed runs (%s) x %s" % (
                len(done),
                fmt_minutes(longest),
                fmt_num(LONGEST_RUN_MARGIN, 2),
            )
        if not timeouts:
            limits = [r["req_time_min"] for r in done if r.get("req_time_min")]
            if limits and minutes > max(limits):
                minutes = max(float(max(limits)), longest)
                res["basis"] += ", limited to the longest limit that sufficed"

    if timeouts:
        worst = max(timeouts, key=lambda r: r["req_time_min"])
        for run in timeouts:
            res["notes"].append(
                "job %s hit its %s limit on %s"
                % (_jid(run), fmt_minutes(run["req_time_min"]), fmt_date(run.get("end_ts")))
            )
        raised = worst["req_time_min"] * cfg.timeout_raise
        if minutes is None or raised > minutes:
            minutes = raised
            res["basis"] = "%s x the limit of job %s, which timed out" % (
                fmt_num(cfg.timeout_raise, 2),
                _jid(worst),
            )

    if minutes is None:
        res["notes"].append(
            "%d run(s) completed; %d are needed for a time limit" % (len(done), cfg.min_runs)
        )
        return

    minutes = max(minutes, float(cfg.min_time_min))
    suggested = round_time_min(minutes, cfg.min_time_min)
    requested = res["requested"]
    if requested is not None and suggested < requested and cpus_lowered:
        res.update(suggested=requested, action="keep", change=0.0)
        res["notes"].append(
            "not shortened together with the CPU count: fewer cores can lengthen the run "
            "(history alone suggests %s)" % fmt_minutes(suggested)
        )
        return
    if session and requested is not None and minutes > requested:
        res.update(suggested=requested, action="keep", change=0.0)
        res["basis"] = None
        return
    _settle(res, minutes, suggested)
    if unlimited and res["action"] == "set":
        res["notes"].append("a finite limit lets the scheduler fit the job into gaps")


def _gpus(res: Dict[str, Any], runs: Sequence[Dict], cfg: Config) -> None:
    held = [
        r
        for r in runs
        if r["m"]["gpus"]
        and r["m"]["gpu_util"] is not None
        and (r.get("elapsed_sec") or 0) >= metrics.CPU_MIN_ELAPSED_SEC
    ]
    res["used"] = metrics.describe([r["m"]["gpu_util"] for r in held])
    res["n"] = len(held)
    mem = [r["m"]["gpu_mem_gb"] for r in held if r["m"]["gpu_mem_gb"] is not None]
    res["gpu_mem_gb"] = max(mem) if mem else None
    requested = res["requested"]
    if not requested:
        return
    if (
        len(held) >= cfg.min_runs
        and max(r["m"]["gpu_util"] for r in held) < metrics.GPU_IDLE_PERCENT
    ):
        res.update(suggested=0, action="lower", change=-1.0)
        res["basis"] = "no run used its GPU (highest utilisation %s%% in %d runs)" % (
            fmt_num(max(r["m"]["gpu_util"] for r in held)),
            len(held),
        )
    else:
        res.update(suggested=requested, action="keep", change=0.0)
        if len(held) < cfg.min_runs:
            res["notes"].append("GPU use was recorded in %d run(s)" % len(held))


# ---------------------------------------------------------------------------
# Requested values and their origin
# ---------------------------------------------------------------------------


def _baseline(
    target: Target, runs: Sequence[Dict]
) -> Dict[str, Tuple[Optional[float], Optional[str]]]:
    """What each resource is compared with: the request, sbatch's default, or the last run."""
    req = target.request
    last = runs[-1] if runs else None
    last_req = request_of_job(last) if last else Request()
    out: Dict[str, Tuple[Optional[float], Optional[str]]] = {}

    if req.mem_mb is not None:
        out["mem"] = (req.mem_mb, "request")
    elif req.mem_all:
        out["mem"] = (None, None)
    else:
        out["mem"] = (last_req.mem_mb, "last run")

    if req.cpus_per_task is not None or req.ntasks is not None:
        out["cpus"] = (req.cpus, "request")
    elif target.explicit:
        out["cpus"] = (1, "default")
    else:
        out["cpus"] = (last_req.cpus if last and last_req.cpus_per_task else None, "last run")

    if req.time_min is not None:
        out["time"] = (req.time_min, "request")
    elif req.time_unlimited:
        out["time"] = (None, None)
    else:
        out["time"] = (last_req.time_min, "last run")

    if req.gpus or target.explicit:
        out["gpus"] = (req.gpus, "request")
    else:
        out["gpus"] = (last_req.gpus if last else (1 if target.cls.get("gpu") else 0), "last run")
    return out


# ---------------------------------------------------------------------------
# Fitted model: peak memory against input size
# ---------------------------------------------------------------------------


def fit_line(points: Sequence[Tuple[float, float]]) -> Optional[Dict[str, float]]:
    """Least-squares line through ``(input_gb, peak_gb)`` points, with its fit quality."""
    n = len(points)
    if n < 2:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    sx, sy = sum(xs), sum(ys)
    sxy = sum(x * y for x, y in points)
    sxx = sum(x * x for x in xs)
    denom = n * sxx - sx * sx
    if denom == 0:
        return None  # every run had the same input size: nothing to fit against
    slope = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    mean = sy / n
    residuals = [y - (slope * x + intercept) for x, y in points]
    ss_res = sum(r * r for r in residuals)
    ss_tot = sum((y - mean) ** 2 for y in ys)
    return {
        "slope": slope,
        "intercept": intercept,
        "r2": 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0,
        "n": n,
        "x_min": min(xs),
        "x_max": max(xs),
        "max_residual": max(residuals),
    }


def best_model(
    history: History,
    tools: Sequence[str],
    cfg: Config,
    since_ts: Optional[int] = None,
    before_ts: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """The best-fitting memory model among the tools of a script, or None.

    Runs that ran out of memory are left out: their peak is the limit they
    died at, a lower bound that would pull the line down.
    """
    best: Optional[Dict[str, Any]] = None
    for tool in tools:
        points = []
        for run in history.with_tool(tool, since_ts, before_ts):
            peak = run["m"]["mem_used_gb"]
            size = run.get("ctx_input_gb")
            if peak is None or not size or size <= 0 or "oom" in run["m"]["flags"]:
                continue
            points.append((float(size), float(peak)))
        if len(points) < cfg.model_min_points:
            continue
        fit = fit_line(points)
        if fit is None or fit["slope"] < 0 or fit["r2"] < cfg.model_min_r2:
            continue
        if best is None or fit["r2"] > best["r2"]:
            best = dict(fit, tool=tool)
    return best


# ---------------------------------------------------------------------------
# The recommendation
# ---------------------------------------------------------------------------


def _confidence(level: str, n: int, mem_used: Optional[Dict[str, float]]) -> Dict[str, str]:
    steps = ["low", "medium", "high"]
    if level in POOLED_LEVELS:
        rank, why = 0, "%d sessions of other users as well" % n
    elif level == "signature":
        rank, why = (1 if n >= 10 else 0), "%d runs of the same program under other names" % n
    else:
        rank, why = (2 if n >= 10 else 1), "%d runs of this job by this user" % n
    if mem_used and mem_used["p50"] > 0 and mem_used["max"] / mem_used["p50"] > 3 and rank > 0:
        rank -= 1
        why += "; peak memory varies %.0f-fold between them" % (mem_used["max"] / mem_used["p50"])
    return {"level": steps[rank], "reason": why}


def _target_dict(target: Target) -> Dict[str, Any]:
    cls = target.cls
    return {
        "label": target.label,
        "user": cls.get("user"),
        "kind": cls.get("kind"),
        "family": cls.get("family"),
        "session": bool(cls.get("session")),
        "gpu": bool(cls.get("gpu")),
        "name": cls.get("name_exact") or None,
        "signature": cls.get("signature"),
        "tools": list(target.tools),
        "input_gb": target.input_gb,
        "request": target.request.to_dict(),
    }


def _options(target: Target, rec: Dict[str, Any]) -> Tuple[List[str], Dict[str, str], List[str]]:
    """``(options, changes, blockers)`` for a recommendation.

    ``options`` is the whole suggested request as sbatch options. ``changes``
    holds the ones that differ from what was asked for, keyed ``cpus``, ``mem``
    and ``time`` -- what ``hpg submit --apply`` passes on. ``blockers`` are the
    reasons nothing may be applied automatically.
    """
    req = target.request
    options: List[str] = []
    changes: Dict[str, str] = {}
    blockers: List[str] = []
    mem, cpus, time_ = rec["mem"], rec["cpus"], rec["time"]

    if rec["source"] != SOURCE_HISTORY:
        blockers.append("the figures do not come from this job's own history")
    if req.het:
        blockers.append("heterogeneous job")
    elif req.multi:
        blockers.append("more than one task or node")
    if req.mem_all:
        blockers.append("--mem=0 asks for the whole node")

    cpus_after = None if req.multi else cpus["suggested"]
    if cpus_after is not None:
        option = "--cpus-per-task=%d" % cpus_after
        options.append(option)
        if cpus["action"] in ("lower", "set"):
            changes["cpus"] = option

    # Memory is stated as a job total. When the CPU count drops, a per-CPU
    # request -- written by the user or applied by the cluster as a default --
    # would shrink with it, so the total is then always spelled out.
    total = mem["suggested"]
    pin = cpus["action"] == "lower" and req.mem_form != "mem"
    if total is None and pin:
        total = mem["requested"]
        if total is None:
            blockers.append(
                "the memory request is unknown, and with fewer CPUs a per-CPU default would shrink"
            )
    if total is not None and not req.multi and not req.mem_all:
        if req.mem_form == "mem-per-cpu" and cpus_after:
            share = round_mem_mb(float(total) / cpus_after, per_unit=True)
            option = "--mem-per-cpu=%s" % sbatch_opts.format_mem(share)
        elif req.mem_form == "mem-per-gpu" and req.gpus:
            share = round_mem_mb(float(total) / req.gpus, per_unit=True)
            option = "--mem-per-gpu=%s" % sbatch_opts.format_mem(share)
        else:
            option = "--mem=%s" % sbatch_opts.format_mem(total)
        options.append(option)
        if mem["action"] in ("lower", "raise", "set") or pin:
            changes["mem"] = option

    if time_["suggested"] is not None:
        option = "--time=%s" % sbatch_opts.format_time(time_["suggested"])
        options.append(option)
        if time_["action"] in ("lower", "raise", "set"):
            changes["time"] = option
    order = ("--mem", "--cpus-per-task", "--time")
    options.sort(key=lambda opt: [opt.startswith(prefix) for prefix in order].index(True))
    return options, changes, blockers


def recommend(
    target: Target,
    history: History,
    cfg: Config,
    now: int,
    before_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """Recommendation for ``target`` from runs that ended before ``before_ts`` (default: now)."""
    cls = target.cls
    session = bool(cls.get("session"))
    level, runs, available, tried = select_evidence(history, cls, cfg, now, before_ts)
    base = _baseline(target, runs)
    rec: Dict[str, Any] = {
        "source": SOURCE_NONE,
        "target": _target_dict(target),
        "level": level,
        "evidence": None,
        "notes": list(target.notes),
        "confidence": {"level": "none", "reason": "no comparable runs"},
    }
    for name in ("mem", "cpus", "time", "gpus"):
        rec[name] = _blank(*base[name])

    if level is not None:
        rec["source"] = SOURCE_HISTORY
        states: Dict[str, int] = {}
        for run in runs:
            states[run["state"]] = states.get(run["state"], 0) + 1
        rec["evidence"] = {
            "level": level,
            "description": describe_level(level, cls),
            "n": len(runs),
            "available": available,
            "lookback_days": cfg.lookback_days,
            "first_end_ts": runs[0].get("end_ts"),
            "last_end_ts": runs[-1].get("end_ts"),
            "states": states,
            "profiled": sum(1 for r in runs if r.get("mon_n_samples")),
            "last_jobs": [_jid(r) for r in runs[-5:]],
        }
        _memory(rec["mem"], runs, cfg, session)
        _cpus(rec["cpus"], runs, cfg, session, target.request.multi)
        _time(
            rec["time"],
            runs,
            cfg,
            session,
            cpus_lowered=rec["cpus"]["action"] == "lower",
            unlimited=target.request.time_unlimited,
        )
        _gpus(rec["gpus"], runs, cfg)
        rec["confidence"] = _confidence(level, len(runs), rec["mem"]["used"])
        sizes = [r.get("ctx_input_gb") for r in runs if r.get("ctx_input_gb")]
        if target.input_gb and sizes and target.input_gb > 1.2 * max(sizes):
            rec["notes"].append(
                "the input (%s) is larger than in any of these runs (largest %s): "
                "memory may have to grow with it" % (fmt_gb(target.input_gb), fmt_gb(max(sizes)))
            )
    else:
        end = int(before_ts) if before_ts is not None else int(now) + 1
        since = end - int(cfg.lookback_days * DAY)
        model = None
        if target.tools and target.input_gb:
            model = best_model(history, target.tools, cfg, since, end)
        if model is not None:
            predicted = model["slope"] * target.input_gb + model["intercept"]
            upper = predicted + max(model["max_residual"], 0.0)
            if upper > 0:
                rec["source"] = SOURCE_MODEL
                rec["model"] = model
                mem = rec["mem"]
                mem["n"] = int(model["n"])
                mem["basis"] = (
                    "%s runs using %s: %s GB per GB of input + %s GB (R2 %.2f), "
                    "plus the largest miss above that line (%s), x %s"
                    % (
                        model["n"],
                        model["tool"],
                        fmt_num(model["slope"], 2),
                        fmt_num(model["intercept"]),
                        model["r2"],
                        fmt_gb(max(model["max_residual"], 0.0)),
                        fmt_num(cfg.mem_headroom, 2),
                    )
                )
                figure = upper * cfg.mem_headroom * 1024.0
                _settle(mem, figure, round_mem_mb(figure), lower_threshold=0.0)
                rec["confidence"] = {
                    "level": "low",
                    "reason": "no history of this job; fitted from other jobs using %s"
                    % model["tool"],
                }
                if not model["x_min"] / 2.0 <= target.input_gb <= model["x_max"] * 2.0:
                    rec["notes"].append(
                        "the input (%s) is outside the range the fit was made on (%s to %s)"
                        % (fmt_gb(target.input_gb), fmt_gb(model["x_min"]), fmt_gb(model["x_max"]))
                    )
        if rec["source"] == SOURCE_NONE:
            cpus_req = int(rec["cpus"]["requested"] or 1)
            cold = _cold_start_mem(target.tools, target.input_gb or 0.0, cpus_req)
            if cold is not None:
                rec["source"] = SOURCE_COLD
                mem = rec["mem"]
                known = [t for t in target.tools if t in _COLD_START]
                mem["basis"] = "rule of thumb for %s with %s of input" % (
                    ", ".join(known) if known else "an unrecognised program",
                    fmt_gb(target.input_gb or 0.0),
                )
                _settle(mem, cold * 1024.0, round_mem_mb(cold * 1024.0), lower_threshold=0.0)
                hint = _cold_start_cpus(target.tools, cpus_req)
                if hint != cpus_req:
                    rec["cpus"]["notes"].append("these tools are usually run with %d CPUs" % hint)
                rec["confidence"] = {
                    "level": "guess",
                    "reason": "no history of this job; a rule of thumb, not a measurement",
                }
        if rec["source"] != SOURCE_HISTORY:
            found = ", ".join("%s: %d of %d" % item for item in tried) or "none"
            rec["notes"].append(
                "no usable history for %s in the last %d days (runs found per level -- %s)"
                % (cls.get("family"), cfg.lookback_days, found)
            )
            rec["notes"].append(
                "after its first run, or for a similar stored job now: hpg recommend --like JOBID"
            )

    rec["tried"] = [{"level": lv, "found": found, "needed": need} for lv, found, need in tried]
    options, changes, blockers = _options(target, rec)
    rec["options"] = options
    rec["changes"] = changes
    rec["apply_blockers"] = blockers
    rec["can_apply"] = not blockers and bool(changes)
    return rec


# ---------------------------------------------------------------------------
# Text
# ---------------------------------------------------------------------------


def _triple(used: Optional[Dict[str, float]], fmt) -> str:
    if not used:
        return "-"
    return "%s / %s / %s" % (fmt(used["p50"]), fmt(used["p95"]), fmt(used["max"]))


def _mem_triple(used: Optional[Dict[str, float]]) -> str:
    if not used:
        return "-"
    if used["max"] < 1:
        return _triple(used, lambda gb: "%.0f" % (gb * 1024)) + " MB"
    return _triple(used, fmt_num) + " GB"


def _requested_text(res: Dict[str, Any], fmt) -> str:
    if res["requested"] is None:
        return "-"
    origin = res.get("requested_from")
    suffix = "" if origin in (None, "request") else " (%s)" % origin
    return fmt(res["requested"]) + suffix


def _change_text(res: Dict[str, Any]) -> str:
    action = res.get("action")
    if action in ("lower", "raise"):
        return fmt_pct(res["change"], signed=True)
    if action == "keep":
        return "keep"
    return ""


def render_text(rec: Dict[str, Any]) -> str:
    """The pre-job report: requested, used by past runs, suggested."""
    target = rec["target"]
    lines = [
        "Job         %s  [%s, user %s]" % (target["label"], target["kind"], target["user"]),
        "Family      %s" % target["family"],
    ]
    ev = rec["evidence"]
    if ev:
        states = ", ".join(
            "%d %s" % (n, s) for s, n in sorted(ev["states"].items(), key=lambda kv: -kv[1])
        )
        more = "" if ev["available"] == ev["n"] else " (the latest of %d)" % ev["available"]
        lines.append(
            "Based on    %d %s%s, %s to %s: %s"
            % (
                ev["n"],
                ev["description"],
                more,
                fmt_date(ev["first_end_ts"]),
                fmt_date(ev["last_end_ts"]),
                states,
            )
        )
    elif rec["source"] == SOURCE_MODEL:
        lines.append("Based on    a fit of memory against input size (no history of this job)")
    elif rec["source"] == SOURCE_COLD:
        lines.append("Based on    a rule of thumb (no history of this job)")
    else:
        lines.append("Based on    nothing: no comparable runs are stored")

    def count(res: Dict[str, Any]) -> str:
        return "n=%d" % res["n"] if res.get("n") else ""

    mem, cpus, time_, gpus = rec["mem"], rec["cpus"], rec["time"], rec["gpus"]
    rows = [
        [
            "Memory",
            _requested_text(mem, fmt_mem_mb),
            _mem_triple(mem["used"]),
            count(mem),
            fmt_mem_mb(mem["suggested"]),
            _change_text(mem),
        ],
        [
            "CPUs",
            _requested_text(cpus, lambda v: "%d" % v),
            _triple(cpus["used"], fmt_num) + (" cores" if cpus["used"] else ""),
            count(cpus),
            "-" if cpus["suggested"] is None else "%d" % cpus["suggested"],
            _change_text(cpus),
        ],
        [
            "Time",
            _requested_text(time_, fmt_minutes),
            _triple(time_["used"], fmt_minutes),
            count(time_),
            fmt_minutes(time_["suggested"]),
            _change_text(time_),
        ],
    ]
    if gpus["requested"] or gpus["used"]:
        rows.append(
            [
                "GPUs",
                _requested_text(gpus, lambda v: "%d" % v),
                _triple(gpus["used"], fmt_num) + ("% busy" if gpus["used"] else ""),
                count(gpus),
                "-" if gpus["suggested"] is None else "%d" % gpus["suggested"],
                _change_text(gpus),
            ]
        )
    lines.append("")
    lines.append(
        table(
            ["", "Requested", "Used by past runs (p50 / p95 / max)", "", "Suggested", "Change"],
            rows,
        )
    )

    why = [
        (label, res["basis"])
        for label, res in (("Memory", mem), ("CPUs", cpus), ("Time", time_), ("GPUs", gpus))
        if res.get("basis") and res.get("action") in ("lower", "raise", "set")
    ]
    if why:
        lines.append("")
        lines.append("Why")
        lines.extend("  %-7s %s" % (label, text) for label, text in why)

    notes = list(rec["notes"])
    for res in (mem, cpus, time_, gpus):
        notes.extend(res["notes"])
    if cpus.get("peak") is not None:
        notes.append("busiest moment recorded by the profiler: %s cores" % fmt_num(cpus["peak"]))
    if notes:
        lines.append("")
        lines.append("Notes")
        lines.extend("  - %s" % note for note in dict.fromkeys(notes))

    lines.append("")
    lines.append("Confidence  %s -- %s" % (rec["confidence"]["level"], rec["confidence"]["reason"]))
    if rec["options"]:
        lines.append("")
        if rec["source"] == SOURCE_HISTORY:
            lines.append("Suggested request")
        else:
            lines.append(
                "Starting point (not from this job's history; check it after the first run)"
            )
        lines.extend("  #SBATCH %s" % option for option in rec["options"])
        if gpus.get("action") == "lower":
            lines.append("  (and remove the GPU request)")
    return "\n".join(lines)
