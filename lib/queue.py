"""Pending and running jobs, next to what their job family has used.

A burst of jobs submitted together cannot be sized when it is submitted:
nothing of its kind has finished yet. Once the first ones have, the ones still
waiting can be compared with them, and a pending job can be given a smaller
request without being resubmitted (``scontrol update``).

Open jobs are read from ``sacct`` rather than ``squeue`` so that they carry
the same fields as stored jobs -- in particular the submit line, which is what
tells two ``--wrap`` jobs apart -- and are classified by the same rules.
"""

from typing import Any, Dict, List, Sequence

from lib import classify, collect, db, metrics, recommender, sbatch_opts, slurm
from lib.config import Config
from lib.fmt import fmt_gb, fmt_int, fmt_mem_mb, fmt_minutes, fmt_num, fmt_seconds, fmt_ts, table
from lib.history import History

OPEN_QUERY_STATES = "PENDING,RUNNING,SUSPENDED,REQUEUED,RESIZING"


def open_jobs_cmd(cfg: Config, fields: Sequence[str] = slurm.FIELDS) -> List[str]:
    """sacct call for the jobs of the configured accounts that are not finished."""
    if not cfg.accounts:
        raise ValueError("no Slurm account configured")
    return [
        cfg.sacct,
        "-a",
        "-A",
        ",".join(cfg.accounts),
        "-s",
        OPEN_QUERY_STATES,
        "-n",
        "-p",
        "--delimiter=" + slurm.SEP,
        "-o",
        ",".join(fields),
    ]


def fetch_open(cfg: Config) -> List[Dict[str, Any]]:
    """Unfinished jobs as classified records. Raises ``collect.HarvestError`` if sacct fails."""
    fields, _lacking = collect.sacct_fields(cfg)
    try:
        text = collect.run_sacct(open_jobs_cmd(cfg, fields))
    except FileNotFoundError:
        raise collect.HarvestError("'%s' not found; is Slurm on PATH?" % cfg.sacct) from None
    try:
        rows = slurm.records_to_dicts(slurm.split_records(text, nfields=len(fields)), fields)
    except slurm.SacctFormatError as exc:
        raise collect.HarvestError(str(exc)) from exc
    groups, _orphans = slurm.group_records(rows)
    jobs = []
    for job_row, step_rows in groups:
        if not slurm.is_open(job_row):
            continue
        record, _steps = slurm.merge_job(job_row, step_rows)
        if db.is_internal_job(record.get("job_name")):
            continue
        jobs.append(record)
    classify.annotate(jobs, cfg)
    metrics.annotate(jobs, cfg)
    return jobs


def _scontrol_id(job: Dict[str, Any]) -> str:
    """The job id as ``scontrol update`` takes it: ``123_[5-9]`` without a ``%limit``."""
    job_id = str(job.get("job_id") or "")
    if "[" in job_id and "%" in job_id:
        return job_id.split("%", 1)[0] + "]"
    return job_id


def _history_text(rec: Dict[str, Any]) -> str:
    ev = rec["evidence"]
    if not ev:
        return "no finished runs yet"
    parts = ["%d runs" % ev["n"]]
    if rec["mem"]["used"]:
        parts.append("memory up to %s" % fmt_gb(rec["mem"]["used"]["max"]))
    if rec["cpus"]["used"]:
        parts.append("%s cores busy" % fmt_num(rec["cpus"]["used"]["max"]))
    if rec["time"]["used"]:
        parts.append("longest %s" % fmt_minutes(rec["time"]["used"]["max"]))
    return ", ".join(parts)


def build(cfg: Config, history: History, now: int) -> Dict[str, Any]:
    """Open jobs with the recommendation each would get now."""
    rows = []
    for job in sorted(
        fetch_open(cfg), key=lambda j: (j.get("state") != "RUNNING", j.get("submit_ts") or 0)
    ):
        target = recommender.target_from_job(job, cfg)
        rec = recommender.recommend(target, history, cfg, now)
        pending = job.get("state") == "PENDING"
        single = (job.get("req_nodes") or job.get("alloc_nodes") or 1) <= 1
        update: List[str] = []
        risks: List[str] = []
        if pending and rec["source"] == recommender.SOURCE_HISTORY:
            mem, time_ = rec["mem"], rec["time"]
            if mem["action"] == "lower" and single:
                update.append("MinMemoryNode=%d" % int(mem["suggested"]))
            if time_["action"] == "lower":
                update.append("TimeLimit=%s" % sbatch_opts.format_time(time_["suggested"]))
            if mem["action"] == "raise":
                risks.append(
                    "asks for %s; %s -- resubmit with %s"
                    % (
                        fmt_mem_mb(mem["requested"]),
                        mem["notes"][0] if mem["notes"] else mem["basis"],
                        rec["changes"].get("mem", "more memory"),
                    )
                )
            if time_["action"] == "raise":
                risks.append(
                    "limit %s; %s -- resubmit with %s"
                    % (
                        fmt_minutes(time_["requested"]),
                        time_["notes"][0] if time_["notes"] else time_["basis"],
                        rec["changes"].get("time", "a longer limit"),
                    )
                )
        rows.append(
            {
                "job_id": job.get("job_id"),
                "user": job.get("user"),
                "state": job.get("state"),
                "job_name": job.get("job_name"),
                "family": job.get("family"),
                "kind": job.get("kind"),
                "elapsed_sec": job.get("elapsed_sec") if job.get("state") != "PENDING" else None,
                "req_time_min": job.get("req_time_min"),
                "cpus": job.get("req_cpus") or job.get("alloc_cpus"),
                "req_mem_mb": job.get("req_mem_mb"),
                "gpus": job.get("req_gpus") or job.get("alloc_gpus") or 0,
                "submit_ts": job.get("submit_ts"),
                "history": _history_text(rec),
                "source": rec["source"],
                "changes": rec["changes"],
                "confidence": rec["confidence"]["level"],
                "scontrol": (
                    "scontrol update JobId=%s %s" % (_scontrol_id(job), " ".join(update))
                    if update
                    else None
                ),
                "resize": {
                    "mem_mb": rec["mem"]["suggested"] if rec["mem"]["action"] == "lower" else None,
                    "time_min": rec["time"]["suggested"]
                    if rec["time"]["action"] == "lower"
                    else None,
                    "runs": rec["evidence"]["n"] if rec["evidence"] else 0,
                },
                "risks": risks,
            }
        )
    states: Dict[str, int] = {}
    for row in rows:
        states[row["state"]] = states.get(row["state"], 0) + 1
    return {"accounts": list(cfg.accounts), "as_of_ts": now, "states": states, "jobs": rows}


def render_text(result: Dict[str, Any], users: Sequence[str] = ()) -> str:
    rows = [r for r in result["jobs"] if not users or r["user"] in users]
    counts = ", ".join(
        "%s %s" % (fmt_int(n), state.lower()) for state, n in sorted(result["states"].items())
    )
    lines = [
        "Queue of account %s at %s: %s"
        % (
            ", ".join(result["accounts"]),
            fmt_ts(result["as_of_ts"]),
            counts or "no unfinished jobs",
        )
    ]
    if not rows:
        return "\n".join(lines)
    lines += [
        "",
        table(
            [
                "Job",
                "User",
                "State",
                "Name",
                "Run / limit",
                "CPUs",
                "Memory",
                "Its family so far",
                "Would suggest",
            ],
            [
                [
                    r["job_id"],
                    r["user"],
                    r["state"],
                    r["job_name"],
                    "%s / %s"
                    % (
                        "-" if r["elapsed_sec"] is None else fmt_seconds(r["elapsed_sec"]),
                        "unlimited"
                        if r["req_time_min"] is None
                        else fmt_minutes(r["req_time_min"]),
                    ),
                    fmt_int(r["cpus"]),
                    fmt_mem_mb(r["req_mem_mb"]),
                    r["history"],
                    " ".join(r["changes"].values())
                    or ("-" if r["source"] != "history" else "as requested"),
                ]
                for r in rows
            ],
        ),
    ]
    resizable = [r for r in rows if r["scontrol"]]
    if resizable:
        lines += [
            "",
            "Pending jobs that ask for more than their family has used can be resized in place:",
        ]
        for r in resizable:
            what = []
            if r["resize"]["mem_mb"] is not None:
                what.append(
                    "memory %s -> %s"
                    % (fmt_mem_mb(r["req_mem_mb"]), fmt_mem_mb(r["resize"]["mem_mb"]))
                )
            if r["resize"]["time_min"] is not None:
                what.append(
                    "limit %s -> %s"
                    % (fmt_minutes(r["req_time_min"]), fmt_minutes(r["resize"]["time_min"]))
                )
            lines.append(
                "  %s   # %s: %s (%d runs)"
                % (r["scontrol"], r["job_name"], ", ".join(what), r["resize"]["runs"])
            )
        lines.append(
            "  Only a job's owner can run these, and only while it is pending. "
            "A different CPU count needs a resubmission."
        )
    risky = [r for r in rows if r["risks"]]
    if risky:
        lines += ["", "Pending jobs that ask for less than their family needed:"]
        for r in risky:
            for risk in r["risks"]:
                lines.append("  %s (%s, %s): %s" % (r["job_id"], r["user"], r["job_name"], risk))
    return "\n".join(lines)
