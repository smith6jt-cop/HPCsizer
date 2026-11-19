"""Stored jobs, classified and measured, and how to find the runs comparable to a job.

"Comparable" is tried at several levels, narrowest first. The first level with
enough informative runs is used for the whole recommendation, so one line can
say what a figure is based on.

Batch and ``--wrap`` jobs -- always the same user:

``exact``      the same job name
``family``     the same name apart from numbers (``mplx12k-x`` ~ ``mplx30k-x``),
               or the same program for jobs whose name says nothing (``wrap``)
``signature``  the same script or program under any job name

Interactive sessions (Open OnDemand apps, configured session names, salloc):

``family``     the same user's sessions of that kind
``kind``       sessions of that kind by any user (needs more runs)

Other users' batch jobs are never pooled: a name such as ``align`` means
different work for different people.
"""

import bisect
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from lib import classify, db, metrics
from lib.config import Config

DAY = 86400

POOLED_LEVELS = frozenset({"kind"})

LevelKey = Tuple[str, tuple]


def level_keys(c: Mapping[str, Any]) -> List[LevelKey]:
    """Evidence levels of a classified job or target, narrowest first."""
    user = c.get("user") or "?"
    gpu = bool(c.get("gpu"))
    if c.get("session"):
        return [("family", (c["family"],)), ("kind", (c["kind"], gpu))]
    keys: List[LevelKey] = []
    name_key = c.get("name_key") or ""
    if c.get("name_exact") and not name_key.startswith("cmd:"):
        keys.append(("exact", (user, c["kind"], c["name_exact"], gpu)))
    keys.append(("family", (c["family"],)))
    if c.get("signature"):
        keys.append(("signature", (user, c["signature"], gpu)))
    return keys


def describe_level(level: str, c: Mapping[str, Any]) -> str:
    """Which runs a level compares with, as a phrase: ``runs named mplx12k``."""
    user = c.get("user") or "?"
    gpu = " with a GPU" if c.get("gpu") else ""
    if c.get("session"):
        if level == "kind":
            return "%s sessions%s of all users" % (c["kind"], gpu)
        return "%s sessions%s of %s" % (c["kind"], gpu, user)
    name_key = c.get("name_key") or ""
    if level == "exact":
        return "runs named %s by %s" % (c.get("name_exact"), user)
    if level == "family":
        if name_key.startswith("cmd:"):
            return "runs of %s by %s" % (name_key[4:], user)
        return "runs named like %s by %s" % (name_key, user)
    return "runs of %s by %s under any job name" % (c.get("signature"), user)


def informative(run: Mapping[str, Any]) -> bool:
    """Does a finished run say anything about what the job needs?

    A job cancelled while pending, or one that failed within seconds, does
    not: it must not count towards the number of runs a suggestion rests on.
    """
    m = run["m"]
    return bool(
        m["mem_used_gb"] is not None
        or m["cores_used"] is not None
        or (run.get("state") == "COMPLETED" and (run.get("elapsed_sec") or 0) > 0)
        or (run.get("state") == "TIMEOUT" and (run.get("elapsed_sec") or 0) > 0)
        or "oom" in m["flags"]
    )


def context_tools(run: Mapping[str, Any]) -> List[str]:
    raw = run.get("ctx_tools_json")
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except ValueError:
        return []
    return [str(v) for v in value] if isinstance(value, list) else []


class History:
    """Finished jobs indexed by evidence level, each list ordered by end time."""

    def __init__(self, jobs: Sequence[Dict[str, Any]]):
        self.jobs = sorted(jobs, key=lambda j: (j.get("end_ts") or 0, j.get("id") or 0))
        # [0]: every run, [1]: informative runs only -- the ones evidence is drawn from.
        self._runs: Tuple[Dict[LevelKey, List[Dict[str, Any]]], ...] = ({}, {})
        self._ends: Tuple[Dict[LevelKey, List[int]], ...] = ({}, {})
        self._tools: Dict[str, List[Dict[str, Any]]] = {}
        for job in self.jobs:
            end = job.get("end_ts") or 0
            subsets = (0, 1) if informative(job) else (0,)
            for key in level_keys(job):
                for subset in subsets:
                    self._runs[subset].setdefault(key, []).append(job)
                    self._ends[subset].setdefault(key, []).append(end)
            if job.get("ctx_input_gb"):
                for tool in context_tools(job):
                    self._tools.setdefault(tool, []).append(job)

    def runs(
        self,
        level: str,
        key: tuple,
        since_ts: Optional[int] = None,
        before_ts: Optional[int] = None,
        informative_only: bool = False,
        last: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """``(runs, count)`` at a level that ended in ``[since_ts, before_ts)``, oldest first.

        ``last`` keeps only the most recent that many runs; ``count`` is how
        many there were before that cut.
        """
        subset = 1 if informative_only else 0
        ends = self._ends[subset].get((level, key))
        if not ends:
            return [], 0
        lo = bisect.bisect_left(ends, since_ts) if since_ts is not None else 0
        hi = bisect.bisect_left(ends, before_ts) if before_ts is not None else len(ends)
        count = max(hi - lo, 0)
        if last is not None:
            lo = max(lo, hi - int(last))
        return self._runs[subset][(level, key)][lo:hi], count

    def with_tool(
        self, tool: str, since_ts: Optional[int] = None, before_ts: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Runs of any user whose recorded submit context names ``tool`` and an input size."""
        return [
            j
            for j in self._tools.get(tool, [])
            if (since_ts is None or (j.get("end_ts") or 0) >= since_ts)
            and (before_ts is None or (j.get("end_ts") or 0) < before_ts)
        ]


def annotate(jobs: List[Dict[str, Any]], cfg: Config) -> List[Dict[str, Any]]:
    """Classify stored rows and derive their metrics, in place."""
    classify.annotate(jobs, cfg)
    metrics.annotate(jobs, cfg)
    return jobs


def load(
    cfg: Config,
    since_ts: Optional[int] = None,
    until_ts: Optional[int] = None,
    user: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Annotated jobs of a window and what is known about the data's freshness."""
    with db.reader(cfg.db_path) as conn:
        jobs = db.fetch_jobs(conn, since_ts=since_ts, until_ts=until_ts, user=user)
        info = {
            "db_path": cfg.db_path,
            "watermark_ts": db.get_meta_int(conn, "watermark_ts"),
            "rows": len(jobs),
        }
    return annotate(jobs, cfg), info
