#!/usr/bin/env python3
"""One-time backfill: queries sacct WITHOUT -X to get .batch MaxRSS,
then merges parent + .batch lines before inserting.

This differs from the regular harvester (harvest.sh) by querying without
``-X`` and doing a two-pass merge of parent + ``.batch`` lines so that
``MaxRSS`` is actually populated.

Environment variables
---------------------
HPCSIZER_DB    path to profiles.db  (required)
HPCSIZER_ACCT  SLURM account string (required)
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.db import init_db, insert_job

FIELDS = (
    "JobID,User,JobName,Account,QOS,State,Submit,Start,End,"
    "ReqMem,NCPUS,Timelimit,ReqTRES,MaxRSS,Elapsed,TotalCPU"
)


def parse_mem(s):
    if not s or s.strip() in ("", "0"):
        return None
    s = s.strip().rstrip("nc")
    suffix = s[-1].upper() if s[-1] in "KMGTkmgt" else "M"
    try:
        n = float(s[:-1] if s[-1] in "KMGTkmgt" else s)
    except ValueError:
        return None
    return n * {"K": 1 / 1048576, "M": 1 / 1024, "G": 1.0, "T": 1024.0}.get(
        suffix, 1 / 1024
    )


def parse_elapsed(t):
    t = t.strip()
    m = re.match(r"(\d+)-(\d+):(\d+):(\d+)", t)
    if m:
        d, h, mi, s = [int(x) for x in m.groups()]
        return d * 86400 + h * 3600 + mi * 60 + s
    m = re.match(r"(\d+):(\d+):(\d+)", t)
    if m:
        h, mi, s = [int(x) for x in m.groups()]
        return h * 3600 + mi * 60 + s
    return 0


def parse_gpus(tres):
    m = re.search(r"gres/gpu(?::\w+)?=(\d+)", tres or "")
    return int(m.group(1)) if m else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since",
        default="2026-01-01",
        help="sacct start date (YYYY-MM-DD), default: 2026-01-01",
    )
    args = parser.parse_args()

    db = os.environ.get("HPCSIZER_DB")
    acct = os.environ.get("HPCSIZER_ACCT")
    if not db or not acct:
        print(
            "Error: HPCSIZER_DB and HPCSIZER_ACCT must be set.", file=sys.stderr
        )
        sys.exit(1)

    init_db(db)

    result = subprocess.run(
        [
            "sacct", "-S", args.since, "-a", "-A", acct,
            "--noheader", "--parsable2", "-o", FIELDS,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"sacct failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    # Two-pass: collect parent lines, then merge .batch MaxRSS
    parents = {}
    batch_rss = {}
    fnames = FIELDS.replace("\n", "").split(",")

    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split("|")
        data = dict(zip(fnames, parts + [""] * (len(fnames) - len(parts))))
        job_id = data.get("JobID", "").strip()

        if ".batch" in job_id:
            parent_id = job_id.split(".")[0]
            rss = parse_mem(data.get("MaxRSS", ""))
            if rss is not None:
                batch_rss[parent_id] = rss
        elif ".extern" in job_id:
            continue
        elif "." not in job_id:
            state = data.get("State", "").strip()
            if state in ("RUNNING", "PENDING"):
                continue
            parents[job_id] = data

    count = 0
    for job_id, data in parents.items():
        state = data.get("State", "").strip()
        elapsed_sec = parse_elapsed(data.get("Elapsed", ""))
        cpu_time_sec = parse_elapsed(data.get("TotalCPU", ""))
        req_mem_gb = parse_mem(data.get("ReqMem", ""))
        ncpus = int(data.get("NCPUS", 1) or 1)

        # Prefer .batch MaxRSS over parent line
        rss_gb = batch_rss.get(job_id) or parse_mem(data.get("MaxRSS", ""))

        cpu_eff = (
            (cpu_time_sec / (elapsed_sec * ncpus)) if elapsed_sec and ncpus else None
        )
        mem_eff = (rss_gb / req_mem_gb) if rss_gb and req_mem_gb else None
        waste = max(req_mem_gb - (rss_gb or 0), 0) if req_mem_gb else None

        flags = []
        if state == "OUT_OF_MEMORY":
            flags.append("oom_killed")
        if mem_eff is not None and mem_eff < 0.25:
            flags.append("mem_overrequest")
        if cpu_eff is not None and cpu_eff < 0.05:
            flags.append("idle_cpu")
        if ncpus > 2 and cpu_eff is not None and cpu_eff < (1.5 / ncpus):
            flags.append("single_threaded")

        insert_job(
            {
                "job_id": job_id,
                "user": data.get("User", ""),
                "job_name": data.get("JobName", ""),
                "account": data.get("Account", ""),
                "qos": data.get("QOS", ""),
                "state": state,
                "submit_time": data.get("Submit", ""),
                "start_time": data.get("Start", ""),
                "end_time": data.get("End", ""),
                "req_mem_gb": req_mem_gb,
                "req_cpus": ncpus,
                "req_gpus": parse_gpus(data.get("ReqTRES", "")),
                "sacct_peak_rss_gb": rss_gb,
                "sacct_elapsed_sec": elapsed_sec,
                "sacct_cpu_time_sec": cpu_time_sec,
                "cpu_efficiency": cpu_eff,
                "mem_efficiency": mem_eff,
                "waste_gb": waste,
                "flags": json.dumps(flags),
            },
            db_path=db,
        )
        count += 1

    print(f"Backfilled {count} jobs ({len(batch_rss)} had .batch MaxRSS).")


if __name__ == "__main__":
    main()
