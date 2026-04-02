#!/usr/bin/env bash
# harvest.sh — sacct harvester for HPCsizer.
#
# Queries sacct for group jobs completed in the last 20 minutes, merges
# .batch MaxRSS with parent lines, and upserts them into the profile database.
#
# Called every 15 minutes by scheduler.sh (self-resubmitting SLURM job).
#
# Environment variables:
#   HPCSIZER_DB   — path to profiles.db (default: <repo>/profiles.db)
#   HPCSIZER_ACCT — SLURM account to harvest (default: all accessible)
#   HPCSIZER_ROOT — repo root directory (default: directory of this script's parent)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${HPCSIZER_ROOT:-$(dirname "$SCRIPT_DIR")}"
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"
# Prefer conda environment if available, then fall back to system python
if command -v conda &>/dev/null && conda env list 2>/dev/null | grep -q "^hpcsizer "; then
    eval "$(conda shell.bash hook 2>/dev/null)"
    conda activate hpcsizer
    PYTHON="$(command -v python3)"
elif [[ -x "${REPO_ROOT}/env/bin/python" ]]; then
    PYTHON="${REPO_ROOT}/env/bin/python"
else
    PYTHON="$(command -v python3 || command -v python || true)"
fi

if [[ -z "${PYTHON:-}" || ! -x "${PYTHON:-}" ]]; then
    echo "[harvest] ERROR: No Python interpreter found. Set HPCSIZER_ROOT or install Python." >&2
    exit 1
fi

DB_PATH="${HPCSIZER_DB:-${REPO_ROOT}/profiles.db}"
ACCT_FLAG=()
if [[ -n "${HPCSIZER_ACCT:-}" ]]; then
    ACCT_FLAG=(-A "${HPCSIZER_ACCT}")
fi

LOOKBACK_MINUTES=20
START_TIME="$(date -d "-${LOOKBACK_MINUTES} minutes" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null \
             || date -v "-${LOOKBACK_MINUTES}M" '+%Y-%m-%dT%H:%M:%S')"

FIELDS="JobID,User,JobName,Account,QOS,State,Submit,Start,End,ReqMem,NCPUS,Timelimit,ReqTRES,MaxRSS,Elapsed,TotalCPU"

echo "[harvest] $(date -Iseconds): querying sacct since ${START_TIME}"

sacct --noheader --parsable2 \
    "${ACCT_FLAG[@]}" \
    -S "${START_TIME}" \
    -o "${FIELDS}" \
| "$PYTHON" - <<'PYEOF'
import sys, os, json, re
sys.path.insert(0, os.environ.get("PYTHONPATH", "").split(":")[0])
from lib.db import init_db, insert_job

DB_PATH = os.environ.get("HPCSIZER_DB", "profiles.db")
init_db(DB_PATH)

FIELDS = "JobID,User,JobName,Account,QOS,State,Submit,Start,End,ReqMem,NCPUS,Timelimit,ReqTRES,MaxRSS,Elapsed,TotalCPU".split(",")

def parse_mem(s):
    if not s or s.strip() in ("", "0"):
        return None
    s = s.strip().rstrip("nc")
    suffix = s[-1].upper() if s[-1] in "KMGTkmgt" else "M"
    try:
        n = float(s[:-1] if s[-1] in "KMGTkmgt" else s)
    except ValueError:
        return None
    return n * {"K": 1/1024/1024, "M": 1/1024, "G": 1.0, "T": 1024.0}.get(suffix, 1/1024)

def parse_elapsed(t):
    t = t.strip()
    m = re.match(r"(\d+)-(\d+):(\d+):(\d+)", t)
    if m:
        d,h,mi,s = [int(x) for x in m.groups()]
        return d*86400+h*3600+mi*60+s
    m = re.match(r"(\d+):(\d+):(\d+)", t)
    if m:
        h,mi,s = [int(x) for x in m.groups()]
        return h*3600+mi*60+s
    return 0

def parse_gpus(tres):
    m = re.search(r"gres/gpu(?::\w+)?=(\d+)", tres or "")
    return int(m.group(1)) if m else 0

# Two-pass: collect parent lines, then merge .batch MaxRSS
parents = {}
batch_rss = {}
for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("---"):
        continue
    parts = line.split("|")
    data = dict(zip(FIELDS, parts + [""]*(max(0, len(FIELDS)-len(parts)))))
    job_id = data.get("JobID","").strip()
    if not job_id:
        continue
    if ".batch" in job_id:
        parent_id = job_id.split(".")[0]
        rss = parse_mem(data.get("MaxRSS",""))
        if rss is not None:
            batch_rss[parent_id] = rss
    elif ".extern" in job_id:
        continue
    elif "." not in job_id:
        state = data.get("State","").strip()
        if state in ("RUNNING","PENDING"):
            continue
        parents[job_id] = data

count = 0
for job_id, data in parents.items():
    state = data.get("State","").strip()
    if state not in ("COMPLETED","FAILED","TIMEOUT","CANCELLED","OUT_OF_MEMORY"):
        continue
    elapsed_sec = parse_elapsed(data.get("Elapsed",""))
    cpu_time_sec = parse_elapsed(data.get("TotalCPU",""))
    req_mem_gb = parse_mem(data.get("ReqMem",""))
    ncpus = int(data.get("NCPUS",1) or 1)
    # Prefer .batch MaxRSS over parent line
    rss_gb = batch_rss.get(job_id) or parse_mem(data.get("MaxRSS",""))
    cpu_eff = (cpu_time_sec / (elapsed_sec * ncpus)
               if elapsed_sec and ncpus else None)
    mem_eff = (rss_gb / req_mem_gb if rss_gb and req_mem_gb else None)
    waste_gb = max(req_mem_gb - (rss_gb or 0), 0) if req_mem_gb else None
    flags = []
    if state == "OUT_OF_MEMORY":
        flags.append("oom_killed")
    if mem_eff is not None and mem_eff < 0.25:
        flags.append("mem_overrequest")
    if cpu_eff is not None and cpu_eff < 0.05:
        flags.append("idle_cpu")
    if ncpus > 2 and cpu_eff is not None and cpu_eff < (1.5 / ncpus):
        flags.append("single_threaded")
    job = {
        "job_id": job_id,
        "user": data.get("User",""),
        "job_name": data.get("JobName",""),
        "account": data.get("Account",""),
        "qos": data.get("QOS",""),
        "state": state,
        "submit_time": data.get("Submit",""),
        "start_time": data.get("Start",""),
        "end_time": data.get("End",""),
        "req_mem_gb": req_mem_gb,
        "req_cpus": ncpus,
        "req_gpus": parse_gpus(data.get("ReqTRES","")),
        "sacct_peak_rss_gb": rss_gb,
        "sacct_elapsed_sec": elapsed_sec,
        "sacct_cpu_time_sec": cpu_time_sec,
        "cpu_efficiency": cpu_eff,
        "waste_gb": waste_gb,
        "mem_efficiency": mem_eff,
        "flags": json.dumps(flags),
    }
    insert_job(job, db_path=DB_PATH)
    count += 1

print(f"[harvest] Upserted {count} jobs ({len(batch_rss)} had .batch MaxRSS).")
PYEOF

echo "[harvest] Done."
