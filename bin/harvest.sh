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

# Default window, and the furthest back a resumed harvest will reach.
LOOKBACK_MINUTES=20
MAX_LOOKBACK_MINUTES=10080  # 7 days
LOG_DIR="${REPO_ROOT}/logs"
STATE_FILE="${LOG_DIR}/.harvest_state"
# Retry policy. Overridable so the test suite does not pay for real backoff.
RETRIES="${HPCSIZER_HARVEST_RETRIES:-3}"
RETRY_DELAY="${HPCSIZER_HARVEST_RETRY_DELAY:-5}"

_ts_minutes_ago() {
    date -d "-$1 minutes" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null \
        || date -v "-$1M" '+%Y-%m-%dT%H:%M:%S'
}

# Resume from the last successful harvest rather than a fixed window.
#
# With a fixed 20-minute lookback on a 15-minute cadence, two consecutive
# failures lose jobs permanently -- and harvest failed 3519 times in 6331 runs
# over six months (a stalled shared filesystem making the Python environment
# unreadable at startup), so consecutive failures were common. Resuming means a
# gap of any length is backfilled by the next run that succeeds.
START_TIME=""
if [[ -r "$STATE_FILE" ]]; then
    LAST_OK="$(tr -d '[:space:]' < "$STATE_FILE" 2>/dev/null || true)"
    if [[ "$LAST_OK" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$ ]]; then
        FLOOR="$(_ts_minutes_ago "$MAX_LOOKBACK_MINUTES")"
        # Clamp to the floor; lexical compare is valid for this timestamp format.
        if [[ "$LAST_OK" < "$FLOOR" ]]; then
            START_TIME="$FLOOR"
            echo "[harvest] last success ${LAST_OK} predates the 7-day cap; resuming from ${FLOOR}"
        else
            START_TIME="$LAST_OK"
        fi
    fi
fi
if [[ -z "$START_TIME" ]]; then
    START_TIME="$(_ts_minutes_ago "$LOOKBACK_MINUTES")"
fi

FIELDS="JobID,User,JobName,Account,QOS,State,Submit,Start,End,ReqMem,NCPUS,Timelimit,ReqTRES,MaxRSS,Elapsed,TotalCPU"

# Captured before the query, not after: writing the post-query time as the
# watermark would skip anything that finished while sacct was running. Resuming
# from just before the query instead re-covers a few seconds, and the upserts
# are idempotent, so overlap is free where a gap would be permanent.
HARVEST_WATERMARK="$(date '+%Y-%m-%dT%H:%M:%S')"

echo "[harvest] $(date -Iseconds): querying sacct since ${START_TIME}"

# Materialize sacct output before handing it to Python.
# Running `sacct | python` directly races: when Python finishes reading stdin
# and exits, the kernel can deliver SIGPIPE to sacct mid-flush, producing
# exit status 141 under `set -o pipefail` even though the harvest succeeded.
#
# We must also NOT pipe the data into `python - <<'PYEOF'`: the heredoc is the
# program for `python -`, so it occupies stdin and silently discards anything
# piped in (Python would read an empty stdin and upsert zero jobs). Instead we
# write the rows to a temp file and pass its path to the program via an env var.
SACCT_FILE="$(mktemp "${TMPDIR:-/tmp}/hpcsizer-sacct.XXXXXX")"
PY_PROG="$(mktemp "${TMPDIR:-/tmp}/hpcsizer-upsert.XXXXXX.py")"
trap 'rm -f "$SACCT_FILE" "$PY_PROG"' EXIT

# The upsert program is written out once and invoked by the retry loop below,
# rather than being a heredoc inside the loop.
cat > "$PY_PROG" <<'UPSERT_EOF'
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
with open(os.environ["HPCSIZER_SACCT_FILE"], encoding="utf-8") as _sacct_fh:
    sacct_lines = _sacct_fh.readlines()
for line in sacct_lines:
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
UPSERT_EOF

# Both the sacct query and the upsert are retried. The failures seen in practice
# are transient -- slurmdbd briefly unreachable, or a stalled filesystem making
# the interpreter's own stdlib unreadable -- and a single attempt turned every
# one of them into permanently lost data.
run_harvest() {
    local attempt delay sacct_out
    delay="$RETRY_DELAY"
    for attempt in $(seq 1 "$RETRIES"); do
        if sacct_out="$(sacct --noheader --parsable2 \
                "${ACCT_FLAG[@]}" \
                -S "${START_TIME}" \
                -o "${FIELDS}")"; then
            printf '%s\n' "$sacct_out" > "$SACCT_FILE"
            # -I -S is deliberate, and is the fix for the dominant failure mode.
            # Every one of the 3519 harvest failures was Python dying during
            # site initialization, because `pip install -e .` leaves an
            # __editable__*.pth in site-packages whose finder imports
            # pathlib -> glob -> contextlib -> collections at interpreter
            # startup. On a stalled shared filesystem that is fatal before the
            # program begins. -S skips site.py (and therefore every .pth), and
            # -I keeps the temp directory off sys.path. The program re-adds the
            # repo root from $PYTHONPATH itself, and both it and lib/db import
            # only the standard library, so nothing from site-packages is
            # needed. Anything added here that needs a third-party package must
            # revisit this.
            if HPCSIZER_SACCT_FILE="$SACCT_FILE" "$PYTHON" -I -S "$PY_PROG"; then
                return 0
            fi
        fi
        if (( attempt < RETRIES )); then
            echo "[harvest] attempt ${attempt}/${RETRIES} failed; retrying in ${delay}s" >&2
            (( delay > 0 )) && sleep "$delay"
            delay=$(( delay * 3 ))
        fi
    done
    return 1
}

if ! run_harvest; then
    echo "[harvest] ERROR: all ${RETRIES} attempts failed; window from ${START_TIME} not recorded" >&2
    echo "[harvest]   The next successful run resumes from ${START_TIME}, so nothing is lost." >&2
    exit 1
fi

# Advance the watermark only once the data is actually in the database.
mkdir -p "$LOG_DIR" 2>/dev/null || true
if ! printf '%s\n' "$HARVEST_WATERMARK" > "$STATE_FILE" 2>/dev/null; then
    echo "[harvest] WARNING: could not update ${STATE_FILE}; next run falls back to a ${LOOKBACK_MINUTES}m window" >&2
fi

echo "[harvest] Done."
