#!/usr/bin/env bash
# scheduler.sh — Self-resubmitting SLURM scheduler for HPCsizer.
#
# Replaces crontab (unavailable to regular users on HiPerGator) with a
# lightweight SLURM job that resubmits itself every 15 minutes.
#
# Each invocation:
#   1. Resubmits itself to run again in 15 minutes (so the chain continues
#      even if the current run fails).
#   2. Runs harvest.sh to pull recent sacct data.
#   3. If the current hour is 3 AM (first run of that hour), runs
#      update_models.py.
#
# Usage — start the scheduler:
#   sbatch bin/scheduler.sh
#
# Usage — stop the scheduler:
#   scancel --name=hpcsizer-scheduler
#
# Environment variables (set in your ~/.bashrc or export before first sbatch):
#   HPCSIZER_DB   — path to profiles.db
#   HPCSIZER_ACCT — SLURM account to harvest
#   HPCSIZER_ROOT — repo root directory

#SBATCH --job-name=hpcsizer-scheduler
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=512mb
#SBATCH --time=00:05:00
#SBATCH --qos=minimal
# ^^^ Adjust --qos to whatever your cluster allows for small utility jobs.
#     Remove the line if your cluster has no such QOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${HPCSIZER_ROOT:-$(dirname "$SCRIPT_DIR")}"
LOG_DIR="${REPO_ROOT}/logs"
mkdir -p "$LOG_DIR"

# Redirect all scheduler output to a timestamped log file
SCHEDULER_LOG="${LOG_DIR}/scheduler.log"
exec >> "$SCHEDULER_LOG" 2>&1

echo ""
echo "========================================"
echo "[scheduler] $(date -Iseconds): Starting"
echo "  REPO_ROOT=${REPO_ROOT}"
echo "  HPCSIZER_DB=${HPCSIZER_DB:-<not set, using default>}"
echo "  HPCSIZER_ACCT=${HPCSIZER_ACCT:-<not set>}"

# ── 1. Resubmit ourselves to run again in 15 minutes ───────────────────────
#    We do this FIRST so the chain is not broken if later steps fail.
NEXT_JOB_ID=$(sbatch --begin=now+15minutes \
    --parsable \
    --export=ALL \
    "$0") || {
    echo "[scheduler] ERROR: Failed to resubmit. Chain is broken!"
    echo "  Check QOS and account settings."
}
echo "[scheduler] Resubmitted as job ${NEXT_JOB_ID:-FAILED} (in 15 min)"

# ── 2. Run harvest ─────────────────────────────────────────────────────────
echo "[scheduler] Running harvest..."
if bash "${REPO_ROOT}/bin/harvest.sh" >> "${LOG_DIR}/harvest.log" 2>&1; then
    echo "[scheduler] harvest.sh succeeded"
else
    HARVEST_STATUS=$?
    echo "[scheduler] WARNING: harvest.sh exited with status ${HARVEST_STATUS}"
    echo "[scheduler]   Check ${LOG_DIR}/harvest.log for details"
    # Show last few lines of harvest log for quick diagnosis
    echo "[scheduler]   Last 5 lines of harvest.log:"
    tail -5 "${LOG_DIR}/harvest.log" 2>/dev/null | sed 's/^/    /' || true
fi

# ── 3. Run nightly model update at 3 AM ────────────────────────────────────
CURRENT_HOUR=$(date +%H)
CURRENT_MIN=$(date +%M)
# Run during the 3:00 AM window (minutes 0-14, matching our 15-min cadence)
if [[ "$CURRENT_HOUR" == "03" && "$CURRENT_MIN" -lt 15 ]]; then
    echo "[scheduler] 3 AM window — running model update..."

    # Resolve Python
    if command -v conda &>/dev/null && conda env list 2>/dev/null | grep -q "^hpcsizer "; then
        PYTHON="$(conda run -n hpcsizer which python)"
    elif [[ -x "${REPO_ROOT}/env/bin/python" ]]; then
        PYTHON="${REPO_ROOT}/env/bin/python"
    else
        PYTHON="$(command -v python3 || command -v python)"
    fi

    DB_PATH="${HPCSIZER_DB:-${REPO_ROOT}/profiles.db}"
    "$PYTHON" "${REPO_ROOT}/bin/update_models.py" --db "$DB_PATH" \
        >> "${LOG_DIR}/models.log" 2>&1 || \
        echo "[scheduler] WARNING: update_models.py exited with status $?"
fi

echo "[scheduler] $(date -Iseconds): Done."
