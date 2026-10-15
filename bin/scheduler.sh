#!/usr/bin/env bash
# scheduler.sh — Self-resubmitting SLURM scheduler for HPCsizer.
#
# Replaces crontab (unavailable to regular users on HiPerGator) with a
# lightweight SLURM job that resubmits itself every 15 minutes.
#
# This script runs in one of two modes, selected by its first argument:
#
#   primary  (default) — the normal worker:
#     1. Resubmits itself (primary) to run again in 15 minutes, so the chain
#        continues even if the current run fails partway through.
#     2. Submits a short-lived *watchdog* job (see below) as an independent
#        safety net.
#     3. Runs harvest.sh to pull recent sacct data.
#     4. If the current hour is 3 AM (first run of that hour), runs
#        update_models.py.
#
#   watchdog — a backstop that only re-arms the chain if it has died:
#     1. Checks whether any primary scheduler job is pending or running.
#     2. If one is, it exits quietly (the chain is healthy).
#     3. If none is, the chain has broken (e.g. a primary run failed to even
#        start — see below) and the watchdog restarts it.
#
# Why a watchdog?  The primary chain is a single point of failure: each run is
# the only thing that schedules the next one.  If a single run fails *before*
# it can resubmit — for example a transient node/filesystem error that makes
# SLURM unable to exec the batch script at all (exit code 126/127, elapsed a
# couple of seconds) — then no successor is ever queued and the whole system
# stops silently.  The watchdog is submitted independently every cycle, so the
# death of one primary run no longer ends the chain.
#
# Usage — start the scheduler:
#   sbatch bin/scheduler.sh
#
# Usage — stop the scheduler (cancel both primary and watchdog jobs):
#   scancel --name=hpcsizer-scheduler
#   scancel --name=hpcsizer-watchdog
#
# Environment variables (set in your ~/.bashrc or export before first sbatch):
#   HPCSIZER_DB   — path to profiles.db
#   HPCSIZER_ACCT — SLURM account to harvest
#   HPCSIZER_ROOT — repo root directory

#SBATCH --job-name=hpcsizer-scheduler
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=512mb
# 30 minutes, not 5: a stalled shared filesystem blocks this script in
# uninterruptible I/O wait, and a 5-minute limit then kills it before it can
# queue a successor -- which is exactly how the chain died on 2026-10-14.
# The job normally finishes in seconds; the headroom only matters during a stall.
#SBATCH --time=00:30:00
#SBATCH --qos=minimal
# ^^^ Adjust --qos to whatever your cluster allows for small utility jobs.
#     Remove the line if your cluster has no such QOS.
#
# Discard SLURM's default per-job stdout/stderr. The script re-opens its own
# log file (scheduler.log) below, so the SLURM-managed slurm-<jobid>.out file
# would otherwise be created (empty) in the submission directory on every run.
#SBATCH --output=/dev/null
#SBATCH --error=/dev/null

set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────
PRIMARY_NAME="hpcsizer-scheduler"
WATCHDOG_NAME="hpcsizer-watchdog"
# Cadence for the primary chain and the lead time for the watchdog. The
# watchdog fires a few minutes *after* the next primary is due, so a healthy
# primary has already resubmitted (and the watchdog finds it and exits).
RESUBMIT_DELAY="now+15minutes"
WATCHDOG_DELAY="now+18minutes"
# A long-interval backstop, submitted a few times a day rather than every cycle.
# A filesystem stall kills every job scheduled *inside* it, so recovery needs a
# watchdog queued well beyond the stall window. Without this, the 2026-10-14
# stall left the chain down for a day because the only watchdog in flight was
# 18 minutes out and died alongside the primary.
WATCHDOG_LONG_DELAY="now+6hours"
# When the watchdog restarts a dead chain, start the new primary promptly.
RESTART_DELAY="now+1minutes"

MODE="${1:-primary}"

# squeue needs a user to filter on, and `set -u` turns a missing $USER into a
# fatal "unbound variable" rather than a degraded health check. USER is normally
# exported by --export=ALL, but falling back keeps the watchdog working if it is
# not.
SCHED_USER="${USER:-$(id -un)}"

# Submit a job and echo the outcome. Never returns non-zero, so it is safe under
# `set -e`. The SLURM job name is set explicitly per mode so that watchdog jobs
# are not counted as primaries (both share this one script, and therefore the
# same static `#SBATCH --job-name`). Args: <begin> <mode-arg> <label>
#
# Note this touches only $0 and sbatch. Under SLURM $0 is the node-local copy of
# the batch script, so submitting a successor needs no access to the shared
# filesystem -- which is what makes the early resubmit below safe during a stall.
submit_job() {
    local begin="$1" mode_arg="$2" label="$3" name jid
    if [[ "$mode_arg" == "watchdog" ]]; then
        name="$WATCHDOG_NAME"
    else
        name="$PRIMARY_NAME"
    fi
    if jid=$(sbatch --begin="$begin" --job-name="$name" --parsable --export=ALL "$0" "$mode_arg"); then
        echo "[${label}] Submitted ${mode_arg} job ${jid} (at ${begin})"
    else
        echo "[${label}] ERROR: Failed to submit ${mode_arg} job (check QOS/account)."
    fi
}

# ── Resubmit FIRST, before touching the shared filesystem ──────────────────
# The resubmit used to sit after mkdir/exec on $REPO_ROOT. When that filesystem
# stalls, those two calls block in uninterruptible I/O wait, the job burns its
# walltime and is killed, and no successor is ever queued -- the 2026-10-14
# failure. Doing it here means a stall can cost a cycle's data but not the chain.
# Output is buffered because the log file is not open yet.
EARLY_LOG=""
if [[ "$MODE" != "watchdog" ]]; then
    EARLY_LOG="$(submit_job "$RESUBMIT_DELAY" primary scheduler 2>&1)"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${HPCSIZER_ROOT:-$(dirname "$SCRIPT_DIR")}"
LOG_DIR="${REPO_ROOT}/logs"
mkdir -p "$LOG_DIR"

# Redirect all scheduler output to a single rolling log file.
SCHEDULER_LOG="${LOG_DIR}/scheduler.log"
exec >> "$SCHEDULER_LOG" 2>&1

# Count primary scheduler jobs other than this one that are pending or running.
# Prints an integer, or "UNKNOWN" when squeue could not be consulted.
#
# Excluding our own job id matters: a watchdog overlapping a running primary
# would otherwise always see >=1 and declare the chain healthy. On 2026-10-12
# that cost a full cycle -- the 19:36:39 watchdog reported "OK: 1 primary" on a
# chain that had already failed to queue a successor.
#
# "squeue failed" and "squeue returned nothing" must not be conflated either:
# discarding stderr and counting lines made both look like zero, and zero means
# restart. An unknown state is reported as such so callers can fail safe.
count_primary_jobs() {
    local out status
    out="$(squeue -u "$SCHED_USER" --name="$PRIMARY_NAME" -h -o '%i' -t PENDING,RUNNING 2>&1)"
    status=$?
    if (( status != 0 )); then
        echo "UNKNOWN"
        return 0
    fi
    printf '%s\n' "$out" \
        | grep -v "^${SLURM_JOB_ID:-__none__}\(_\|$\)" \
        | grep -c . || true
}

# ── Watchdog mode ──────────────────────────────────────────────────────────
if [[ "$MODE" == "watchdog" ]]; then
    echo ""
    echo "----------------------------------------"
    echo "[watchdog] $(date -Iseconds): Checking chain health"
    PRIMARIES="$(count_primary_jobs)"
    if [[ "$PRIMARIES" == "UNKNOWN" ]]; then
        # squeue could not be reached, so the chain's state is unknown. Standing
        # down is the safe choice: a missed restart costs one cycle and the next
        # watchdog retries, while a restart on bad information duplicates the
        # chain permanently.
        echo "[watchdog] squeue unavailable — chain state unknown, standing down."
        echo "[watchdog] $(date -Iseconds): Done."
        exit 0
    fi
    if [[ "$PRIMARIES" -gt 0 ]]; then
        echo "[watchdog] OK: ${PRIMARIES} primary job(s) pending/running. Chain is healthy."
        echo "[watchdog] $(date -Iseconds): Done."
        exit 0
    fi

    echo "[watchdog] WARNING: no primary scheduler job found. Chain appears broken."
    echo "[watchdog] Restarting the chain..."
    # Restart the primary chain, and re-arm one watchdog so the restarted chain
    # is protected until its first primary run submits a fresh watchdog.
    submit_job "$RESTART_DELAY" primary watchdog
    submit_job "$WATCHDOG_DELAY" watchdog watchdog
    submit_job "$WATCHDOG_LONG_DELAY" watchdog watchdog
    echo "[watchdog] $(date -Iseconds): Done."
    exit 0
fi

# ── Primary mode ───────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo "[scheduler] $(date -Iseconds): Starting"
echo "  REPO_ROOT=${REPO_ROOT}"
echo "  HPCSIZER_DB=${HPCSIZER_DB:-<not set, using default>}"
echo "  HPCSIZER_ACCT=${HPCSIZER_ACCT:-<not set>}"

# ── 1. Report the resubmit that already happened above ─────────────────────
[[ -n "$EARLY_LOG" ]] && echo "$EARLY_LOG"

# ── 2. Submit an independent watchdog as a safety net ──────────────────────
#    Submitted every cycle so the chain survives even if a primary run fails
#    to start at all (and therefore never reaches the resubmit above). The
#    watchdog does nothing unless it finds the chain dead, so these do not
#    accumulate: each healthy watchdog simply exits.
submit_job "$WATCHDOG_DELAY" watchdog scheduler

# ── 2b. Long-interval backstop, four times a day ───────────────────────────
#    The +18min watchdog shares a stall with the primary that queued it, so it
#    cannot recover from an outage longer than its own lead time. One queued
#    6 hours out survives any plausible stall. Submitted only in the first
#    15-minute window of every sixth hour, so this adds ~4 jobs/day rather
#    than one per cycle.
if [[ "$((10#$(date +%H) % 6))" -eq 0 && "$((10#$(date +%M)))" -lt 15 ]]; then
    submit_job "$WATCHDOG_LONG_DELAY" watchdog scheduler
fi

# ── 3. Run harvest ─────────────────────────────────────────────────────────
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

# ── 4. Run nightly model update at 3 AM ────────────────────────────────────
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
