#!/usr/bin/env bash
# harvest.sh -- launcher for the HPCsizer collector.
#
# Run by the scrontab entry that `hpg schedule install` writes, or as a single
# self-resubmitting job (`hpg schedule install --mode chain`) on clusters
# without scrontab. All collection logic is in `hpg harvest`; this script only
# keeps a stalled shared filesystem from taking the schedule down or leaving
# core dumps behind.
#
# Settings come from hpcsizer.ini next to the repository, because scrontab jobs
# start without the user's shell environment. Optional overrides:
#   HPCSIZER_ROOT    repository root (default: parent of this script's directory)
#   HPCSIZER_PYTHON  interpreter to use (default: /usr/bin/python3, then python3)
#
#SBATCH --job-name=hpcsizer-harvest
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=/dev/null
#SBATCH --error=/dev/null

set -uo pipefail

NAME="hpcsizer-harvest"
MODE="${1:-}"
INTERVAL="${2:-1hour}"

# -- chain mode: queue the successor FIRST, before touching shared storage ----
# Under Slurm $0 is the node-local copy of this script, so resubmitting needs
# no access to the shared filesystem; a stall there can cost one harvest but
# not the schedule. A run resubmits only when no successor is already pending,
# and --dependency=singleton lets one job of this name run at a time, so an
# accidental second chain collapses into the first. If squeue cannot be asked,
# resubmit anyway: a duplicate heals itself, a missing successor does not.
if [[ "$MODE" == "--chain" ]]; then
    me="${USER:-$(id -un 2>/dev/null || echo)}"
    pending=0
    if out="$(squeue -h -u "$me" --name="$NAME" -t PENDING -o '%i' 2>/dev/null)"; then
        pending="$(printf '%s\n' "$out" | grep -c . || true)"
    fi
    if (( pending == 0 )); then
        # shellcheck disable=SC2086
        sbatch --parsable --begin="now+${INTERVAL}" --dependency=singleton \
            --job-name="$NAME" ${HPCSIZER_SBATCH_OPTS:-} --export=ALL \
            "$0" --chain "$INTERVAL" >/dev/null 2>&1 || true
    fi
fi

# Under scrontab this script is run by its real path. In chain mode $0 is a
# spooled copy, so the root must come from HPCSIZER_ROOT (set at install).
ROOT="${HPCSIZER_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." 2>/dev/null && pwd)}"

ulimit -c 0 2>/dev/null || true      # never leave core dumps behind
cd "${TMPDIR:-/tmp}" 2>/dev/null || cd /

pick_python() {
    local cand
    for cand in "${HPCSIZER_PYTHON:-}" /usr/bin/python3 python3; do
        [[ -n "$cand" ]] || continue
        if "$cand" -I -S -c 'import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)' \
            2>/dev/null; then
            printf '%s' "$cand"
            return 0
        fi
    done
    return 1
}

LOG_DIR="${ROOT}/logs"
LOG="${LOG_DIR}/harvest.log"
mkdir -p "$LOG_DIR" 2>/dev/null || true
# Keep one previous log; the file would otherwise grow without bound.
if [[ -f "$LOG" ]] && (( $(wc -c < "$LOG" 2>/dev/null || echo 0) > 5000000 )); then
    mv -f "$LOG" "${LOG}.1" 2>/dev/null || true
fi

if ! PY="$(pick_python)"; then
    echo "[harvest] $(date -Iseconds) ERROR: no Python 3.9+ found (set HPCSIZER_PYTHON)" \
        >> "$LOG" 2>/dev/null
    exit 1
fi

# -I -S: start without site-packages and .pth files. The collector needs only
# the standard library, and an editable install's .pth file made interpreter
# start-up itself fail whenever the shared filesystem stalled.
HPCSIZER_ROOT="$ROOT" "$PY" -I -S "${ROOT}/bin/hpg" harvest --trigger schedule \
    >> "$LOG" 2>&1
status=$?
if (( status != 0 )); then
    echo "[harvest] $(date -Iseconds) exit status ${status}" >> "$LOG" 2>/dev/null
fi
exit "$status"
