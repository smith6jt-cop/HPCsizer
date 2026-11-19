"""hpg — Main CLI entry point for HPCsizer v2.

Commands
--------
hpg submit <script.sbatch>         Analyze, recommend, confirm, submit with monitoring
hpg interactive [rstudio|jupyter|vscode|bash]  Launch interactive session with monitoring
hpg plot <job_id>                  Multi-panel time-series plot
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

# Allow importing lib from parent directory
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import click
except ImportError:
    sys.exit("ERROR: 'click' is required. Install it with: pip install click")

from lib.legacy_analyzer import analyze_script
from lib.legacy_db import get_job, init_db
from lib.legacy_recommender import recommend

_REPO_ROOT = Path(__file__).parent.parent
_DEFAULT_DB = os.environ.get("HPCSIZER_DB", str(_REPO_ROOT / "profiles.db"))
_DEFAULT_TS_DIR = str(_REPO_ROOT / "timeseries")
_DEFAULT_PLOTS_DIR = str(_REPO_ROOT / "plots")
_MONITOR_PY = str(_REPO_ROOT / "bin" / "monitor.py")
_FINALIZE_PY = str(_REPO_ROOT / "bin" / "finalize.py")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _ensure_db() -> None:
    init_db(_DEFAULT_DB)


def _format_mem(gb: Optional[float]) -> str:
    if gb is None:
        return "unknown"
    if gb >= 1:
        return f"{gb:.1f}GB"
    return f"{gb * 1024:.0f}MB"


def _format_walltime(minutes: Optional[int]) -> str:
    """Render minutes as the HH:MM:SS that #SBATCH --time expects."""
    if minutes is None:
        return "unknown"
    return f"{int(minutes) // 60:02d}:{int(minutes) % 60:02d}:00"


def _print_recommendation(meta: Dict[str, Any], rec: Dict[str, Any]) -> None:
    click.echo("\n" + "=" * 60)
    click.echo("  HPCsizer Resource Recommendation")
    click.echo("=" * 60)
    click.echo(f"  Script       : {meta.get('script_path', 'N/A')}")
    click.echo(f"  Language     : {meta.get('language', 'unknown')}")
    click.echo(f"  Tools found  : {', '.join(meta.get('static_tools', [])) or 'none'}")
    click.echo(f"  Input data   : {_format_mem(meta.get('total_input_gb', 0))}")
    click.echo("")
    click.echo(f"  Recommended Memory : {_format_mem(rec['mem_gb'])}")
    click.echo(f"  Recommended CPUs   : {rec['cpus']}")
    if rec.get("time_min"):
        click.echo(
            f"  Recommended Time   : {_format_walltime(rec['time_min'])} "
            f"(n={rec.get('time_samples', 0)})"
        )
    else:
        click.echo("  Recommended Time   : unknown (too few completed jobs)")
    if rec.get("req_gpus"):
        click.echo(f"  Recommended GPUs   : {rec['req_gpus']}")
    click.echo(f"  Source             : {rec['source']} (n={rec.get('samples', 0)})")
    if rec.get("r_squared") is not None:
        click.echo(f"  Model R²           : {rec['r_squared']:.3f}")
    click.echo("")
    click.echo(
        f"  Current request    : {_format_mem(meta.get('req_mem_gb'))} mem, "
        f"{meta.get('req_cpus', 1)} CPUs, "
        f"{_format_walltime(meta.get('req_time_min'))}"
    )
    click.echo("=" * 60)
    for warning in rec.get("warnings", []):
        click.echo(f"  WARNING: {warning}")
    if rec.get("warnings"):
        click.echo("=" * 60)
    click.echo("")


def _inject_sidecar(script_text: str, job_id_var: str, ts_path: str, context_path: str) -> str:
    """Return script text with sidecar monitor injected.

    For multi-node jobs (SLURM_JOB_NUM_NODES > 1), uses srun --overlap to
    launch the monitor on every node. Each node writes its own time-series
    file with the hostname embedded in the filename.
    """
    python_bin = sys.executable
    monitor_launch = (
        f"\n# --- HPCsizer sidecar monitor (injected) ---\n"
        f"HPG_JOB_ID=${{SLURM_JOB_ID}}\n"
        f'HPG_TS_PATH="{ts_path}/${{HPG_JOB_ID}}.csv.gz"\n'
        f'mkdir -p "{ts_path}"\n'
        f"HPG_NUM_NODES=${{SLURM_JOB_NUM_NODES:-1}}\n"
        f'if [ "$HPG_NUM_NODES" -gt 1 ]; then\n'
        f"  srun --overlap --ntasks-per-node=1 --nodes=${{HPG_NUM_NODES}} \\\n"
        f"    {python_bin} {_MONITOR_PY} ${{HPG_JOB_ID}} ${{HPG_TS_PATH}} &\n"
        f"else\n"
        f"  {python_bin} {_MONITOR_PY} ${{HPG_JOB_ID}} ${{HPG_TS_PATH}} &\n"
        f"fi\n"
        f"HPG_MONITOR_PID=$!\n"
        f"trap 'kill $HPG_MONITOR_PID 2>/dev/null; wait $HPG_MONITOR_PID 2>/dev/null; "
        f"{python_bin} {_FINALIZE_PY} ${{HPG_JOB_ID}} "
        f"--db {_DEFAULT_DB} --ts-dir {ts_path} --context {context_path}' EXIT\n"
        f"# --- end HPCsizer injection ---\n"
    )
    # Insert after the last #SBATCH line
    lines = script_text.splitlines(keepends=True)
    last_sbatch = -1
    for i, line in enumerate(lines):
        if line.startswith("#SBATCH"):
            last_sbatch = i
    if last_sbatch >= 0:
        lines.insert(last_sbatch + 1, monitor_launch)
    else:
        lines.insert(1 if lines and lines[0].startswith("#!") else 0, monitor_launch)
    return "".join(lines)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    """HPCsizer — Automated SLURM Job Profiling and Resource Sizing."""
    pass


# ---------------------------------------------------------------------------
# hpg submit
# ---------------------------------------------------------------------------


@cli.command("submit")
@click.argument("script", type=click.Path(exists=True))
@click.option("--yes", "-y", is_flag=True, help="Submit without confirmation")
@click.option("--as-is", is_flag=True, help="Submit with original resource requests")
@click.option("--user", default=None, help="Use only this user's job history")
@click.option("--days", default=90, show_default=True, help="Days of history to consider")
def submit(script: str, yes: bool, as_is: bool, user: Optional[str], days: int) -> None:
    """Analyze SCRIPT, recommend resources, then submit with monitoring."""
    _ensure_db()
    meta = analyze_script(script)
    rec = recommend(
        meta["static_tools"],
        meta.get("total_input_gb", 0),
        meta.get("req_cpus", 1),
        meta.get("req_gpus", 0),
        db_path=_DEFAULT_DB,
        user=user,
        days=days,
    )
    _print_recommendation(meta, rec)

    if rec.get("mem_gb") is None and not as_is:
        # Nothing defensible to apply; don't let "use recommended" quietly mean
        # "submit unchanged".
        click.echo("No memory recommendation is available, so there is nothing to apply.")
        if not yes and not click.confirm("Submit the script unchanged?", default=False):
            click.echo("Aborted.")
            return
        as_is = True

    if not as_is:
        if not yes:
            choice = (
                click.prompt(
                    "Submit with recommended resources? [y]es / [n]o (submit as-is) / [q]uit",
                    default="y",
                )
                .strip()
                .lower()
            )
            if choice == "q":
                click.echo("Aborted.")
                return
            use_rec = choice != "n"
        else:
            use_rec = True
    else:
        use_rec = False

    # Write the (possibly modified) script to a temp file
    script_text = Path(script).read_text(errors="replace")
    if use_rec:
        script_text = _apply_recommendations(script_text, rec)

    # Write static context to a temp file for finalize.py
    ctx_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, dir="/tmp")
    json.dump(
        {
            "static_tools": meta["static_tools"],
            "input_files": meta["input_files"],
            "script_hash": meta["script_hash"],
        },
        ctx_file,
    )
    ctx_file.flush()
    ctx_file.close()

    # Inject sidecar
    modified = _inject_sidecar(script_text, "SLURM_JOB_ID", _DEFAULT_TS_DIR, ctx_file.name)
    tmp_script = tempfile.NamedTemporaryFile(mode="w", suffix=".sbatch", delete=False, dir="/tmp")
    tmp_script.write(modified)
    tmp_script.flush()
    tmp_script.close()

    click.echo("Submitting job...")
    result = subprocess.run(["sbatch", tmp_script.name], capture_output=False)
    sys.exit(result.returncode)


def _set_directive(script_text: str, pattern: str, directive: str) -> str:
    """Replace a #SBATCH directive, or insert it if the script has none.

    Plain re.sub silently returns the script unchanged when the directive is
    absent, so accepting a recommendation applied nothing. Insert after the last
    #SBATCH line so the new directive stays inside the header block.
    """
    import re

    if re.search(pattern, script_text):
        return re.sub(pattern, directive, script_text)
    lines = script_text.splitlines(keepends=True)
    last = max(
        (i for i, ln in enumerate(lines) if ln.lstrip().startswith("#SBATCH")),
        default=None,
    )
    if last is None:
        # No header at all: insert after the shebang, or at the very top.
        last = 0 if lines and lines[0].startswith("#!") else -1
    lines.insert(last + 1, directive + "\n")
    return "".join(lines)


def _apply_recommendations(script_text: str, rec: Dict[str, Any]) -> str:
    """Overwrite #SBATCH --mem, --cpus-per-task and --time with recommended values."""
    import re

    mem_gb = rec.get("mem_gb")
    cpus = rec["cpus"]
    if mem_gb is not None:
        # Preserve --mem-per-cpu if the script uses it; divide total by CPUs
        if re.search(r"#SBATCH\s+--mem-per-cpu=", script_text):
            per_cpu_gb = mem_gb / max(cpus, 1)
            if per_cpu_gb >= 1:
                mem_str = f"{per_cpu_gb:.0f}G"
            else:
                mem_str = f"{per_cpu_gb * 1024:.0f}M"
            script_text = _set_directive(
                script_text,
                r"#SBATCH\s+--mem-per-cpu=\S+",
                f"#SBATCH --mem-per-cpu={mem_str}",
            )
        else:
            script_text = _set_directive(
                script_text,
                r"#SBATCH\s+--mem=\S+",
                f"#SBATCH --mem={mem_gb:.0f}G",
            )
    script_text = _set_directive(
        script_text,
        r"#SBATCH\s+--cpus-per-task=\d+",
        f"#SBATCH --cpus-per-task={cpus}",
    )
    if rec.get("time_min"):
        script_text = _set_directive(
            script_text,
            r"#SBATCH\s+--time=\S+",
            f"#SBATCH --time={_format_walltime(rec['time_min'])}",
        )
    return script_text


# ---------------------------------------------------------------------------
# hpg interactive
# ---------------------------------------------------------------------------


@cli.command("interactive")
@click.argument(
    "session_type", default="bash", type=click.Choice(["rstudio", "jupyter", "vscode", "bash"])
)
@click.option("--time", "walltime", default="04:00:00", help="Wall time (HH:MM:SS)")
@click.option("--mem", default="16GB", help="Memory request")
@click.option("--cpus", default=4, help="CPU count")
def interactive(session_type: str, walltime: str, mem: str, cpus: int) -> None:
    """Launch an interactive session with monitoring."""
    click.echo(
        f"Launching {session_type} interactive session ({mem} mem, {cpus} CPUs, {walltime})..."
    )
    click.echo("(Tip: Use 'hpg report' after your session to see resource usage.)")

    sbatch_lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name=hpg-interactive-{session_type}",
        f"#SBATCH --time={walltime}",
        f"#SBATCH --mem={mem}",
        f"#SBATCH --cpus-per-task={cpus}",
    ]
    if session_type == "rstudio":
        sbatch_lines.append("module load rstudio\nrstudio-server start")
    elif session_type == "jupyter":
        sbatch_lines.append("jupyter lab --no-browser")
    elif session_type == "vscode":
        sbatch_lines.append("code-server --bind-addr 0.0.0.0:8888")
    else:
        sbatch_lines.append("bash --login")

    script_text = "\n".join(sbatch_lines)
    ctx_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, dir="/tmp")
    json.dump({"static_tools": [session_type], "input_files": []}, ctx_file)
    ctx_file.flush()
    ctx_file.close()

    modified = _inject_sidecar(script_text, "SLURM_JOB_ID", _DEFAULT_TS_DIR, ctx_file.name)
    tmp_script = tempfile.NamedTemporaryFile(mode="w", suffix=".sbatch", delete=False, dir="/tmp")
    tmp_script.write(modified)
    tmp_script.flush()
    tmp_script.close()

    result = subprocess.run(["sbatch", tmp_script.name])
    sys.exit(result.returncode)


# ---------------------------------------------------------------------------
# hpg plot
# ---------------------------------------------------------------------------


@cli.command("plot")
@click.argument("job_id")
@click.option("--output-dir", default=_DEFAULT_PLOTS_DIR, help="Output directory for PNGs")
def plot(job_id: str, output_dir: str) -> None:
    """Generate a multi-panel time-series plot for JOB_ID."""
    _ensure_db()
    try:
        from lib.plotter import plot_job
    except ImportError as exc:
        click.echo(f"ERROR: {exc}")
        sys.exit(1)

    job_meta = get_job(job_id, db_path=_DEFAULT_DB)
    out = plot_job(
        job_id,
        timeseries_dir=_DEFAULT_TS_DIR,
        output_dir=output_dir,
        job_meta=job_meta,
    )
    if out:
        click.echo(f"Plot saved: {out}")
    else:
        click.echo(f"No time-series data found for job {job_id}.")


if __name__ == "__main__":
    cli()
