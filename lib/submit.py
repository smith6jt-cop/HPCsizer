"""``hpg submit``: sbatch, with a look at history first and a sampler inside the job.

    hpg submit [--apply | --as-is] [--yes] [--no-profile] <what you would give sbatch>

What it does, and what it deliberately does not do:

* The pre-job report goes to stderr; sbatch's own output and exit status are
  passed through untouched, so ``jid=$(hpg submit --parsable job.sh)`` works.
* By default the request is submitted as written. ``--apply`` changes memory,
  CPUs and time limit to what the job's own history supports -- by adding
  options to the command line, before the script token, because sbatch hands
  everything after the script to the script. The user's file is never edited.
* The job is profiled by one line added to a temporary copy of the script (or
  in front of a ``--wrap`` command). The line starts the sampler in the
  background and cannot fail the job; there is no exit trap and nothing runs
  when the job ends.
* Whatever goes wrong on the way -- no database, no spool, an option this code
  does not know -- the job is still submitted, exactly as given.
"""

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from lib import __version__, analyzer, sbatch_opts
from lib.classify import SAMPLER_MARK
from lib.config import Config

# Options a change replaces (the caller's own occurrences are taken out).
_REPLACES = {
    "cpus": ("cpus-per-task",),
    "mem": ("mem", "mem-per-cpu", "mem-per-gpu"),
    "time": ("time",),
}
_PASS_STRAIGHT = ("help", "usage", "version")
_JOB_ID_RE = re.compile(r"Submitted batch job (\d+)")
_PARSABLE_RE = re.compile(r"^(\d+)(?:;.*)?$")


# ---------------------------------------------------------------------------
# The sampler line
# ---------------------------------------------------------------------------


def sampler_path(cfg: Config) -> str:
    return os.path.join(cfg.root, "bin", "monitor.py")


def sampler_line(cfg: Config) -> str:
    """One shell line that starts the sampler and returns at once.

    The subshell with ``&`` inside means the sampler is not a child of the job
    script's shell: a ``wait`` in the script does not wait for it, and
    ``set -e`` never sees its exit status. It watches the script's shell
    (``$$``) and ends with it.
    """
    command = "%s exec %s -S %s --spool %s --pid $$" % (
        SAMPLER_MARK,
        shlex.quote(cfg.compute_python),
        shlex.quote(sampler_path(cfg)),
        shlex.quote(cfg.spool_dir),
    )
    if cfg.profile_perf:
        command += " --perf"
    if cfg.profile_multi_node:
        command += " --fan-out"
    return "( %s </dev/null >/dev/null 2>&1 & )" % command


def spool_problem(cfg: Config) -> Optional[str]:
    """Why jobs cannot be profiled right now, or None."""
    if not os.path.isdir(cfg.spool_dir):
        return "the spool directory %s does not exist" % cfg.spool_dir
    if not os.access(cfg.spool_dir, os.W_OK | os.X_OK):
        return "you cannot write to the spool directory %s" % cfg.spool_dir
    if not os.path.isfile(sampler_path(cfg)):
        return "the sampler %s is missing" % sampler_path(cfg)
    return None


def inject_script(text: str, line: str) -> str:
    """The script with the sampler line placed after the last ``#SBATCH`` line sbatch reads."""
    lines = text.splitlines(keepends=True)
    directives = analyzer.directive_lines(text)
    if directives:
        at = directives[-1][0] + 1
    elif lines and lines[0].startswith("#!"):
        at = 1
    else:
        at = 0
    if at > 0 and not lines[at - 1].endswith("\n"):
        lines[at - 1] += "\n"
    lines.insert(at, line + "  # added by hpg submit: records what the job uses\n")
    return "".join(lines)


def inject_wrap(command: str, line: str) -> str:
    return "%s; %s" % (line, command)


# ---------------------------------------------------------------------------
# Editing the sbatch command line
# ---------------------------------------------------------------------------


def edit_args(
    args: Sequence[str], parsed: sbatch_opts.ParsedArgs, changes: Dict[str, str]
) -> List[str]:
    """``args`` with the changed options placed where sbatch will read them.

    They go immediately before the script token (or at the end when there is
    none): after it they would be arguments of the script. The caller's own
    occurrences of the replaced options are removed, except where one token
    carries several short options at once -- sbatch lets the last one win.
    """
    if not changes:
        return list(args)
    drop: set = set()
    by_start: Dict[int, int] = {}
    for opt in parsed.options:
        by_start[opt.start] = by_start.get(opt.start, 0) + 1
    replaced = {name for key in changes for name in _REPLACES[key]}
    for opt in parsed.options:
        if opt.name in replaced and by_start[opt.start] == 1:
            drop.update(range(opt.start, opt.end))
    end = parsed.script_index if parsed.script_index is not None else len(args)
    head = [tok for index, tok in enumerate(args[:end]) if index not in drop]
    if head and head[-1] == "--":
        head = head[:-1] + [changes[k] for k in ("mem", "cpus", "time") if k in changes] + ["--"]
    else:
        head += [changes[k] for k in ("mem", "cpus", "time") if k in changes]
    return head + list(args[end:])


def replace_wrap(args: Sequence[str], parsed: sbatch_opts.ParsedArgs, command: str) -> List[str]:
    """``args`` with the value of the (last) ``--wrap`` replaced."""
    last = [opt for opt in parsed.options if opt.name == "wrap"][-1]
    new = ["--wrap=" + command] if last.end - last.start == 1 else ["--wrap", command]
    return list(args[: last.start]) + new + list(args[last.end :])


# ---------------------------------------------------------------------------
# Submission
# ---------------------------------------------------------------------------


def job_id_from(stdout: str) -> Optional[str]:
    """The job id in sbatch's output, in the normal and the ``--parsable`` form."""
    match = _JOB_ID_RE.search(stdout or "")
    if match:
        return match.group(1)
    for line in (stdout or "").splitlines():
        match = _PARSABLE_RE.match(line.strip())
        if match:
            return match.group(1)
    return None


def write_context(cfg: Config, job_id: str, payload: Dict[str, Any]) -> Optional[str]:
    """Record what was submitted, for the harvester. Returns the path, or None if it cannot.

    The folder is writable by the whole group, so whatever already sits under
    the job's name is not trusted: the record is written to a new file, created
    exclusively and without following links, and renamed into place. A link
    that another member left there is replaced or, in a sticky folder, makes
    the rename fail -- its target is never opened. The folder itself must be a
    real directory of the spool's owner; only the owner's harvest creates it.
    """
    if not str(job_id).isdigit():
        return None
    name = "%s.json" % job_id
    data = json.dumps(payload, sort_keys=True).encode("utf-8")
    spool_fd = folder_fd = fd = -1
    tmp: Optional[str] = None
    try:
        spool_fd = os.open(cfg.spool_dir, os.O_RDONLY | os.O_DIRECTORY)
        folder_fd = os.open("submit", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=spool_fd)
        if os.fstat(folder_fd).st_uid != os.fstat(spool_fd).st_uid:
            return None
        tmp = ".%s.%d.%s.tmp" % (job_id, os.getpid(), os.urandom(4).hex())
        fd = os.open(
            tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o640, dir_fd=folder_fd
        )
        os.fchmod(fd, 0o640)  # whatever the umask: the harvester must be able to read it
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view) :]
        os.close(fd)
        fd = -1
        os.replace(tmp, name, src_dir_fd=folder_fd, dst_dir_fd=folder_fd)
        tmp = None
    except OSError:
        return None
    finally:
        if fd >= 0:
            os.close(fd)
        if tmp is not None and folder_fd >= 0:
            try:
                os.unlink(tmp, dir_fd=folder_fd)
            except OSError:
                pass
        for handle in (folder_fd, spool_fd):
            if handle >= 0:
                os.close(handle)
    return os.path.join(cfg.spool_dir, "submit", name)


class Plan:
    """What ``hpg submit`` is going to hand to sbatch, and why."""

    def __init__(self, args: Sequence[str]):
        self.args: List[str] = list(args)  # as given
        self.final: List[str] = list(args)  # as submitted
        self.submission: Optional[analyzer.Submission] = None
        self.recommendation: Optional[Dict[str, Any]] = None
        self.applied: Dict[str, str] = {}
        self.profiled = False
        self.notes: List[str] = []
        self.tmpdir: Optional[str] = None

    def cleanup(self) -> None:
        if self.tmpdir:
            shutil.rmtree(self.tmpdir, ignore_errors=True)
            self.tmpdir = None


def passes_straight_through(parsed: sbatch_opts.ParsedArgs) -> Optional[str]:
    """A reason to hand the arguments to sbatch untouched, or None."""
    if any(parsed.has(name) for name in _PASS_STRAIGHT):
        return "informational option"
    if parsed.script is None and parsed.wrap is None:
        return "the script comes from standard input"
    if parsed.het:
        return "heterogeneous job"
    if parsed.uncertain:
        return "sbatch option(s) not known to hpg: %s" % " ".join(parsed.unknown)
    return None


def prepare(
    args: Sequence[str],
    cfg: Config,
    profile: bool = True,
    changes: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None,
) -> Plan:
    """Build the final sbatch arguments: apply ``changes``, add the sampler.

    Never raises for a reason that should stop a submission; what could not be
    done is listed in ``plan.notes``.
    """
    plan = Plan(args)
    parsed = sbatch_opts.parse_argv(plan.args)
    reason = passes_straight_through(parsed)
    if reason:
        plan.notes.append("submitted exactly as given, unprofiled: %s" % reason)
        return plan
    try:
        sub = analyzer.analyze_submission(plan.args, cwd=cwd, roots=cfg.data_roots)
    except OSError as exc:
        plan.notes.append("submitted exactly as given: %s" % exc)
        return plan
    plan.submission = sub

    final = list(plan.args)
    if changes:
        final = edit_args(final, parsed, changes)
        plan.applied = dict(changes)
        parsed = sbatch_opts.parse_argv(final)

    if profile:
        problem = spool_problem(cfg)
        if problem:
            plan.notes.append("not profiled: %s" % problem)
        elif parsed.has("test-only"):
            pass
        elif sub.wrap is not None:
            final = replace_wrap(final, parsed, inject_wrap(sub.wrap, sampler_line(cfg)))
            plan.profiled = True
        elif not sub.shell:
            plan.notes.append("not profiled: the script is not run by sh or bash")
        else:
            try:
                text = analyzer.read_text(sub.script_path)
                if text is None:
                    raise OSError("cannot read %s" % sub.script_path)
                if analyzer.looks_up_its_own_path(text):
                    raise OSError(
                        "the script reads its own path from 'scontrol show job', "
                        "which would name the temporary copy that carries the sampler"
                    )
                plan.tmpdir = tempfile.mkdtemp(prefix="hpg-")
                copy = os.path.join(plan.tmpdir, os.path.basename(sub.script_path))
                with open(copy, "w") as handle:
                    handle.write(inject_script(text, sampler_line(cfg)))
                final[parsed.script_index] = copy
                plan.profiled = True
            except OSError as exc:
                plan.cleanup()
                plan.notes.append("not profiled: %s" % exc)
    plan.final = final
    return plan


def context_payload(
    plan: Plan, job_id: str, user: str, now: Optional[int] = None
) -> Dict[str, Any]:
    sub = plan.submission
    payload: Dict[str, Any] = {
        "job_id": job_id,
        "submitted_ts": int(time.time()) if now is None else int(now),
        "user": user,
        "cwd": os.getcwd(),
        "hpcsizer": __version__,
        "profiled": plan.profiled,
        "applied": plan.applied,
    }
    if sub is not None:
        payload.update(sub.context())
        payload["request"] = sub.request.to_dict()
        payload["array"] = sub.request.array
    rec = plan.recommendation
    if rec is not None:
        payload["suggested"] = {
            "source": rec["source"],
            "options": rec["options"],
            "changes": rec["changes"],
            "confidence": rec["confidence"]["level"],
        }
    return payload


def run_sbatch(
    cfg: Config,
    plan: Plan,
    user: str,
    out=None,
    runner: Callable[..., Any] = subprocess.run,
) -> int:
    """Run sbatch with the plan's arguments. Its stdout and exit status are the caller's."""
    out = sys.stdout if out is None else out
    try:
        done = runner([cfg.sbatch] + plan.final, stdout=subprocess.PIPE, universal_newlines=True)
    except FileNotFoundError:
        print("hpg submit: '%s' not found; is Slurm on PATH?" % cfg.sbatch, file=sys.stderr)
        return 127
    finally:
        plan.cleanup()
    out.write(done.stdout or "")
    out.flush()
    if done.returncode == 0 and plan.submission is not None:
        job_id = job_id_from(done.stdout or "")
        if job_id and not sbatch_opts.parse_argv(plan.final).has("test-only"):
            if spool_problem(cfg) is None:
                write_context(cfg, job_id, context_payload(plan, job_id, user))
    return int(done.returncode)


def describe(plan: Plan, rec: Optional[Dict[str, Any]], mode: str) -> List[str]:
    """The lines after the report: what is being done with it."""
    lines: List[str] = []
    if plan.applied:
        applied = [plan.applied[key] for key in ("mem", "cpus", "time") if key in plan.applied]
        lines.append("Submitting with: %s" % " ".join(applied))
    elif rec is not None and rec.get("changes") and mode == "advise":
        how = "hpg submit --apply ..." if rec.get("can_apply") else None
        lines.append(
            "Submitting as requested.%s"
            % (" To use the suggested request: %s" % how if how else "")
        )
    elif mode == "apply" and rec is not None:
        blockers = rec.get("apply_blockers") or []
        why = "; ".join(blockers) if blockers else "the request already fits its history"
        lines.append("Submitting as requested: nothing to apply (%s)." % why)
    lines.extend(plan.notes)
    if plan.profiled:
        lines.append("The job is profiled; afterwards: hpg job <jobid>")
    return lines


def ask(prompt: str, stream=None, reader: Callable[[], str] = input) -> str:
    """One line from the terminal, lower-cased; empty on end of input."""
    stream = sys.stderr if stream is None else stream
    stream.write(prompt)
    stream.flush()
    try:
        return reader().strip().lower()
    except EOFError:
        return ""


def interactive() -> bool:
    return sys.stdin.isatty() and sys.stderr.isatty()


def split_changes(rec: Optional[Dict[str, Any]]) -> Tuple[Dict[str, str], List[str]]:
    """``(changes that may be applied, reasons nothing may be)`` of a recommendation."""
    if rec is None:
        return {}, ["no recommendation is available"]
    if not rec.get("can_apply"):
        return {}, list(rec.get("apply_blockers") or [])
    return dict(rec["changes"]), []
