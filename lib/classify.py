"""What kind of job is this, and which earlier jobs is it comparable to?

The previous analytics matched jobs by tools detected in an sbatch script, a
field only ``hpg submit`` could fill and no stored job had. A job family is
derived from what Slurm records for every job: the user, the job name, and the
submit line.

Classification is computed when a report is produced and is never stored, so
changing a rule or a configured pattern takes effect on all history at once.

Kinds
-----
``ood:<app>``   Open OnDemand session (job name ``.../sys/dashboard/sys/<app>``)
``<session>``   a configured session name, by default ``vscode``
``interactive`` salloc/srun session (has an ``.interactive`` step)
``wrap``        ``sbatch --wrap "command"``
``batch``       everything else

Session kinds are sized differently from batch work: they end when the user
stops or when the limit cuts them off, so a timeout is not a failure.
"""

import os
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence

from lib import sbatch_opts
from lib.config import Config
from lib.fmt import clean

_OOD_MARK = "sys/dashboard/"
_ENV_ASSIGN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
_DIGITS_RE = re.compile(r"\d+")
_SEPARATORS = ("&&", "||", ";", "|", "&")
_BRACKETS = frozenset("(){}")
# What `hpg submit` puts in front of a --wrap command to start the in-job sampler.
SAMPLER_MARK = "HPCSIZER=sampler"

_SAMPLER_PREFIX = re.compile(r"\(\s*" + re.escape(SAMPLER_MARK) + r"\b.*?&\s*\)\s*;?\s*")
# The temporary copy of a script that `hpg submit` hands to sbatch: <tmp>/hpg-XXXXXXXX/<name>
_SCRIPT_COPY = re.compile(r"(?<!\S)\S*/hpg-[A-Za-z0-9_]{8}/(?=[^/\s])")


def shown_command(submit_line: str) -> str:
    """A stored submit line without what ``hpg submit`` added to it.

    Accounting records the command as sbatch received it: with the line that
    starts the sampler in front of a ``--wrap`` command, or with the path of
    the temporary script copy. Neither is what the user typed.
    """
    return _SCRIPT_COPY.sub("", _SAMPLER_PREFIX.sub("", submit_line))


# Launchers that only run something else; the program is what follows them.
_PASS_THROUGH = frozenset(
    "srun time nice nohup stdbuf exec command env timeout ionice taskset numactl xvfb-run".split()
)
# Shell housekeeping that precedes the real command in a wrapped line.
_PRELUDE = frozenset(
    "module ml source . cd export set unset ulimit umask echo printf mkdir date hostname true :".split()
)
_ENV_MANAGERS = frozenset("conda mamba micromamba".split())
_CONTAINERS = frozenset("apptainer singularity".split())
_SHELLS = frozenset("bash sh zsh ksh".split())
_INTERPRETER_RE = re.compile(
    r"^(python|rscript|r|perl|julia|matlab|octave|node|java|ruby|nextflow|snakemake)[0-9.]*$"
)
_SCRIPT_SUFFIXES = (".py", ".r", ".sh", ".jl", ".pl", ".m", ".nf", ".smk", ".rb", ".js", ".ipynb")
# Options of pass-through launchers and containers that consume the next token.
_VALUE_OPTIONS = frozenset(
    "-n -N -c -t -p -A -q -J -w -x -B -H -W -G --bind --home --pwd --env --overlay "
    "--ntasks --nodes --cpus-per-task --time --mem --partition --account --qos --gres".split()
)


def collapse_digits(text: str) -> str:
    """``mplx12k-spleen`` -> ``mplx#k-spleen``: runs that differ only by a number are one family."""
    return _DIGITS_RE.sub("#", text)


def _clean(token: str) -> str:
    return token.strip("'\"")


def _segments(tokens: Sequence[str]) -> List[List[str]]:
    """Split a command line at shell separators (quotes are not preserved in SubmitLine)."""
    out: List[List[str]] = [[]]
    for tok in tokens:
        tok = _clean(tok)
        if not tok:
            continue
        if tok in _SEPARATORS:
            out.append([])
            continue
        trailing = tok.endswith(";") and len(tok) > 1
        word = tok.rstrip(";") if trailing else tok
        if word and not set(word) <= _BRACKETS:  # "(", ")" and "{" group commands, nothing more
            out[-1].append(word)
        if trailing:
            out.append([])
    return [seg for seg in out if seg and SAMPLER_MARK not in seg]


def _skip_options(tokens: Sequence[str], i: int) -> int:
    while i < len(tokens) and tokens[i].startswith("-"):
        i += 2 if tokens[i] in _VALUE_OPTIONS and "=" not in tokens[i] else 1
    return i


def _segment_signature(tokens: Sequence[str]) -> Optional[str]:
    """The program a command segment runs: ``python:segment.py``, ``Rscript:fit.R``, ``cellranger``."""
    i, n = 0, len(tokens)
    while i < n:
        tok = tokens[i]
        if _ENV_ASSIGN_RE.match(tok):
            i += 1
            continue
        base = os.path.basename(tok)
        low = base.lower()
        if low in _PASS_THROUGH:
            i = _skip_options(tokens, i + 1)
            if low in ("timeout", "taskset") and i < n:
                i += 1  # their first positional argument is a duration / mask
            continue
        if low in _ENV_MANAGERS and i + 1 < n and tokens[i + 1] == "run":
            i = _skip_options_conda(tokens, i + 2)
            continue
        if low in _CONTAINERS and i + 1 < n and tokens[i + 1] in ("exec", "run", "shell"):
            i = _skip_options(tokens, i + 2) + 1  # options, then the image
            continue
        if low in _SHELLS and i + 1 < n and tokens[i + 1] in ("-c", "-lc", "-ic", "-l"):
            i += 2
            continue
        match = _INTERPRETER_RE.match(low)
        if match or low in _SHELLS:
            name = "Rscript" if low in ("rscript", "r") else (match.group(1) if match else low)
            j = i + 1
            while j < n:
                arg = tokens[j]
                if arg == "-m" and j + 1 < n:
                    return "%s:%s" % (name, tokens[j + 1])
                if arg in ("-c", "-e", "-r", "-batch"):
                    return "%s:%s" % (name, arg)
                if arg == "-jar" and j + 1 < n:
                    return "%s:%s" % (name, os.path.basename(tokens[j + 1]))
                if name == "nextflow" and arg == "run" and j + 1 < n:
                    return "%s:%s" % (name, os.path.basename(tokens[j + 1]))
                if arg.startswith("-"):
                    j += 1
                    continue
                return "%s:%s" % (name, os.path.basename(arg))
            return name
        return base
    return None


def _skip_options_conda(tokens: Sequence[str], i: int) -> int:
    while i < len(tokens) and tokens[i].startswith("-"):
        i += 2 if tokens[i] in ("-n", "--name", "-p", "--prefix") else 1
    return i


def command_signature(tokens: Sequence[str]) -> Optional[str]:
    """Program signature of a wrapped command line, skipping shell preludes.

    ``module load R && Rscript fit.R --seed 3`` -> ``Rscript:fit.R``. Arguments
    never enter the signature: they are what varies between runs of one family.
    """
    fallback: Optional[str] = None
    for seg in _segments(tokens):
        sig = _segment_signature(seg)
        if not sig:
            continue
        program = sig.split(":", 1)[0].lower()
        if program in _PRELUDE or program in _ENV_MANAGERS:
            continue  # "module load x", "conda activate y": setup, not the job
        if program == "sleep":
            fallback = fallback or sig  # a real command may still follow
            continue
        return sig
    return fallback


def submit_signature(submit_line: Optional[str]) -> Optional[str]:
    """Signature from a stored SubmitLine: the wrapped program, or the script's file name."""
    if not submit_line:
        return None
    tokens = sbatch_opts.strip_sbatch_word(submit_line.split())
    if not tokens:
        return None
    for index, tok in enumerate(tokens):
        if tok == "--wrap":
            return command_signature(tokens[index + 1 :])
        if tok.startswith("--wrap="):
            return command_signature([tok[len("--wrap=") :]] + list(tokens[index + 1 :]))
    parsed = sbatch_opts.parse_argv(tokens)
    if parsed.script:
        return os.path.basename(_clean(parsed.script))
    return None


def is_wrap(submit_line: Optional[str]) -> bool:
    return bool(submit_line) and any(
        tok == "--wrap" or tok.startswith("--wrap=") for tok in submit_line.split()
    )


def job_kind(job: Mapping[str, Any], cfg: Config) -> str:
    name = (job.get("job_name") or "").strip()
    if _OOD_MARK in name:
        return "ood:" + name.rstrip("/").rsplit("/", 1)[-1]
    low = name.lower()
    for session in cfg.session_names:
        if low == session.lower():
            return session.lower()
    if low.startswith("hpg-interactive"):
        return "interactive"
    submit_line = job.get("submit_line") or ""
    first = os.path.basename(submit_line.split()[0]) if submit_line.split() else ""
    if job.get("has_interactive_step") or first in ("salloc", "srun"):
        return "interactive"
    if is_wrap(submit_line):
        return "wrap"
    return "batch"


def is_session_kind(kind: str, cfg: Config) -> bool:
    return (
        kind.startswith("ood:")
        or kind == "interactive"
        or kind in {s.lower() for s in cfg.session_names}
    )


def classify(job: Mapping[str, Any], cfg: Config) -> Dict[str, Any]:
    """Kind, family and the keys used to find comparable jobs.

    ``family`` is ``user/kind/name_key`` (plus ``+gpu`` for GPU jobs, which
    separates e.g. a CPU and a GPU variant submitted under one name).
    ``name_exact`` and ``signature`` are the narrower and broader keys the
    recommender falls back through.
    """
    kind = job_kind(job, cfg)
    session = is_session_kind(kind, cfg)
    name = (job.get("job_name") or "").strip()
    signature = submit_signature(job.get("submit_line"))
    generic = name.lower() in {g.lower() for g in cfg.generic_names}

    if session:
        name_key = "-"
    elif (kind == "wrap" or generic or not name) and signature:
        name_key = "cmd:" + collapse_digits(signature)
    else:
        name_key = collapse_digits(name) if name else "(unnamed)"

    gpu = bool((job.get("alloc_gpus") or 0) > 0 or (job.get("req_gpus") or 0) > 0)
    user = job.get("user") or "?"
    family = "%s/%s" % (user, kind) if session else "%s/%s/%s" % (user, kind, name_key)
    if gpu:
        family += "+gpu"
    return {
        "kind": kind,
        "session": session,
        "name_key": name_key,
        "name_exact": name,
        "signature": collapse_digits(signature) if signature else None,
        "gpu": gpu,
        "array": job.get("array_job_id") is not None,
        "family": family,
    }


# Text that whoever submits a job writes, or that comes from inside the job.
_TEXT_FIELDS = (
    "job_name",
    "submit_line",
    "work_dir",
    "node_list",
    "state_raw",
    "user",
    "account",
    "partition",
    "qos",
    "mon_top_comm",
)


def annotate(jobs: Sequence[Dict[str, Any]], cfg: Config) -> Sequence[Dict[str, Any]]:
    """Add the classification keys to each job dict in place.

    This is where stored rows become what the reports work with, so it is also
    where their free text is made safe to show (``fmt.clean``): once, before a
    family name is derived from it, so that every report, table and suggested
    command sees the same harmless spelling. The database keeps what Slurm
    reported.
    """
    for job in jobs:
        for name in _TEXT_FIELDS:
            value = job.get(name)
            if isinstance(value, str):
                job[name] = clean(value)
        job.update(classify(job, cfg))
    return jobs
