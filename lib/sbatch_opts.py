"""sbatch's command-line grammar: enough to find the script and edit options safely.

Two callers need this. Classification reads a stored SubmitLine to find which
script or wrapped command a job ran. ``hpg submit --apply`` must put its
options *before* the script token, because sbatch stops parsing options there
and hands everything after it to the script -- appending ``--mem=8G`` to
``sbatch job.sh`` would silently pass ``--mem=8G`` to ``job.sh``.

The tables follow the sbatch man page (Slurm 25.11). An option that is not in
them and carries no ``=value`` has unknown arity; the result is then marked
``uncertain`` and callers that would edit the command line must decline.
"""

import math
import re
from typing import List, NamedTuple, Optional, Sequence, Tuple

# Short options that take a value (attached "-c8" or separate "-c 8").
SHORT_WITH_ARG = frozenset("AabBcCdDeFGiJLmMnNopqStwx")
# Short flags. "-k" (no-kill) takes a value only when attached.
SHORT_FLAGS = frozenset("hHkOQsvVW")

SHORT_TO_LONG = {
    "A": "account",
    "a": "array",
    "b": "begin",
    "B": "extra-node-info",
    "c": "cpus-per-task",
    "C": "constraint",
    "d": "dependency",
    "D": "chdir",
    "e": "error",
    "F": "nodefile",
    "G": "gpus",
    "i": "input",
    "J": "job-name",
    "L": "licenses",
    "m": "distribution",
    "M": "clusters",
    "n": "ntasks",
    "N": "nodes",
    "o": "output",
    "p": "partition",
    "q": "qos",
    "S": "core-spec",
    "t": "time",
    "w": "nodelist",
    "x": "exclude",
    "h": "help",
    "H": "hold",
    "k": "no-kill",
    "O": "overcommit",
    "Q": "quiet",
    "s": "oversubscribe",
    "v": "verbose",
    "V": "version",
    "W": "wait",
}

LONG_WITH_ARG = frozenset(
    """
    account acctg-freq array batch bb bbf begin chdir cluster-constraint clusters comment
    constraint container container-id core-spec cores-per-socket cpu-freq cpus-per-gpu
    cpus-per-task deadline delay-boot dependency distribution error exclude export export-file
    extra extra-node-info gid gpu-bind gpu-freq gpus gpus-per-node gpus-per-socket
    gpus-per-task gres gres-flags hint input job-name kill-on-invalid-dep licenses mail-type
    mail-user mcs-label mem mem-bind mem-per-cpu mem-per-gpu mincpus network nodefile nodelist
    nodes ntasks ntasks-per-core ntasks-per-gpu ntasks-per-node ntasks-per-socket open-mode
    output partition power prefer priority profile qos reservation segment signal
    sockets-per-node switches thread-spec threads-per-core time time-min tmp tres-bind
    tres-per-task uid wait-all-nodes wckey wrap
    """.split()
)

LONG_FLAGS = frozenset(
    """
    contiguous help hold ignore-pbs no-requeue overcommit oversubscribe parsable quiet reboot
    requeue spread-job stepmgr test-only usage use-min-nodes verbose version wait
    """.split()
)

# A value is only recognised in the attached form (--name=value).
LONG_OPTIONAL = frozenset(
    "exclusive get-user-env nice no-kill oom-kill-step propagate resv-ports".split()
)


class Option(NamedTuple):
    name: str  # canonical long name, without leading dashes
    value: Optional[str]
    start: int  # index of the first token of this option
    end: int  # one past its last token


class ParsedArgs(NamedTuple):
    options: List[Option]
    script_index: Optional[int]  # index of the script token; None for --wrap or stdin
    script: Optional[str]
    script_args: List[str]
    uncertain: bool  # an option of unknown arity was seen
    unknown: List[str]
    het: bool = False  # heterogeneous job: components separated by ":"

    def get(self, name: str) -> Optional[str]:
        """Value of the last occurrence of an option (sbatch lets the last one win)."""
        value = None
        for opt in self.options:
            if opt.name == name:
                value = opt.value if opt.value is not None else ""
        return value

    def has(self, name: str) -> bool:
        return any(opt.name == name for opt in self.options)

    @property
    def wrap(self) -> Optional[str]:
        return self.get("wrap")


def parse_argv(tokens: Sequence[str]) -> ParsedArgs:
    """Parse sbatch arguments (without the leading ``sbatch`` word)."""
    options: List[Option] = []
    unknown: List[str] = []
    uncertain = False
    het = False
    i, n = 0, len(tokens)
    script_index: Optional[int] = None

    while i < n:
        tok = tokens[i]
        if tok == ":":
            # "sbatch -c 4 : -c 1 job.sh": the options of a second component follow.
            het = True
            i += 1
            continue
        if tok == "--":
            if i + 1 < n:
                script_index = i + 1
            break
        if tok.startswith("--") and len(tok) > 2:
            name, eq, value = tok[2:].partition("=")
            if eq:
                options.append(Option(name, value, i, i + 1))
                if (
                    name not in LONG_WITH_ARG
                    and name not in LONG_OPTIONAL
                    and name not in LONG_FLAGS
                ):
                    unknown.append(tok)
                i += 1
            elif name in LONG_WITH_ARG:
                if i + 1 >= n:
                    options.append(Option(name, None, i, i + 1))
                    uncertain = True
                    i += 1
                else:
                    options.append(Option(name, tokens[i + 1], i, i + 2))
                    i += 2
            elif name in LONG_FLAGS or name in LONG_OPTIONAL:
                options.append(Option(name, None, i, i + 1))
                i += 1
            else:
                # Could be a flag or take the next token: arity unknown.
                options.append(Option(name, None, i, i + 1))
                unknown.append(tok)
                uncertain = True
                i += 1
            continue
        if tok.startswith("-") and len(tok) > 1:
            consumed = 1
            j = 1
            while j < len(tok):
                ch = tok[j]
                if ch in SHORT_WITH_ARG:
                    rest = tok[j + 1 :]
                    if rest:
                        options.append(Option(SHORT_TO_LONG[ch], rest.lstrip("="), i, i + 1))
                    elif i + 1 < n:
                        options.append(Option(SHORT_TO_LONG[ch], tokens[i + 1], i, i + 2))
                        consumed = 2
                    else:
                        options.append(Option(SHORT_TO_LONG[ch], None, i, i + 1))
                        uncertain = True
                    break
                if ch in SHORT_FLAGS:
                    options.append(Option(SHORT_TO_LONG[ch], None, i, i + 1))
                    if ch == "k":
                        break  # an attached value, if any, belongs to -k
                    j += 1
                    continue
                unknown.append(tok)
                uncertain = True
                break
            i += consumed
            continue
        script_index = i
        break

    script = tokens[script_index] if script_index is not None else None
    script_args = list(tokens[script_index + 1 :]) if script_index is not None else []
    return ParsedArgs(options, script_index, script, script_args, uncertain, unknown, het)


def strip_sbatch_word(tokens: Sequence[str]) -> List[str]:
    """Drop a leading ``sbatch`` (bare or with a path) from a submit line's tokens."""
    tokens = list(tokens)
    if tokens and tokens[0].rsplit("/", 1)[-1] == "sbatch":
        return tokens[1:]
    return tokens


# ---------------------------------------------------------------------------
# Option values
# ---------------------------------------------------------------------------

_MEM_RE = re.compile(r"^(\d+(?:\.\d+)?)([KMGT])?B?$", re.IGNORECASE)
_MEM_TO_MB = {"K": 1.0 / 1024, "M": 1.0, "G": 1024.0, "T": 1024.0 * 1024}
_UNLIMITED = frozenset({"UNLIMITED", "INFINITE", "-1"})


def parse_mem(spec: Optional[str]) -> Optional[float]:
    """Megabytes from ``--mem`` / ``--mem-per-cpu`` / ``--mem-per-gpu`` (default unit: M).

    ``0`` is returned as 0.0: for ``--mem`` it means "all memory of the node",
    which callers must treat as a request of unknown size, not as zero.
    """
    if spec is None:
        return None
    match = _MEM_RE.match(spec.strip())
    if not match:
        return None
    return float(match.group(1)) * _MEM_TO_MB[(match.group(2) or "M").upper()]


def format_mem(mb: float) -> str:
    """A whole number with a unit sbatch accepts (``13G``, ``768M``); never ``0``."""
    mb = max(int(math.ceil(mb)), 1)
    if mb % 1024 == 0:
        return "%dG" % (mb // 1024)
    return "%dM" % mb


def parse_time(spec: Optional[str]) -> Tuple[Optional[int], bool]:
    """``(minutes, unlimited)`` from an sbatch time limit.

    Accepts every form sbatch documents: ``minutes``, ``minutes:seconds``,
    ``hours:minutes:seconds``, ``days-hours``, ``days-hours:minutes`` and
    ``days-hours:minutes:seconds``. Seconds round up to the next minute, as
    Slurm does. ``UNLIMITED``, ``INFINITE`` and ``0`` mean no limit. An
    unreadable value gives ``(None, False)``.
    """
    if spec is None:
        return None, False
    text = spec.strip()
    if text.upper() in _UNLIMITED:
        return None, True
    days_text, dash, rest = text.partition("-")
    if not dash:
        days_text, rest = "", text
    parts = rest.split(":")
    if not all(p.isdigit() for p in parts) or (dash and not days_text.isdigit()):
        return None, False
    numbers = [int(p) for p in parts]
    if dash:
        if len(numbers) > 3:
            return None, False
        hours, minutes, seconds = (numbers + [0, 0])[:3]
    elif len(numbers) == 1:
        hours, minutes, seconds = 0, numbers[0], 0
    elif len(numbers) == 2:
        hours, (minutes, seconds) = 0, numbers
    elif len(numbers) == 3:
        hours, minutes, seconds = numbers
    else:
        return None, False
    total = int(days_text or 0) * 86400 + hours * 3600 + minutes * 60 + seconds
    if total == 0:
        return None, True
    return (total + 59) // 60, False


def format_time(minutes: float) -> str:
    """``D-HH:MM:SS`` (``HH:MM:SS`` under a day) for a limit in minutes; at least one minute."""
    minutes = max(int(math.ceil(minutes)), 1)
    days, rest = divmod(minutes, 1440)
    hours, mins = divmod(rest, 60)
    if days:
        return "%d-%02d:%02d:00" % (days, hours, mins)
    return "%02d:%02d:00" % (hours, mins)


def parse_count(spec: Optional[str]) -> Optional[int]:
    """A node or task count; ``min-max`` ranges give the minimum, ``2k`` is 2048."""
    if spec is None:
        return None
    text = spec.strip().split("-", 1)[0].split(",", 1)[0]
    scale = 1
    if text[-1:].lower() == "k":
        text, scale = text[:-1], 1024
    elif text[-1:].lower() == "m":
        text, scale = text[:-1], 1024 * 1024
    return int(text) * scale if text.isdigit() else None


def parse_gpus(spec: Optional[str]) -> Tuple[int, Optional[str]]:
    """``(count, type)`` from ``--gpus``-style values: ``2``, ``a100:2``, ``a100``."""
    if not spec:
        return 0, None
    parts = [p for p in spec.strip().split(":") if p]
    if not parts:
        return 0, None
    if parts[-1].isdigit():
        return int(parts[-1]), (parts[-2] if len(parts) > 1 else None)
    return 1, parts[-1]


def parse_gres_gpus(spec: Optional[str]) -> Tuple[int, Optional[str]]:
    """GPUs per node from ``--gres``: ``gpu``, ``gpu:2``, ``gpu:a100:2``, ``gpu:1,lustre:1``."""
    count, kind = 0, None
    for item in (spec or "").split(","):
        parts = [p for p in item.strip().split(":") if p]
        if not parts or parts[0].rsplit("/", 1)[-1] != "gpu":
            continue
        n, k = parse_gpus(":".join(parts[1:])) if len(parts) > 1 else (1, None)
        count += n
        kind = kind or k
    return count, kind
