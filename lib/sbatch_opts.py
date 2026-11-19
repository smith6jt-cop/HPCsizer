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

from typing import List, NamedTuple, Optional, Sequence

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
    i, n = 0, len(tokens)
    script_index: Optional[int] = None

    while i < n:
        tok = tokens[i]
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
    return ParsedArgs(options, script_index, script, script_args, uncertain, unknown)


def strip_sbatch_word(tokens: Sequence[str]) -> List[str]:
    """Drop a leading ``sbatch`` (bare or with a path) from a submit line's tokens."""
    tokens = list(tokens)
    if tokens and tokens[0].rsplit("/", 1)[-1] == "sbatch":
        return tokens[1:]
    return tokens
