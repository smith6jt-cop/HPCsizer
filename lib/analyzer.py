"""What a job is about to ask for: its request, its program and its inputs.

Read before submission (``hpg recommend``, ``hpg submit``) from the sbatch
command line and the ``#SBATCH`` lines of the script, following sbatch's rules:

* directives are read until the first line that is not a comment;
* an unquoted ``#`` ends a directive line;
* options on the command line override directives;
* ``--mem`` is per node, ``--mem-per-cpu`` is multiplied by the CPUs of the job
  and ``--mem-per-gpu`` by its GPUs. The product is what Slurm reports as the
  request of a finished job (``mem`` in ``ReqTRES``), so requests read here and
  requests stored from accounting are comparable.

Tools and input sizes feed the two fallback tiers of the recommender (a fit of
memory against input size, and the cold-start table); they are not needed for
a job that has history.
"""

import glob
import hashlib
import os
import re
import shlex
import stat
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from lib import sbatch_opts
from lib.classify import command_signature
from lib.config import DEFAULT_DATA_ROOTS

GIB = 1024.0**3
MAX_SCRIPT_BYTES = 2 * 1024 * 1024
MAX_FOLLOWED_SCRIPTS = 8
MAX_GLOB_MATCHES = 2000

# ---------------------------------------------------------------------------
# The request
# ---------------------------------------------------------------------------

_MEM_OPTIONS = ("mem", "mem-per-cpu", "mem-per-gpu")
_GPU_OPTIONS = ("gpus", "gres", "gpus-per-node", "gpus-per-task", "gpus-per-socket")


@dataclass
class Request:
    """Resources asked for, as written. Totals are derived by the properties."""

    job_name: Optional[str] = None
    cpus_per_task: Optional[int] = None
    ntasks: Optional[int] = None
    nodes: Optional[int] = None
    mem_form: Optional[str] = None  # "mem" | "mem-per-cpu" | "mem-per-gpu"
    mem_value_mb: Optional[float] = None  # the value of that option, in MB
    time_min: Optional[int] = None
    time_unlimited: bool = False
    gpus: int = 0
    gpu_type: Optional[str] = None
    array: Optional[str] = None
    partition: Optional[str] = None
    qos: Optional[str] = None
    account: Optional[str] = None
    het: bool = False
    problems: List[str] = field(default_factory=list)

    @property
    def cpus(self) -> int:
        """CPUs of the whole job (tasks x CPUs per task)."""
        return max(self.cpus_per_task or 1, 1) * max(self.ntasks or 1, 1)

    @property
    def multi(self) -> bool:
        """More than one task or node: a per-task CPU count cannot be resized safely."""
        return (self.ntasks or 1) > 1 or (self.nodes or 1) > 1 or self.het

    @property
    def mem_all(self) -> bool:
        """``--mem=0``: every byte of the node, an amount that is not known here."""
        return self.mem_form == "mem" and self.mem_value_mb == 0

    @property
    def mem_mb(self) -> Optional[float]:
        """Memory of the whole job in MB, or None when not stated or not computable."""
        if self.mem_form is None or self.mem_value_mb is None or self.mem_value_mb <= 0:
            return None
        if self.mem_form == "mem":
            return self.mem_value_mb * max(self.nodes or 1, 1)
        if self.mem_form == "mem-per-cpu":
            return self.mem_value_mb * self.cpus
        if self.mem_form == "mem-per-gpu":
            return self.mem_value_mb * self.gpus if self.gpus else None
        return None

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out.update(cpus=self.cpus, mem_mb=self.mem_mb, multi=self.multi)
        return out


def _int(value: Optional[str]) -> Optional[int]:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def request_from_options(*sources: Mapping[str, Optional[str]]) -> Request:
    """Build a request from option mappings, lowest precedence first.

    Pass the script's directives, then the command line: a later source
    overrides an earlier one, and a memory or GPU option in a later source
    replaces every memory or GPU option of the earlier ones (they are
    alternative ways to state one request).
    """
    merged: Dict[str, Optional[str]] = {}
    for source in sources:
        if any(name in source for name in _MEM_OPTIONS):
            for name in _MEM_OPTIONS:
                merged.pop(name, None)
        if any(name in source for name in _GPU_OPTIONS):
            for name in _GPU_OPTIONS:
                merged.pop(name, None)
        merged.update(source)

    req = Request()
    req.job_name = merged.get("job-name") or None
    req.array = merged.get("array") or None
    req.partition = merged.get("partition") or None
    req.qos = merged.get("qos") or None
    req.account = merged.get("account") or None
    req.het = "hetjob" in merged

    if "cpus-per-task" in merged:
        req.cpus_per_task = _int(merged["cpus-per-task"])
        if req.cpus_per_task is None:
            req.problems.append("--cpus-per-task=%s is not a number" % merged["cpus-per-task"])
    if "nodes" in merged:
        req.nodes = sbatch_opts.parse_count(merged["nodes"])
    if "ntasks" in merged:
        req.ntasks = sbatch_opts.parse_count(merged["ntasks"])
    elif "ntasks-per-node" in merged:
        per_node = sbatch_opts.parse_count(merged["ntasks-per-node"])
        if per_node:
            req.ntasks = per_node * max(req.nodes or 1, 1)

    counts: List[Tuple[int, Optional[str]]] = []
    if "gpus" in merged:
        counts.append(sbatch_opts.parse_gpus(merged["gpus"]))
    if "gres" in merged:
        n, kind = sbatch_opts.parse_gres_gpus(merged["gres"])
        counts.append((n * max(req.nodes or 1, 1), kind))
    if "gpus-per-node" in merged:
        n, kind = sbatch_opts.parse_gpus(merged["gpus-per-node"])
        counts.append((n * max(req.nodes or 1, 1), kind))
    if "gpus-per-task" in merged:
        n, kind = sbatch_opts.parse_gpus(merged["gpus-per-task"])
        counts.append((n * max(req.ntasks or 1, 1), kind))
    if "gpus-per-socket" in merged:
        n, kind = sbatch_opts.parse_gpus(merged["gpus-per-socket"])
        counts.append((n, kind))
        req.problems.append("--gpus-per-socket: the GPU count depends on the node")
    if counts:
        req.gpus = max(n for n, _kind in counts)
        req.gpu_type = next((kind for _n, kind in counts if kind), None)

    for name in _MEM_OPTIONS:
        if name in merged:
            value = sbatch_opts.parse_mem(merged[name])
            if value is None:
                req.problems.append("--%s=%s is not a memory size" % (name, merged[name]))
            else:
                req.mem_form, req.mem_value_mb = name, value

    if "time" in merged:
        req.time_min, req.time_unlimited = sbatch_opts.parse_time(merged["time"])
        if req.time_min is None and not req.time_unlimited:
            req.problems.append("--time=%s is not a time limit" % merged["time"])
    return req


def options_dict(parsed: sbatch_opts.ParsedArgs) -> Dict[str, Optional[str]]:
    """Option name -> value, the last occurrence winning as in sbatch."""
    out: Dict[str, Optional[str]] = {}
    for opt in parsed.options:
        out[opt.name] = opt.value if opt.value is not None else ""
    if parsed.het:
        out["hetjob"] = ""
    return out


# ---------------------------------------------------------------------------
# #SBATCH directives
# ---------------------------------------------------------------------------

_DIRECTIVE = "#SBATCH"


def _directive_tokens(rest: str) -> List[str]:
    try:
        return shlex.split(rest, comments=True)
    except ValueError:  # unbalanced quote: read what can be read
        return rest.split("#", 1)[0].split()


def directive_lines(text: str) -> List[Tuple[int, str]]:
    """``(line_index, arguments)`` of every ``#SBATCH`` line sbatch would read.

    sbatch stops at the first line that is neither blank nor a comment, so a
    ``#SBATCH`` line below the first command is ignored -- by sbatch and here.
    """
    found: List[Tuple[int, str]] = []
    for index, line in enumerate(text.splitlines()):
        if line.startswith(_DIRECTIVE) and line[len(_DIRECTIVE) : len(_DIRECTIVE) + 1] in (
            "",
            " ",
            "\t",
        ):
            found.append((index, line[len(_DIRECTIVE) :]))
            continue
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            break
    return found


def parse_sbatch_directives(text: str) -> Dict[str, Optional[str]]:
    """Options set by a script's ``#SBATCH`` lines (name -> value, last wins)."""
    options: Dict[str, Optional[str]] = {}
    for _index, rest in directive_lines(text):
        tokens = _directive_tokens(rest)
        if tokens in (["hetjob"], ["packjob"]):
            options["hetjob"] = ""
            continue
        options.update(options_dict(sbatch_opts.parse_argv(tokens)))
    return options


def ignored_directives(text: str) -> List[int]:
    """1-based numbers of ``#SBATCH`` lines below the first command, which sbatch ignores."""
    read = {index for index, _rest in directive_lines(text)}
    return [
        index + 1
        for index, line in enumerate(text.splitlines())
        if line.startswith(_DIRECTIVE) and index not in read
    ]


# ---------------------------------------------------------------------------
# Language and tools
# ---------------------------------------------------------------------------

_SHEBANG_LANG = {
    "python": "python",
    "Rscript": "R",
    "perl": "perl",
    "bash": "bash",
    "sh": "bash",
}

_MODULE_LANG = {
    re.compile(r"module load\s+R/"): "R",
    re.compile(r"module load\s+python"): "python",
    re.compile(r"conda activate"): "python",
    re.compile(r"Rscript\s"): "R",
    re.compile(r"\bpython3?\s"): "python",
}

_SUFFIX_LANG = {".py": "python", ".r": "R", ".rmd": "R", ".qmd": "R", ".sh": "bash", ".pl": "perl"}


def detect_language(text: str) -> str:
    """Primary language of a script.

    Content (module loads, interpreter calls) takes priority over the shebang:
    sbatch scripts start with ``#!/bin/bash`` and then run R or Python.
    """
    for pattern, lang in _MODULE_LANG.items():
        if pattern.search(text):
            return lang
    lines = text.splitlines()
    if lines:
        for key, lang in _SHEBANG_LANG.items():
            if key in lines[0]:
                return lang
    return "unknown"


def is_shell_script(text: str) -> bool:
    """True when the script is run by sh or bash, the only kind a sampler line can be added to."""
    first = text.splitlines()[0] if text else ""
    if not first.startswith("#!"):
        return True  # sbatch runs a script without a shebang with /bin/sh
    words = first[2:].split()
    if not words:
        return True
    program = os.path.basename(words[0])
    if program == "env":  # "#!/usr/bin/env bash"
        rest = [w for w in words[1:] if not w.startswith("-") and "=" not in w]
        program = os.path.basename(rest[0]) if rest else ""
    return program in ("sh", "bash")


_R_LIB_RE = re.compile(r'(?:library|require)\(\s*["\']?(\w+)["\']?\s*\)')
_R_NS_RE = re.compile(r"(\w+)::")
_PY_IMPORT_RE = re.compile(r"^\s*(?:import|from)\s+([\w.]+)", re.MULTILINE)

_KNOWN_TOOLS = [
    "Seurat",
    "scanpy",
    "scVI",
    "QuPath",
    "cellranger",
    "scimap",
    "SCTransform",
    "FindMarkers",
    "readRDS",
    "anndata",
    "squidpy",
    "spatialdata",
    "starspace",
    "STAR",
    "salmon",
    "kallisto",
    "featureCounts",
    "DESeq2",
    "edgeR",
    "limma",
]


def detect_tools(text: str, language: str) -> List[str]:
    """Packages and tools a script refers to."""
    tools: set = set()
    if language == "R":
        tools.update(_R_LIB_RE.findall(text))
        tools.update(_R_NS_RE.findall(text))
    elif language == "python":
        for match in _PY_IMPORT_RE.finditer(text):
            tools.add(match.group(1).split(".")[0])
    for tool in _KNOWN_TOOLS:
        if re.search(r"\b%s\b" % re.escape(tool), text):
            tools.add(tool)
    return sorted(tools)


# ---------------------------------------------------------------------------
# Input files
# ---------------------------------------------------------------------------

_PATH_RES: Dict[Tuple[str, ...], "re.Pattern"] = {}


def _path_re(roots: Optional[Sequence[str]]) -> "re.Pattern":
    """Pattern for absolute paths under the file systems that hold data."""
    key = tuple(r.rstrip("/") for r in (roots or DEFAULT_DATA_ROOTS) if r.rstrip("/"))
    if key not in _PATH_RES:
        _PATH_RES[key] = re.compile(
            r"(?<![\w./~-])((?:%s)/[^\s\"'`;,()<>|&=${}\\]+)" % "|".join(re.escape(r) for r in key)
        )
    return _PATH_RES[key]


# Programs, containers and settings are not data the job loads into memory.
_NOT_DATA_SUFFIXES = frozenset(
    ".py .r .rmd .qmd .sh .bash .sbatch .slurm .pl .jl .m .nf .smk .ipynb .groovy .js .jar "
    ".sif .simg .img .yaml .yml .toml .cfg .ini .conf .log .out .err .lock".split()
)

_OUTPUT_OPTIONS = (
    "o|out|output|outdir|out[-_]dir|output[-_]dir|output[-_]file|outfile|out[-_]file|"
    "outpath|out[-_]path|output[-_]path|e|err|error|log|log[-_]file|logfile|log[-_]dir|"
    "results?|results?[-_]dir|prefix|out[-_]prefix|output[-_]prefix|save|save[-_]path|"
    "save[-_]dir|dest|destination|tmp|tmp[-_]?dir|temp[-_]?dir|workdir|work[-_]dir|chdir|D"
)
_OUTPUT_BEFORE_RE = re.compile(
    r"""(?:
          (?:^|\s)(?:\d|&)?>>?\s*["']?$                      # shell redirection
        | (?:^|[\s;|&])tee(?:\s+-a)?\s+["']?$
        | (?:^|[\s"'])--?(?:%s)(?:\s+|=)["']?$               # an option naming an output
        | \b(?:save[\w.]*|write[\w.]*|fwrite|imwrite|imsave|ggsave|pdf|png|jpeg|tiff|svg|sink|
              to_\w+|dump\w*|mkdir\w*|makedirs|dir\.create|setwd|chdir|rmtree|remove|unlink)
          \s*\([^()]*$                                       # inside a call that writes
        | (?:^|[\s;|&])(?:mkdir|cd|rm|touch|rmdir)\s+(?:-\w+\s+)*["']?$
        )"""
    % _OUTPUT_OPTIONS,
    re.VERBOSE,
)
_WRITE_MODE_AFTER_RE = re.compile(r"""^["']?\s*,\s*(?:mode\s*=\s*)?["'][wax]""")


def _looks_like_output(before: str, after: str) -> bool:
    return bool(_OUTPUT_BEFORE_RE.search(before) or _WRITE_MODE_AFTER_RE.match(after))


def _stat_input(path: str) -> Optional[Dict[str, Any]]:
    """The regular file at ``path``, or the regular files a glob pattern matches."""
    members: Dict[str, int] = {}
    if any(ch in path for ch in "*?["):
        for match in glob.iglob(path):
            try:
                st = os.stat(match)
            except OSError:
                continue
            if stat.S_ISREG(st.st_mode) and _is_data_path(match):
                members[os.path.realpath(match)] = st.st_size
                if len(members) >= MAX_GLOB_MATCHES:
                    break
    else:
        try:
            st = os.stat(path)
        except OSError:
            return None
        if stat.S_ISREG(st.st_mode):  # a directory is not read whole
            members[os.path.realpath(path)] = st.st_size
    if not members:
        return None
    return {"path": path, "_members": members}


def _finish_inputs(*groups: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """One entry per named path, each file counted once even if a pattern also matches it."""
    counted: set = set()
    out: List[Dict[str, Any]] = []
    for group in groups:
        for entry in group:
            members = entry.get("_members")
            if members is None:  # already finished
                out.append(dict(entry))
                continue
            fresh = {path: size for path, size in members.items() if path not in counted}
            if not fresh:
                continue
            counted.update(fresh)
            out.append(
                {"path": entry["path"], "size_gb": sum(fresh.values()) / GIB, "files": len(fresh)}
            )
    return out


def _is_data_path(path: str) -> bool:
    return os.path.splitext(path)[1].lower() not in _NOT_DATA_SUFFIXES


def detect_input_files(
    text: str, roots: Optional[Sequence[str]] = None, raw: bool = False
) -> List[Dict[str, Any]]:
    """Existing data files a script names, with their sizes.

    Counted: regular files under the data roots (``roots``), and the matches of
    glob patterns. Not counted: directories, programs and container images,
    paths that are written rather than read (redirections, ``--output``-style
    options, ``saveRDS(...)``/``to_csv(...)``/``open(..., "w")``), ``#SBATCH``
    lines and comments. A result file left by an earlier run would otherwise be
    added to the input size of the next one.
    """
    results: List[Dict[str, Any]] = []
    seen: set = set()
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        for match in _path_re(roots).finditer(line):
            path = match.group(1).rstrip("/.,;:'\"")
            if path in seen:
                continue
            seen.add(path)
            if not _is_data_path(path):
                continue
            if _looks_like_output(line[: match.start(1)], line[match.end(1) :]):
                continue
            entry = _stat_input(path)
            if entry:
                results.append(entry)
    return results if raw else _finish_inputs(results)


def inputs_from_arguments(
    tokens: Iterable[str], base_dir: Optional[str] = None, raw: bool = False
) -> List[Dict]:
    """Existing data files among command-line arguments (absolute, or relative to ``base_dir``)."""
    results: List[Dict[str, Any]] = []
    seen: set = set()
    previous = ""
    for token in tokens:
        before = previous + " "
        candidate = token
        if token.startswith("-"):
            option, eq, value = token.partition("=")
            previous = token
            if not eq:
                continue
            before, candidate = option + "=", value
        else:
            previous = token
        if not candidate:
            continue
        path = os.path.expanduser(candidate)
        if not os.path.isabs(path):
            looks_like_a_file = os.sep in path or "." in path
            if base_dir is None or not looks_like_a_file:
                continue
            path = os.path.join(base_dir, path)
        if path in seen or not _is_data_path(path):
            continue
        seen.add(path)
        if _looks_like_output(before, ""):
            continue
        entry = _stat_input(path)
        if entry:
            results.append(entry)
    return results if raw else _finish_inputs(results)


def total_gb(files: Sequence[Mapping[str, Any]]) -> float:
    return float(sum(f["size_gb"] for f in files if f.get("size_gb")))


# ---------------------------------------------------------------------------
# Scripts
# ---------------------------------------------------------------------------

_FOLLOW_SUFFIXES = (".py", ".r", ".rmd", ".qmd", ".jl", ".pl")
_SCRIPT_TOKEN_RE = re.compile(r"""(?<![\w./~-])([\w./~+-]+\.(?:py|R|r|Rmd|qmd|jl|pl))(?![\w.])""")


def script_hash(text: str) -> str:
    """SHA-256 of the script text."""
    return hashlib.sha256(text.encode()).hexdigest()


def read_text(path: str, limit: int = MAX_SCRIPT_BYTES) -> Optional[str]:
    try:
        with open(path, "rb") as handle:
            return handle.read(limit).decode("utf-8", errors="replace")
    except OSError:
        return None


def referenced_scripts(text: str, base_dirs: Sequence[str]) -> List[str]:
    """R/Python/Julia/Perl programs a shell script runs, resolved to existing files.

    An sbatch script is usually a few lines of bash around ``Rscript x.R`` or
    ``python x.py``; the packages and the data paths are in that program.
    """
    found: List[str] = []
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        for match in _SCRIPT_TOKEN_RE.finditer(line):
            token = os.path.expanduser(match.group(1))
            candidates = (
                [token] if os.path.isabs(token) else [os.path.join(b, token) for b in base_dirs]
            )
            for candidate in candidates:
                if os.path.isfile(candidate):
                    real = os.path.realpath(candidate)
                    if real not in found:
                        found.append(real)
                    break
            if len(found) >= MAX_FOLLOWED_SCRIPTS:
                return found
    return found


def _scan(
    text: str, language: Optional[str], roots: Optional[Sequence[str]]
) -> Tuple[str, List[str], List[Dict[str, Any]]]:
    language = language or detect_language(text)
    return language, detect_tools(text, language), detect_input_files(text, roots, raw=True)


def _merge_inputs(*groups: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for group in groups:
        for entry in group:
            merged.setdefault(entry["path"], dict(entry))
    return list(merged.values())


def _analyze_text(
    text: str, base_dirs: Sequence[str], roots: Optional[Sequence[str]] = None
) -> Dict[str, Any]:
    language, tools, inputs = _scan(text, None, roots)
    followed: List[str] = []
    for path in referenced_scripts(text, base_dirs):
        sub = read_text(path)
        if sub is None:
            continue
        followed.append(path)
        sub_lang = _SUFFIX_LANG.get(os.path.splitext(path)[1].lower())
        _lang, sub_tools, sub_inputs = _scan(sub, sub_lang, roots)
        tools = sorted(set(tools) | set(sub_tools))
        inputs = _merge_inputs(inputs, sub_inputs)
        if language in ("unknown", "bash") and sub_lang:
            language = sub_lang
    return {"language": language, "tools": tools, "input_files": inputs, "followed": followed}


def analyze_script(
    script_path: str,
    cwd: Optional[str] = None,
    roots: Optional[Sequence[str]] = None,
    raw: bool = False,
) -> Dict[str, Any]:
    """Directives, language, tools and inputs of an sbatch script (and the programs it runs)."""
    text = read_text(script_path)
    if text is None:
        raise OSError("cannot read %s" % script_path)
    here = os.path.dirname(os.path.abspath(script_path))
    base_dirs = [d for d in dict.fromkeys([cwd or os.getcwd(), here]) if d]
    info = _analyze_text(text, base_dirs, roots)
    if not raw:
        info["input_files"] = _finish_inputs(info["input_files"])
    info.update(
        script_path=os.path.abspath(script_path),
        script_hash=script_hash(text),
        options=parse_sbatch_directives(text),
        ignored_directive_lines=ignored_directives(text),
        shell=is_shell_script(text),
        total_input_gb=total_gb(_finish_inputs(info["input_files"])),
    )
    return info


# ---------------------------------------------------------------------------
# A whole submission
# ---------------------------------------------------------------------------


@dataclass
class Submission:
    """An sbatch invocation, analysed: what it runs and what it asks for."""

    args: List[str]
    parsed: sbatch_opts.ParsedArgs
    request: Request
    job_name: str
    submit_line: str
    script_path: Optional[str] = None
    wrap: Optional[str] = None
    shell: bool = False
    script_hash: Optional[str] = None
    language: str = "unknown"
    tools: List[str] = field(default_factory=list)
    input_files: List[Dict[str, Any]] = field(default_factory=list)
    followed: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def input_gb(self) -> float:
        return total_gb(self.input_files)

    def context(self) -> Dict[str, Any]:
        """What is recorded next to the job so later runs can be sized from it."""
        return {
            "script_hash": self.script_hash,
            "tools": self.tools,
            "input_gb": round(self.input_gb, 6),
            "inputs": [
                {"path": f["path"], "size_gb": round(f["size_gb"], 6), "files": f.get("files", 1)}
                for f in self.input_files
            ],
            "language": self.language,
            "submit_line": self.submit_line,
        }


def analyze_submission(
    args: Sequence[str], cwd: Optional[str] = None, roots: Optional[Sequence[str]] = None
) -> Submission:
    """Analyse ``sbatch <args>`` without running it.

    ``args`` is exactly what would follow the word ``sbatch``; ``roots`` are the
    file systems searched for input data. Raises ``OSError`` when a named
    script cannot be read.
    """
    args = list(args)
    cwd = cwd or os.getcwd()
    parsed = sbatch_opts.parse_argv(args)
    cli_options = options_dict(parsed)
    notes: List[str] = []
    if parsed.unknown:
        notes.append("unrecognised sbatch option(s): %s" % " ".join(parsed.unknown))

    script_path = None
    info: Dict[str, Any] = {"language": "unknown", "tools": [], "input_files": [], "followed": []}
    directives: Dict[str, Optional[str]] = {}
    shell = False
    digest = None
    wrap = parsed.wrap

    if wrap is not None:
        try:
            tokens = shlex.split(wrap)
        except ValueError:
            tokens = wrap.split()
        info = _analyze_text(wrap, [cwd], roots)
        info["input_files"] = _finish_inputs(
            info["input_files"], inputs_from_arguments(tokens, cwd, raw=True)
        )
        if info["language"] == "unknown":
            program = (command_signature(tokens) or "").split(":", 1)[0]
            info["language"] = {"python": "python", "Rscript": "R"}.get(program, "unknown")
        digest = script_hash(wrap)
        default_name = "wrap"
        shell = True
    elif parsed.script is not None:
        script_path = (
            parsed.script if os.path.isabs(parsed.script) else os.path.join(cwd, parsed.script)
        )
        info = analyze_script(script_path, cwd=cwd, roots=roots, raw=True)
        directives = info["options"]
        shell = info["shell"]
        digest = info["script_hash"]
        info["input_files"] = _finish_inputs(
            info["input_files"], inputs_from_arguments(parsed.script_args, cwd, raw=True)
        )
        if info["ignored_directive_lines"]:
            notes.append(
                "#SBATCH on line %s comes after the first command and is ignored by sbatch"
                % ", ".join(str(n) for n in info["ignored_directive_lines"])
            )
        default_name = os.path.basename(parsed.script)
    else:
        default_name = "sbatch"  # script on standard input
        notes.append("no script or --wrap given: the script is read from standard input")

    request = request_from_options(directives, cli_options)
    notes.extend(request.problems)
    job_name = request.job_name or default_name
    return Submission(
        args=args,
        parsed=parsed,
        request=request,
        job_name=job_name,
        submit_line=" ".join(["sbatch"] + args),
        script_path=script_path,
        wrap=wrap,
        shell=shell,
        script_hash=digest,
        language=info["language"],
        tools=list(info["tools"]),
        input_files=list(info["input_files"]),
        followed=list(info["followed"]),
        notes=notes,
    )
