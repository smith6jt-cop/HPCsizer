"""Pre-submit script analyzer for HPCsizer.

Performs static analysis of an sbatch script to extract:
  - Language (R, Python, shell, etc.)
  - Packages/tools used
  - Input file paths and sizes
  - SBATCH resource directives
"""

import hashlib
import os
import re
from pathlib import Path
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# SBATCH directive parsing
# ---------------------------------------------------------------------------

_SBATCH_RE = re.compile(r"^#SBATCH\s+(.*)", re.MULTILINE)
_MEM_RE = re.compile(r"--mem(?:-per-cpu)?=(\d+)([KMGTkmgt]?)")
_CPU_RE = re.compile(r"--cpus-per-task=(\d+)")
_TIME_RE = re.compile(r"--time=(\S+)")
_GRES_RE = re.compile(r"--gres=gpu(?::\w+)?:(\d+)")
_JOB_NAME_RE = re.compile(r"--job-name=(\S+)")


def _parse_mem(value: str, suffix: str) -> float:
    """Convert memory string to GB."""
    n = float(value)
    s = suffix.upper() if suffix else "M"
    multipliers = {"K": 1 / 1024 / 1024, "M": 1 / 1024, "G": 1.0, "T": 1024.0}
    return n * multipliers.get(s, 1 / 1024)


def _parse_time(t: str) -> int:
    """Convert SLURM time string to minutes."""
    t = t.strip()
    # D-HH:MM:SS
    m = re.match(r"(\d+)-(\d+):(\d+):(\d+)", t)
    if m:
        d, h, mi, s = (int(x) for x in m.groups())
        return d * 1440 + h * 60 + mi + (1 if s else 0)
    # HH:MM:SS
    m = re.match(r"(\d+):(\d+):(\d+)", t)
    if m:
        h, mi, s = (int(x) for x in m.groups())
        return h * 60 + mi + (1 if s else 0)
    # MM:SS
    m = re.match(r"(\d+):(\d+)$", t)
    if m:
        mi, s = (int(x) for x in m.groups())
        return mi + (1 if s else 0)
    # plain minutes
    return int(t)


def parse_sbatch_directives(text: str) -> Dict[str, Any]:
    """Extract resource requests from #SBATCH directives."""
    result: Dict[str, Any] = {
        "req_mem_gb": None,
        "req_cpus": 1,
        "req_time_min": None,
        "req_gpus": 0,
        "job_name": None,
    }
    for directive in _SBATCH_RE.findall(text):
        m = _MEM_RE.search(directive)
        if m:
            result["req_mem_gb"] = _parse_mem(m.group(1), m.group(2))
        m = _CPU_RE.search(directive)
        if m:
            result["req_cpus"] = int(m.group(1))
        m = _TIME_RE.search(directive)
        if m:
            result["req_time_min"] = _parse_time(m.group(1))
        m = _GRES_RE.search(directive)
        if m:
            result["req_gpus"] = int(m.group(1))
        m = _JOB_NAME_RE.search(directive)
        if m:
            result["job_name"] = m.group(1)
    return result


# ---------------------------------------------------------------------------
# Language detection
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


def detect_language(text: str) -> str:
    """Detect the primary language used in the script.

    Content-based patterns (module loads, interpreter invocations) take
    priority over the shebang line because sbatch scripts commonly start
    with ``#!/bin/bash`` while invoking R or Python inside.
    """
    for pattern, lang in _MODULE_LANG.items():
        if pattern.search(text):
            return lang
    lines = text.splitlines()
    if lines:
        shebang = lines[0]
        for key, lang in _SHEBANG_LANG.items():
            if key in shebang:
                return lang
    return "unknown"


# ---------------------------------------------------------------------------
# Tool / package detection
# ---------------------------------------------------------------------------

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
    """Detect packages/tools referenced in the script."""
    tools: set = set()
    if language == "R":
        tools.update(_R_LIB_RE.findall(text))
        tools.update(_R_NS_RE.findall(text))
    elif language == "python":
        for m in _PY_IMPORT_RE.finditer(text):
            pkg = m.group(1).split(".")[0]
            tools.add(pkg)
    # Always scan for known CLI tools regardless of language
    for tool in _KNOWN_TOOLS:
        if re.search(rf"\b{re.escape(tool)}\b", text):
            tools.add(tool)
    return sorted(tools)


# ---------------------------------------------------------------------------
# Input file detection
# ---------------------------------------------------------------------------

_FILE_PATH_RE = re.compile(r'["\']?((?:/blue|/orange|/red|/scratch|/home)\S+?)["\'\s;,)]')
_FORMAT_MULTIPLIERS = {
    ".rds": 2.5,
    ".RDS": 2.5,
    ".h5ad": 1.5,  # dense default
    ".qptiff": None,  # computed separately
    ".tif": 1.0,
    ".tiff": 1.0,
    ".csv": 1.0,
    ".h5": 1.5,
    ".loom": 1.5,
}


def detect_input_files(text: str) -> List[Dict[str, Any]]:
    """Find input file paths in the script, stat their sizes, apply multipliers."""
    results = []
    seen: set = set()
    for m in _FILE_PATH_RE.finditer(text):
        path = m.group(1).rstrip("/.,;'\"")
        if path in seen:
            continue
        seen.add(path)
        entry: Dict[str, Any] = {"path": path, "size_gb": None, "mem_estimate_gb": None}
        try:
            stat = os.stat(path)
            size_gb = stat.st_size / 1024**3
            entry["size_gb"] = size_gb
            suffix = Path(path).suffix
            mult = _FORMAT_MULTIPLIERS.get(suffix)
            if mult is not None:
                entry["mem_estimate_gb"] = size_gb * mult
        except OSError:
            pass
        results.append(entry)
    return results


# ---------------------------------------------------------------------------
# Script hash
# ---------------------------------------------------------------------------


def script_hash(text: str) -> str:
    """Return SHA-256 hex digest of the script content."""
    return hashlib.sha256(text.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Main analysis entry point
# ---------------------------------------------------------------------------


def analyze_script(script_path: str) -> Dict[str, Any]:
    """Fully analyze an sbatch script and return a metadata dict."""
    path = Path(script_path)
    text = path.read_text(errors="replace")

    language = detect_language(text)
    tools = detect_tools(text, language)
    input_files = detect_input_files(text)
    directives = parse_sbatch_directives(text)

    total_input_gb = sum(f["size_gb"] for f in input_files if f["size_gb"] is not None)

    return {
        "script_path": str(path.resolve()),
        "script_hash": script_hash(text),
        "language": language,
        "static_tools": tools,
        "input_files": input_files,
        "total_input_gb": total_input_gb,
        **directives,
    }
