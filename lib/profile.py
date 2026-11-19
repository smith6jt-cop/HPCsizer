"""In-job profiles: reading what the sampler wrote, and storing its summary.

``bin/monitor.py`` appends samples to ``<spool>/<jobid>.<node>.tsv`` while a
job runs and ``hpg submit`` leaves ``<spool>/submit/<jobid>.json`` describing
what was submitted. Nothing is finalised inside the job. The harvester calls
``ingest_spool`` once the job is stored: it summarises the series into
``job_monitor``, copies the submit context into ``job_context``, and archives
the series, compressed, under ``timeseries/``.

The spool is group-writable so that every member's jobs can write to it, which
also means anyone in the group could put anything there under any name: a file
of their own, a link to somebody else's file, a pipe. The harvest runs as the
owner of the database, so an entry is only read when it is a plain file with a
single name that belongs to the user the job belongs to; it is opened without
following links, its size is bounded, and what is stored from it is limited to
what the sampler and ``hpg submit`` write.
"""

import errno
import glob
import gzip
import json
import math
import os
import pwd
import re
import stat
import statistics
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from lib import db
from lib.config import Config
from lib.fmt import clean, fmt_bytes, fmt_num, fmt_pct

Row = Dict[str, Any]
Series = List[Tuple[float, float]]

ORPHAN_DAYS = 30  # series of jobs that never show up in accounting are removed after this
START_SLACK_SEC = 600
GRACE_SEC = 86400  # a spool file that cannot be used is removed this long after its job ended
SERIES_MAX_BYTES = 64 << 20  # a month of samples is about 10 MB
CONTEXT_MAX_BYTES = 1 << 20

# Thresholds of the informational notes, as in the previous version.
IDLE_CORES = 0.05
IDLE_SHARE = 0.50
NUMA_MISS_RATE = 0.20
LUSTRE_META_OPS = 100.0
IO_MB_S = 10.0
LUSTRE_MB_S = 50.0

# ---------------------------------------------------------------------------
# Reading a series
# ---------------------------------------------------------------------------


def parse_series(text: str) -> Tuple[Dict[str, str], List[Row]]:
    """``(meta, rows)`` of one series file.

    The sampler can be killed mid-write, so a last line without its newline is
    dropped, as is any line that does not have every column.
    """
    meta: Dict[str, str] = {}
    columns: Optional[List[str]] = None
    rows: List[Row] = []
    lines = text.split("\n")
    if lines and lines[-1] != "":
        lines = lines[:-1]  # cut off mid-line
    for line in lines:
        if not line:
            continue
        if line.startswith("#hpcsizer-sampler"):
            for token in line.split()[1:]:
                key, eq, value = token.partition("=")
                if eq and key not in meta:
                    meta[key] = value
            continue
        if line.startswith("#"):
            continue
        if columns is None or line.startswith("t\t"):
            columns = line.split("\t")
            continue
        parts = line.split("\t")
        if len(parts) != len(columns):
            continue
        row: Row = {}
        try:
            for name, value in zip(columns, parts):
                if name == "top":
                    row[name] = value
                elif value == "":
                    row[name] = None
                else:
                    row[name] = float(value) if "." in value else int(value)
        except ValueError:
            continue
        if row.get("t") is not None:
            rows.append(row)
    rows.sort(key=lambda r: r["t"])
    return meta, rows


class Rejected(OSError):
    """A spool entry that is not what a job's sampler or ``hpg submit`` leaves there."""


def read_plain_file(path: str, limit: int, single_name: bool = True) -> Tuple[bytes, int, float]:
    """``(content, owner uid, mtime)`` of a plain file, read without trusting the name.

    Links are not followed, a pipe cannot block the caller, and nothing beyond
    ``limit`` bytes is read. Raises ``Rejected`` for anything that is not a
    regular file (with one name, when ``single_name``) of at most ``limit``
    bytes, and ``OSError`` when it cannot be read.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise Rejected("it is a symbolic link") from None
        raise
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise Rejected("it is not a regular file")
        if single_name and info.st_nlink != 1:
            raise Rejected("it has %d names" % info.st_nlink)
        chunks = []
        total = 0
        while total <= limit:
            chunk = os.read(fd, min(1 << 20, limit + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        if total > limit:
            raise Rejected("it is larger than %s" % fmt_bytes(limit))
        return b"".join(chunks), info.st_uid, info.st_mtime
    finally:
        os.close(fd)


def uid_of(user: Optional[str]) -> Optional[int]:
    """The uid of a user name as accounting reports it, or None when it cannot be resolved."""
    if not user:
        return None
    try:
        return pwd.getpwnam(user).pw_uid
    except KeyError:
        return int(user) if user.isdigit() else None


def read_series(path: str, owner: Optional[str] = None) -> Tuple[Dict[str, str], List[Row]]:
    """``(meta, rows)`` of a series file: an archive, or a file still in the spool.

    With ``owner``, the file must belong to that user -- which is what makes a
    file in the group-writable spool believable. Raises ``OSError``.
    """
    archived = path.endswith(".gz")
    data, uid, _mtime = read_plain_file(path, SERIES_MAX_BYTES, single_name=not archived)
    if owner is not None and uid != uid_of(owner):
        raise Rejected("%s does not belong to %s" % (os.path.basename(path), owner))
    if archived:
        data = gzip.decompress(data)
    return parse_series(data.decode("utf-8", "replace"))


# ---------------------------------------------------------------------------
# One node
# ---------------------------------------------------------------------------


def _increase(rows: Sequence[Row], key: str) -> Optional[float]:
    """Total growth of a cumulative counter, ignoring the drops when it restarts."""
    total = 0.0
    previous = None
    seen = False
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        if previous is not None and value > previous:
            total += value - previous
        previous = value
        seen = True
    return total if seen else None


def rate_series(rows: Sequence[Row], key: str) -> Series:
    """``(time, rate per second)`` of a cumulative counter between consecutive samples."""
    out: Series = []
    previous = None
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        if previous is not None:
            dt = row["t"] - previous[0]
            if dt > 0:
                out.append((row["t"], max(value - previous[1], 0.0) / dt))
        previous = (row["t"], value)
    return out


def value_series(rows: Sequence[Row], key: str) -> Series:
    return [(row["t"], row[key]) for row in rows if row.get(key) is not None]


def weighted_quantile(series: Series, q: float) -> Optional[float]:
    """Quantile of a rate series, each value weighted by the time it covers."""
    if not series:
        return None
    weighted = []
    previous_t = None
    for t, value in series:
        weight = (t - previous_t) if previous_t is not None else 0.0
        previous_t = t
        weighted.append((value, weight if weight > 0 else 0.0))
    if not any(w for _v, w in weighted):
        weighted = [(v, 1.0) for v, _w in weighted]
    weighted.sort()
    total = sum(w for _v, w in weighted)
    running = 0.0
    for value, weight in weighted:
        running += weight
        if running >= q * total - 1e-12:
            return value
    return weighted[-1][0]


def summarize_node(meta: Dict[str, str], rows: Sequence[Row]) -> Dict[str, Any]:
    """What one node's samples say; counters prefixed ``node_`` cover the whole node."""
    first, last = rows[0], rows[-1]
    gaps = [b["t"] - a["t"] for a, b in zip(rows, rows[1:]) if b["t"] > a["t"]]
    cpu_first = first.get("cpu")
    grown = _increase(rows, "cpu")
    cpu_sec = None if grown is None else grown + (cpu_first or 0.0)
    cores = rate_series(rows, "cpu")
    mem_rows = [r for r in rows if r.get("mem") is not None]
    peak_row = max(mem_rows, key=lambda r: r["mem"]) if mem_rows else None
    usage = [r[k] for r in rows for k in ("usage", "usage_peak") if r.get(k) is not None]
    duration = float(last["t"] - first["t"])
    hit = _increase(rows, "node_numa_hit")
    miss = _increase(rows, "node_numa_miss")
    numa = None
    if hit is not None and miss is not None and hit + miss > 0:
        numa = miss / (hit + miss)  # over the run, not since the node booted
    meta_ops = _increase(rows, "node_lustre_meta")
    oom = [r["oom"] for r in rows if r.get("oom") is not None]
    ipc = [r["perf_ipc"] for r in rows if r.get("perf_ipc") is not None]
    cmiss = [r["perf_cmiss"] for r in rows if r.get("perf_cmiss") is not None]
    return {
        "node": meta.get("node", "?"),
        "source": meta.get("source", "?"),
        "cpus": int(meta["cpus"]) if meta.get("cpus", "").isdigit() else None,
        "start_ts": first["t"],
        "end_ts": last["t"],
        "duration_sec": duration,
        "n_samples": len(rows),
        "interval_sec": statistics.median(gaps) if gaps else None,
        "peak_mem_bytes": peak_row["mem"] if peak_row else None,
        "peak_usage_bytes": max(usage) if usage else None,
        "top_comm": peak_row.get("top") or None if peak_row else None,
        "cpu_sec": cpu_sec,
        "avg_cores": cpu_sec / duration if cpu_sec is not None and duration > 0 else None,
        "max_cores": max((v for _t, v in cores), default=None),
        "p95_cores": weighted_quantile(cores, 0.95),
        "gpu_util_max": max(
            (r["gpu_util"] for r in rows if r.get("gpu_util") is not None), default=None
        ),
        "gpu_mem_max_bytes": max(
            (r["gpu_mem"] for r in rows if r.get("gpu_mem") is not None), default=None
        ),
        "oom_kills": max(oom) if oom else None,
        "read_bytes": _increase(rows, "rd"),
        "write_bytes": _increase(rows, "wr"),
        "node_lustre_read_bytes": _increase(rows, "node_lustre_rd"),
        "node_lustre_write_bytes": _increase(rows, "node_lustre_wr"),
        "node_lustre_meta_ops_s": (
            meta_ops / duration if meta_ops is not None and duration > 0 else None
        ),
        "node_numa_miss_rate": numa,
        "perf_ipc": sum(ipc) / len(ipc) if ipc else None,
        "perf_cache_miss_rate": sum(cmiss) / len(cmiss) if cmiss else None,
    }


# ---------------------------------------------------------------------------
# Several nodes
# ---------------------------------------------------------------------------


def aligned_sum(series_list: Sequence[Series]) -> Series:
    """Sum of step functions: at each sample time, every series contributes its latest value."""
    times = sorted({t for series in series_list for t, _v in series})
    out: Series = []
    cursors = [0] * len(series_list)
    current: List[Optional[float]] = [None] * len(series_list)
    for t in times:
        for index, series in enumerate(series_list):
            while cursors[index] < len(series) and series[cursors[index]][0] <= t:
                current[index] = series[cursors[index]][1]
                cursors[index] += 1
        out.append((t, sum(v for v in current if v is not None)))
    return out


def node_imbalance(nodes: Sequence[Dict[str, Any]]) -> Optional[float]:
    """Coefficient of variation of the average cores in use across nodes; above 1 is severe."""
    values = [n["avg_cores"] for n in nodes if n.get("avg_cores") is not None]
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    if mean == 0:
        return None
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return round(variance**0.5 / mean, 4)


def _notes(
    nodes: Sequence[Dict[str, Any]],
    cores: Series,
    mem: Series,
    io: Series,
    lustre: Series,
    alloc_cpus: Optional[int],
    p95_cores: Optional[float],
) -> Tuple[List[str], List[str]]:
    """Informational flags and the sentences that explain them."""
    flags: List[str] = []
    notes: List[str] = []

    def add(flag: str, text: str) -> None:
        flags.append(flag)
        notes.append(text)

    values = [v for _t, v in cores]
    if len(values) >= 3:
        idle = sum(1 for v in values if v < IDLE_CORES) / float(len(values))
        if idle > IDLE_SHARE:
            add(
                "idle_cpu",
                "under %.2f cores in use for %s of the samples" % (IDLE_CORES, fmt_pct(idle)),
            )
        if alloc_cpus and alloc_cpus > 2 and p95_cores is not None and p95_cores < 1.5:
            add(
                "single_threaded",
                "at most about one core busy (%s sustained) of %d allocated"
                % (fmt_num(p95_cores), alloc_cpus),
            )
    if len(values) >= 10:
        half = len(values) // 2
        early = sum(values[:half]) / half
        late = sum(values[half:]) / (len(values) - half)
        if early > 0.10 and late < IDLE_CORES:
            add(
                "stalled",
                "CPU use stopped part-way: %s cores in the first half of the run, %s in the second"
                % (fmt_num(early, 2), fmt_num(late, 2)),
            )
    mem_values = [v for _t, v in mem]
    if len(mem_values) >= 10:
        cut = max(1, len(mem_values) // 10)
        early_peak = max(mem_values[:cut])
        rest = sorted(mem_values[cut:])
        if rest and early_peak > 0:
            later = rest[min(int(0.95 * len(rest)), len(rest) - 1)]
            if later < 0.6 * early_peak:
                add(
                    "mem_spike_plateau",
                    "memory peaked at %s in the first tenth of the run and stayed under %s after"
                    % (fmt_bytes(early_peak), fmt_bytes(later)),
                )
    if io and values:
        busy = dict(cores)
        heavy = [1 for t, rate in io if rate > IO_MB_S * 1024 * 1024 and busy.get(t, 1.0) < 0.10]
        if len(heavy) / float(len(io)) > 0.50:
            add("io_dominant", "mostly reading or writing (over %g MB/s) with idle CPUs" % IO_MB_S)
    if lustre and values:
        busy = dict(cores)
        heavy = [
            1 for t, rate in lustre if rate > LUSTRE_MB_S * 1024 * 1024 and busy.get(t, 1.0) < 0.10
        ]
        if len(heavy) / float(len(lustre)) > 0.50:
            add(
                "lustre_io_dominant",
                "the node moved over %g MB/s through Lustre while this job's CPUs were idle "
                "(node-wide counter)" % LUSTRE_MB_S,
            )
    meta_ops = [
        n["node_lustre_meta_ops_s"] for n in nodes if n.get("node_lustre_meta_ops_s") is not None
    ]
    if meta_ops and max(meta_ops) > LUSTRE_META_OPS:
        add(
            "lustre_metadata_heavy",
            "%s Lustre metadata operations per second on the node (node-wide counter)"
            % fmt_num(max(meta_ops)),
        )
    numa = [n["node_numa_miss_rate"] for n in nodes if n.get("node_numa_miss_rate") is not None]
    if numa and max(numa) > NUMA_MISS_RATE:
        add(
            "numa_misplaced",
            "%s of memory allocations on the node missed their NUMA node (node-wide counter)"
            % fmt_pct(max(numa)),
        )
    kills = sum(n["oom_kills"] or 0 for n in nodes)
    if kills:
        add(
            "oom_kill", "the kernel killed %d process(es) inside the job for lack of memory" % kills
        )
    if len(nodes) > 1:
        idle_nodes = [n["node"] for n in nodes if (n.get("avg_cores") or 0.0) < IDLE_CORES]
        if idle_nodes:
            add(
                "idle_nodes",
                "%d of %d nodes did almost nothing: %s"
                % (len(idle_nodes), len(nodes), ", ".join(idle_nodes[:6])),
            )
        cv = node_imbalance(nodes)
        if cv is not None and cv > 1.0:
            add(
                "node_imbalance",
                "CPU use differs strongly between nodes (variation %s)" % fmt_num(cv, 2),
            )
    return flags, notes


def summarize(
    node_series: Sequence[Tuple[Dict[str, str], Sequence[Row]]],
    job_start_ts: Optional[int] = None,
    job_elapsed_sec: Optional[int] = None,
    alloc_cpus: Optional[int] = None,
    alloc_nodes: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Job-level summary of one or more nodes' series, in the columns of ``job_monitor``.

    ``coverage`` is the share of the job the samples saw: of its run time and,
    for a job on several nodes, of its nodes.
    """
    usable = [(meta, rows) for meta, rows in node_series if rows]
    if not usable:
        return None
    nodes = [summarize_node(meta, rows) for meta, rows in usable]
    cores = aligned_sum([rate_series(rows, "cpu") for _meta, rows in usable])
    mem = aligned_sum([value_series(rows, "mem") for _meta, rows in usable])
    io_series = [
        aligned_sum([rate_series(rows, key) for _meta, rows in usable]) for key in ("rd", "wr")
    ]
    io = aligned_sum(io_series)
    lustre = aligned_sum(
        [
            rate_series(rows, key)
            for _meta, rows in usable
            for key in ("node_lustre_rd", "node_lustre_wr")
        ]
    )

    def total(key: str) -> Optional[float]:
        values = [n[key] for n in nodes if n.get(key) is not None]
        return sum(values) if values else None

    def largest(key: str) -> Optional[float]:
        values = [n[key] for n in nodes if n.get(key) is not None]
        return max(values) if values else None

    start = min(n["start_ts"] for n in nodes)
    end = max(n["end_ts"] for n in nodes)
    duration = float(end - start)
    cpu_sec = total("cpu_sec")
    p95 = weighted_quantile(cores, 0.95)
    if alloc_cpus is None:
        counted = [n["cpus"] for n in nodes if n.get("cpus")]
        alloc_cpus = sum(counted) if counted else None
    coverage = None
    if job_elapsed_sec:
        coverage = round(min(max(duration, 0.0) / float(job_elapsed_sec), 1.0), 4)
    peak_node = max(nodes, key=lambda n: n.get("peak_mem_bytes") or 0)
    flags, notes = _notes(nodes, cores, mem, io, lustre, alloc_cpus, p95)
    if coverage is not None and coverage < 0.9:
        notes.append("the samples cover %s of the job's run time" % fmt_pct(coverage))
    if alloc_nodes and alloc_nodes > len(nodes):
        notes.append(
            "profiled on %d of the job's %d nodes: a lower bound for the job as a whole"
            % (len(nodes), alloc_nodes)
        )
        if coverage is not None:
            coverage = round(coverage * len(nodes) / float(alloc_nodes), 4)
    sources = sorted({n["source"] for n in nodes})
    intervals = [n["interval_sec"] for n in nodes if n.get("interval_sec")]
    detail = {
        "flags": flags,
        "notes": notes,
        "start_ts": start,
        "end_ts": end,
        "duration_sec": duration,
        "read_bytes": total("read_bytes"),
        "write_bytes": total("write_bytes"),
        "node_imbalance_cv": node_imbalance(nodes),
        "nodes_allocated": alloc_nodes,
        "nodes": [
            {
                key: node.get(key)
                for key in (
                    "node",
                    "source",
                    "n_samples",
                    "peak_mem_bytes",
                    "peak_usage_bytes",
                    "cpu_sec",
                    "avg_cores",
                    "max_cores",
                    "node_lustre_read_bytes",
                    "node_lustre_write_bytes",
                    "node_lustre_meta_ops_s",
                    "node_numa_miss_rate",
                    "perf_ipc",
                    "perf_cache_miss_rate",
                )
            }
            for node in nodes
        ],
    }
    return {
        "source": sources[0] if len(sources) == 1 else "mixed",
        "n_samples": sum(n["n_samples"] for n in nodes),
        "interval_sec": statistics.median(intervals) if intervals else None,
        "peak_mem_bytes": int(max(v for _t, v in mem)) if mem else None,
        "peak_usage_bytes": int(total("peak_usage_bytes")) if total("peak_usage_bytes") else None,
        "cpu_sec": cpu_sec,
        "max_cores": max((v for _t, v in cores), default=None),
        "avg_cores": cpu_sec / duration if cpu_sec is not None and duration > 0 else None,
        "p95_cores": p95,
        "gpu_util_max": largest("gpu_util_max"),
        "gpu_mem_max_bytes": total("gpu_mem_max_bytes"),
        "oom_kills": int(total("oom_kills")) if total("oom_kills") is not None else None,
        "top_comm": peak_node.get("top_comm"),
        "coverage": coverage,
        "summary_json": json.dumps(detail, sort_keys=True),
        "series_path": None,
    }


# ---------------------------------------------------------------------------
# Finding the series of a job
# ---------------------------------------------------------------------------


def split_name(path: str) -> Optional[Tuple[str, str]]:
    """``(job id, node)`` from ``<jobid>.<node>.tsv[.gz]`` or ``<jobid>-<submit>.<node>.tsv.gz``."""
    name = os.path.basename(path)
    for suffix in (".tsv.gz", ".tsv"):
        if name.endswith(suffix):
            job, dot, node = name[: -len(suffix)].partition(".")
            job = job.split("-", 1)[0]
            if dot and job.isdigit() and node:
                return job, node
    return None


def archive_pattern(cfg: Config, job_id_raw: int, submit_ts: Optional[int]) -> str:
    return os.path.join(cfg.ts_dir, "%d-%d.*.tsv.gz" % (int(job_id_raw), int(submit_ts or 0)))


def series_files(cfg: Config, job: Dict[str, Any]) -> List[str]:
    """Series of a stored job: the archive when it has been ingested, else the spool."""
    pattern = job.get("mon_series_path")
    found = sorted(glob.glob(pattern)) if pattern else []
    if not found and job.get("job_id_raw") is not None:
        found = sorted(glob.glob(os.path.join(cfg.spool_dir, "%d.*.tsv" % int(job["job_id_raw"]))))
    return found


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


_UNSAFE_IN_NODE = re.compile(r"[^A-Za-z0-9_-]")
_CONTEXT_KEYS = (
    "job_id",
    "submitted_ts",
    "user",
    "cwd",
    "hpcsizer",
    "profiled",
    "applied",
    "script_hash",
    "tools",
    "input_gb",
    "inputs",
    "language",
    "submit_line",
    "request",
    "array",
    "suggested",
)


def safe_node(name: Any) -> str:
    """A node name fit to be part of a file name: no separators, no dots, bounded."""
    return _UNSAFE_IN_NODE.sub("_", str(name or ""))[:64]


def _remove(path: str) -> None:
    try:
        os.unlink(path)  # a link is removed, never what it points to
    except OSError:
        pass


def _age(path: str, now: int) -> Optional[float]:
    try:
        return now - os.lstat(path).st_mtime
    except OSError:
        return None


def _remove_orphan(path: str, now: int) -> None:
    age = _age(path, now)
    if age is not None and age > ORPHAN_DAYS * 86400:
        _remove(path)


def _archive(cfg: Config, data: bytes, job_id_raw: int, submit_ts: Optional[int], node: str) -> str:
    """Store one node's series, compressed. ``data`` is what was read and checked, not a path."""
    # Archives are read by the people who read the database: both follow the
    # directory the database lies in.
    home = os.path.dirname(os.path.abspath(cfg.db_path))
    if not os.path.isdir(cfg.ts_dir):
        os.makedirs(cfg.ts_dir, exist_ok=True)
        _file_mode, dir_mode = db.shared_modes(home)
        os.chmod(cfg.ts_dir, (os.stat(cfg.ts_dir).st_mode & 0o7000) | dir_mode)
    name = "%d-%d.%s.tsv.gz" % (int(job_id_raw), int(submit_ts or 0), safe_node(node) or "node")
    dst = os.path.join(cfg.ts_dir, name)
    tmp = dst + ".tmp"
    with gzip.open(tmp, "wb") as target:
        target.write(data)
    db.set_shared(tmp, home)
    os.replace(tmp, dst)
    return dst


def _ingest_series(
    conn, cfg: Config, job_id: str, paths: Sequence[str], now: int, log: Callable[[str], None]
) -> bool:
    row = db.latest_job_pk(conn, int(job_id))
    if row is None:
        # Not finished, not harvested yet, or not a job of this account.
        for path in paths:
            _remove_orphan(path, now)
        return False
    if db.has_monitor(conn, row["id"]):
        # Stored by an earlier harvest, and that harvest was published: the spool copy can go.
        if glob.glob(archive_pattern(cfg, int(job_id), row["submit_ts"])):
            for path in paths:
                _remove(path)
        return False

    # What cannot be used is left for a day after the job ended (a node may
    # still be writing), then removed, so that it is not read again every hour.
    settled = now - (row["end_ts"] or now) > GRACE_SEC
    after = (row["submit_ts"] or 0) - START_SLACK_SEC
    before = (row["end_ts"] or now) + START_SLACK_SEC
    owner_uid = uid_of(row["user"])
    series = []
    kept: List[Tuple[str, bytes, str]] = []
    labels: Dict[str, int] = {}
    for path in paths:
        name = os.path.basename(path)
        try:
            if owner_uid is None:
                raise OSError("user %s is not known on this node" % row["user"])
            data, uid, _mtime = read_plain_file(path, SERIES_MAX_BYTES)
            if uid != owner_uid:
                raise Rejected("it is not owned by %s, whose job %s is" % (row["user"], job_id))
        except Rejected as exc:
            log("[profile] %s ignored: %s" % (name, exc))
            if settled:
                _remove(path)
            continue
        except OSError as exc:
            log("[profile] cannot read %s: %s" % (path, exc))
            continue
        meta, rows = parse_series(data.decode("utf-8", "replace"))
        if not rows or not after <= rows[0]["t"] <= before:
            if settled:  # nothing recorded, or an earlier use of this job id
                _remove(path)
            continue
        node = safe_node(meta.get("node")) or safe_node((split_name(path) or ("", ""))[1]) or "node"
        labels[node] = labels.get(node, 0) + 1
        if labels[node] > 1:
            node = "%s_%d" % (node, labels[node])
        meta["node"] = node
        series.append((meta, rows))
        kept.append((path, data, node))
    summary = summarize(
        series,
        job_start_ts=row["start_ts"],
        job_elapsed_sec=row["elapsed_sec"],
        alloc_cpus=row["alloc_cpus"],
        alloc_nodes=row["alloc_nodes"],
    )
    if summary is None:
        if settled:
            for path, _data, _node in kept:
                _remove(path)
        return False
    for _path, data, node in kept:
        _archive(cfg, data, int(job_id), row["submit_ts"], node)
    summary["series_path"] = archive_pattern(cfg, int(job_id), row["submit_ts"])
    db.upsert_monitor(conn, row["id"], summary, now)
    return True


def _bounded(value: Any, depth: int = 0) -> Any:
    """``value`` reduced to plain JSON of limited size and depth."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value if abs(value) < 1 << 63 else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return value[:4000]
    if depth >= 4:
        return None
    if isinstance(value, list):
        return [_bounded(item, depth + 1) for item in value[:200]]
    if isinstance(value, dict):
        return {
            str(key)[:200]: _bounded(item, depth + 1) for key, item in list(value.items())[:100]
        }
    return None


def clean_context(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """The fields ``hpg submit`` writes, and nothing else that a file may contain."""
    return {key: _bounded(ctx[key]) for key in _CONTEXT_KEYS if key in ctx}


def _ingest_context(conn, cfg: Config, path: str, now: int, log: Callable[[str], None]) -> bool:
    """Copy what ``hpg submit`` recorded to the job, or to every task of the array, it submitted."""
    name = os.path.basename(path)[: -len(".json")]
    if not name.isdigit():
        return False
    age = _age(path, now)
    if age is None:
        return False
    rows = db.jobs_of_submission(conn, int(name))
    if not rows:
        if age > ORPHAN_DAYS * 86400:
            _remove(path)
        return False
    owner_uid = uid_of(rows[0]["user"])
    try:
        if owner_uid is None:
            raise OSError("user %s is not known on this node" % rows[0]["user"])
        data, uid, _mtime = read_plain_file(path, CONTEXT_MAX_BYTES)
        if uid != owner_uid:
            raise Rejected("it is not owned by %s" % rows[0]["user"])
        ctx = json.loads(data.decode("utf-8", "replace"))
        if not isinstance(ctx, dict):
            raise Rejected("it is not a submit record")
    except (Rejected, ValueError, RecursionError) as exc:
        log("[profile] %s.json ignored: %s" % (name, exc))
        if age > GRACE_SEC:
            _remove(path)
        return False
    except OSError as exc:
        log("[profile] cannot read %s: %s" % (path, exc))
        return False
    ctx = clean_context(ctx)
    submitted = ctx.get("submitted_ts")
    input_gb = ctx.get("input_gb")
    if isinstance(input_gb, bool) or not isinstance(input_gb, (int, float)) or input_gb < 0:
        input_gb = None
    tools = ctx.get("tools")
    tools = [t[:64] for t in tools[:50] if isinstance(t, str)] if isinstance(tools, list) else []
    script_hash = ctx.get("script_hash")
    record = {
        "script_hash": script_hash[:128] if isinstance(script_hash, str) else None,
        "tools_json": json.dumps(tools),
        "input_gb": float(input_gb) if input_gb is not None else None,
        "detail_json": json.dumps(ctx, sort_keys=True),
    }
    stored = pending = 0
    for row in rows:
        if isinstance(submitted, (int, float)) and row["submit_ts"]:
            if abs(submitted - row["submit_ts"]) > 6 * 3600:
                continue  # an earlier use of this job id
        if db.has_context(conn, row["id"]):
            continue
        db.upsert_context(conn, row["id"], record, now)
        stored += 1
        pending += 1
    # A single job's file can go once its row has been published (seen on the
    # next run). Tasks of an array finish over days, so theirs is kept a while.
    if not pending and (not ctx.get("array") or age > ORPHAN_DAYS * 86400):
        _remove(path)
    return bool(stored)


def _is_own_dir(path: str, owner_uid: int) -> bool:
    """True when ``path`` itself (not something it links to) is a directory of ``owner_uid``."""
    try:
        info = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISDIR(info.st_mode) and info.st_uid == owner_uid


def ensure_spool(cfg: Config, log: Callable[[str], None] = print) -> bool:
    """Create the spool (and its ``submit`` folder) if it is missing. True when it exists.

    An existing directory is left exactly as it is: its owner may have chosen
    other permissions.
    """
    created = False
    submit = os.path.join(cfg.spool_dir, "submit")
    try:
        if not os.path.isdir(cfg.spool_dir):
            os.makedirs(cfg.spool_dir, exist_ok=True)
            os.chmod(cfg.spool_dir, cfg.spool_mode)  # explicitly: mkdir's mode is cut by the umask
            created = True
        if not os.path.lexists(submit):
            os.mkdir(submit)
            os.chmod(submit, cfg.spool_mode)
            created = True
    except OSError as exc:
        log("[profile] cannot create the spool %s: %s" % (cfg.spool_dir, exc))
        return os.path.isdir(cfg.spool_dir)
    if created:
        log(
            "[profile] created the spool %s (mode %o): jobs submitted with 'hpg submit' "
            "write their profiles there" % (cfg.spool_dir, cfg.spool_mode)
        )
    return True


def submit_folder(cfg: Config, log: Callable[[str], None] = print) -> Optional[str]:
    """The folder ``hpg submit`` writes to, when it can be believed: a real directory of the
    spool's owner. In a group-writable spool anything else could have been put there by anyone.
    """
    folder = os.path.join(cfg.spool_dir, "submit")
    try:
        owner = os.stat(cfg.spool_dir).st_uid
    except OSError:
        return None
    if not _is_own_dir(folder, owner):
        if os.path.lexists(folder):
            log(
                "[profile] %s is not a directory owned by the spool's owner; "
                "submit records are not read" % folder
            )
        return None
    return folder


def ingest_spool(
    conn, cfg: Config, log: Callable[[str], None] = print, now: Optional[int] = None
) -> int:
    """Store the profiles and submit contexts of jobs that have finished. Returns profiles stored.

    Called inside the harvest's write transaction. A spool file is removed
    only on a later run, once the database that references its archive has
    been published; nothing here can make a harvest fail.
    """
    now = int(time.time()) if now is None else int(now)
    # File names in the spool are chosen by whoever creates the file; nothing
    # of them may reach a log, or a terminal, as anything but visible text.
    raw_log = log
    log = lambda message: raw_log(clean(message))  # noqa: E731
    if not ensure_spool(cfg, log):
        return 0
    groups: Dict[str, List[str]] = {}
    for path in sorted(glob.glob(os.path.join(cfg.spool_dir, "*.tsv"))):
        parts = split_name(path)
        if parts:
            groups.setdefault(parts[0], []).append(path)
    stored = 0
    for job_id, paths in groups.items():
        try:
            stored += bool(_ingest_series(conn, cfg, job_id, paths, now, log))
        except Exception as exc:  # noqa: BLE001 - one bad file must not stop the harvest
            log("[profile] job %s: %s" % (job_id, exc))
    folder = submit_folder(cfg, log)
    if folder is None:
        return stored
    for path in sorted(glob.glob(os.path.join(folder, "*.json"))):
        try:
            _ingest_context(conn, cfg, path, now, log)
        except Exception as exc:  # noqa: BLE001
            log("[profile] %s: %s" % (os.path.basename(path), exc))
    for path in glob.glob(os.path.join(folder, ".*.tmp")):
        age = _age(path, now)  # left by an 'hpg submit' that was killed while writing
        if age is not None and age > GRACE_SEC:
            _remove(path)
    return stored


def describe(summary: Dict[str, Any]) -> List[str]:
    """Lines for people from a stored summary's detail (``job_monitor.summary_json``)."""
    lines = list(summary.get("notes") or [])
    nodes = summary.get("nodes") or []
    if len(nodes) > 1:
        for node in nodes:
            lines.append(
                "%s: peak memory %s, %s cores on average"
                % (
                    clean(node.get("node")),
                    fmt_bytes(node.get("peak_mem_bytes")),
                    fmt_num(node.get("avg_cores")),
                )
            )
    return lines
