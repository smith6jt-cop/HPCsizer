#!/usr/bin/env python3
"""In-job sampler for HPCsizer: what a job uses while it runs.

Slurm's accounting samples a job once a minute and records no CPU time for a
job it has to kill, so the jobs most worth sizing -- the ones that ran out of
memory or time, and interactive sessions, which always end by being stopped --
are the ones it knows least about. This sampler fills that in.

It is started in the background by one line that ``hpg submit`` adds to a job
script, and it does one thing: append a line per sample to

    <spool>/<jobid>.<node>.tsv

It never opens the database and writes no summary at exit, so it can be killed
at any moment without losing what it has recorded; the harvester reads the
file after the job has finished. It cannot change the job's exit status.

What is measured, in order of preference:

  the job's cgroup   (v2, then v1) everything the job ran on this node,
                     including processes that have already exited
  the process tree   below --pid, when no Slurm job cgroup is found

Columns prefixed ``node_`` are counters of the whole node, not of the job;
they are only meaningful when the job has the node to itself.

Runs with Python 3.6 or later and the standard library only.
"""

import argparse
import errno
import glob
import os
import re
import signal
import socket
import subprocess
import sys
import time

VERSION = 1

COLUMNS = (
    "t",  # epoch seconds
    "mem",  # bytes of memory that cannot be dropped (anonymous + shared)
    "usage",  # bytes charged to the job, page cache included
    "usage_peak",  # the kernel's high-water mark of that, or the largest value seen
    "cpu",  # CPU seconds used so far (cumulative)
    "nproc",
    "threads",
    "rd",  # bytes read by the processes alive now (cumulative per process)
    "wr",
    "gpu_util",  # percent, busiest GPU
    "gpu_mem",  # bytes, all GPUs of the job
    "oom",  # out-of-memory kills so far
    "top",  # largest processes: name:MB,...
    "node_lustre_rd",
    "node_lustre_wr",
    "node_lustre_meta",
    "node_numa_hit",
    "node_numa_miss",
    "perf_ipc",  # instructions per cycle of the busiest process
    "perf_cmiss",  # cache misses per cache reference of that process
)

MAX_PIDS = 512
MAX_ROWS_FAST = 20000  # beyond this, sample every five minutes
JOB_COMPONENT = re.compile(r"^job_(\d+)$")

_stop = []


def _on_signal(signum, _frame):
    _stop.append(signum)


# ---------------------------------------------------------------------------
# Small readers. Every one returns None (or an empty result) instead of raising.
# ---------------------------------------------------------------------------


def read_text(path):
    try:
        with open(path) as handle:
            return handle.read()
    except (OSError, IOError, UnicodeDecodeError):
        return None


def read_int(path):
    text = read_text(path)
    if text is None:
        return None
    try:
        return int(text.split()[0])
    except (ValueError, IndexError):
        return None  # "max", or empty


def read_keyed(path):
    """``key value`` lines as a dict of ints (memory.stat, cpu.stat, memory.events)."""
    out = {}
    text = read_text(path)
    if text is None:
        return out
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            try:
                out[parts[0]] = int(parts[1])
            except ValueError:
                pass
    return out


def proc_status(proc_root, pid):
    """VmRSS (bytes), thread count and name of a process."""
    out = {}
    text = read_text(os.path.join(proc_root, str(pid), "status"))
    if text is None:
        return out
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            out["rss"] = int(line.split()[1]) * 1024
        elif line.startswith("Threads:"):
            out["threads"] = int(line.split()[1])
        elif line.startswith("Name:"):
            out["name"] = line.split(None, 1)[1].strip() if len(line.split()) > 1 else ""
    return out


def proc_stat(proc_root, pid):
    """``(ppid, cpu_ticks, reaped_children_ticks)`` from /proc/<pid>/stat, or None."""
    text = read_text(os.path.join(proc_root, str(pid), "stat"))
    if text is None:
        return None
    # The command name is in parentheses and may contain spaces or parentheses.
    close = text.rfind(")")
    fields = text[close + 2 :].split()
    try:
        ppid = int(fields[1])
        own = int(fields[11]) + int(fields[12])  # utime + stime
        reaped = int(fields[13]) + int(fields[14])  # cutime + cstime
    except (IndexError, ValueError):
        return None
    return ppid, own, reaped


def proc_io(proc_root, pid):
    """``(rchar, wchar)``: bytes passed to read and write calls, whatever the file system."""
    rd = wr = None
    text = read_text(os.path.join(proc_root, str(pid), "io"))
    if text is None:
        return None, None
    for line in text.splitlines():
        if line.startswith("rchar:"):
            rd = int(line.split()[1])
        elif line.startswith("wchar:"):
            wr = int(line.split()[1])
    return rd, wr


def clock_ticks():
    try:
        return os.sysconf("SC_CLK_TCK")
    except (AttributeError, ValueError, OSError):
        return 100


def describe_processes(proc_root, pids):
    """Count, threads, I/O and the three largest processes among ``pids``."""
    threads = 0
    rd = wr = 0
    seen_io = False
    largest = []
    count = 0
    for pid in pids[:MAX_PIDS]:
        status = proc_status(proc_root, pid)
        if not status:
            continue
        count += 1
        threads += status.get("threads", 0)
        largest.append((status.get("rss", 0), status.get("name", "?")))
        r, w = proc_io(proc_root, pid)
        if r is not None:
            seen_io = True
            rd += r
            wr += w or 0
    largest.sort(reverse=True)
    top = ",".join(
        "%s:%d"
        % (name.replace(",", "_").replace(":", "_").replace("\t", " "), rss // (1024 * 1024))
        for rss, name in largest[:3]
        if rss > 0
    )
    return {
        "nproc": count if len(pids) <= MAX_PIDS else len(pids),
        "threads": threads,
        "rd": rd if seen_io else None,
        "wr": wr if seen_io else None,
        "top": top,
        "rss_sum": sum(rss for rss, _name in largest),
    }


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


def cgroup_pids(path):
    """PIDs in a cgroup and the cgroups below it."""
    pids = []
    for folder, _dirs, files in os.walk(path):
        if "cgroup.procs" in files:
            text = read_text(os.path.join(folder, "cgroup.procs"))
            if text:
                for token in text.split():
                    if token.isdigit():
                        pids.append(int(token))
        if len(pids) > 4 * MAX_PIDS:
            break
    return pids


class CgroupV2(object):
    name = "cgroup-v2"

    def __init__(self, path, proc_root):
        self.path = path
        self.proc_root = proc_root
        self.peak = 0

    def usable(self):
        return os.path.exists(os.path.join(self.path, "memory.current")) or os.path.exists(
            os.path.join(self.path, "cpu.stat")
        )

    def pids(self):
        return cgroup_pids(self.path)

    def sample(self):
        stat = read_keyed(os.path.join(self.path, "memory.stat"))
        usage = read_int(os.path.join(self.path, "memory.current"))
        mem = None
        if "anon" in stat:
            mem = stat["anon"] + stat.get("shmem", 0)
        # memory.peak exists from Linux 5.19; without it keep the largest value seen.
        peak = read_int(os.path.join(self.path, "memory.peak"))
        self.peak = max(self.peak, usage or 0, peak or 0)
        cpu = read_keyed(os.path.join(self.path, "cpu.stat")).get("usage_usec")
        events = read_keyed(os.path.join(self.path, "memory.events"))
        out = {
            "mem": mem,
            "usage": usage,
            "usage_peak": self.peak or None,
            "cpu": cpu / 1e6 if cpu is not None else None,
            "oom": events.get("oom_kill"),
        }
        out.update(describe_processes(self.proc_root, self.pids()))
        if out["mem"] is None and out["rss_sum"]:
            out["mem"] = out["rss_sum"]
        return out


class CgroupV1(object):
    name = "cgroup-v1"

    def __init__(self, memory_path, cpu_path, proc_root):
        self.memory = memory_path
        self.cpuacct = cpu_path
        self.proc_root = proc_root
        self.peak = 0

    def usable(self):
        return bool(
            (self.memory and os.path.exists(os.path.join(self.memory, "memory.usage_in_bytes")))
            or (self.cpuacct and os.path.exists(os.path.join(self.cpuacct, "cpuacct.usage")))
        )

    def pids(self):
        return cgroup_pids(self.memory or self.cpuacct)

    def sample(self):
        mem = usage = oom = None
        if self.memory:
            stat = read_keyed(os.path.join(self.memory, "memory.stat"))
            usage = read_int(os.path.join(self.memory, "memory.usage_in_bytes"))
            rss = stat.get("total_rss", stat.get("rss"))
            if rss is not None:
                mem = rss + stat.get("total_shmem", stat.get("shmem", 0))
            peak = read_int(os.path.join(self.memory, "memory.max_usage_in_bytes"))
            self.peak = max(self.peak, usage or 0, peak or 0)
            oom = read_keyed(os.path.join(self.memory, "memory.oom_control")).get("oom_kill")
        cpu = read_int(os.path.join(self.cpuacct, "cpuacct.usage")) if self.cpuacct else None
        out = {
            "mem": mem,
            "usage": usage,
            "usage_peak": self.peak or None,
            "cpu": cpu / 1e9 if cpu is not None else None,
            "oom": oom,
        }
        out.update(describe_processes(self.proc_root, self.pids()))
        if out["mem"] is None and out["rss_sum"]:
            out["mem"] = out["rss_sum"]
        return out


class ProcessTree(object):
    """Everything below one process. Used when the job has no cgroup of its own."""

    name = "proc"

    def __init__(self, root_pid, proc_root):
        self.root = int(root_pid)
        self.proc_root = proc_root
        self.ticks = clock_ticks()
        self.cpu_seen = 0.0

    def usable(self):
        return proc_stat(self.proc_root, self.root) is not None

    def members(self):
        """The root and its descendants, with their CPU ticks."""
        stats = {}
        try:
            names = os.listdir(self.proc_root)
        except OSError:
            return {}
        for name in names:
            if name.isdigit():
                stat = proc_stat(self.proc_root, int(name))
                if stat is not None:
                    stats[int(name)] = stat
        children = {}
        for pid, (ppid, _own, _reaped) in stats.items():
            children.setdefault(ppid, []).append(pid)
        tree = {}
        todo = [self.root]
        while todo:
            pid = todo.pop()
            if pid in tree or pid not in stats:
                continue
            tree[pid] = stats[pid]
            todo.extend(children.get(pid, ()))
        return tree

    def pids(self):
        return sorted(self.members())

    def sample(self):
        tree = self.members()
        # CPU of the live processes plus that of the children they have reaped.
        # A process that exits takes its own time out of the sum until its
        # parent reaps it, so only increases are kept.
        cpu = sum(own + reaped for _ppid, own, reaped in tree.values()) / float(self.ticks)
        self.cpu_seen = max(self.cpu_seen, cpu)
        out = {"usage": None, "usage_peak": None, "cpu": self.cpu_seen, "oom": None}
        out.update(describe_processes(self.proc_root, sorted(tree)))
        out["mem"] = out["rss_sum"]
        return out


def job_cgroup_relpath(path, job_id=None):
    """``/a/job_123/step_batch/task_0`` -> ``/a/job_123``; None when there is no job component."""
    parts = path.strip().split("/")
    for index, part in enumerate(parts):
        match = JOB_COMPONENT.match(part)
        if match and (job_id is None or match.group(1) == str(job_id)):
            return "/".join(parts[: index + 1])
    if job_id is not None:
        return job_cgroup_relpath(path, None)
    return None


def find_source(pid, job_id, proc_root="/proc", cgroup_root="/sys/fs/cgroup", tree_root=None):
    """The best available source: the job cgroup ``pid`` is in, else the tree below ``tree_root``."""
    text = read_text(os.path.join(proc_root, str(pid), "cgroup")) or ""
    v1_memory = v1_cpu = None
    for line in text.splitlines():
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        _hierarchy, controllers, path = parts
        rel = job_cgroup_relpath(path, job_id)
        if rel is None:
            continue
        if controllers == "":
            source = CgroupV2(os.path.join(cgroup_root, rel.lstrip("/")), proc_root)
            if source.usable():
                return source
            continue
        names = controllers.split(",")
        for mount in (controllers, ",".join(reversed(names))) + tuple(names):
            candidate = os.path.join(cgroup_root, mount, rel.lstrip("/"))
            if not os.path.isdir(candidate):
                continue
            if "memory" in names and v1_memory is None:
                v1_memory = candidate
            if "cpuacct" in names and v1_cpu is None:
                v1_cpu = candidate
    if v1_memory or v1_cpu:
        source = CgroupV1(v1_memory, v1_cpu, proc_root)
        if source.usable():
            return source
    source = ProcessTree(tree_root or pid, proc_root)
    return source if source.usable() else None


# ---------------------------------------------------------------------------
# Optional readers: GPU, node counters, perf
# ---------------------------------------------------------------------------


def run_quietly(cmd, timeout):
    """stdout of a command, or None. Never raises, never leaves the child running."""
    try:
        child = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
    except (OSError, ValueError):
        return None
    try:
        out, err = child.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        child.kill()
        child.communicate()
        return None
    except Exception:
        return None
    if child.returncode != 0:
        return None
    return out + "\n" + err


def job_has_gpus(environ):
    """Was this job given a GPU? Device lists name indices, so "0" there is a GPU."""
    for key in ("SLURM_JOB_GPUS", "SLURM_STEP_GPUS", "CUDA_VISIBLE_DEVICES"):
        value = environ.get(key, "").strip()
        if value and value not in ("NoDevFiles", "-1"):
            return True
    count = environ.get("SLURM_GPUS_ON_NODE", "").strip()
    return count.isdigit() and int(count) > 0


def parse_nvidia_smi(text):
    """``(busiest utilisation %, total memory bytes)`` from ``utilization.gpu, memory.used`` rows."""
    util = None
    mem = 0
    seen = False
    for line in (text or "").splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            continue
        try:
            u, m = float(parts[0]), float(parts[1])
        except ValueError:
            continue
        seen = True
        util = u if util is None else max(util, u)
        mem += int(m * 1024 * 1024)
    return (util, mem) if seen else (None, None)


def read_gpus():
    out = run_quietly(
        ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used", "--format=csv,noheader,nounits"],
        timeout=15,
    )
    return parse_nvidia_smi(out)


def read_lustre(base="/proc/fs/lustre/llite"):
    """Node-wide Lustre client counters: bytes read, bytes written, metadata operations."""
    rd = wr = meta = 0
    found = False
    for path in glob.glob(os.path.join(base, "*", "stats")):
        text = read_text(path)
        if text is None:
            continue
        found = True
        for line in text.splitlines():
            parts = line.split()
            if not parts:
                continue
            try:
                if parts[0] == "read_bytes" and len(parts) >= 7:
                    rd += int(parts[6])
                elif parts[0] == "write_bytes" and len(parts) >= 7:
                    wr += int(parts[6])
                elif parts[0] in ("open", "close", "mmap", "seek", "getattr", "statfs"):
                    meta += int(parts[1])
            except ValueError:
                pass
    return (rd, wr, meta) if found else (None, None, None)


def read_numa(base="/sys/devices/system/node"):
    """Node-wide NUMA allocation counters since boot; the rate is taken between samples."""
    hit = miss = 0
    found = False
    for path in glob.glob(os.path.join(base, "node*", "numastat")):
        stats = read_keyed(path)
        if stats:
            found = True
            hit += stats.get("numa_hit", 0)
            miss += stats.get("numa_miss", 0)
    return (hit, miss) if found else (None, None)


PERF_EVENTS = "cycles:u,instructions:u,cache-references:u,cache-misses:u"


def parse_perf_csv(text):
    """``(instructions per cycle, cache misses per reference)`` from ``perf stat -x,`` output."""
    counts = {}
    for line in (text or "").splitlines():
        parts = line.split(",")
        if len(parts) < 3:
            continue
        try:
            value = float(parts[0])
        except ValueError:
            continue  # "<not counted>", "<not supported>"
        counts[parts[2].split(":")[0]] = value
    ipc = cmiss = None
    if counts.get("cycles"):
        ipc = counts.get("instructions", 0.0) / counts["cycles"]
    if counts.get("cache-references"):
        cmiss = counts.get("cache-misses", 0.0) / counts["cache-references"]
    return ipc, cmiss


def read_perf(pid, seconds=2):
    out = run_quietly(
        ["perf", "stat", "-x,", "-e", PERF_EVENTS, "-p", str(pid), "--", "sleep", str(seconds)],
        timeout=seconds + 10,
    )
    return parse_perf_csv(out)


def busiest_pid(proc_root, pids, previous):
    """The process whose CPU time grew most since ``previous`` (a dict the caller keeps)."""
    best, best_delta = None, 0
    current = {}
    for pid in pids[:MAX_PIDS]:
        stat = proc_stat(proc_root, pid)
        if stat is None:
            continue
        current[pid] = stat[1]
        delta = stat[1] - previous.get(pid, stat[1])
        if delta > best_delta:
            best, best_delta = pid, delta
    previous.clear()
    previous.update(current)
    return best


# ---------------------------------------------------------------------------
# The loop
# ---------------------------------------------------------------------------


def poll_interval(elapsed, rows=0):
    """Seconds to the next sample: dense at first, when accounting sees nothing at all.

    Slurm samples once a minute, so a job shorter than two minutes has no
    usable accounting; those two minutes are sampled every 2 seconds here.
    """
    if rows >= MAX_ROWS_FAST:
        return 300
    if elapsed < 120:
        return 2
    if elapsed < 600:
        return 10
    if elapsed < 3600:
        return 30
    return 60


def alive(pid):
    try:
        os.kill(pid, 0)
    except OSError as exc:
        return exc.errno == errno.EPERM
    return True


def fmt(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return "%.3f" % value
    return str(value)


def open_series(spool, job_id, node):
    """Open the series file for appending, creating it group-readable. Returns ``(fd, path)``."""
    path = os.path.join(spool, "%s.%s.tsv" % (job_id, node))
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
    try:
        os.fchmod(fd, 0o640)  # whatever the job's umask: the harvester must be able to read it
    except OSError:
        pass
    return fd, path


def write_line(fd, text):
    os.write(fd, (text + "\n").encode("utf-8", "replace"))


def fan_out(args, environ):
    """On a multi-node job, start a sampler on every other node of the allocation."""
    try:
        nodes = int(environ.get("SLURM_JOB_NUM_NODES", "1"))
    except ValueError:
        return None
    if nodes < 2:
        return None
    cmd = [
        "srun",
        "--overlap",
        "--nodes=%d" % nodes,
        "--ntasks=%d" % nodes,
        "--ntasks-per-node=1",
        "--quiet",
        sys.executable,
        "-S",
        os.path.abspath(__file__),
        "--spool",
        args.spool,
        "--no-fan-out",
        "--skip-node",
        node_name(environ),
    ]
    if args.interval:
        cmd += ["--interval", str(args.interval)]
    try:
        with open(os.devnull, "r+") as null:
            return subprocess.Popen(cmd, stdin=null, stdout=null, stderr=null)
    except (OSError, ValueError):
        return None


def node_name(environ):
    return (environ.get("SLURMD_NODENAME") or socket.gethostname()).split(".")[0]


def sample_row(source, state, args, environ):
    """One sample as a list of column values, or None when there is nothing left to measure."""
    data = source.sample()
    if source.name == "proc" and not data.get("nproc"):
        return None  # the process tree has ended; a row of zeros would only blur the series
    row = dict.fromkeys(COLUMNS)
    row["t"] = int(time.time())
    for key in ("mem", "usage", "usage_peak", "cpu", "nproc", "threads", "rd", "wr", "oom", "top"):
        row[key] = data.get(key)
    if state["gpus"]:
        util, mem = read_gpus()
        if util is None:
            state["gpu_failures"] += 1
            if state["gpu_failures"] >= 3:
                state["gpus"] = False
        else:
            row["gpu_util"], row["gpu_mem"] = util, mem
    if state["node_counters"]:
        row["node_lustre_rd"], row["node_lustre_wr"], row["node_lustre_meta"] = read_lustre()
        row["node_numa_hit"], row["node_numa_miss"] = read_numa()
    if state["perf"] and row["t"] - state["perf_last"] >= 300:
        state["perf_last"] = row["t"]
        target = busiest_pid(args.proc_root, source.pids(), state["perf_ticks"])
        if target is not None:
            ipc, cmiss = read_perf(target)
            if ipc is None and cmiss is None:
                state["perf_failures"] += 1
                if state["perf_failures"] >= 2:
                    state["perf"] = False
            row["perf_ipc"], row["perf_cmiss"] = ipc, cmiss
    return [fmt(row[name]) for name in COLUMNS]


def run(args, environ=None):
    environ = os.environ if environ is None else environ
    node = node_name(environ)
    if args.skip_node and node == args.skip_node:
        return 0
    job_id = args.job or environ.get("SLURM_JOB_ID")
    if not job_id:
        return 0  # not inside a job: nothing to record, nothing to complain about
    watch = args.pid or os.getppid()
    # The cgroup is looked up from this process, which Slurm placed in the job;
    # the tree fallback starts at the job script's shell.
    source = find_source(os.getpid(), job_id, args.proc_root, args.cgroup_root, tree_root=watch)
    if source is None:
        return 0
    try:
        fd, _path = open_series(args.spool, job_id, node)
    except OSError:
        return 0  # no spool to write to: the job is simply not profiled

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        try:
            signal.signal(signum, _on_signal)
        except (OSError, ValueError):
            pass

    started = time.time()
    try:
        write_line(
            fd,
            "#hpcsizer-sampler v%d job=%s node=%s source=%s pid=%d cpus=%s start=%d user=%s"
            % (
                VERSION,
                job_id,
                node,
                source.name,
                watch,
                environ.get("SLURM_CPUS_ON_NODE", ""),
                int(started),
                environ.get("USER", ""),
            ),
        )
        write_line(fd, "\t".join(COLUMNS))
    except OSError:
        return 0

    state = {
        "gpus": job_has_gpus(environ),
        "gpu_failures": 0,
        "node_counters": not args.no_node_counters,
        "perf": bool(args.perf),
        "perf_last": int(started),
        "perf_failures": 0,
        "perf_ticks": {},
    }
    child = None if args.no_fan_out else fan_out(args, environ)

    rows = 0
    limit = started + args.max_days * 86400.0
    try:
        while True:
            try:
                values = sample_row(source, state, args, environ)
                if values is not None:
                    write_line(fd, "\t".join(values))
                    rows += 1
            except OSError as exc:
                if exc.errno in (errno.ENOSPC, errno.EDQUOT, errno.EROFS):
                    break  # nowhere to write: stop quietly
            if _stop or time.time() > limit:
                break
            wait = args.interval or poll_interval(time.time() - started, rows)
            deadline = time.time() + wait
            gone = False
            while time.time() < deadline and not _stop:
                if not alive(watch):
                    gone = True
                    break
                time.sleep(min(1.0, max(deadline - time.time(), 0.05)))
            if gone:
                # The job script has ended: one last sample, then leave.
                try:
                    values = sample_row(source, state, args, environ)
                    if values is not None:
                        write_line(fd, "\t".join(values))
                except OSError:
                    pass
                break
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
        if child is not None and child.poll() is None:
            try:
                child.terminate()
            except OSError:
                pass
    return 0


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--spool", required=True, help="directory the series file is written to")
    parser.add_argument("--pid", type=int, help="the job script's shell; sampling ends with it")
    parser.add_argument("--job", help="job id (default: $SLURM_JOB_ID)")
    parser.add_argument(
        "--interval", type=float, help="seconds between samples (default: adaptive)"
    )
    parser.add_argument("--max-days", type=float, default=31.0)
    parser.add_argument("--perf", action="store_true", help="also sample perf counters every 5 min")
    parser.add_argument("--no-node-counters", action="store_true")
    parser.add_argument(
        "--no-fan-out", action="store_true", help="do not start samplers on other nodes"
    )
    parser.add_argument("--skip-node", help=argparse.SUPPRESS)
    parser.add_argument("--proc-root", default="/proc", help=argparse.SUPPRESS)
    parser.add_argument("--cgroup-root", default="/sys/fs/cgroup", help=argparse.SUPPRESS)
    return parser


def main(argv=None):
    try:
        args = build_parser().parse_args(argv)
        return run(args)
    except SystemExit:
        raise
    except BaseException:  # noqa: B902 - whatever happens here must not reach the job
        return 0


if __name__ == "__main__":
    sys.exit(main())
