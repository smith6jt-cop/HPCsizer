"""Tests for bin/monitor.py, the in-job sampler.

It is exercised against simulated cgroup trees (v2 and v1), against the real
process tree of this test run, and as a separate process the way an injected
job script starts it.
"""

import ast
import importlib.util
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from helpers import deadline

from lib import profile

REPO = Path(__file__).resolve().parent.parent
SAMPLER = REPO / "bin" / "monitor.py"

spec = importlib.util.spec_from_file_location("hpg_sampler", str(SAMPLER))
sampler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sampler)

MIB = 1024 * 1024


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def fake_process(
    proc, pid, rss_kb=2048, name="python", threads=2, rchar=1000, wchar=500, ticks=(300, 100)
):
    write(
        proc / str(pid) / "status",
        "Name:\t%s\nVmRSS:\t%d kB\nThreads:\t%d\n" % (name, rss_kb, threads),
    )
    write(
        proc / str(pid) / "io",
        "rchar: %d\nwchar: %d\nread_bytes: 0\nwrite_bytes: 0\n" % (rchar, wchar),
    )
    stat = "%d (%s) S 1 1 1 0 -1 0 0 0 0 0 %d %d 0 0 20 0 1 0 0 0 0\n" % (
        pid,
        name,
        ticks[0],
        ticks[1],
    )
    write(proc / str(pid) / "stat", stat)


@pytest.fixture
def v2(tmp_path):
    """A node as Slurm sets it up with cgroup v2: job 4242, one step, two processes."""
    proc, cg = tmp_path / "proc", tmp_path / "cgroup"
    rel = "system.slice/slurmstepd.scope/job_4242"
    write(proc / "100" / "cgroup", "0::/%s/step_batch/user/task_0\n" % rel)
    job = cg / rel
    write(job / "memory.current", "%d\n" % (900 * MIB))
    write(job / "memory.peak", "%d\n" % (1500 * MIB))
    write(job / "memory.stat", "anon %d\nfile %d\nshmem %d\n" % (600 * MIB, 250 * MIB, 50 * MIB))
    write(job / "memory.events", "low 0\nhigh 0\nmax 3\noom 1\noom_kill 1\n")
    write(job / "cpu.stat", "usage_usec 7500000\nuser_usec 7000000\nsystem_usec 500000\n")
    write(job / "step_batch" / "user" / "task_0" / "cgroup.procs", "100\n101\n")
    write(job / "step_extern" / "cgroup.procs", "")
    fake_process(proc, 100, rss_kb=4096, name="bash")
    fake_process(proc, 101, rss_kb=600 * 1024, name="python3", threads=8, rchar=5 * MIB, wchar=MIB)
    return proc, cg, job


@pytest.fixture
def v1(tmp_path):
    proc, cg = tmp_path / "proc", tmp_path / "cgroup"
    rel = "slurm/uid_1001/job_4242"
    write(
        proc / "100" / "cgroup",
        "9:memory:/%s/step_batch/task_0\n4:cpu,cpuacct:/%s/step_batch/task_0\n1:name=systemd:/x\n"
        % (rel, rel),
    )
    mem = cg / "memory" / rel
    write(mem / "memory.usage_in_bytes", "%d\n" % (900 * MIB))
    write(mem / "memory.max_usage_in_bytes", "%d\n" % (1500 * MIB))
    write(
        mem / "memory.stat",
        "rss 1\ntotal_rss %d\ntotal_shmem %d\ntotal_cache 9\n" % (600 * MIB, 50 * MIB),
    )
    write(mem / "memory.oom_control", "oom_kill_disable 0\nunder_oom 0\noom_kill 2\n")
    write(mem / "step_batch" / "task_0" / "cgroup.procs", "100\n")
    write(cg / "cpu,cpuacct" / rel / "cpuacct.usage", "7500000000\n")
    fake_process(proc, 100, rss_kb=600 * 1024)
    return proc, cg


class TestJobCgroup:
    def test_path_is_cut_at_the_job(self):
        cut = sampler.job_cgroup_relpath
        assert cut("/system.slice/slurmstepd.scope/job_4242/step_batch/user/task_0") == (
            "/system.slice/slurmstepd.scope/job_4242"
        )
        assert cut("/slurm/uid_1/job_7/step_0", job_id="7") == "/slurm/uid_1/job_7"
        assert cut("/user.slice/user-1000.slice/session-3.scope") is None

    def test_cgroup_v2_measures_the_whole_job(self, v2):
        proc, cg, _job = v2
        source = sampler.find_source(100, "4242", str(proc), str(cg))
        assert source.name == "cgroup-v2"
        data = source.sample()
        assert data["mem"] == 650 * MIB, "anonymous + shared memory: what cannot be dropped"
        assert data["usage"] == 900 * MIB and data["usage_peak"] == 1500 * MIB
        assert data["cpu"] == 7.5 and data["oom"] == 1
        assert data["nproc"] == 2 and data["threads"] == 10
        assert data["rd"] == 5 * MIB + 1000 and data["top"] == "python3:600,bash:4"

    def test_without_memory_peak_the_largest_value_seen_is_kept(self, v2):
        proc, cg, job = v2
        (job / "memory.peak").unlink()
        source = sampler.find_source(100, "4242", str(proc), str(cg))
        assert source.sample()["usage_peak"] == 900 * MIB
        write(job / "memory.current", "%d\n" % (400 * MIB))
        assert source.sample()["usage_peak"] == 900 * MIB

    def test_cgroup_v1(self, v1):
        proc, cg = v1
        source = sampler.find_source(100, "4242", str(proc), str(cg))
        assert source.name == "cgroup-v1"
        data = source.sample()
        assert data["mem"] == 650 * MIB and data["usage_peak"] == 1500 * MIB
        assert data["cpu"] == 7.5 and data["oom"] == 2 and data["nproc"] == 1

    def test_falls_back_to_the_process_tree(self, tmp_path):
        proc = tmp_path / "proc"
        write(proc / "100" / "cgroup", "0::/user.slice/session-3.scope\n")
        fake_process(proc, 100, name="bash")
        source = sampler.find_source(100, "4242", str(proc), str(tmp_path / "none"))
        assert source.name == "proc"
        assert sampler.find_source(999, "4242", str(proc), str(tmp_path / "none")) is None


class TestProcessTree:
    def test_real_children_are_found_and_measured(self):
        child = subprocess.Popen(
            [sys.executable, "-c", "x = bytearray(60 * 1024 * 1024); import time; time.sleep(30)"]
        )
        try:
            time.sleep(0.5)
            tree = sampler.ProcessTree(os.getpid(), "/proc")
            assert child.pid in tree.members() and os.getpid() in tree.members()
            data = tree.sample()
            assert data["mem"] > 60 * MIB and data["nproc"] >= 2
            first = data["cpu"]
            sum(range(2_000_000))
            assert tree.sample()["cpu"] >= first
        finally:
            child.kill()
            child.wait()

    def test_cpu_time_of_finished_children_is_kept(self):
        tree = sampler.ProcessTree(os.getpid(), "/proc")
        before = tree.sample()["cpu"]
        subprocess.run([sys.executable, "-c", "sum(range(6_000_000))"], check=True)
        assert tree.sample()["cpu"] > before + 0.05, "a reaped child's time stays with its parent"

    def test_command_names_with_spaces_and_parentheses(self, tmp_path):
        proc = tmp_path / "proc"
        write(proc / "7" / "stat", "7 (my (odd) name) S 1 1 1 0 -1 0 0 0 0 0 50 25 5 5 20 0 1 0\n")
        assert sampler.proc_stat(str(proc), 7) == (1, 75, 10)


class TestOptionalReaders:
    def test_nvidia_smi(self):
        assert sampler.parse_nvidia_smi("38, 1000\n91, 30500\n") == (91.0, 31500 * MIB)
        assert sampler.parse_nvidia_smi("No devices were found\n") == (None, None)
        assert sampler.parse_nvidia_smi(None) == (None, None)

    def test_which_jobs_have_gpus(self):
        assert sampler.job_has_gpus({"SLURM_JOB_GPUS": "0,1"})
        assert sampler.job_has_gpus({"CUDA_VISIBLE_DEVICES": "0"}), (
            "a device list: index 0 is a GPU"
        )
        assert sampler.job_has_gpus({"SLURM_GPUS_ON_NODE": "2"})
        assert not sampler.job_has_gpus({"SLURM_GPUS_ON_NODE": "0"}), "a count: zero is none"
        assert not sampler.job_has_gpus({"CUDA_VISIBLE_DEVICES": "NoDevFiles"})
        assert not sampler.job_has_gpus({"CUDA_VISIBLE_DEVICES": ""}) and not sampler.job_has_gpus(
            {}
        )

    def test_perf_counts_become_rates(self):
        """Version 2 stored the raw cache-miss count and called it a rate."""
        text = (
            "2000000,,cycles:u,1000,100.00,,\n3000000,,instructions:u,1000,100.00,1.50,insn per cycle\n"
            "40000,,cache-references:u,1000,100.00,,\n1000,,cache-misses:u,1000,100.00,2.5,of all cache refs\n"
        )
        assert sampler.parse_perf_csv(text) == (1.5, 0.025)
        assert sampler.parse_perf_csv("<not counted>,,cycles:u,0,0.00,,\n") == (None, None)

    def test_lustre_and_numa_counters_are_cumulative(self, tmp_path):
        write(
            tmp_path / "llite" / "blue-ffff" / "stats",
            "snapshot_time 1.0 secs.nsecs\nread_bytes 10 samples [bytes] 1 4096 5000\n"
            "write_bytes 4 samples [bytes] 1 4096 700\nopen 12 samples [regs]\nclose 11 samples [regs]\n",
        )
        assert sampler.read_lustre(str(tmp_path / "llite")) == (5000, 700, 23)
        assert sampler.read_lustre(str(tmp_path / "absent")) == (None, None, None)
        write(
            tmp_path / "node" / "node0" / "numastat",
            "numa_hit 900\nnuma_miss 100\nnuma_foreign 3\n",
        )
        write(tmp_path / "node" / "node1" / "numastat", "numa_hit 100\nnuma_miss 0\n")
        assert sampler.read_numa(str(tmp_path / "node")) == (1000, 100)

    def test_interval_grows_with_the_run(self):
        times = (0, 119, 120, 599, 600, 3599, 3600)
        assert [sampler.poll_interval(t) for t in times] == [2, 2, 10, 10, 30, 30, 60]
        assert sampler.poll_interval(100000, rows=sampler.MAX_ROWS_FAST) == 300


JOB_SCRIPT = """#!/bin/bash
( HPCSIZER=sampler exec {python} -S {sampler} --spool {spool} --pid $$ --interval 0.3 --no-node-counters </dev/null >/dev/null 2>&1 & )
{python} -c "
import time
x = bytearray(80 * 1024 * 1024)
t = time.time()
while time.time() - t < 1.5: sum(range(20000))
" &
wait
echo done
"""


def sampler_running(spool):
    """Is a sampler writing to this spool still alive? (Read from /proc: no pgrep needed.)"""
    needle = ("--spool %s" % spool).encode()
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open("/proc/%s/cmdline" % name, "rb") as handle:
                command = handle.read().replace(b"\0", b" ")
        except OSError:
            continue
        if b"monitor.py" in command and needle in command:
            return True
    return False


class TestAsAProcess:
    def run_job(self, tmp_path, spool, env=None):
        script = tmp_path / "job.sh"
        script.write_text(JOB_SCRIPT.format(python=sys.executable, sampler=SAMPLER, spool=spool))
        environ = dict(
            os.environ, SLURM_JOB_ID="4242", SLURM_CPUS_ON_NODE="4", SLURMD_NODENAME="c1"
        )
        environ.update(env or {})
        started = time.time()
        done = subprocess.run(
            ["bash", str(script)], stdout=subprocess.PIPE, env=environ, timeout=60
        )
        return done, time.time() - started

    def test_injected_line_records_the_job_and_gets_out_of_its_way(self, tmp_path):
        spool = tmp_path / "spool"
        spool.mkdir()
        done, took = self.run_job(tmp_path, spool)
        assert done.returncode == 0 and done.stdout == b"done\n"
        assert took < 4, "the script's own `wait` must not wait for the sampler"
        series = spool / "4242.c1.tsv"
        deadline = time.time() + 10
        while time.time() < deadline and sampler_running(spool):
            time.sleep(0.2)
        assert not sampler_running(spool), "the sampler ends with the job script"
        assert oct(series.stat().st_mode & 0o777) == "0o640"
        meta, rows = profile.read_series(str(series))
        assert meta["job"] == "4242" and meta["node"] == "c1" and meta["source"] == "proc"
        assert len(rows) >= 3
        assert max(r["mem"] for r in rows) > 80 * MIB
        assert rows[-1]["cpu"] > 1.0 and all(b["cpu"] >= a["cpu"] for a, b in zip(rows, rows[1:]))
        assert any("python" in (r["top"] or "") for r in rows)

    def test_missing_spool_changes_nothing_for_the_job(self, tmp_path):
        done, _took = self.run_job(tmp_path, tmp_path / "no-such-dir")
        assert done.returncode == 0 and done.stdout == b"done\n"

    def test_outside_a_job_it_does_nothing(self, tmp_path):
        environ = {k: v for k, v in os.environ.items() if not k.startswith("SLURM")}
        done = subprocess.run(
            [sys.executable, "-S", str(SAMPLER), "--spool", str(tmp_path)], env=environ, timeout=30
        )
        assert done.returncode == 0 and list(tmp_path.iterdir()) == []

    def test_sigterm_ends_it_cleanly_with_whole_lines(self, tmp_path):
        environ = dict(os.environ, SLURM_JOB_ID="77", SLURMD_NODENAME="c2")
        child = subprocess.Popen(
            [sys.executable, "-S", str(SAMPLER), "--spool", str(tmp_path), "--pid", str(os.getpid()),
             "--interval", "0.2", "--no-node-counters"],
            env=environ,
        )  # fmt: skip
        series = tmp_path / "77.c2.tsv"
        deadline = time.time() + 10
        while time.time() < deadline and (
            not series.exists() or series.read_text().count("\n") < 4
        ):
            time.sleep(0.1)
        child.send_signal(signal.SIGTERM)
        assert child.wait(timeout=10) == 0
        text = series.read_text()
        assert text.endswith("\n")
        _meta, rows = profile.parse_series(text)
        assert len(rows) >= 2

    def test_runs_on_the_python_of_old_compute_nodes(self):
        ast.parse(SAMPLER.read_text(), feature_version=(3, 6))


class TestSeriesFile:
    """The spool is writable by the whole group: the name a job would use may be taken."""

    def test_new_file_is_group_readable_whatever_the_umask(self, tmp_path):
        before = os.umask(0o077)
        try:
            fd, path = sampler.open_series(str(tmp_path), "4242", "c1")
        finally:
            os.umask(before)
        os.close(fd)
        assert path == str(tmp_path / "4242.c1.tsv")
        assert oct(os.stat(path).st_mode & 0o777) == "0o640"

    def test_requeued_job_appends_to_its_own_file(self, tmp_path):
        fd, path = sampler.open_series(str(tmp_path), "4242", "c1")
        sampler.write_line(fd, "first run")
        os.close(fd)
        fd, again = sampler.open_series(str(tmp_path), "4242", "c1")
        sampler.write_line(fd, "second run")
        os.close(fd)
        assert again == path and open(path).read() == "first run\nsecond run\n"

    def test_link_to_one_of_the_users_files_is_not_written_through(self, tmp_path):
        """Somebody links the name to a file of the job's user: appending to it would damage
        that file, and the chmod would open a private one to the group."""
        spool = tmp_path / "spool"
        spool.mkdir()
        private = tmp_path / "private.txt"
        private.write_text("mine\n")
        private.chmod(0o600)
        os.symlink(str(private), str(spool / "4242.c1.tsv"))
        fd, path = sampler.open_series(str(spool), "4242", "c1")
        sampler.write_line(fd, "sample")
        os.close(fd)
        assert private.read_text() == "mine\n"
        assert oct(private.stat().st_mode & 0o777) == "0o600"
        assert os.path.basename(path) == "4242.c1-%d-0.tsv" % os.getpid()
        assert open(path).read() == "sample\n" and not os.path.islink(path)

    def test_dangling_link_does_not_create_its_target(self, tmp_path):
        spool = tmp_path / "spool"
        spool.mkdir()
        target = tmp_path / "new-file-in-your-home"
        os.symlink(str(target), str(spool / "4242.c1.tsv"))
        fd, path = sampler.open_series(str(spool), "4242", "c1")
        os.close(fd)
        assert not target.exists() and path != str(spool / "4242.c1.tsv")

    def test_second_name_of_one_of_the_users_files_is_not_written_to(self, tmp_path):
        spool = tmp_path / "spool"
        spool.mkdir()
        data = tmp_path / "results.tsv"
        data.write_text("precious\n")
        os.link(str(data), str(spool / "4242.c1.tsv"))
        fd, path = sampler.open_series(str(spool), "4242", "c1")
        sampler.write_line(fd, "sample")
        os.close(fd)
        assert data.read_text() == "precious\n" and path != str(spool / "4242.c1.tsv")

    def test_somebody_elses_file_under_the_name(self, tmp_path, monkeypatch):
        fd, taken = sampler.open_series(str(tmp_path), "4242", "c1")
        os.close(fd)
        monkeypatch.setattr(sampler.os, "geteuid", lambda: os.getuid() + 1)
        fd, path = sampler.open_series(str(tmp_path), "4242", "c1")
        os.close(fd)
        assert path != taken and open(taken).read() == ""

    def test_pipe_under_the_name_cannot_hold_the_sampler(self, tmp_path):
        os.mkfifo(str(tmp_path / "4242.c1.tsv"))
        reader = os.open(str(tmp_path / "4242.c1.tsv"), os.O_RDONLY | os.O_NONBLOCK)
        try:
            with deadline():
                fd, path = sampler.open_series(str(tmp_path), "4242", "c1")
        finally:
            os.close(reader)
        os.close(fd)
        assert path == str(tmp_path / ("4242.c1-%d-0.tsv" % os.getpid()))

    def test_names_are_made_of_safe_characters_only(self, tmp_path):
        fd, path = sampler.open_series(str(tmp_path), "4242", "../evil node")
        os.close(fd)
        assert path == str(tmp_path / "4242.___evil_node.tsv")
        with pytest.raises(OSError):
            sampler.open_series(str(tmp_path), "../4242", "c1")
        with pytest.raises(OSError):
            sampler.open_series(str(tmp_path), "", "c1")

    def test_series_under_its_own_name_is_still_ingested_by_job_id(self, tmp_path):
        spool = tmp_path / "spool"
        spool.mkdir()
        os.symlink("/nonexistent", str(spool / "4242.c1.tsv"))
        fd, path = sampler.open_series(str(spool), "4242", "c1")
        os.close(fd)
        assert profile.split_name(path)[0] == "4242"

    def test_run_with_a_bad_job_id_does_nothing(self, tmp_path):
        args = sampler.build_parser().parse_args(["--spool", str(tmp_path), "--job", "../x"])
        assert sampler.run(args, {"SLURMD_NODENAME": "c1"}) == 0
        assert list(tmp_path.iterdir()) == []


class TestSeveralNodes:
    def test_sampler_on_the_batch_node_starts_one_on_each_other_node(self, tmp_path, monkeypatch):
        calls = tmp_path / "srun.calls"
        fake = tmp_path / "bin" / "srun"
        write(fake, '#!/bin/sh\nprintf "%%s\\n" "$*" >> %s\n' % calls)
        fake.chmod(0o755)
        monkeypatch.setenv("PATH", "%s:%s" % (fake.parent, os.environ["PATH"]))
        args = sampler.build_parser().parse_args(["--spool", "/sp", "--interval", "5"])
        child = sampler.fan_out(args, {"SLURM_JOB_NUM_NODES": "3", "SLURMD_NODENAME": "c1"})
        assert child is not None and child.wait(timeout=10) == 0
        call = calls.read_text()
        assert call.startswith("--overlap --nodes=3 --ntasks=3 --ntasks-per-node=1 --quiet ")
        assert "monitor.py --spool /sp --no-fan-out --skip-node c1 --interval 5.0" in call

    def test_single_node_jobs_start_nothing(self):
        args = sampler.build_parser().parse_args(["--spool", "/sp"])
        assert sampler.fan_out(args, {"SLURM_JOB_NUM_NODES": "1"}) is None
        assert sampler.fan_out(args, {}) is None

    def test_the_copy_started_on_the_batch_node_steps_aside(self, tmp_path):
        environ = dict(os.environ, SLURM_JOB_ID="5", SLURMD_NODENAME="c1")
        done = subprocess.run(
            [sys.executable, "-S", str(SAMPLER), "--spool", str(tmp_path), "--skip-node", "c1"],
            env=environ,
            timeout=30,
        )
        assert done.returncode == 0 and list(tmp_path.iterdir()) == []
