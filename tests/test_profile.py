"""Tests for lib/profile.py: reading sampler series, summarising and ingesting them."""

import gzip
import json
import os
import pwd
import sqlite3
import time

import pytest
from helpers import deadline
from sacct_fixture import simple_job

from lib import collect, db, history, profile

MIB = 1024 * 1024
GIB = 1024 * MIB
T0 = 1_794_000_000
ME = pwd.getpwuid(os.getuid()).pw_name
# An account that exists on every system and is not the one running the tests.
OTHER = "nobody" if ME != "nobody" else "daemon"

COLUMNS = (
    "t", "mem", "usage", "usage_peak", "cpu", "nproc", "threads", "rd", "wr", "gpu_util", "gpu_mem",
    "oom", "top", "node_lustre_rd", "node_lustre_wr", "node_lustre_meta", "node_numa_hit",
    "node_numa_miss", "perf_ipc", "perf_cmiss",
)  # fmt: skip


def series_text(samples, job="4242", node="c1", source="cgroup-v2", cpus=8, start=T0, step=10):
    """A series file. ``samples`` are dicts of column values; ``t`` defaults to start + i*step."""
    lines = [
        "#hpcsizer-sampler v1 job=%s node=%s source=%s pid=1 cpus=%s start=%d user=%s"
        % (job, node, source, cpus, start, ME),
        "\t".join(COLUMNS),
    ]
    for i, sample in enumerate(samples):
        row = dict(sample)
        row.setdefault("t", start + i * step)
        lines.append("\t".join("" if row.get(c) is None else str(row[c]) for c in COLUMNS))
    return "\n".join(lines) + "\n"


def steady(n=30, mem_gb=10.0, cores=4.0, step=10):
    return [
        {"mem": int(mem_gb * GIB), "cpu": round(i * step * cores, 3), "nproc": 3} for i in range(n)
    ]


def parsed(samples, **kwargs):
    return profile.parse_series(series_text(samples, **kwargs))


class TestParse:
    def test_meta_and_rows(self):
        meta, rows = parsed(steady(3), node="c9")
        assert meta["job"] == "4242" and meta["node"] == "c9" and meta["source"] == "cgroup-v2"
        assert [r["t"] for r in rows] == [T0, T0 + 10, T0 + 20]
        assert rows[1]["cpu"] == 40.0 and rows[1]["usage"] is None and rows[1]["top"] == ""

    def test_a_line_cut_off_by_a_kill_is_dropped(self):
        text = series_text(steady(4))
        cut = text[: -len(text.splitlines()[-1]) // 2 - 1]  # the last line, half written
        assert not cut.endswith("\n")
        _meta, rows = profile.parse_series(cut)
        assert len(rows) == 3

    def test_damaged_lines_are_skipped_not_fatal(self):
        text = series_text(steady(3)) + "garbage\n%d\tnot-a-number\n" % (T0 + 99)
        assert len(profile.parse_series(text)[1]) == 3

    def test_a_restarted_sampler_appends_a_second_header(self):
        text = series_text(steady(2)) + series_text(steady(2), start=T0 + 100)
        meta, rows = profile.parse_series(text)
        assert len(rows) == 4 and meta["start"] == str(T0)

    def test_empty(self):
        assert profile.parse_series("") == ({}, [])


class TestOneNode:
    def test_steady_job(self):
        summary = profile.summarize([parsed(steady(30))], job_elapsed_sec=290, alloc_cpus=8)
        assert summary["source"] == "cgroup-v2" and summary["n_samples"] == 30
        assert summary["interval_sec"] == 10 and summary["coverage"] == 1.0
        assert summary["peak_mem_bytes"] == 10 * GIB
        assert summary["cpu_sec"] == pytest.approx(1160) and summary["avg_cores"] == pytest.approx(
            4
        )
        assert summary["max_cores"] == pytest.approx(4) and summary["p95_cores"] == pytest.approx(4)
        assert json.loads(summary["summary_json"])["flags"] == []

    def test_cpu_used_before_the_first_sample_is_counted(self):
        """The cgroup counter runs from the start of the job, the sampler from a little later."""
        samples = [{"mem": GIB, "cpu": 50.0 + i * 10} for i in range(4)]
        assert profile.summarize([parsed(samples)])["cpu_sec"] == pytest.approx(80)

    def test_peak_is_the_largest_sample_and_names_what_held_it(self):
        samples = [
            {"mem": m * GIB, "cpu": i, "top": top}
            for i, (m, top) in enumerate([(2, "bash:1"), (9, "R:9000"), (3, "R:3000")])
        ]
        summary = profile.summarize([parsed(samples)])
        assert summary["peak_mem_bytes"] == 9 * GIB and summary["top_comm"] == "R:9000"

    def test_kernel_high_water_mark_of_usage(self):
        samples = [
            {"mem": GIB, "usage": 2 * GIB, "usage_peak": 7 * GIB, "cpu": i} for i in range(3)
        ]
        assert profile.summarize([parsed(samples)])["peak_usage_bytes"] == 7 * GIB

    def test_sustained_cores_ignore_a_short_burst(self):
        """Two cores for most of the run, sixteen for one sample: size for two, know about sixteen."""
        cpu, samples = 0.0, []
        for i in range(60):
            cpu += 10 * (16.0 if i == 30 else 2.0)
            samples.append({"mem": GIB, "cpu": round(cpu, 3)})
        summary = profile.summarize([parsed(samples)])
        assert summary["max_cores"] == pytest.approx(16) and summary["p95_cores"] == pytest.approx(
            2
        )

    def test_counter_that_restarts_is_not_counted_as_negative(self):
        samples = [
            {"mem": GIB, "cpu": c, "rd": r} for c, r in [(0, 100), (40, 900), (5, 50), (45, 450)]
        ]
        node = profile.summarize_node(*parsed(samples))
        assert node["cpu_sec"] == 80 and node["read_bytes"] == 1200

    def test_gpu_and_oom(self):
        samples = [
            {"mem": GIB, "cpu": i, "gpu_util": u, "gpu_mem": 4 * GIB, "oom": k}
            for i, (u, k) in enumerate([(10, 0), (95, 0), (40, 2)])
        ]
        summary = profile.summarize([parsed(samples)])
        assert summary["gpu_util_max"] == 95 and summary["gpu_mem_max_bytes"] == 4 * GIB
        assert summary["oom_kills"] == 2
        assert "oom_kill" in json.loads(summary["summary_json"])["flags"]

    def test_partial_coverage_is_recorded(self):
        summary = profile.summarize([parsed(steady(10))], job_elapsed_sec=900)
        assert summary["coverage"] == 0.1
        assert any("cover 10%" in note for note in json.loads(summary["summary_json"])["notes"])

    def test_nothing_usable(self):
        assert profile.summarize([({}, [])]) is None and profile.summarize([]) is None


class TestNotes:
    def flags(self, samples, **kwargs):
        return json.loads(profile.summarize([parsed(samples)], **kwargs)["summary_json"])["flags"]

    def test_idle(self):
        assert "idle_cpu" in self.flags(steady(20, cores=0.01))

    def test_single_threaded(self):
        assert "single_threaded" in self.flags(steady(20, cores=1.0), alloc_cpus=16)
        assert "single_threaded" not in self.flags(steady(20, cores=1.0), alloc_cpus=2)
        assert "single_threaded" not in self.flags(steady(20, cores=6.0), alloc_cpus=16)

    def test_stalled(self):
        cpu, samples = 0.0, []
        for i in range(40):
            cpu += 10 * (4.0 if i < 20 else 0.0)
            samples.append({"mem": GIB, "cpu": cpu})
        assert "stalled" in self.flags(samples)

    def test_memory_spike_then_plateau(self):
        samples = [{"mem": (40 if i < 3 else 8) * GIB, "cpu": i * 10} for i in range(40)]
        flags = self.flags(samples)
        assert "mem_spike_plateau" in flags
        assert "mem_spike_plateau" not in self.flags(steady(40))

    def test_node_wide_counters_are_rates_over_the_run(self):
        """Version 2 reported the NUMA miss ratio since the node booted: the same for every job."""
        samples = [
            {"mem": GIB, "cpu": i * 10, "node_numa_hit": 10**9 + i * 600, "node_numa_miss": 5 + i * 400,
             "node_lustre_meta": i * 5000}
            for i in range(20)
        ]  # fmt: skip
        summary = profile.summarize([parsed(samples)])
        detail = json.loads(summary["summary_json"])
        assert detail["nodes"][0]["node_numa_miss_rate"] == pytest.approx(0.4)
        assert detail["nodes"][0]["node_lustre_meta_ops_s"] == pytest.approx(500)
        assert "numa_misplaced" in detail["flags"] and "lustre_metadata_heavy" in detail["flags"]
        assert all("node-wide" in n for n in detail["notes"] if "NUMA" in n or "Lustre" in n)


class TestSeveralNodes:
    def test_peak_is_of_the_sum_not_the_sum_of_peaks(self):
        """Node a peaks early, node b late: together they never held both peaks."""
        a = [{"mem": (30 if i < 5 else 5) * GIB, "cpu": i * 10 * 4.0} for i in range(20)]
        b = [{"mem": (5 if i < 15 else 30) * GIB, "cpu": i * 10 * 4.0} for i in range(20)]
        summary = profile.summarize([parsed(a, node="a"), parsed(b, node="b")], job_elapsed_sec=190)
        assert summary["peak_mem_bytes"] == 35 * GIB
        assert summary["cpu_sec"] == pytest.approx(2 * 760) and summary[
            "avg_cores"
        ] == pytest.approx(8)
        assert summary["n_samples"] == 40
        detail = json.loads(summary["summary_json"])
        assert [n["node"] for n in detail["nodes"]] == ["a", "b"]
        assert detail["node_imbalance_cv"] == 0

    def test_nodes_sampled_at_different_times_are_aligned(self):
        a = parsed([{"mem": 10 * GIB, "cpu": 0}, {"mem": 10 * GIB, "cpu": 40}], node="a", start=T0)
        b = parsed(
            [{"mem": 6 * GIB, "cpu": 0}, {"mem": 6 * GIB, "cpu": 40}], node="b", start=T0 + 3
        )
        assert profile.summarize([a, b])["peak_mem_bytes"] == 16 * GIB

    def test_profile_of_some_of_a_jobs_nodes_covers_that_share_of_it(self):
        one = parsed(steady(30), node="c1")
        summary = profile.summarize([one], job_elapsed_sec=290, alloc_nodes=2)
        assert summary["coverage"] == 0.5
        detail = json.loads(summary["summary_json"])
        assert detail["nodes_allocated"] == 2
        assert any("profiled on 1 of the job's 2 nodes" in note for note in detail["notes"])
        both = profile.summarize(
            [one, parsed(steady(30), node="c2")], job_elapsed_sec=290, alloc_nodes=2
        )
        assert both["coverage"] == 1.0
        assert profile.summarize([one], job_elapsed_sec=290, alloc_nodes=1)["coverage"] == 1.0

    def test_idle_nodes_and_imbalance(self):
        busy = parsed(steady(20, cores=8.0), node="a")
        idle = parsed(steady(20, cores=0.0), node="b")
        detail = json.loads(profile.summarize([busy, idle])["summary_json"])
        assert "idle_nodes" in detail["flags"] and detail["node_imbalance_cv"] == 1.0
        assert any("1 of 2 nodes did almost nothing: b" in n for n in detail["notes"])
        assert profile.node_imbalance([{"avg_cores": 4.0}]) is None
        assert profile.node_imbalance([{"avg_cores": 0.0}, {"avg_cores": 0.0}]) is None

    def test_describe_lists_each_node(self):
        busy = parsed(steady(20, cores=8.0), node="a")
        idle = parsed(steady(20, cores=0.0, mem_gb=1.0), node="b")
        lines = profile.describe(json.loads(profile.summarize([busy, idle])["summary_json"]))
        assert "a: peak memory 10.0 GB, 8 cores on average" in lines
        assert "b: peak memory 1.0 GB, 0 cores on average" in lines


@pytest.fixture
def spool(cfg):
    os.makedirs(os.path.join(cfg.spool_dir, "submit"))
    return cfg.spool_dir


def harvest(cfg, now, log=lambda _m: None):
    return collect.harvest(cfg, now=now, log=log, ingest=profile.ingest_spool)


def everything_stored(cfg):
    """The whole database as text, to prove something is NOT in it."""
    conn = sqlite3.connect(cfg.db_path)
    try:
        return "\n".join(conn.iterdump())
    finally:
        conn.close()


def stored(cfg):
    with db.reader(cfg.db_path) as conn:
        return {j["job_id"]: j for j in history.annotate(db.fetch_jobs(conn), cfg)}


class TestIngest:
    @pytest.fixture
    def now(self):
        return int(time.time())

    def job(self, now, jid=4242, **kwargs):
        kwargs.setdefault("user", ME)
        kwargs.setdefault("submit", now - 3600)
        kwargs.setdefault("elapsed", 290)
        return simple_job(jid, **kwargs)

    def write(self, spool, now, jid=4242, node="c1", **kwargs):
        path = os.path.join(spool, "%d.%s.tsv" % (jid, node))
        kwargs.setdefault("start", now - 3600 + 30)
        with open(path, "w") as handle:
            handle.write(series_text(steady(30), job=str(jid), node=node, **kwargs))
        return path

    def test_profile_of_a_finished_job_is_stored_and_archived(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now, state="TIMEOUT", total_cpu="00:00.100"))
        path = self.write(spool, now)
        stats = harvest(cfg, now)
        assert stats["n_profiles"] == 1
        job = stored(cfg)["4242"]
        assert job["mon_source"] == "cgroup-v2" and job["mon_n_samples"] == 30
        assert job["mon_peak_mem_bytes"] == 10 * GIB and job["mon_coverage"] == 1.0
        assert job["m"]["mem_source"] == "profile" and job["m"]["mem_used_gb"] == 10
        assert job["m"]["cores_used"] == pytest.approx(4), "CPU of a job Slurm killed"
        (archive,) = profile.series_files(cfg, job)
        assert archive.endswith(".c1.tsv.gz") and archive.startswith(cfg.ts_dir)
        with gzip.open(archive, "rt") as handle:
            assert handle.read() == open(path).read()

    def test_spool_file_is_removed_once_the_next_harvest_sees_it_published(
        self, cfg, stubs, spool, now
    ):
        stubs.sacct_default(self.job(now))
        path = self.write(spool, now)
        harvest(cfg, now)
        assert os.path.exists(path), "kept until the database that names its archive is published"
        assert harvest(cfg, now + 60)["n_profiles"] == 0
        assert not os.path.exists(path)
        assert stored(cfg)["4242"]["mon_n_samples"] == 30

    def test_series_of_a_job_not_yet_finished_waits(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now, jid=1))
        path = self.write(spool, now, jid=4242)
        assert harvest(cfg, now)["n_profiles"] == 0 and os.path.exists(path)

    def test_file_owned_by_someone_else_is_not_believed(self, cfg, stubs, spool, now):
        """The spool is group-writable: anyone could drop a file named after another's job."""
        stubs.sacct_default(self.job(now, user=OTHER))
        path = self.write(spool, now)
        messages = []
        assert harvest(cfg, now, messages.append)["n_profiles"] == 0
        assert any("4242.c1.tsv ignored: it is not owned by %s" % OTHER in m for m in messages)
        assert stored(cfg)["4242"]["mon_n_samples"] is None
        assert os.path.exists(path), "kept while a node of the job may still be writing"

    def test_what_cannot_be_used_is_removed_a_day_after_the_job_ended(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now, user=OTHER, submit=now - 3 * 86400))
        foreign = self.write(spool, now, start=now - 3 * 86400 + 30)
        empty = os.path.join(spool, "4242.c2.tsv")
        with open(empty, "w") as handle:
            handle.write("not a series\n")
        harvest(cfg, now)
        assert not os.path.exists(foreign) and not os.path.exists(empty)

    def test_job_of_a_user_this_node_does_not_know_waits(self, cfg, stubs, spool, now):
        """A name service that is down must not look like a file with the wrong owner."""
        stubs.sacct_default(self.job(now, user="no-such-user", submit=now - 3 * 86400))
        path = self.write(spool, now, start=now - 3 * 86400 + 30)
        messages = []
        assert harvest(cfg, now, messages.append)["n_profiles"] == 0
        assert any("no-such-user is not known on this node" in m for m in messages)
        assert os.path.exists(path)

    def test_a_link_is_never_followed(self, cfg, stubs, spool, now, tmp_path):
        """A member links another job's name to a file of the job's user: it is not read."""
        stubs.sacct_default(self.job(now))
        elsewhere = self.write(str(tmp_path), now)
        os.symlink(elsewhere, os.path.join(spool, "4242.c1.tsv"))
        messages = []
        assert harvest(cfg, now, messages.append)["n_profiles"] == 0
        assert any("4242.c1.tsv ignored: it is a symbolic link" in m for m in messages)
        assert os.path.exists(elsewhere)

    def test_a_second_name_of_a_file_is_not_believed(self, cfg, stubs, spool, now, tmp_path):
        stubs.sacct_default(self.job(now))
        elsewhere = self.write(str(tmp_path), now)
        os.link(elsewhere, os.path.join(spool, "4242.c1.tsv"))
        messages = []
        assert harvest(cfg, now, messages.append)["n_profiles"] == 0
        assert any("it has 2 names" in m for m in messages)

    def test_a_pipe_cannot_stall_the_harvest(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        os.mkfifo(os.path.join(spool, "4242.c1.tsv"))
        os.mkfifo(os.path.join(spool, "submit", "4242.json"))
        messages = []
        with deadline():
            stats = harvest(cfg, now, messages.append)
        assert stats["n_inserted"] == 1 and stats["n_profiles"] == 0
        assert sum("it is not a regular file" in m for m in messages) == 2

    def test_an_oversized_file_is_not_read(self, cfg, stubs, spool, now, monkeypatch):
        stubs.sacct_default(self.job(now))
        self.write(spool, now)
        monkeypatch.setattr(profile, "SERIES_MAX_BYTES", 1000)
        messages = []
        assert harvest(cfg, now, messages.append)["n_profiles"] == 0
        assert any("it is larger than" in m for m in messages)

    def test_node_name_in_a_series_cannot_leave_the_archive_folder(self, cfg, stubs, spool, now):
        """The node label is taken from the file's content, which its owner writes."""
        stubs.sacct_default(self.job(now))
        path = os.path.join(spool, "4242.c1.tsv")
        with open(path, "w") as handle:
            handle.write(series_text(steady(30), node="../../../escaped/x", start=now - 3570))
        assert harvest(cfg, now)["n_profiles"] == 1
        (archive,) = profile.series_files(cfg, stored(cfg)["4242"])
        assert os.path.dirname(archive) == cfg.ts_dir
        assert os.path.basename(archive) == "4242-%d.%sescaped_x.tsv.gz" % (now - 3600, "_" * 9)
        assert not os.path.exists(os.path.join(cfg.root, "escaped"))

    def test_two_files_claiming_one_node_are_both_kept(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        for name in ("4242.c1.tsv", "4242.c1-99-0.tsv"):
            with open(os.path.join(spool, name), "w") as handle:
                handle.write(series_text(steady(30), node="c1", start=now - 3570))
        assert harvest(cfg, now)["n_profiles"] == 1
        names = sorted(os.path.basename(p) for p in profile.series_files(cfg, stored(cfg)["4242"]))
        assert [n.split(".")[1] for n in names] == ["c1", "c1_2"]

    def test_names_in_the_log_cannot_forge_lines_or_colour_a_terminal(self, cfg, stubs, spool, now):
        """The node part of a spool file name is chosen by whoever creates the file."""
        stubs.sacct_default(self.job(now, user=OTHER))
        name = "4242.c1\x1b[31m\n[harvest] all is well.tsv"
        with open(os.path.join(spool, name), "w") as handle:
            handle.write(series_text(steady(30), start=now - 3570))
        messages = []
        harvest(cfg, now, messages.append)
        about = [m for m in messages if "4242." in m]
        assert about, "the file is reported"
        assert all("\x1b" not in m and "\n" not in m for m in messages)
        assert any("\\x1b[31m\\x0a[harvest] all is well.tsv ignored" in m for m in about)

    def test_archive_follows_the_databases_directory(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        self.write(spool, now)
        os.chmod(cfg.root, 0o750)
        try:
            harvest(cfg, now)
            (archive,) = profile.series_files(cfg, stored(cfg)["4242"])
            assert oct(os.stat(archive).st_mode & 0o777) == "0o640"
            assert oct(os.stat(cfg.ts_dir).st_mode & 0o777) == "0o750"
            assert oct(os.stat(cfg.db_path).st_mode & 0o777) == "0o640"
        finally:
            os.chmod(cfg.root, 0o755)

    def test_archive_is_readable_like_the_database(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        self.write(spool, now)
        before = os.umask(0o077)
        try:
            harvest(cfg, now)
        finally:
            os.umask(before)
        (archive,) = profile.series_files(cfg, stored(cfg)["4242"])
        assert oct(os.stat(archive).st_mode & 0o777) == "0o644"

    def test_series_from_an_earlier_use_of_the_job_id_is_ignored(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        self.write(spool, now, start=now - 90 * 86400)
        assert harvest(cfg, now)["n_profiles"] == 0

    def test_two_node_job_profiled_on_one_node_is_a_lower_bound(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now, nodes=2, batch_max_rss="31457280K"))
        self.write(spool, now, node="c1")
        assert harvest(cfg, now)["n_profiles"] == 1
        job = stored(cfg)["4242"]
        assert job["mon_coverage"] == 0.5 and job["mon_peak_mem_bytes"] == 10 * GIB
        assert job["m"]["mem_used_gb"] == pytest.approx(30, abs=0.1)
        assert job["m"]["mem_source"] == "sacct"
        assert job["m"]["cores_p95"] is None, "not a statement about the whole job"

    def test_several_nodes(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now, nodes=2))
        self.write(spool, now, node="c1")
        self.write(spool, now, node="c2")
        assert harvest(cfg, now)["n_profiles"] == 1
        job = stored(cfg)["4242"]
        assert job["mon_peak_mem_bytes"] == 20 * GIB and job["mon_n_samples"] == 60
        assert len(profile.series_files(cfg, job)) == 2
        with db.reader(cfg.db_path) as conn:
            detail = db.fetch_monitor_summary(conn, job["id"])
        assert [n["node"] for n in detail["nodes"]] == ["c1", "c2"]

    def test_submit_context(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        ctx = os.path.join(spool, "submit", "4242.json")
        with open(ctx, "w") as handle:
            json.dump(
                {
                    "script_hash": "abc",
                    "tools": ["Seurat"],
                    "input_gb": 12.5,
                    "submitted_ts": now - 3600,
                },
                handle,
            )
        harvest(cfg, now)
        job = stored(cfg)["4242"]
        assert job["ctx_input_gb"] == 12.5 and json.loads(job["ctx_tools_json"]) == ["Seurat"]
        assert os.path.exists(ctx)
        harvest(cfg, now + 60)
        assert not os.path.exists(ctx)

    def test_context_of_an_earlier_use_of_the_job_id(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        with open(os.path.join(spool, "submit", "4242.json"), "w") as handle:
            json.dump({"tools": [], "input_gb": 1, "submitted_ts": now - 40 * 86400}, handle)
        harvest(cfg, now)
        assert stored(cfg)["4242"]["ctx_input_gb"] is None

    def test_link_to_a_private_file_is_not_stored(self, cfg, stubs, spool, now, tmp_path):
        """The harvest runs as the owner of the database, which every member can read:
        a link from the owner's own job to one of the owner's files must not be opened."""
        stubs.sacct_default(self.job(now))
        secret = tmp_path / "credentials.json"
        secret.write_text(json.dumps({"token": "s3cr3t-value", "submitted_ts": now - 3600}))
        os.symlink(str(secret), os.path.join(spool, "submit", "4242.json"))
        messages = []
        harvest(cfg, now, messages.append)
        assert any("4242.json ignored: it is a symbolic link" in m for m in messages)
        assert "s3cr3t-value" not in everything_stored(cfg)
        assert stored(cfg)["4242"]["ctx_tools_json"] is None

    def test_only_what_hpg_submit_writes_is_stored(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        with open(os.path.join(spool, "submit", "4242.json"), "w") as handle:
            json.dump(
                {
                    "tools": ["Seurat", 7, "x" * 500],
                    "input_gb": "a lot",
                    "script_hash": ["not", "text"],
                    "submitted_ts": now - 3600,
                    "cwd": "/blue/lab/" + "d" * 10000,
                    "password": "hunter2",
                    "request": {"a": {"b": {"c": {"d": {"e": "too deep"}}}}},
                },
                handle,
            )
        harvest(cfg, now)
        job = stored(cfg)["4242"]
        assert json.loads(job["ctx_tools_json"]) == ["Seurat", "x" * 64]
        assert job["ctx_input_gb"] is None and job["ctx_script_hash"] is None
        dump = everything_stored(cfg)
        assert "hunter2" not in dump and "too deep" not in dump
        assert "d" * 3990 in dump and "d" * 3991 not in dump, "text is cut at 4000 characters"

    def test_submit_folder_that_is_a_link_is_not_read(self, cfg, stubs, now, tmp_path):
        stubs.sacct_default(self.job(now))
        os.makedirs(cfg.spool_dir)
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / "4242.json").write_text(json.dumps({"tools": ["Seurat"], "input_gb": 3}))
        os.symlink(str(elsewhere), os.path.join(cfg.spool_dir, "submit"))
        messages = []
        harvest(cfg, now, messages.append)
        assert any("submit records are not read" in m for m in messages)
        assert stored(cfg)["4242"]["ctx_tools_json"] is None

    def test_file_left_by_a_killed_hpg_submit_is_cleared(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        fresh = os.path.join(spool, "submit", ".4243.1.aa.tmp")
        old = os.path.join(spool, "submit", ".4244.1.bb.tmp")
        for path in (fresh, old):
            open(path, "w").close()
        os.utime(old, (now - 2 * 86400, now - 2 * 86400))
        harvest(cfg, now)
        assert os.path.exists(fresh) and not os.path.exists(old)

    def test_unreadable_files_do_not_stop_the_harvest(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now))
        with open(os.path.join(spool, "4242.c1.tsv"), "w") as handle:
            handle.write("not a series\n")
        with open(os.path.join(spool, "submit", "4242.json"), "w") as handle:
            handle.write("{not json")
        with open(os.path.join(spool, "README.tsv"), "w") as handle:
            handle.write("x")
        stats = harvest(cfg, now)
        assert stats["n_profiles"] == 0 and stats["n_inserted"] == 1

    def test_old_orphans_are_cleared(self, cfg, stubs, spool, now):
        stubs.sacct_default(self.job(now, jid=1))
        fresh = self.write(spool, now, jid=7)
        old = self.write(spool, now, jid=8)
        long_ago = now - 40 * 86400
        os.utime(old, (long_ago, long_ago))
        harvest(cfg, now)
        assert os.path.exists(fresh) and not os.path.exists(old)

    def test_no_spool_directory(self, cfg, stubs, now):
        stubs.sacct_default(self.job(now))
        assert harvest(cfg, now)["n_profiles"] == 0


class TestSpoolSetup:
    def test_first_harvest_creates_it_for_the_group(self, cfg, stubs):
        stubs.sacct_default(simple_job(1))
        messages = []
        collect.harvest(cfg, now=int(time.time()), log=messages.append, ingest=profile.ingest_spool)
        for folder in (cfg.spool_dir, os.path.join(cfg.spool_dir, "submit")):
            assert oct(os.stat(folder).st_mode & 0o7777) == "0o3770"
        assert any("created the spool" in m and "mode 3770" in m for m in messages)

    def test_an_existing_spool_is_left_as_its_owner_set_it(self, cfg, stubs):
        os.makedirs(cfg.spool_dir, mode=0o700)
        os.chmod(cfg.spool_dir, 0o700)
        messages = []
        assert profile.ensure_spool(cfg, messages.append) is True
        assert oct(os.stat(cfg.spool_dir).st_mode & 0o7777) == "0o700"
        assert oct(os.stat(os.path.join(cfg.spool_dir, "submit")).st_mode & 0o7777) == "0o3770"

    def test_configurable_mode(self, cfg):
        cfg.spool_mode = 0o700
        profile.ensure_spool(cfg, lambda _m: None)
        assert oct(os.stat(cfg.spool_dir).st_mode & 0o7777) == "0o700"


class TestReadingASpoolFile:
    """What 'hpg plot' reads before a profile has been archived."""

    def test_series_of_the_jobs_user(self, tmp_path):
        path = tmp_path / "1.c1.tsv"
        path.write_text(series_text(steady(5)))
        assert len(profile.read_series(str(path), owner=ME)[1]) == 5

    def test_somebody_elses_file_under_the_jobs_name(self, tmp_path):
        path = tmp_path / "1.c1.tsv"
        path.write_text(series_text(steady(5)))
        with pytest.raises(profile.Rejected, match="does not belong to %s" % OTHER):
            profile.read_series(str(path), owner=OTHER)

    def test_link_and_pipe(self, tmp_path):
        target = tmp_path / "real.tsv"
        target.write_text(series_text(steady(5)))
        os.symlink(str(target), str(tmp_path / "1.c1.tsv"))
        with pytest.raises(profile.Rejected, match="symbolic link"):
            profile.read_series(str(tmp_path / "1.c1.tsv"), owner=ME)
        os.mkfifo(str(tmp_path / "2.c1.tsv"))
        with deadline(), pytest.raises(profile.Rejected, match="not a regular file"):
            profile.read_series(str(tmp_path / "2.c1.tsv"), owner=ME)

    def test_archive_may_have_a_second_name(self, tmp_path):
        """A backup made of hard links must not make archived profiles unreadable."""
        path = tmp_path / "1-2.c1.tsv.gz"
        with gzip.open(str(path), "wt") as handle:
            handle.write(series_text(steady(5)))
        os.link(str(path), str(tmp_path / "copy.gz"))
        assert len(profile.read_series(str(path))[1]) == 5
