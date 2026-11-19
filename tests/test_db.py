"""Tests for lib/db.py: schema v3, read-only access, snapshot publishing, migration."""

import os
import sqlite3
import stat
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from sacct_fixture import BASE_TS, simple_job  # noqa: E402

from lib import db  # noqa: E402
from lib.slurm import merge_job  # noqa: E402


def make_job(jid, **kwargs):
    rows = simple_job(jid, **kwargs)
    return merge_job(rows[0], rows[1:])


@pytest.fixture
def dbfile(tmp_path):
    return str(tmp_path / "store" / "profiles.db")


def write_jobs(path, *jobs, now=1000):
    with db.SnapshotWriter(path) as conn:
        return [db.upsert_job(conn, job, steps, now=now) for job, steps in jobs]


def files_beside(path):
    return sorted(os.listdir(os.path.dirname(path)))


class TestSnapshotWriter:
    def test_creates_and_publishes_a_single_file(self, dbfile):
        assert write_jobs(dbfile, make_job(1)) == ["inserted"]
        assert files_beside(dbfile) == ["profiles.db"], "no -wal, -shm, journal, lock or temp file"
        assert stat.S_IMODE(os.stat(dbfile).st_mode) == 0o644

    def test_published_file_is_not_in_wal_mode(self, dbfile):
        write_jobs(dbfile, make_job(1))
        conn = sqlite3.connect(dbfile)
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "delete"
        conn.close()

    def test_exception_publishes_nothing_and_releases_the_lock(self, dbfile):
        write_jobs(dbfile, make_job(1))
        before = Path(dbfile).read_bytes()
        with pytest.raises(RuntimeError):
            with db.SnapshotWriter(dbfile) as conn:
                db.upsert_job(conn, *make_job(2))
                raise RuntimeError("harvest failed half way")
        assert Path(dbfile).read_bytes() == before
        assert not os.path.exists(dbfile + ".lock")
        with db.reader(dbfile) as conn:
            assert len(db.fetch_jobs(conn)) == 1

    def test_second_writer_is_refused(self, dbfile):
        with db.SnapshotWriter(dbfile):
            with pytest.raises(db.LockBusy):
                with db.SnapshotWriter(dbfile):
                    pass
        assert not os.path.exists(dbfile + ".lock")

    def test_stale_lock_is_taken_over(self, dbfile):
        os.makedirs(os.path.dirname(dbfile))
        lock = dbfile + ".lock"
        Path(lock).write_text('{"host": "gone", "pid": 1, "time": 0}')
        os.utime(lock, (1, 1))
        assert write_jobs(dbfile, make_job(1)) == ["inserted"]
        assert not os.path.exists(lock)

    def test_fresh_lock_is_respected(self, dbfile):
        os.makedirs(os.path.dirname(dbfile))
        Path(dbfile + ".lock").write_text('{"host": "other", "pid": 7, "time": 0}')
        with pytest.raises(db.LockBusy) as err:
            write_jobs(dbfile, make_job(1))
        assert "other" in str(err.value)

    def test_lock_of_a_writer_that_died_on_this_host_is_taken_over_at_once(self, dbfile):
        """After a kill -9 the next harvest on the same node need not wait half an hour."""
        import socket
        import subprocess
        import sys

        os.makedirs(os.path.dirname(dbfile))
        gone = subprocess.Popen([sys.executable, "-c", "pass"])
        gone.wait()
        Path(dbfile + ".lock").write_text(
            '{"host": "%s", "pid": %d, "time": 0}' % (socket.gethostname(), gone.pid)
        )
        assert write_jobs(dbfile, make_job(1)) == ["inserted"]

    def test_lock_of_a_live_writer_on_this_host_is_respected(self, dbfile):
        import socket

        os.makedirs(os.path.dirname(dbfile))
        Path(dbfile + ".lock").write_text(
            '{"host": "%s", "pid": %d, "time": 0}' % (socket.gethostname(), os.getpid())
        )
        with pytest.raises(db.LockBusy):
            write_jobs(dbfile, make_job(1))

    def test_working_copies_of_killed_writers_are_swept_after_a_day(self, dbfile, tmp_path):
        tmp = tmp_path / "localtmp"
        old = tmp / "hpcsizer-dead1"
        fresh = tmp / "hpcsizer-live2"
        other = tmp / "somebody-elses"
        for folder in (old, fresh, other):
            folder.mkdir(parents=True)
            (folder / "snapshot.db").write_bytes(b"x" * 10)
        long_ago = time.time() - 2 * 86400
        os.utime(str(old), (long_ago, long_ago))
        os.utime(str(other), (long_ago, long_ago))
        with db.SnapshotWriter(dbfile, tmp_dir=str(tmp)) as conn:
            db.upsert_job(conn, *make_job(1))
        assert not old.exists(), "left by a writer killed two days ago"
        assert fresh.exists(), "could belong to a writer that is still running"
        assert other.exists(), "not ours"
        assert sorted(p.name for p in tmp.iterdir()) == ["hpcsizer-live2", "somebody-elses"]

    def test_previous_snapshot_is_kept(self, dbfile):
        write_jobs(dbfile, make_job(1))
        write_jobs(dbfile, make_job(2))
        prev = sqlite3.connect(dbfile + ".prev")
        assert prev.execute("select count(*) from jobs").fetchone()[0] == 1
        prev.close()
        with db.reader(dbfile) as conn:
            assert len(db.fetch_jobs(conn)) == 2

    def test_file_replaced_by_another_writer_is_not_overwritten(self, dbfile, tmp_path):
        write_jobs(dbfile, make_job(1))
        other = str(tmp_path / "other.db")
        write_jobs(other, make_job(1), make_job(2), make_job(3))
        with pytest.raises(db.PublishConflict):
            with db.SnapshotWriter(dbfile) as conn:
                db.upsert_job(conn, *make_job(9))
                os.replace(other, dbfile)  # someone else published meanwhile
        with db.reader(dbfile) as conn:
            assert {j["job_id"] for j in db.fetch_jobs(conn)} == {"1", "2", "3"}
        assert not [f for f in files_beside(dbfile) if ".tmp." in f]

    def test_interrupted_publish_leaves_the_old_file_intact(self, dbfile, monkeypatch):
        write_jobs(dbfile, make_job(1))
        before = Path(dbfile).read_bytes()

        def boom(src, dst):
            raise OSError("stalled filesystem")

        monkeypatch.setattr(db.os, "replace", boom)
        with pytest.raises(OSError):
            write_jobs(dbfile, make_job(2))
        monkeypatch.undo()
        assert Path(dbfile).read_bytes() == before
        assert not os.path.exists(dbfile + ".lock")
        assert not [f for f in files_beside(dbfile) if ".tmp." in f]

    def test_a_reader_open_across_a_publish_keeps_its_snapshot(self, dbfile):
        write_jobs(dbfile, make_job(1))
        old = db.connect_ro(dbfile)
        write_jobs(dbfile, make_job(2))
        assert len(db.fetch_jobs(old)) == 1
        old.close()
        with db.reader(dbfile) as conn:
            assert len(db.fetch_jobs(conn)) == 2


class TestReadOnly:
    def test_read_works_without_write_permission_and_creates_nothing(self, dbfile):
        write_jobs(dbfile, make_job(1))
        folder = os.path.dirname(dbfile)
        os.chmod(dbfile, 0o444)
        os.chmod(folder, 0o555)
        try:
            with db.reader(dbfile) as conn:
                assert len(db.fetch_jobs(conn)) == 1
            assert files_beside(dbfile) == ["profiles.db"]
        finally:
            os.chmod(folder, 0o755)

    def test_missing_database(self, dbfile):
        with pytest.raises(db.DatabaseMissing):
            db.connect_ro(dbfile)


class TestUpsert:
    def test_update_keeps_first_seen_and_replaces_steps(self, dbfile):
        job, steps = make_job(5, state="COMPLETED")
        write_jobs(dbfile, (job, steps), now=1000)
        job2 = dict(job, state="FAILED", exit_code="1:0")
        assert write_jobs(dbfile, (job2, steps[:1]), now=2000) == ["updated"]
        with db.reader(dbfile) as conn:
            (row,) = db.fetch_jobs(conn)
            assert row["state"] == "FAILED"
            assert (row["first_seen_ts"], row["updated_ts"]) == (1000, 2000)
            assert len(db.fetch_steps(conn, row["id"])) == 1

    def test_harvesting_again_does_not_erase_monitor_or_context(self, dbfile):
        """The old INSERT OR REPLACE deleted the row, and the monitor data with it."""
        job, steps = make_job(6)
        with db.SnapshotWriter(dbfile) as conn:
            db.upsert_job(conn, job, steps)
            pk = db.latest_job_pk(conn, 6)["id"]
            db.upsert_monitor(conn, pk, {"source": "proc", "peak_mem_bytes": 123, "max_cores": 3.5})
            db.upsert_context(conn, pk, {"script_hash": "abc", "tools_json": '["Seurat"]'})
        write_jobs(dbfile, (dict(job, state="TIMEOUT"), steps))
        with db.reader(dbfile) as conn:
            (row,) = db.fetch_jobs(conn)
            assert row["state"] == "TIMEOUT"
            assert row["mon_peak_mem_bytes"] == 123 and row["mon_max_cores"] == 3.5
            assert row["ctx_script_hash"] == "abc"

    def test_reused_job_id_is_two_jobs(self, dbfile):
        first = make_job(77, submit=BASE_TS, name="spring")
        second = make_job(77, submit=BASE_TS + 100 * 86400, name="autumn")
        assert write_jobs(dbfile, first, second) == ["inserted", "inserted"]
        with db.reader(dbfile) as conn:
            found = db.find_jobs(conn, "77")
            assert [j["job_name"] for j in found] == ["autumn", "spring"]

    def test_same_job_twice_is_one_row(self, dbfile):
        assert write_jobs(dbfile, make_job(8), make_job(8)) == ["inserted", "updated"]

    def test_job_without_identity_is_rejected(self, dbfile):
        job, steps = make_job(9)
        job["submit_ts"] = None
        with pytest.raises(ValueError):
            write_jobs(dbfile, (job, steps))


class TestFetch:
    def test_window_user_and_account_filters(self, dbfile):
        write_jobs(
            dbfile,
            make_job(1, user="alice", submit=BASE_TS),
            make_job(2, user="bob", submit=BASE_TS + 86400),
            make_job(3, user="alice", submit=BASE_TS + 2 * 86400, account="otherlab"),
        )
        with db.reader(dbfile) as conn:
            assert len(db.fetch_jobs(conn)) == 3
            assert [j["job_id"] for j in db.fetch_jobs(conn, since_ts=BASE_TS + 86400)] == [
                "2",
                "3",
            ]
            assert [j["job_id"] for j in db.fetch_jobs(conn, user="alice")] == ["1", "3"]
            assert [j["job_id"] for j in db.fetch_jobs(conn, accounts=["otherlab"])] == ["3"]
            assert "raw_json" not in db.fetch_jobs(conn)[0]

    def test_find_by_displayed_or_raw_id(self, dbfile):
        write_jobs(dbfile, make_job(507, display="500_7"))
        with db.reader(dbfile) as conn:
            assert db.find_jobs(conn, "500_7")[0]["job_id_raw"] == 507
            assert db.find_jobs(conn, "507")[0]["job_id"] == "500_7"
            assert db.find_jobs(conn, "999") == []
            assert db.fetch_raw(conn, db.find_jobs(conn, "507")[0]["id"])["JobIDRaw"] == "507"

    def test_internal_jobs_helper(self):
        assert db.is_internal_job("hpcsizer-harvest")
        assert not db.is_internal_job("hpg-interactive-rstudio")
        assert not db.is_internal_job(None)

    def test_meta_round_trip(self, dbfile):
        with db.SnapshotWriter(dbfile) as conn:
            db.set_meta(conn, "watermark_ts", 42)
            db.set_meta(conn, "watermark_ts", 43)
        with db.reader(dbfile) as conn:
            assert db.get_meta_int(conn, "watermark_ts") == 43
            assert db.get_meta_int(conn, "absent") is None


# ---------------------------------------------------------------------------
# Migration from the version 2 schema
# ---------------------------------------------------------------------------

V2_SCHEMA = """
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY, user TEXT, job_name TEXT, account TEXT, qos TEXT, state TEXT,
    submit_time TEXT, start_time TEXT, end_time TEXT,
    req_mem_gb REAL, req_cpus INTEGER, req_time_min INTEGER, req_gpus INTEGER DEFAULT 0,
    sacct_peak_rss_gb REAL, sacct_elapsed_sec INTEGER, sacct_cpu_time_sec INTEGER,
    sidecar_peak_gb REAL, static_tools TEXT, input_files TEXT,
    mem_efficiency REAL, cpu_efficiency REAL, waste_gb REAL, flags TEXT,
    has_sidecar INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE tool_models (tool TEXT PRIMARY KEY);
CREATE TABLE daily_summary (id INTEGER PRIMARY KEY);
"""

V2_INSERT = (
    "INSERT INTO jobs (job_id, user, job_name, account, qos, state, submit_time, start_time,"
    " end_time, req_mem_gb, req_cpus, req_gpus, sacct_peak_rss_gb, sacct_elapsed_sec,"
    " sacct_cpu_time_sec, cpu_efficiency, flags) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)


def v2_row(job_id, state="COMPLETED", name="xen", cpu=0, peak=2.0, user="smith6jt"):
    return (
        job_id,
        user,
        name,
        "maigan",
        "maigan",
        state,
        "2026-03-01T10:00:00",
        "2026-03-01T10:01:00",
        "2026-03-01T11:01:00",
        64.0,
        8,
        0,
        peak,
        3600,
        cpu,
        0.0,
        '["idle_cpu", "single_threaded"]',
    )


@pytest.fixture
def v2db(tmp_path):
    """A v2 database in WAL mode whose newest rows exist only in the -wal file."""
    path = str(tmp_path / "live" / "profiles.db")
    os.makedirs(os.path.dirname(path))
    setup = sqlite3.connect(path)
    setup.execute("PRAGMA journal_mode=WAL")
    setup.executescript(V2_SCHEMA)
    setup.execute(V2_INSERT, v2_row("100", cpu=7200))
    setup.commit()
    setup.close()  # checkpointed into the main file

    holder = sqlite3.connect(path)
    holder.execute("PRAGMA wal_autocheckpoint=0")
    holder.executemany(
        V2_INSERT,
        [
            v2_row("101", cpu=0),  # the TotalCPU parsing bug: stored as 0
            v2_row("102", state="CANCELLED by 6084"),
            v2_row("500_7", name="array-step"),
            v2_row("103", name="hpcsizer-scheduler"),
            v2_row("104", state="RUNNING"),
            v2_row("105", peak=None, user="bjones", name="ondemand/sys/dashboard/sys/rstudio"),
        ],
    )
    holder.commit()
    assert os.path.getsize(path + "-wal") > 0
    yield path, holder
    holder.close()


class TestMigrateV2:
    def test_v2_is_detected_everywhere(self, v2db):
        path, _ = v2db
        assert db.detect_version(path) == 0
        with pytest.raises(db.NeedsMigration):
            db.connect_ro(path)
        with pytest.raises(db.NeedsMigration):
            with db.SnapshotWriter(path):
                pass

    def test_rows_only_in_the_wal_are_recovered(self, v2db, tmp_path):
        path, _holder = v2db
        stats = db.migrate_v2(path, str(tmp_path / "backup"))
        assert stats["wal_bytes"] > 0
        assert stats["v2_rows"] == 7
        assert stats["migrated"] == 5
        assert stats["dropped_internal"] == 1 and stats["dropped_unfinished"] == 1
        assert db.detect_version(path) == 3
        assert not os.path.exists(path + "-wal") and not os.path.exists(path + "-shm")
        assert sorted(os.path.basename(p).split(".db")[-1] for p in stats["backup"]) == [
            "",
            "-shm",
            "-wal",
        ]

    def test_legacy_rows_are_mapped_honestly(self, v2db, tmp_path):
        path, _ = v2db
        stats = db.migrate_v2(path, str(tmp_path / "backup"))
        with db.reader(path) as conn:
            rows = {j["job_id"]: j for j in db.fetch_jobs(conn)}
        assert set(rows) == {"100", "101", "102", "500_7", "105"}
        assert all(j["source"] == "legacy" and j["job_id_raw"] is None for j in rows.values())
        assert rows["100"]["cpu_sec"] == 7200
        assert rows["101"]["cpu_sec"] is None, "a stored 0 was a parsing failure, not a measurement"
        assert stats["cpu_time_unknown"] == 4
        assert rows["102"]["state"] == "CANCELLED"
        assert rows["102"]["state_raw"] == "CANCELLED by 6084"
        assert (rows["500_7"]["array_job_id"], rows["500_7"]["array_task_id"]) == (500, 7)
        assert rows["100"]["req_mem_mb"] == 65536 and rows["100"]["alloc_cpus"] == 8
        assert rows["100"]["peak_mem_bytes"] == 2 * 1024**3
        assert rows["105"]["peak_mem_bytes"] is None
        assert rows["100"]["submit_ts"] is not None and rows["100"]["elapsed_sec"] == 3600

    def test_a_sacct_record_replaces_its_legacy_twin(self, v2db, tmp_path):
        path, _ = v2db
        db.migrate_v2(path, str(tmp_path / "backup"))
        with db.reader(path) as conn:
            legacy = db.find_jobs(conn, "101")[0]
        twin = make_job(101, submit=legacy["submit_ts"] + 3600)  # zone-shifted by an hour
        other = make_job(100, submit=legacy["submit_ts"] + 200 * 86400)  # a reused id, not a twin
        write_jobs(path, twin, other)
        with db.reader(path) as conn:
            assert [j["source"] for j in db.find_jobs(conn, "101")] == ["sacct"]
            assert sorted(j["source"] for j in db.find_jobs(conn, "100")) == ["legacy", "sacct"]

    def test_refuses_a_database_that_is_already_current(self, dbfile, tmp_path):
        write_jobs(dbfile, make_job(1))
        with pytest.raises(db.DatabaseError):
            db.migrate_v2(dbfile, str(tmp_path / "backup"))

    def test_database_with_the_full_version_2_schema(self, tmp_path):
        """Every column version 2 created, written the way version 2 wrote."""
        import v2_schema

        path = str(tmp_path / "profiles.db")
        v2_schema.create(path)
        v2_schema.insert_job(
            path,
            {
                "job_id": "9001",
                "user": "smith6jt",
                "job_name": "wrap",
                "state": "COMPLETED",
                "submit_time": "2026-05-01T08:00:00",
                "end_time": "2026-05-01T08:10:00",
                "req_mem_gb": 4.0,
                "req_cpus": 1,
                "sacct_peak_rss_gb": 0.04,
                "sacct_elapsed_sec": 600,
                "sacct_cpu_time_sec": 0,
                "flags": "[]",
            },
        )
        stats = db.migrate_v2(path, str(tmp_path / "backup"))
        assert stats["migrated"] == 1
        with db.reader(path) as conn:
            assert db.fetch_jobs(conn)[0]["job_name"] == "wrap"
