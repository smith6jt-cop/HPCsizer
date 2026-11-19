"""SQLite storage for HPCsizer (schema v3).

Storage model
-------------
The database lives on a shared parallel filesystem and is read from many hosts.
SQLite's WAL mode needs shared memory between all processes using a database,
which only works on one host; on Lustre it ended in SIGBUS at the first write to
the ``-shm`` mapping. So this module never opens the published file for writing
and never uses WAL:

* Readers open the file read-only with ``immutable=1``: no locks, no journal,
  no side files. Read commands therefore work for anyone who can read the file.
* The single writer copies the file to node-local disk, applies all changes in
  one transaction there, and publishes the result with an atomic rename. A
  reader sees the old file or the new one, never a partial write, and a writer
  that dies leaves the published file untouched.

What is stored
--------------
Only what Slurm reported, in base units, plus the raw record. Efficiency,
waste, flags, job kind and family are derived at read time, so changing those
rules never requires a migration or a re-harvest.

A job is identified by ``(job_id_raw, submit_ts)``: Slurm reuses job ids after
``MaxJobId``, and the archive outlives that cycle.
"""

import calendar
import contextlib
import json
import os
import re
import shutil
import socket
import sqlite3
import stat
import tempfile
import time
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence
from urllib.parse import quote

SCHEMA_VERSION = 3
MIN_SQLITE = (3, 24, 0)  # INSERT ... ON CONFLICT DO UPDATE

# Job names belonging to HPCsizer's own infrastructure; never stored.
INTERNAL_JOB_NAME_PREFIXES = ("hpcsizer-",)

GIB = 1024**3


class DatabaseError(Exception):
    """Base class for storage errors with a message fit to show the user."""


class DatabaseMissing(DatabaseError):
    pass


class NeedsMigration(DatabaseError):
    pass


class LockBusy(DatabaseError):
    pass


class PublishConflict(DatabaseError):
    pass


def is_internal_job(job_name: Optional[str]) -> bool:
    """True if job_name belongs to HPCsizer's own infrastructure."""
    if not job_name:
        return False
    return job_name.startswith(INTERNAL_JOB_NAME_PREFIXES)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

# Columns written from sacct, in insert order. first_seen_ts / updated_ts and
# the surrogate key are managed here; job_monitor and job_context hold what the
# in-job sampler and hpg submit add, so a sacct upsert cannot erase them.
JOB_COLUMNS = (
    "job_id_raw",
    "submit_ts",
    "job_id",
    "array_job_id",
    "array_task_id",
    "het_offset",
    "source",
    "user",
    "account",
    "partition",
    "qos",
    "job_name",
    "submit_line",
    "work_dir",
    "state",
    "state_raw",
    "exit_code",
    "derived_exit_code",
    "eligible_ts",
    "start_ts",
    "end_ts",
    "elapsed_sec",
    "req_time_min",
    "req_cpus",
    "alloc_cpus",
    "req_nodes",
    "alloc_nodes",
    "node_list",
    "req_mem_mb",
    "alloc_mem_mb",
    "req_gpus",
    "alloc_gpus",
    "gpu_type",
    "cpu_sec",
    "cpu_src",
    "user_cpu_sec",
    "sys_cpu_sec",
    "peak_mem_bytes",
    "mem_src",
    "oom",
    "disk_read_bytes",
    "disk_write_bytes",
    "gpu_util_max",
    "gpu_util_ave",
    "gpu_mem_max_bytes",
    "n_steps",
    "n_tasks",
    "has_interactive_step",
    "restarts",
    "parser_version",
    "raw_json",
)

STEP_COLUMNS = (
    "step_id",
    "state",
    "exit_code",
    "start_ts",
    "end_ts",
    "elapsed_sec",
    "ntasks",
    "alloc_cpus",
    "cpu_sec",
    "cpu_src",
    "max_rss_bytes",
    "mem_tot_bytes",
    "disk_read_bytes",
    "disk_write_bytes",
    "gpu_util_max",
    "gpu_util_ave",
    "gpu_mem_max_bytes",
    "raw_json",
)

HARVEST_COLUMNS = (
    "started_ts",
    "finished_ts",
    "host",
    "trigger",
    "window_start_ts",
    "deep",
    "n_records",
    "n_inserted",
    "n_updated",
    "n_open",
    "n_internal",
    "n_unparsed",
    "n_late",
    "n_users",
    "n_profiles",
    "parser_version",
)

MONITOR_COLUMNS = (
    "source",
    "n_samples",
    "interval_sec",
    "peak_mem_bytes",
    "peak_usage_bytes",
    "cpu_sec",
    "max_cores",
    "avg_cores",
    "p95_cores",
    "gpu_util_max",
    "gpu_mem_max_bytes",
    "oom_kills",
    "top_comm",
    "coverage",
    "summary_json",
    "series_path",
)

CONTEXT_COLUMNS = ("script_hash", "tools_json", "input_gb", "detail_json")

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY,
    job_id_raw      INTEGER,            -- NULL for rows migrated from v2
    submit_ts       INTEGER,
    job_id          TEXT NOT NULL,      -- as displayed: 123, 123_4, 123+0
    array_job_id    INTEGER,
    array_task_id   INTEGER,
    het_offset      INTEGER,
    source          TEXT NOT NULL DEFAULT 'sacct',   -- sacct | legacy
    "user"          TEXT,
    account         TEXT,
    "partition"     TEXT,
    qos             TEXT,
    job_name        TEXT,
    submit_line     TEXT,
    work_dir        TEXT,
    state           TEXT NOT NULL,
    state_raw       TEXT,
    exit_code       TEXT,
    derived_exit_code TEXT,
    eligible_ts     INTEGER,
    start_ts        INTEGER,
    end_ts          INTEGER,
    elapsed_sec     INTEGER,
    req_time_min    INTEGER,            -- NULL = unlimited
    req_cpus        INTEGER,
    alloc_cpus      INTEGER,
    req_nodes       INTEGER,
    alloc_nodes     INTEGER,
    node_list       TEXT,
    req_mem_mb      INTEGER,            -- job total
    alloc_mem_mb    INTEGER,
    req_gpus        INTEGER,
    alloc_gpus      INTEGER,
    gpu_type        TEXT,
    cpu_sec         REAL,
    cpu_src         TEXT,               -- rusage | polled | mixed
    user_cpu_sec    REAL,
    sys_cpu_sec     REAL,
    peak_mem_bytes  INTEGER,
    mem_src         TEXT,               -- step | overlap_sum | ntasks_est | job | legacy_batch
    oom             INTEGER NOT NULL DEFAULT 0,
    disk_read_bytes  INTEGER,
    disk_write_bytes INTEGER,
    gpu_util_max    REAL,
    gpu_util_ave    REAL,
    gpu_mem_max_bytes INTEGER,
    n_steps         INTEGER,
    n_tasks         INTEGER,
    has_interactive_step INTEGER NOT NULL DEFAULT 0,
    restarts        INTEGER,
    parser_version  INTEGER NOT NULL,
    first_seen_ts   INTEGER NOT NULL,
    updated_ts      INTEGER NOT NULL,
    raw_json        TEXT,
    UNIQUE (job_id_raw, submit_ts)
);
CREATE INDEX IF NOT EXISTS jobs_end ON jobs(end_ts);
CREATE INDEX IF NOT EXISTS jobs_user_end ON jobs("user", end_ts);
CREATE INDEX IF NOT EXISTS jobs_job_id ON jobs(job_id);

CREATE TABLE IF NOT EXISTS steps (
    job_pk          INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    step_id         TEXT NOT NULL,      -- batch, extern, 0, interactive
    state           TEXT,
    exit_code       TEXT,
    start_ts        INTEGER,
    end_ts          INTEGER,
    elapsed_sec     INTEGER,
    ntasks          INTEGER,
    alloc_cpus      INTEGER,
    cpu_sec         REAL,
    cpu_src         TEXT,
    max_rss_bytes   INTEGER,
    mem_tot_bytes   INTEGER,
    disk_read_bytes  INTEGER,
    disk_write_bytes INTEGER,
    gpu_util_max    REAL,
    gpu_util_ave    REAL,
    gpu_mem_max_bytes INTEGER,
    raw_json        TEXT,
    PRIMARY KEY (job_pk, step_id)
) WITHOUT ROWID;

-- One row per successful harvest. A failed harvest publishes nothing, so its
-- trace is the log, the exit status and the age of the last row here.
CREATE TABLE IF NOT EXISTS harvest_runs (
    id              INTEGER PRIMARY KEY,
    started_ts      INTEGER NOT NULL,
    finished_ts     INTEGER NOT NULL,
    host            TEXT,
    trigger         TEXT,
    window_start_ts INTEGER,
    deep            INTEGER NOT NULL DEFAULT 0,
    n_records       INTEGER,
    n_inserted      INTEGER,
    n_updated       INTEGER,
    n_open          INTEGER,
    n_internal      INTEGER,
    n_unparsed      INTEGER,
    n_late          INTEGER,
    n_users         INTEGER,
    n_profiles      INTEGER,
    parser_version  INTEGER
);

-- Written by the harvester from the in-job sampler's spool files.
CREATE TABLE IF NOT EXISTS job_monitor (
    job_pk          INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    source          TEXT,               -- cgroup-v2 | cgroup-v1 | proc
    n_samples       INTEGER,
    interval_sec    REAL,
    peak_mem_bytes  INTEGER,            -- resident memory high-water mark
    peak_usage_bytes INTEGER,           -- cgroup usage incl. page cache
    cpu_sec         REAL,
    max_cores       REAL,               -- busiest sample
    avg_cores       REAL,
    p95_cores       REAL,               -- cores in use that 95% of the samples stay under
    gpu_util_max    REAL,
    gpu_mem_max_bytes INTEGER,
    oom_kills       INTEGER,
    top_comm        TEXT,
    coverage        REAL,               -- share of the job's run time the samples span
    summary_json    TEXT,
    series_path     TEXT,
    ingested_ts     INTEGER
);

-- Written by the harvester from what hpg submit recorded at submission.
CREATE TABLE IF NOT EXISTS job_context (
    job_pk          INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    script_hash     TEXT,
    tools_json      TEXT,
    input_gb        REAL,
    detail_json     TEXT,
    ingested_ts     INTEGER
);
"""


def check_sqlite() -> None:
    if sqlite3.sqlite_version_info < MIN_SQLITE:
        raise DatabaseError(
            "SQLite %s is too old; HPCsizer needs %s or newer"
            % (sqlite3.sqlite_version, ".".join(map(str, MIN_SQLITE)))
        )


def _q(columns: Sequence[str]) -> str:
    return ", ".join('"%s"' % c for c in columns)


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )


def schema_version(conn: sqlite3.Connection) -> int:
    """3 for the current schema, 0 for a v2 database, -1 for an empty file."""
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "meta" in tables:
        row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if row:
            return int(row[0])
    return 0 if "jobs" in tables else -1


def get_meta(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_meta(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


def get_meta_int(conn: sqlite3.Connection, key: str) -> Optional[int]:
    value = get_meta(conn, key)
    return int(value) if value not in (None, "") else None


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def _uri(path: str, params: str) -> str:
    return "file:" + quote(os.path.abspath(path)) + "?" + params


def connect_ro(db_path: str) -> sqlite3.Connection:
    """Open the published database read-only and immutable.

    ``immutable=1`` tells SQLite the file cannot change underneath it, which is
    true by construction (the writer replaces the file, never edits it), and
    makes SQLite skip all locking and journal handling.
    """
    if not os.path.exists(db_path):
        raise DatabaseMissing(
            "no database at %s -- run 'hpg harvest' (or 'hpg db migrate' for a v2 database)"
            % db_path
        )
    conn = sqlite3.connect(_uri(db_path, "mode=ro&immutable=1"), uri=True)
    conn.row_factory = sqlite3.Row
    version = 0 if _has_wal(db_path) else schema_version(conn)
    if version != SCHEMA_VERSION:
        conn.close()
        if version == 0:
            raise NeedsMigration(
                "%s is a version 2 database -- run 'hpg db migrate' once" % db_path
            )
        raise DatabaseError("%s has schema version %s; expected %s" % (db_path, version, 3))
    return conn


def _has_wal(db_path: str) -> bool:
    """True when a non-empty ``-wal`` sits beside the file.

    Version 3 never creates one, so its presence means a v2 database -- whose
    most recent rows may exist only in that WAL, invisible to a plain copy.
    """
    try:
        return os.path.getsize(db_path + "-wal") > 0
    except OSError:
        return False


def detect_version(db_path: str) -> Optional[int]:
    """Schema version of the file at ``db_path``; None when it does not exist."""
    if not os.path.exists(db_path):
        return None
    if _has_wal(db_path):
        return 0
    conn = sqlite3.connect(_uri(db_path, "mode=ro&immutable=1"), uri=True)
    try:
        return schema_version(conn)
    finally:
        conn.close()


_JOB_SELECT = (
    "SELECT "
    + ", ".join('j."%s"' % c for c in ("id",) + JOB_COLUMNS[:-1] + ("first_seen_ts", "updated_ts"))
    + ", "
    + ", ".join('m."%s" AS "mon_%s"' % (c, c) for c in MONITOR_COLUMNS if c != "summary_json")
    + ", "
    + ", ".join('c."%s" AS "ctx_%s"' % (c, c) for c in ("script_hash", "tools_json", "input_gb"))
    + " FROM jobs j"
    + " LEFT JOIN job_monitor m ON m.job_pk = j.id"
    + " LEFT JOIN job_context c ON c.job_pk = j.id"
)


def fetch_jobs(
    conn: sqlite3.Connection,
    since_ts: Optional[int] = None,
    until_ts: Optional[int] = None,
    user: Optional[str] = None,
    accounts: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Finished jobs ordered by end time, without the raw record.

    The window is on ``end_ts`` (epoch), so it does not depend on the time zone
    of whoever runs the report.
    """
    clauses: List[str] = []
    params: List[Any] = []
    if since_ts is not None:
        clauses.append("j.end_ts >= ?")
        params.append(int(since_ts))
    if until_ts is not None:
        clauses.append("j.end_ts < ?")
        params.append(int(until_ts))
    if user:
        clauses.append('j."user" = ?')
        params.append(user)
    if accounts:
        clauses.append("j.account IN (%s)" % ", ".join("?" * len(accounts)))
        params.extend(accounts)
    for prefix in INTERNAL_JOB_NAME_PREFIXES:
        clauses.append("COALESCE(j.job_name, '') NOT LIKE ?")
        params.append(prefix + "%")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(_JOB_SELECT + where + " ORDER BY j.end_ts, j.id", params).fetchall()
    return [dict(r) for r in rows]


def find_jobs(conn: sqlite3.Connection, job_id: str) -> List[Dict[str, Any]]:
    """Jobs matching a displayed id (``123_4``) or a raw id, newest first.

    More than one row means the id was reused after Slurm's job-id counter
    wrapped.
    """
    params: List[Any] = [job_id]
    clause = "j.job_id = ?"
    if job_id.isdigit():
        clause = "(j.job_id = ? OR j.job_id_raw = ?)"
        params.append(int(job_id))
    rows = conn.execute(
        _JOB_SELECT + " WHERE " + clause + " ORDER BY j.submit_ts DESC, j.id DESC", params
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_steps(conn: sqlite3.Connection, job_pk: int) -> List[Dict[str, Any]]:
    cols = [c for c in STEP_COLUMNS if c != "raw_json"]
    rows = conn.execute(
        "SELECT %s FROM steps WHERE job_pk=? ORDER BY start_ts, step_id" % _q(cols), (job_pk,)
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_raw(conn: sqlite3.Connection, job_pk: int) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT raw_json FROM jobs WHERE id=?", (job_pk,)).fetchone()
    if not row or not row[0]:
        return None
    return json.loads(row[0])


def fetch_monitor_summary(conn: sqlite3.Connection, job_pk: int) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT summary_json FROM job_monitor WHERE job_pk=?", (job_pk,)).fetchone()
    if not row or not row[0]:
        return None
    return json.loads(row[0])


def last_harvest(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM harvest_runs ORDER BY id DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def summary_counts(conn: sqlite3.Connection, since_ts: Optional[int] = None) -> Dict[str, Any]:
    """Row counts per user and per state, for ``hpg status``."""
    where, params = "", []
    if since_ts is not None:
        where, params = " WHERE end_ts >= ?", [int(since_ts)]
    by_user = conn.execute(
        "SELECT COALESCE(\"user\", '?'), COUNT(*) FROM jobs"
        + where
        + " GROUP BY 1 ORDER BY 2 DESC",
        params,
    ).fetchall()
    by_state = conn.execute(
        "SELECT state, COUNT(*) FROM jobs" + where + " GROUP BY 1 ORDER BY 2 DESC", params
    ).fetchall()
    by_source = conn.execute("SELECT source, COUNT(*) FROM jobs GROUP BY 1").fetchall()
    return {
        "users": [(r[0], r[1]) for r in by_user],
        "states": [(r[0], r[1]) for r in by_state],
        "sources": {r[0]: r[1] for r in by_source},
        "total": sum(r[1] for r in by_user),
    }


# ---------------------------------------------------------------------------
# Writing: lock, local build, atomic publish
# ---------------------------------------------------------------------------


def local_tmp_dir() -> str:
    """A directory on node-local storage for building the next snapshot."""
    for key in ("HPCSIZER_TMPDIR", "SLURM_TMPDIR", "TMPDIR"):
        value = os.environ.get(key)
        if value and os.path.isdir(value) and os.access(value, os.W_OK):
            return value
    return tempfile.gettempdir()


def _fsync_file(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _signature(path: str) -> Optional[tuple]:
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    return (st.st_ino, st.st_size, st.st_mtime_ns)


class WriteLock:
    """Exclusive-create lock file naming its holder.

    ``O_CREAT | O_EXCL`` is atomic on Lustre. A lock older than
    ``stale_seconds`` is taken over: its holder was killed, and because the
    watermark is published together with the data, the worst a wrongly broken
    lock can cause is one harvest's work being repeated.
    """

    def __init__(self, path: str, stale_seconds: float = 1800.0):
        self.path = path
        self.stale_seconds = stale_seconds
        self.held = False

    def acquire(self) -> None:
        for attempt in (1, 2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                if attempt == 1 and self._break_if_stale():
                    continue
                raise LockBusy(
                    "another HPCsizer writer holds %s (%s)" % (self.path, self.holder())
                ) from None
            with os.fdopen(fd, "w") as handle:
                json.dump(
                    {"host": socket.gethostname(), "pid": os.getpid(), "time": int(time.time())},
                    handle,
                )
            self.held = True
            return

    def holder(self) -> str:
        try:
            with open(self.path) as handle:
                info = json.load(handle)
            return "host %s pid %s" % (info.get("host"), info.get("pid"))
        except (OSError, ValueError):
            return "holder unknown"

    def _holder_is_dead(self) -> bool:
        """True when the lock was taken on this host by a process that no longer exists."""
        try:
            with open(self.path) as handle:
                info = json.load(handle)
            if info.get("host") != socket.gethostname():
                return False  # another node: only the age of the lock can tell
            os.kill(int(info["pid"]), 0)
        except ProcessLookupError:
            return True
        except (OSError, ValueError, KeyError, TypeError):
            return False
        return False

    def _break_if_stale(self) -> bool:
        try:
            age = time.time() - os.stat(self.path).st_mtime
        except FileNotFoundError:
            return True
        if age <= self.stale_seconds and not self._holder_is_dead():
            return False
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.path)
        return True

    def release(self) -> None:
        if self.held:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(self.path)
            self.held = False


WORKDIR_PREFIX = "hpcsizer-"
WORKDIR_MAX_AGE = 86400


def sweep_workdirs(tmp_root: str, now: Optional[float] = None) -> int:
    """Remove this user's working copies that a killed writer left behind, a day or more ago."""
    now = time.time() if now is None else now
    removed = 0
    try:
        names = os.listdir(tmp_root)
    except OSError:
        return 0
    for name in names:
        if not name.startswith(WORKDIR_PREFIX):
            continue
        path = os.path.join(tmp_root, name)
        try:
            info = os.lstat(path)
        except OSError:
            continue
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            continue
        if now - info.st_mtime > WORKDIR_MAX_AGE:
            shutil.rmtree(path, ignore_errors=True)
            removed += 1
    return removed


class SnapshotWriter:
    """Context manager yielding a writable connection to a local copy.

    On a clean exit the copy is checked and published over ``db_path`` with an
    atomic rename; the previous file stays reachable as ``<db>.prev``. On an
    exception nothing is published.
    """

    def __init__(
        self,
        db_path: str,
        stale_seconds: float = 1800.0,
        tmp_dir: Optional[str] = None,
        allow_create: bool = True,
    ):
        check_sqlite()
        self.db_path = os.path.abspath(db_path)
        self.lock = WriteLock(self.db_path + ".lock", stale_seconds)
        self.tmp_root = tmp_dir
        self.allow_create = allow_create
        self.workdir: Optional[str] = None
        self.local: Optional[str] = None
        self.conn: Optional[sqlite3.Connection] = None
        self._origin: Optional[tuple] = None

    def __enter__(self) -> sqlite3.Connection:
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.lock.acquire()
        try:
            tmp_root = self.tmp_root or local_tmp_dir()
            sweep_workdirs(tmp_root)
            self.workdir = tempfile.mkdtemp(prefix=WORKDIR_PREFIX, dir=tmp_root)
            self.local = os.path.join(self.workdir, "snapshot.db")
            self._origin = _signature(self.db_path)
            if self._origin is not None and _has_wal(self.db_path):
                raise NeedsMigration(
                    "%s is a version 2 database -- run 'hpg db migrate' once" % self.db_path
                )
            if self._origin is not None:
                shutil.copyfile(self.db_path, self.local)
            elif not self.allow_create:
                raise DatabaseMissing("no database at %s" % self.db_path)
            conn = sqlite3.connect(self.local, isolation_level=None)
            conn.row_factory = sqlite3.Row
            # The copy is private and disposable: durability comes from the
            # publish step, not from this file's journal.
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA foreign_keys=ON")
            version = schema_version(conn)
            if version == 0:
                conn.close()
                raise NeedsMigration(
                    "%s is a version 2 database -- run 'hpg db migrate' once" % self.db_path
                )
            if version not in (-1, SCHEMA_VERSION):
                conn.close()
                raise DatabaseError("unsupported schema version %s" % version)
            create_schema(conn)
            conn.execute("BEGIN")
            self.conn = conn
            return conn
        except BaseException:
            self._cleanup()
            raise

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            if self.conn is None:
                return False
            if exc_type is not None:
                with contextlib.suppress(sqlite3.Error):
                    self.conn.execute("ROLLBACK")
                self.conn.close()
                return False
            self.conn.execute("COMMIT")
            result = self.conn.execute("PRAGMA quick_check").fetchone()[0]
            self.conn.close()
            if result != "ok":
                raise DatabaseError("integrity check failed before publish: %s" % result)
            self._publish()
            return False
        finally:
            self._cleanup()

    def _publish(self) -> None:
        assert self.local is not None
        staged = "%s.tmp.%s.%d" % (self.db_path, socket.gethostname(), os.getpid())
        try:
            shutil.copyfile(self.local, staged)
            os.chmod(staged, 0o644)
            _fsync_file(staged)
            # If the published file changed since it was copied, another writer
            # got past the lock; publishing would discard its work.
            if _signature(self.db_path) != self._origin:
                raise PublishConflict(
                    "%s changed while this harvest was running; nothing published" % self.db_path
                )
            if self._origin is not None:
                prev = self.db_path + ".prev"
                with contextlib.suppress(OSError):
                    if os.path.exists(prev):
                        os.unlink(prev)
                    os.link(self.db_path, prev)
            os.replace(staged, self.db_path)
            _fsync_dir(os.path.dirname(self.db_path))
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(staged)

    def _cleanup(self) -> None:
        if self.workdir:
            shutil.rmtree(self.workdir, ignore_errors=True)
            self.workdir = None
        self.lock.release()


def _insert(conn: sqlite3.Connection, table: str, columns: Sequence[str], values: Sequence[Any]):
    return conn.execute(
        "INSERT INTO %s (%s) VALUES (%s)" % (table, _q(columns), ", ".join("?" * len(columns))),
        list(values),
    )


def upsert_job(
    conn: sqlite3.Connection,
    job: Mapping[str, Any],
    steps: Sequence[Mapping[str, Any]] = (),
    now: Optional[int] = None,
) -> str:
    """Insert or update a job and replace its steps. Returns 'inserted' or 'updated'.

    Only sacct-owned columns are updated, so ``first_seen_ts`` survives and
    rows in ``job_monitor`` / ``job_context`` are untouched. (The previous
    ``INSERT OR REPLACE`` deleted the row and with it everything the in-job
    monitor had written.)
    """
    now = int(time.time()) if now is None else int(now)
    if job.get("job_id_raw") is None or job.get("submit_ts") is None:
        raise ValueError("a sacct job needs job_id_raw and submit_ts")
    values = [job.get(col) for col in JOB_COLUMNS]
    existing = conn.execute(
        "SELECT id FROM jobs WHERE job_id_raw=? AND submit_ts=?",
        (job["job_id_raw"], job["submit_ts"]),
    ).fetchone()
    update_cols = [c for c in JOB_COLUMNS if c not in ("job_id_raw", "submit_ts")]
    conn.execute(
        "INSERT INTO jobs (%s, first_seen_ts, updated_ts) VALUES (%s, ?, ?) "
        "ON CONFLICT(job_id_raw, submit_ts) DO UPDATE SET %s, updated_ts=excluded.updated_ts"
        % (
            _q(JOB_COLUMNS),
            ", ".join("?" * len(JOB_COLUMNS)),
            ", ".join('"%s"=excluded."%s"' % (c, c) for c in update_cols),
        ),
        values + [now, now],
    )
    job_pk = conn.execute(
        "SELECT id FROM jobs WHERE job_id_raw=? AND submit_ts=?",
        (job["job_id_raw"], job["submit_ts"]),
    ).fetchone()[0]
    conn.execute("DELETE FROM steps WHERE job_pk=?", (job_pk,))
    for step in steps:
        _insert(
            conn,
            "steps",
            ("job_pk",) + STEP_COLUMNS,
            [job_pk] + [step.get(c) for c in STEP_COLUMNS],
        )
    if existing is None:
        _supersede_legacy(conn, job, job_pk)
        return "inserted"
    return "updated"


# A v2 row has no raw id and its submit time was a local-time string, so match
# the twin by displayed id within a window no job-id reuse can fall inside.
_LEGACY_TWIN_WINDOW = 36 * 3600


def _supersede_legacy(conn: sqlite3.Connection, job: Mapping[str, Any], job_pk: int) -> None:
    conn.execute(
        "DELETE FROM jobs WHERE source='legacy' AND job_id=? AND id<>? "
        "AND (submit_ts IS NULL OR ABS(submit_ts - ?) <= ?)",
        (job["job_id"], job_pk, job["submit_ts"], _LEGACY_TWIN_WINDOW),
    )


def insert_harvest_run(conn: sqlite3.Connection, stats: Mapping[str, Any]) -> None:
    _insert(conn, "harvest_runs", HARVEST_COLUMNS, [stats.get(c) for c in HARVEST_COLUMNS])


def upsert_monitor(
    conn: sqlite3.Connection, job_pk: int, summary: Mapping[str, Any], now: Optional[int] = None
) -> None:
    now = int(time.time()) if now is None else int(now)
    cols = ("job_pk",) + MONITOR_COLUMNS + ("ingested_ts",)
    conn.execute(
        "INSERT OR REPLACE INTO job_monitor (%s) VALUES (%s)"
        % (_q(cols), ", ".join("?" * len(cols))),
        [job_pk] + [summary.get(c) for c in MONITOR_COLUMNS] + [now],
    )


def upsert_context(
    conn: sqlite3.Connection, job_pk: int, context: Mapping[str, Any], now: Optional[int] = None
) -> None:
    now = int(time.time()) if now is None else int(now)
    cols = ("job_pk",) + CONTEXT_COLUMNS + ("ingested_ts",)
    conn.execute(
        "INSERT OR REPLACE INTO job_context (%s) VALUES (%s)"
        % (_q(cols), ", ".join("?" * len(cols))),
        [job_pk] + [context.get(c) for c in CONTEXT_COLUMNS] + [now],
    )


def latest_job_pk(conn: sqlite3.Connection, job_id_raw: int) -> Optional[sqlite3.Row]:
    """Most recent stored incarnation of a raw job id, with what profile ingestion needs."""
    return conn.execute(
        'SELECT id, "user", job_id, submit_ts, start_ts, end_ts, elapsed_sec, alloc_cpus, state '
        "FROM jobs WHERE job_id_raw=? ORDER BY submit_ts DESC LIMIT 1",
        (int(job_id_raw),),
    ).fetchone()


def jobs_of_submission(conn: sqlite3.Connection, job_id: int) -> List[sqlite3.Row]:
    """Stored jobs one sbatch call produced: the job itself, or every task of an array."""
    return conn.execute(
        'SELECT id, "user", job_id, submit_ts, end_ts FROM jobs '
        "WHERE job_id_raw=? OR array_job_id=? ORDER BY submit_ts DESC, id",
        (int(job_id), int(job_id)),
    ).fetchall()


def has_monitor(conn: sqlite3.Connection, job_pk: int) -> bool:
    return (
        conn.execute("SELECT 1 FROM job_monitor WHERE job_pk=?", (job_pk,)).fetchone() is not None
    )


def has_context(conn: sqlite3.Connection, job_pk: int) -> bool:
    return (
        conn.execute("SELECT 1 FROM job_context WHERE job_pk=?", (job_pk,)).fetchone() is not None
    )


# ---------------------------------------------------------------------------
# Migration from schema v2
# ---------------------------------------------------------------------------

_ARRAY_RE = re.compile(r"^(\d+)_(\d+)$")


def _legacy_ts(value: Any) -> Optional[int]:
    """v2 stored sacct's local-time strings; read them in this host's zone."""
    if not value or not isinstance(value, str) or value in ("Unknown", "None"):
        return None
    try:
        return int(time.mktime(time.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")))
    except (ValueError, OverflowError):
        return None


def _legacy_created(value: Any, fallback: int) -> int:
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            return calendar.timegm(time.strptime(value[:19], "%Y-%m-%d %H:%M:%S"))
    return fallback


def _legacy_row(old: Mapping[str, Any], now: int) -> Optional[Dict[str, Any]]:
    """Map a v2 ``jobs`` row to v3 columns, or None when it must be dropped."""
    raw_state = (old.get("state") or "").strip()
    state = raw_state.split()[0].upper() if raw_state else "UNKNOWN"
    if state in ("RUNNING", "PENDING", "UNKNOWN"):
        return None  # written by the in-job finalizer before the job had ended
    job_id = str(old.get("job_id") or "").strip()
    if not job_id:
        return None
    match = _ARRAY_RE.match(job_id)
    req_mem_gb = old.get("req_mem_gb")
    peak_gb = old.get("sacct_peak_rss_gb")
    # v2 read TotalCPU below one hour as 0, so a stored 0 means "unknown".
    cpu = old.get("sacct_cpu_time_sec") or None
    cpus = old.get("req_cpus")  # v2 stored NCPUS, the allocated count
    return {
        "job_id_raw": None,
        "submit_ts": _legacy_ts(old.get("submit_time")),
        "job_id": job_id,
        "array_job_id": int(match.group(1)) if match else None,
        "array_task_id": int(match.group(2)) if match else None,
        "het_offset": None,
        "source": "legacy",
        "user": old.get("user") or None,
        "account": old.get("account") or None,
        "partition": None,
        "qos": old.get("qos") or None,
        "job_name": old.get("job_name") or None,
        "submit_line": None,
        "work_dir": None,
        "state": state,
        "state_raw": raw_state,
        "exit_code": None,
        "derived_exit_code": None,
        "eligible_ts": None,
        "start_ts": _legacy_ts(old.get("start_time")),
        "end_ts": _legacy_ts(old.get("end_time")),
        "elapsed_sec": old.get("sacct_elapsed_sec"),
        "req_time_min": old.get("req_time_min"),
        "req_cpus": cpus,
        "alloc_cpus": cpus,
        "req_nodes": None,
        "alloc_nodes": None,
        "node_list": None,
        "req_mem_mb": int(round(req_mem_gb * 1024)) if req_mem_gb is not None else None,
        "alloc_mem_mb": None,
        "req_gpus": old.get("req_gpus") or 0,
        "alloc_gpus": old.get("req_gpus") or 0,
        "gpu_type": None,
        "cpu_sec": float(cpu) if cpu else None,
        "cpu_src": "rusage" if cpu else None,
        "user_cpu_sec": None,
        "sys_cpu_sec": None,
        "peak_mem_bytes": int(round(peak_gb * GIB)) if peak_gb else None,
        "mem_src": "legacy_batch" if peak_gb else None,
        "oom": int(state == "OUT_OF_MEMORY"),
        "disk_read_bytes": None,
        "disk_write_bytes": None,
        "gpu_util_max": None,
        "gpu_util_ave": None,
        "gpu_mem_max_bytes": None,
        "n_steps": None,
        "n_tasks": None,
        "has_interactive_step": 0,
        "restarts": None,
        "parser_version": 0,
        "raw_json": None,
        "_first_seen": _legacy_created(old.get("created_at"), now),
    }


def migrate_v2(
    db_path: str,
    backup_dir: str,
    tmp_dir: Optional[str] = None,
    now: Optional[int] = None,
) -> Dict[str, Any]:
    """Convert a v2 (WAL-mode) database in place and return row counts.

    The v2 file and its ``-wal`` are copied to local disk and opened there, so
    the conversion never maps shared memory on the parallel filesystem. The
    ``-shm`` file is deliberately not copied: SQLite rebuilds it from the WAL.
    Requires that nothing is still writing the v2 database.
    """
    check_sqlite()
    now = int(time.time()) if now is None else int(now)
    db_path = os.path.abspath(db_path)
    if not os.path.exists(db_path):
        raise DatabaseMissing("no database at %s" % db_path)
    version = detect_version(db_path)
    if version == SCHEMA_VERSION:
        raise DatabaseError("%s is already schema version %d" % (db_path, SCHEMA_VERSION))

    lock = WriteLock(db_path + ".lock")
    lock.acquire()
    workdir = tempfile.mkdtemp(prefix="hpcsizer-migrate-", dir=tmp_dir or local_tmp_dir())
    stats: Dict[str, Any] = {
        "v2_rows": 0,
        "migrated": 0,
        "dropped_internal": 0,
        "dropped_unfinished": 0,
        "cpu_time_unknown": 0,
        "wal_bytes": 0,
        "backup": [],
    }
    try:
        old_local = os.path.join(workdir, "v2.db")
        shutil.copyfile(db_path, old_local)
        if os.path.exists(db_path + "-wal"):
            stats["wal_bytes"] = os.path.getsize(db_path + "-wal")
            shutil.copyfile(db_path + "-wal", old_local + "-wal")

        old = sqlite3.connect(old_local)
        old.row_factory = sqlite3.Row
        old.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        old.execute("PRAGMA journal_mode=DELETE")
        check = old.execute("PRAGMA integrity_check").fetchone()[0]
        if check != "ok":
            raise DatabaseError("v2 database failed its integrity check: %s" % check)

        new_local = os.path.join(workdir, "v3.db")
        new = sqlite3.connect(new_local, isolation_level=None)
        new.execute("PRAGMA journal_mode=DELETE")
        new.execute("PRAGMA synchronous=OFF")
        create_schema(new)
        new.execute("BEGIN")
        cols = JOB_COLUMNS + ("first_seen_ts", "updated_ts")
        has_jobs = old.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'"
        ).fetchone()
        for row in old.execute("SELECT * FROM jobs") if has_jobs else ():
            stats["v2_rows"] += 1
            record = dict(row)
            if is_internal_job(record.get("job_name")):
                stats["dropped_internal"] += 1
                continue
            mapped = _legacy_row(record, now)
            if mapped is None:
                stats["dropped_unfinished"] += 1
                continue
            if mapped["cpu_sec"] is None:
                stats["cpu_time_unknown"] += 1
            first_seen = mapped.pop("_first_seen")
            _insert(new, "jobs", cols, [mapped.get(c) for c in JOB_COLUMNS] + [first_seen, now])
            stats["migrated"] += 1
        set_meta(new, "migrated_from_v2_ts", now)
        new.execute("COMMIT")
        if new.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise DatabaseError("converted database failed its integrity check")
        new.close()
        old.close()

        os.makedirs(backup_dir, exist_ok=True)
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now))
        for suffix in ("", "-wal", "-shm"):
            src = db_path + suffix
            if os.path.exists(src):
                dst = os.path.join(backup_dir, "profiles.v2.%s.db%s" % (stamp, suffix))
                shutil.copy2(src, dst)
                stats["backup"].append(dst)

        staged = "%s.tmp.%s.%d" % (db_path, socket.gethostname(), os.getpid())
        shutil.copyfile(new_local, staged)
        os.chmod(staged, 0o644)
        _fsync_file(staged)
        os.replace(staged, db_path)
        for suffix in ("-wal", "-shm"):
            with contextlib.suppress(FileNotFoundError):
                os.unlink(db_path + suffix)
        _fsync_dir(os.path.dirname(db_path))
        return stats
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        lock.release()


@contextlib.contextmanager
def reader(db_path: str) -> Iterator[sqlite3.Connection]:
    conn = connect_ro(db_path)
    try:
        yield conn
    finally:
        conn.close()
