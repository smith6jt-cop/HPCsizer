"""A synthetic Slurm account as sacct would report it: four users, every kind of job.

Used by the end-to-end tests and to produce the example output in the
documentation. Nothing here is random: the same ``now`` gives the same rows.

alice   an editor session per working day (16 cores, 256 GB; a GPU variant
        whose GPU sits idle), a nightly pipeline ``mplx<N>k-spleen`` that asks
        for 64 GB and 16 cores and uses about 10 GB and 3 cores
bjones  RStudio sessions from Open OnDemand, ``align`` jobs of which one timed
        out and one ran out of memory, and unnamed ``--wrap`` jobs of two programs
cchen   an array ``xen_raw_evidence`` and a GPU training job that uses its GPU
dpatel  jobs that never amounted to much: failed in seconds, cancelled while
        pending, lost to a node failure

Plus: a job id that Slurm reused, HPCsizer's own harvest jobs, and jobs that
are still pending or running.
"""

from typing import Dict, List

from sacct_fixture import job_row, simple_job, step_row

HOUR = 3600
DAY = 86400
GIB_K = 1024 * 1024  # kilobytes per GiB: sacct prints memory use in K


def _rss(gigabytes: float) -> str:
    return "%dK" % int(gigabytes * GIB_K)


def _cpu(seconds: float) -> str:
    """CPU time in the forms sacct prints: MM:SS.mmm below an hour, [D-]HH:MM:SS above."""
    seconds = int(seconds)
    if seconds < HOUR:
        return "%02d:%02d.000" % (seconds // 60, seconds % 60)
    days, rest = divmod(seconds, DAY)
    clock = "%02d:%02d:%02d" % (rest // HOUR, rest % HOUR // 60, rest % 60)
    return "%d-%s" % (days, clock) if days else clock


def account_rows(now: int) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    jid = 51_000_000

    def nxt() -> int:
        nonlocal jid
        jid += 1
        return jid

    # -- alice: editor sessions, one per day for 40 days ------------------------
    for day in range(40, 0, -1):
        submit = now - day * DAY + 9 * HOUR
        gpu = day % 5 == 0
        elapsed = 8 * HOUR if day % 4 else 12 * HOUR
        cancelled = day % 4 != 0
        rows += simple_job(
            nxt(),
            user="alice",
            name="vscode",
            submit_line="sbatch /home/alice/vscode_gpu.sh"
            if gpu
            else "sbatch /home/alice/vscode_cpu.sh",
            state="CANCELLED by 1001" if cancelled else "TIMEOUT",
            exit_code="0:15" if cancelled else "0:0",
            submit=submit,
            elapsed=elapsed,
            timelimit=720,
            cpus=16,
            mem="256G",
            gpus=1 if gpu else 0,
            gpu_type="a100" if gpu else None,
            total_cpu="00:00.210",  # rusage is not collected for a step Slurm kills
            batch_polled_cpu=_cpu(elapsed * (0.6 + (day % 7) * 0.1)),
            batch_max_rss=_rss(22 + day % 9),
            batch_gpu_util=0 if gpu else None,
            batch_gpu_mem="0M" if gpu else None,
        )

    # -- alice: nightly pipeline, numbered by sample size ------------------------
    for night in range(30, 0, -1):
        size = (12, 30, 45)[night % 3]
        rows += simple_job(
            nxt(),
            user="alice",
            name="mplx%dk-spleen" % size,
            submit_line="sbatch run_mplx.sh %d" % size,
            submit=now - night * DAY + 22 * HOUR,
            elapsed=40 * 60 + size * 40,
            timelimit=2880,
            cpus=16,
            mem="64G",
            total_cpu=_cpu((40 * 60 + size * 40) * (2.2 + size / 50.0)),
            batch_max_rss=_rss(6 + size / 7.0),
            batch_disk_read=int(size * 3e9),
            batch_disk_write=int(size * 5e8),
        )

    # -- bjones: RStudio sessions --------------------------------------------------
    for day in range(36, 0, -3):
        rows += simple_job(
            nxt(),
            user="bjones",
            name="ondemand/sys/dashboard/sys/rstudio",
            submit_line="sbatch --export=NONE /home/bjones/ondemand/data/sys/dashboard/batch_connect/sys/rstudio/output/script.sh",
            state="TIMEOUT" if day % 2 else "CANCELLED by 1002",
            submit=now - day * DAY + 10 * HOUR,
            elapsed=6 * HOUR,
            timelimit=360,
            cpus=8,
            mem="96G",
            total_cpu="00:00.150",
            batch_polled_cpu=_cpu(6 * HOUR * 0.4),
            batch_max_rss=_rss(30 + day % 11),
        )

    # -- bjones: align jobs; one timed out, a later one ran out of memory ------------
    for i in range(14, 0, -1):
        state, exit_code, elapsed, rss, batch_state = (
            "COMPLETED",
            "0:0",
            50 * 60 + i * 120,
            11.0,
            None,
        )
        cpu = _cpu(elapsed * 7.6)
        if i == 9:
            state, elapsed, cpu = "TIMEOUT", 120 * 60, "00:00.300"
        if i == 4:
            state, exit_code, elapsed, rss, batch_state = (
                "OUT_OF_MEMORY",
                "0:125",
                20 * 60,
                16.0,
                "OUT_OF_MEMORY",
            )
        rows += simple_job(
            nxt(),
            user="bjones",
            name="align",
            submit_line="sbatch align.sbatch",
            state=state,
            exit_code=exit_code,
            submit=now - i * 2 * DAY + 14 * HOUR,
            elapsed=elapsed,
            timelimit=120,
            cpus=8,
            mem="16G",
            total_cpu=cpu,
            batch_polled_cpu=_cpu(elapsed * 7.5) if state == "TIMEOUT" else None,
            batch_max_rss=_rss(rss),
            **({"batch_state": batch_state} if batch_state else {}),
        )

    # -- bjones: unnamed --wrap jobs of two different programs ----------------------
    for i in range(24, 0, -1):
        heavy = i % 2 == 0
        rows += simple_job(
            nxt(),
            user="bjones",
            name="wrap",
            submit_line="sbatch --mem=32G -c 4 --wrap python %s --sample S%03d"
            % ("read_xenium.py" if heavy else "archive.py", i),
            submit=now - i * DAY + 3 * HOUR,
            elapsed=(30 if heavy else 6) * 60,
            timelimit=240,
            cpus=4,
            mem="32G",
            total_cpu=_cpu((30 if heavy else 6) * 60 * (3.4 if heavy else 0.9)),
            batch_max_rss=_rss(26.0 if heavy else 1.5),
        )

    # -- cchen: an array of 20 tasks, submitted together ------------------------------
    array_id = nxt()
    for task in range(1, 21):
        raw = array_id if task == 20 else nxt()
        rows += simple_job(
            raw,
            display="%d_%d" % (array_id, task),
            user="cchen",
            name="xen_raw_evidence",
            submit_line="sbatch --array=1-20%5 xen_evidence.sbatch",
            submit=now - 6 * DAY,
            wait=task * 600,
            elapsed=25 * 60 + task * 30,
            timelimit=480,
            cpus=2,
            mem="48G",
            total_cpu=_cpu((25 * 60 + task * 30) * 1.7),
            batch_max_rss=_rss(9 + task * 0.2),
        )

    # -- cchen: GPU training that uses its GPU ------------------------------------------
    for i in range(8, 0, -1):
        rows += simple_job(
            nxt(),
            user="cchen",
            name="train-unet",
            submit_line="sbatch --gres=gpu:a100:1 train.sbatch",
            submit=now - i * 3 * DAY,
            elapsed=5 * HOUR,
            timelimit=600,
            cpus=8,
            mem="64G",
            gpus=1,
            gpu_type="a100",
            total_cpu=_cpu(5 * HOUR * 5.5),
            batch_max_rss=_rss(41),
            batch_gpu_util=88 + i % 9,
            batch_gpu_mem="%dM" % (30000 + i * 500),
        )

    # -- dpatel: little that ran -----------------------------------------------------------
    for i in range(5):
        rows += simple_job(
            nxt(),
            user="dpatel",
            name="test",
            submit_line="sbatch test.sh",
            state="FAILED",
            exit_code="1:0",
            submit=now - (10 + i) * DAY,
            elapsed=4,
            timelimit=60,
            cpus=1,
            mem="8G",
            total_cpu="00:00.900",
            batch_max_rss="2000K",
            batch_state="FAILED",
        )
    rows += simple_job(
        nxt(),
        user="dpatel",
        name="bigjob",
        submit_line="sbatch big.sh",
        state="CANCELLED by 1004",
        submit=now - 8 * DAY,
        started=False,
        cpus=32,
        mem="500G",
        timelimit=5760,
    )
    rows += simple_job(
        nxt(),
        user="dpatel",
        name="bigjob",
        submit_line="sbatch big.sh",
        state="NODE_FAIL",
        submit=now - 7 * DAY,
        elapsed=3 * HOUR,
        cpus=32,
        mem="500G",
        timelimit=5760,
        total_cpu="00:00.100",
        batch_polled_cpu=_cpu(3 * HOUR * 20),
        batch_max_rss=_rss(310),
        batch_state="CANCELLED",
    )
    # An interactive shell from salloc/srun --pty.
    shell = nxt()
    rows.append(
        job_row(
            shell,
            user="dpatel",
            name="interactive",
            submit_line="srun --pty -c 4 --mem=16G -t 120 bash -i",
            state="COMPLETED",
            submit=now - 5 * DAY,
            elapsed=90 * 60,
            timelimit=120,
            cpus=4,
            mem="16G",
        )
    )
    rows.append(
        step_row(
            shell,
            "interactive",
            start=now - 5 * DAY + 30,
            elapsed=90 * 60,
            cpus=4,
            total_cpu=_cpu(90 * 60 * 0.3),
            max_rss=_rss(3.2),
        )
    )

    # -- a job id Slurm used twice (its counter wrapped) -------------------------------------
    reused = 4_200_001
    rows += simple_job(
        reused, user="cchen", name="old-incarnation", submit=now - 170 * DAY, elapsed=600
    )
    rows += simple_job(
        reused, user="alice", name="new-incarnation", submit=now - 2 * DAY, elapsed=900
    )

    # -- HPCsizer's own jobs must never be analysed -------------------------------------------
    for i in range(6):
        rows += simple_job(
            nxt(),
            user="alice",
            name="hpcsizer-harvest",
            submit=now - i * DAY,
            elapsed=40,
            cpus=1,
            mem="1G",
        )

    # -- still open: one running session, a pending pipeline run, a pending array ----------------
    running = nxt()
    rows.append(
        job_row(
            running,
            user="alice",
            name="vscode",
            submit_line="sbatch /home/alice/vscode_cpu.sh",
            state="RUNNING",
            submit=now - 3 * HOUR,
            elapsed=3 * HOUR - 30,
            timelimit=720,
            cpus=16,
            mem="256G",
            ended=False,
        )
    )
    rows.append(
        step_row(
            running,
            "batch",
            state="RUNNING",
            start=now - 3 * HOUR + 30,
            elapsed=3 * HOUR - 30,
            cpus=16,
            total_cpu="00:00.000",
            max_rss=None,
            ended=False,
        )
    )
    rows.append(
        job_row(
            nxt(),
            user="alice",
            name="mplx30k-spleen",
            submit_line="sbatch run_mplx.sh 30",
            state="PENDING",
            submit=now - 600,
            started=False,
            ended=False,
            timelimit=2880,
            cpus=16,
            mem="64G",
        )
    )
    pending_array = nxt()
    rows.append(
        job_row(
            pending_array,
            display="%d_[1-50%%5]" % pending_array,
            user="bjones",
            name="align",
            submit_line="sbatch --array=1-50%5 align.sbatch",
            state="PENDING",
            submit=now - 300,
            started=False,
            ended=False,
            timelimit=120,
            cpus=8,
            mem="16G",
        )
    )
    return rows
