"""Builders for synthetic sacct output in the exact shape the collector requests.

The value forms are the ones observed on the production cluster (Slurm 25.11):
TotalCPU as ``MM:SS.mmm`` below one hour, job-line State ``CANCELLED by <uid>``,
usage TRES such as ``cpu=00:02:07,fs/disk=1144847403527,mem=86984640K``.
"""

from typing import Any, Dict, Iterable, List, Optional

from lib.slurm import FIELDS, SEP

BASE_TS = 1_794_000_000  # 2026-11-06T21:20:00Z


def _row(**values: Any) -> Dict[str, str]:
    row = {name: "" for name in FIELDS}
    for key, value in values.items():
        if key not in row:
            raise KeyError(key)
        row[key] = "" if value is None else str(value)
    return row


def job_row(
    jid: int,
    *,
    display: Optional[str] = None,
    user: str = "smith6jt",
    account: str = "maigan",
    partition: str = "hpg-default",
    qos: str = "maigan",
    state: str = "COMPLETED",
    exit_code: str = "0:0",
    submit: int = BASE_TS,
    wait: int = 30,
    elapsed: int = 600,
    timelimit: Any = 60,
    cpus: int = 4,
    nodes: int = 1,
    mem: str = "16G",
    gpus: int = 0,
    gpu_type: Optional[str] = None,
    total_cpu: str = "",
    name: str = "job",
    workdir: str = "/blue/maigan/work",
    submit_line: str = "sbatch job.sh",
    started: bool = True,
    ended: bool = True,
) -> Dict[str, str]:
    """A job line. ``started=False`` models a job cancelled while pending."""
    start = submit + wait if started else None
    if not ended:
        end: Optional[int] = None
    elif started:
        end = submit + wait + elapsed
    else:
        end = submit + wait
    tres = f"billing={cpus},cpu={cpus},mem={mem},node={nodes}"
    if gpus:
        tres += f",gres/gpu={gpus}"
        if gpu_type:
            tres += f",gres/gpu:{gpu_type}={gpus}"
    return _row(
        JobIDRaw=jid,
        JobID=display or jid,
        User=user,
        Account=account,
        Partition=partition,
        QOS=qos,
        State=state,
        ExitCode=exit_code,
        DerivedExitCode=exit_code,
        Submit=submit,
        Eligible=submit,
        Start=start if start is not None else "Unknown",
        End=end if end is not None else "Unknown",
        ElapsedRaw=elapsed if started else 0,
        TimelimitRaw=timelimit,
        ReqCPUS=cpus,
        AllocCPUS=cpus if started else 0,
        ReqNodes=nodes,
        NNodes=nodes,
        ReqTRES=tres,
        AllocTRES=tres if started else "",
        TotalCPU=total_cpu,
        Restarts=0,
        NodeList="c0700a-s1" if started else "None assigned",
        JobName=name,
        WorkDir=workdir,
        SubmitLine=submit_line,
    )


def step_row(
    jid: int,
    step: str = "batch",
    *,
    display: Optional[str] = None,
    state: str = "COMPLETED",
    start: int = BASE_TS + 30,
    elapsed: int = 600,
    ntasks: int = 1,
    cpus: int = 4,
    total_cpu: str = "05:00.000",
    max_rss: Optional[str] = "2097152K",
    polled_cpu: Optional[str] = None,
    tot_mem: Optional[str] = None,
    disk_read: Optional[int] = None,
    disk_write: Optional[int] = None,
    gpu_util: Optional[int] = None,
    gpu_mem: Optional[str] = None,
    ended: bool = True,
) -> Dict[str, str]:
    """A step line (``.batch``, ``.extern``, ``.0``, ``.interactive``)."""
    in_tot = []
    in_max = []
    if polled_cpu:
        in_tot.append(f"cpu={polled_cpu}")
        in_max.append(f"cpu={polled_cpu}")
    if disk_read is not None:
        in_tot.append(f"fs/disk={disk_read}")
    if gpu_mem is not None:
        in_max.append(f"gres/gpumem={gpu_mem}")
    if gpu_util is not None:
        in_max.append(f"gres/gpuutil={gpu_util}")
    if max_rss is not None:
        in_max.append(f"mem={max_rss}")
        in_tot.append(f"mem={tot_mem or max_rss}")
    out_tot = [f"fs/disk={disk_write}"] if disk_write is not None else []
    base = display or str(jid)
    return _row(
        JobIDRaw=f"{jid}.{step}",
        JobID=f"{base}.{step}",
        State=state,
        ExitCode="0:0",
        Start=start,
        End=start + elapsed if ended else "Unknown",
        ElapsedRaw=elapsed,
        NTasks=ntasks,
        AllocCPUS=cpus,
        NNodes=1,
        TotalCPU=total_cpu,
        MaxRSS=max_rss,
        TRESUsageInTot=",".join(in_tot),
        TRESUsageInMax=",".join(in_max),
        TRESUsageInAve=(f"gres/gpuutil={gpu_util}" if gpu_util is not None else ""),
        TRESUsageOutTot=",".join(out_tot),
        JobName=step,
    )


def simple_job(jid: int, **kwargs: Any) -> List[Dict[str, str]]:
    """A job line with its ``.batch`` and ``.extern`` steps.

    Step keyword arguments are prefixed ``batch_`` (e.g. ``batch_max_rss``).
    """
    step_kw = {k[6:]: kwargs.pop(k) for k in list(kwargs) if k.startswith("batch_")}
    job = job_row(jid, **kwargs)
    if job["Start"] == "Unknown":
        return [job]
    start = int(job["Start"])
    elapsed = int(job["ElapsedRaw"])
    cpus = int(job["AllocCPUS"])
    display = kwargs.get("display")
    step_kw.setdefault("state", job["State"].split()[0])
    return [
        job,
        step_row(jid, "batch", display=display, start=start, elapsed=elapsed, cpus=cpus, **step_kw),
        step_row(
            jid,
            "extern",
            display=display,
            start=start,
            elapsed=elapsed,
            cpus=cpus,
            total_cpu="00:00.001",
            max_rss="12288K",
            state="COMPLETED",
        ),
    ]


def stream(rows: Iterable[Dict[str, str]]) -> str:
    """Render rows as ``sacct -n -p --delimiter=SEP`` prints them."""
    return "".join(SEP.join(row[name] for name in FIELDS) + SEP + "\n" for row in rows)
