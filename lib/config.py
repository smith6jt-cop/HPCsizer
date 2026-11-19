"""Settings for HPCsizer.

Precedence: environment variables, then ``hpcsizer.ini`` (``HPCSIZER_CONFIG``
or ``<repo>/hpcsizer.ini``), then the defaults below.

scrontab jobs start without the user's environment, so anything the scheduled
harvest needs must be resolvable from the INI file or from the location of the
code itself -- never only from ``~/.bashrc`` exports.
"""

import configparser
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent

# Job names that say nothing about what ran; these are grouped by the program
# found in the submit line instead.
DEFAULT_GENERIC_NAMES = (
    "wrap",
    "sbatch",
    "bash",
    "sh",
    "python",
    "python3",
    "rscript",
    "r",
    "job",
    "run",
    "test",
    "tmp",
    "script",
    "interactive",
    "a.out",
)


DEFAULT_DATA_ROOTS = ("/blue", "/orange", "/red", "/scratch", "/home")


class ConfigError(Exception):
    """The configuration file cannot be read at all."""


def _at_least(low: float) -> Tuple[str, Callable[[float], bool]]:
    return "at least %g" % low, lambda value: value >= low


def _above(low: float) -> Tuple[str, Callable[[float], bool]]:
    return "greater than %g" % low, lambda value: value > low


# What a numeric setting must satisfy for the code to work with it. A value
# outside its rule is not used: the default is, and the problem is reported.
RULES: Dict[str, Tuple[str, Callable[[float], bool]]] = {
    # collector
    "overlap_days": _at_least(0),
    "deep_days": _above(0),
    "deep_every_days": _above(0),
    "first_days": _above(0),
    "slice_days": _above(0),
    "refresh_minutes": _at_least(0),
    "stale_hours": _above(0),
    "lock_stale_minutes": _above(0),
    "retries": _at_least(1),
    "retry_delay": _at_least(0),
    # measurement
    "acct_sample_sec": _at_least(1),
    # policy
    "lookback_days": _above(0),
    "last_n": _at_least(1),
    "min_runs": _at_least(1),
    "min_runs_pooled": _at_least(1),
    "mem_headroom": _at_least(1),
    "cpu_headroom": _at_least(1),
    "time_headroom": _at_least(1),
    "time_percentile": ("greater than 0 and at most 1", lambda value: 0 < value <= 1),
    "min_time_min": _at_least(1),
    "oom_raise": _at_least(1),
    "timeout_raise": _at_least(1),
    "model_min_points": _at_least(2),
    "model_min_r2": ("between 0 and 1", lambda value: 0 <= value <= 1),
}


def convert(key: str, raw: str, whole: bool):
    """``raw`` as the number setting ``key`` takes. Raises ``ValueError`` saying what is wrong."""
    try:
        value = int(raw) if whole else float(raw)
    except ValueError:
        raise ValueError("%r is not a %snumber" % (raw, "whole " if whole else "")) from None
    if not math.isfinite(value):
        raise ValueError("%r is not a finite number" % raw)
    rule = RULES.get(key)
    if rule is not None and not rule[1](value):
        raise ValueError("%r must be %s" % (raw, rule[0]))
    return value


@dataclass
class Config:
    """All tunables in one place. See ``hpcsizer.ini.example`` for the INI keys."""

    root: str
    db_path: str
    accounts: List[str]
    spool_dir: str
    log_dir: str
    ts_dir: str
    backup_dir: str

    # Executables
    sacct: str = "sacct"
    sbatch: str = "sbatch"
    squeue: str = "squeue"
    scrontab: str = "scrontab"
    scontrol: str = "scontrol"
    compute_python: str = "python3"

    # Scheduled harvest job
    sched_account: str = ""
    sched_qos: str = ""
    sched_partition: str = ""
    sched_time: str = "00:20:00"
    sched_mem: str = "1gb"
    mail_user: str = ""
    mail_type: str = "FAIL"

    # Collector
    overlap_days: float = 3.0
    deep_days: float = 35.0
    deep_every_days: float = 7.0
    first_days: float = 182.0
    slice_days: float = 30.0
    refresh_minutes: float = 15.0
    stale_hours: float = 26.0
    lock_stale_minutes: float = 30.0
    retries: int = 3
    retry_delay: float = 5.0

    # Classification
    session_names: List[str] = field(default_factory=lambda: ["vscode"])
    generic_names: List[str] = field(default_factory=lambda: list(DEFAULT_GENERIC_NAMES))

    # In-job sampler: also read perf counters of the busiest process every 5 minutes
    profile_perf: bool = False
    # In-job sampler: on a job with several nodes, also sample the other nodes
    # (through one more srun step in the job)
    profile_multi_node: bool = True
    # Mode the spool is created with: group-writable, setgid and sticky, so that
    # every member's jobs can add files and nobody can remove another's.
    spool_mode: int = 0o3770

    # File systems searched for a job's input data when a script is analysed
    data_roots: List[str] = field(default_factory=lambda: list(DEFAULT_DATA_ROOTS))

    # Measurement: slurmd samples task usage every this many seconds
    acct_sample_sec: int = 60

    # Recommendation policy
    lookback_days: int = 180
    last_n: int = 20
    min_runs: int = 3
    min_runs_pooled: int = 5
    mem_headroom: float = 1.2
    cpu_headroom: float = 1.25
    time_headroom: float = 1.5
    time_percentile: float = 0.95
    min_time_min: int = 15
    oom_raise: float = 1.5
    timeout_raise: float = 1.5
    model_min_points: int = 10
    model_min_r2: float = 0.5

    # Settings that could not be used as written; the defaults apply to them
    problems: List[str] = field(default_factory=list)

    @property
    def lock_path(self) -> str:
        return self.db_path + ".lock"


def _split(value: str) -> List[str]:
    return [v.strip() for v in value.replace(";", ",").split(",") if v.strip()]


def _ini_path(env: Mapping[str, str], root: Path) -> Path:
    explicit = env.get("HPCSIZER_CONFIG")
    return Path(explicit) if explicit else root / "hpcsizer.ini"


def load_config(env: Optional[Mapping[str, str]] = None) -> Config:
    """Build the effective configuration."""
    env = os.environ if env is None else env
    root = Path(env.get("HPCSIZER_ROOT") or REPO_ROOT)

    ini = configparser.ConfigParser(interpolation=None)
    ini_file = _ini_path(env, root)
    if ini_file.is_file():
        try:
            ini.read(ini_file)
        except (configparser.Error, OSError, UnicodeDecodeError) as exc:
            reason = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
            raise ConfigError("cannot read %s: %s" % (ini_file, reason)) from None

    def get(section: str, key: str, default: str = "") -> str:
        if ini.has_option(section, key):
            return ini.get(section, key).strip()
        return default

    def path(env_key: str, key: str, default: Path) -> str:
        value = env.get(env_key, "") if env_key else ""
        value = value or get("paths", key)
        return str(Path(value).expanduser()) if value else str(default)

    accounts = _split(env.get("HPCSIZER_ACCT", "") or get("slurm", "accounts"))

    cfg = Config(
        root=str(root),
        db_path=path("HPCSIZER_DB", "db", root / "profiles.db"),
        accounts=accounts,
        spool_dir=path("HPCSIZER_SPOOL", "spool", root / "spool"),
        log_dir=path("", "logs", root / "logs"),
        ts_dir=path("", "timeseries", root / "timeseries"),
        backup_dir=path("", "backup", root / "backup"),
    )

    for key in ("sacct", "sbatch", "squeue", "scrontab", "scontrol", "compute_python"):
        value = get("slurm", key)
        if value:
            setattr(cfg, key, value)

    for attr, key in (
        ("sched_account", "account"),
        ("sched_qos", "qos"),
        ("sched_partition", "partition"),
        ("sched_time", "time"),
        ("sched_mem", "mem"),
        ("mail_user", "mail_user"),
        ("mail_type", "mail_type"),
    ):
        value = get("schedule", key)
        if value:
            setattr(cfg, attr, value)
    if not cfg.sched_account and len(accounts) == 1:
        cfg.sched_account = accounts[0]

    numeric = {
        "collector": (
            "overlap_days",
            "deep_days",
            "deep_every_days",
            "first_days",
            "slice_days",
            "refresh_minutes",
            "stale_hours",
            "lock_stale_minutes",
            "retries",
            "retry_delay",
        ),
        "measurement": ("acct_sample_sec",),
        "policy": (
            "lookback_days",
            "last_n",
            "min_runs",
            "min_runs_pooled",
            "mem_headroom",
            "cpu_headroom",
            "time_headroom",
            "time_percentile",
            "min_time_min",
            "oom_raise",
            "timeout_raise",
            "model_min_points",
            "model_min_r2",
        ),
    }

    def number(where: str, key: str, raw: str) -> None:
        try:
            setattr(cfg, key, convert(key, raw, isinstance(getattr(cfg, key), int)))
        except ValueError as exc:
            cfg.problems.append("%s: %s; using %s" % (where, exc, getattr(cfg, key)))

    for section, keys in numeric.items():
        for key in keys:
            raw = get(section, key)
            if raw:
                number("%s: [%s] %s" % (ini_file.name, section, key), key, raw)

    for key in ("session_names", "generic_names"):
        raw = get("classify", key)
        if raw:
            setattr(cfg, key, _split(raw))
    if get("paths", "data_roots"):
        cfg.data_roots = _split(get("paths", "data_roots"))
    if get("profile", "perf"):
        cfg.profile_perf = get("profile", "perf").lower() in ("1", "yes", "true", "on")
    if get("profile", "multi_node"):
        cfg.profile_multi_node = get("profile", "multi_node").lower() in ("1", "yes", "true", "on")
    if get("profile", "spool_mode"):
        try:
            cfg.spool_mode = int(get("profile", "spool_mode"), 8) & 0o7777
        except ValueError:
            cfg.problems.append(
                "%s: [profile] spool_mode: %r is not an octal mode; using %o"
                % (ini_file.name, get("profile", "spool_mode"), cfg.spool_mode)
            )

    # Kept from the previous harvester so existing overrides (and the test
    # suite, which zeroes the backoff) continue to work.
    if env.get("HPCSIZER_HARVEST_RETRIES"):
        number("HPCSIZER_HARVEST_RETRIES", "retries", env["HPCSIZER_HARVEST_RETRIES"])
    if env.get("HPCSIZER_HARVEST_RETRY_DELAY"):
        number("HPCSIZER_HARVEST_RETRY_DELAY", "retry_delay", env["HPCSIZER_HARVEST_RETRY_DELAY"])
    return cfg
