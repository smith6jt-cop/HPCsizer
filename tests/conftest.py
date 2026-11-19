"""Shared fixtures: stub Slurm commands on PATH and a configured temporary root.

The stubs record how they were called and replay prepared output, so the
collector, scheduler and CLI are exercised end to end without a cluster.
"""

import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sacct_fixture import stream  # noqa: E402

from lib.config import load_config  # noqa: E402

# Each call consumes the first line of $STUB_DIR/sacct.plan:
#   fail        -> exit 1 with an error on stderr
#   <filename>  -> print $STUB_DIR/<filename>
# With an empty plan it prints $STUB_DIR/sacct.default when that exists.
SACCT_STUB = r"""#!/usr/bin/env bash
printf 'sacct %s\n' "$*" >> "$STUB_DIR/sacct.calls"
printf 'TZ=%s SLURM_TIME_FORMAT=%s\n' "${TZ:-}" "${SLURM_TIME_FORMAT:-}" >> "$STUB_DIR/sacct.env"
plan="$STUB_DIR/sacct.plan"
step=""
if [[ -s "$plan" ]]; then
    step="$(head -n 1 "$plan")"
    sed -i '1d' "$plan"
fi
case "$step" in
    fail) echo "sacct: error: slurmdbd: connection refused" >&2; exit 1 ;;
    "")   if [[ -f "$STUB_DIR/sacct.default" ]]; then cat "$STUB_DIR/sacct.default"; fi ;;
    *)    cat "$STUB_DIR/$step" ;;
esac
"""

RECORDER = r"""#!/usr/bin/env bash
printf '%s %s\n' "$(basename "$0")" "$*" >> "$STUB_DIR/calls"
if [[ -f "$STUB_DIR/$(basename "$0").out" ]]; then cat "$STUB_DIR/$(basename "$0").out"; fi
if [[ -f "$STUB_DIR/$(basename "$0").rc" ]]; then exit "$(cat "$STUB_DIR/$(basename "$0").rc")"; fi
exit 0
"""

# scrontab stub: -l prints the stored table (or the "no crontab" error),
# a file argument replaces it.
SCRONTAB_STUB = r"""#!/usr/bin/env bash
printf 'scrontab %s\n' "$*" >> "$STUB_DIR/calls"
table="$STUB_DIR/scrontab.table"
if [[ "${1:-}" == "-l" ]]; then
    if [[ -f "$table" ]]; then cat "$table"; exit 0; fi
    echo "no crontab for $(id -un)" >&2; exit 1
fi
cp "$1" "$table"
"""


class Stubs:
    def __init__(self, folder: Path):
        self.dir = folder
        self.bin = folder / "bin"
        self.bin.mkdir(parents=True)

    def install(self, name: str, body: str = RECORDER) -> Path:
        path = self.bin / name
        path.write_text(body)
        path.chmod(0o755)
        return path

    def remove(self, name: str) -> None:
        (self.bin / name).unlink()

    # -- sacct ---------------------------------------------------------------
    def sacct_default(self, rows) -> None:
        (self.dir / "sacct.default").write_text(stream(rows))

    def sacct_file(self, name: str, rows=None, text=None) -> None:
        (self.dir / name).write_text(stream(rows) if text is None else text)

    def sacct_plan(self, *steps: str) -> None:
        (self.dir / "sacct.plan").write_text("".join(s + "\n" for s in steps))

    def sacct_calls(self):
        path = self.dir / "sacct.calls"
        return path.read_text().splitlines() if path.exists() else []

    def sacct_env(self):
        path = self.dir / "sacct.env"
        return path.read_text().splitlines() if path.exists() else []

    # -- everything else -----------------------------------------------------
    def output(self, name: str, text: str) -> None:
        (self.dir / (name + ".out")).write_text(text)

    def returncode(self, name: str, code: int) -> None:
        (self.dir / (name + ".rc")).write_text(str(code))

    def calls(self, name=None):
        path = self.dir / "calls"
        lines = path.read_text().splitlines() if path.exists() else []
        return [ln for ln in lines if name is None or ln.startswith(name + " ")]

    def scrontab_table(self) -> str:
        path = self.dir / "scrontab.table"
        return path.read_text() if path.exists() else ""


@pytest.fixture
def stubs(tmp_path, monkeypatch):
    """Stub sacct on PATH; other commands are added per test with ``install``."""
    stub = Stubs(tmp_path / "stubs")
    stub.install("sacct", SACCT_STUB)
    monkeypatch.setenv("STUB_DIR", str(stub.dir))
    monkeypatch.setenv("PATH", "%s%s%s" % (stub.bin, os.pathsep, os.environ["PATH"]))
    return stub


@pytest.fixture
def root(tmp_path, monkeypatch):
    """A temporary HPCsizer root wired through the environment, as the CLI sees it."""
    folder = tmp_path / "root"
    folder.mkdir()
    for name in list(os.environ):
        if name.startswith("HPCSIZER_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("HPCSIZER_ROOT", str(folder))
    monkeypatch.setenv("HPCSIZER_ACCT", "maigan")
    monkeypatch.setenv("HPCSIZER_HARVEST_RETRY_DELAY", "0")
    monkeypatch.setenv("HPCSIZER_TMPDIR", str(tmp_path))
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    return folder


@pytest.fixture
def cfg(root, stubs):
    return load_config()
