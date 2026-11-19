"""A job's name and command line are written by whoever submits it.

Every text report shows them to other people -- the owner of the checkout, the
whole group -- so nothing in them may reach a terminal as a control sequence,
break a table, or add a line to the commands 'hpg queue' prints to be copied.
"""

import csv
import io
import json
import time

import pytest
from sacct_fixture import job_row, simple_job

from lib import cli

HOUR = 3600
NAME = "evil\x1b[2J\nscancel -u $USER #"
LINE = "sbatch --wrap=x\x1b]0;owned\x07 =cmd"
SHOWN = "evil\\x1b[2J\\x0ascancel -u $USER #"


@pytest.fixture
def harvested(root, stubs, capsys):
    now = int(time.time())
    rows = []
    for i in range(6):
        rows += simple_job(
            100 + i,
            user="mallory",
            name=NAME,
            submit_line=LINE,
            mem="64G",
            cpus=16,
            timelimit=2880,
            elapsed=3600,
            submit=now - (6 - i) * 12 * HOUR,
            batch_max_rss="10485760K",
            batch_total_cpu="03:00:00",
        )
    stubs.sacct_default(rows)
    assert cli.main(["harvest"]) == 0
    capsys.readouterr()
    return stubs


def run(capsys, *argv, code=0):
    assert cli.main(list(argv)) == code
    return capsys.readouterr().out


def assert_harmless(text):
    assert "\x1b" not in text and "\x07" not in text and "\r" not in text
    assert not any(line.lstrip().startswith("scancel") for line in text.splitlines())


@pytest.mark.parametrize(
    "argv",
    [
        ("report",),
        ("report", "--format", "md"),
        ("families",),
        ("history", "--user", "mallory"),
        ("flags", "--jobs"),
        ("job", "105"),
        ("recommend", "--like", "105"),
        ("backtest",),
    ],
    ids=lambda argv: " ".join(argv),
)
def test_text_reports_show_the_name_without_obeying_it(harvested, capsys, argv):
    out = run(capsys, *argv)
    assert_harmless(out)
    if argv[0] in ("job", "recommend"):
        assert SHOWN in out


def test_family_can_still_be_asked_for_by_the_name_it_is_shown_under(harvested, capsys):
    families = run(capsys, "families")
    family = next(line.split()[0] for line in families.splitlines() if "mallory/" in line)
    out = run(capsys, "history", family, "--user", "mallory")
    assert_harmless(out)
    assert "6 runs" in out or "100" in out


def test_csv_has_one_line_per_job_and_no_formula(harvested, capsys):
    out = run(capsys, "export", "--format", "csv")
    assert_harmless(out)
    lines = out.splitlines()
    assert len(lines) == 7, "a header and six jobs, whatever their names contain"
    rows = list(csv.DictReader(io.StringIO(out)))
    assert rows[0]["job_name"] == SHOWN
    assert not rows[0]["submit_line"].startswith(("=", "+", "-", "@"))
    out = run(capsys, "report", "--format", "csv")
    assert_harmless(out)
    assert len(out.splitlines()) == 2


def test_json_stays_valid(harvested, capsys):
    for argv in (("export", "--format", "json"), ("report", "--format", "json")):
        out = run(capsys, *argv)
        assert_harmless(out)
        json.loads(out)


def test_queue_prints_nothing_but_its_own_commands(harvested, capsys):
    now = int(time.time())
    pending = job_row(
        200,
        user="mallory",
        name=NAME,
        submit_line=LINE,
        mem="64G",
        cpus=16,
        timelimit=2880,
        submit=now - 60,
        state="PENDING",
        started=False,
        ended=False,
    )
    harvested.sacct_default([pending])
    out = run(capsys, "queue")
    assert_harmless(out)
    commands = [line for line in out.splitlines() if "scontrol update" in line]
    assert commands and all(line.startswith("  scontrol update JobId=200 ") for line in commands)
