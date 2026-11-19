"""Tests for lib/plotter.py and ``hpg plot``.

matplotlib is optional: the tests that draw are skipped without it, the ones
about what happens when it is missing always run.
"""

import importlib.util
import os
import pwd
import time

import pytest
from sacct_fixture import simple_job
from test_profile import series_text, steady

from lib import cli, collect, plotter, profile

HAVE_MATPLOTLIB = importlib.util.find_spec("matplotlib") is not None
needs_matplotlib = pytest.mark.skipif(not HAVE_MATPLOTLIB, reason="matplotlib is not installed")
ME = pwd.getpwuid(os.getuid()).pw_name
JOB = {"job_id": "4242", "job_name": "fit", "user": "alice", "req_mem_mb": 16384, "alloc_cpus": 8}


def one_node(node="c1", **kwargs):
    return profile.parse_series(series_text(steady(30, **kwargs), node=node))


class TestPlot:
    @needs_matplotlib
    def test_png_for_one_node(self, tmp_path):
        out = plotter.plot_job(JOB, [one_node()], str(tmp_path / "job.png"))
        assert os.path.getsize(out) > 5000
        with open(out, "rb") as handle:
            assert handle.read(8) == b"\x89PNG\r\n\x1a\n"

    @needs_matplotlib
    def test_several_nodes_and_a_gpu(self, tmp_path):
        samples = [
            dict(s, gpu_util=50 + i, rd=i * 10**8, wr=i * 10**7) for i, s in enumerate(steady(20))
        ]
        a = profile.parse_series(series_text(samples, node="a"))
        b = profile.parse_series(series_text(samples, node="b"))
        out = plotter.plot_job(dict(JOB, req_mem_mb=None), [a, b], str(tmp_path / "multi.png"))
        assert os.path.getsize(out) > 5000

    @needs_matplotlib
    def test_series_without_samples(self, tmp_path):
        with pytest.raises(plotter.PlotError, match="no samples"):
            plotter.plot_job(JOB, [({}, [])], str(tmp_path / "x.png"))

    def test_without_matplotlib_the_reason_is_given(self, tmp_path, monkeypatch):
        monkeypatch.setattr(plotter, "plt", None)
        assert plotter.available() is False
        with pytest.raises(plotter.PlotError, match="needs matplotlib"):
            plotter.plot_job(JOB, [one_node()], str(tmp_path / "x.png"))

    def test_load_names_each_series_by_its_node(self, tmp_path):
        path = tmp_path / "4242-1.c7.tsv"
        path.write_text(series_text(steady(3), node="c7"))
        ((meta, rows),) = plotter.load([str(path)])
        assert meta["node"] == "c7" and len(rows) == 3
        assert plotter.default_name({"job_id": "12_3"}) == "hpg-job-12_3.png"


class TestCommand:
    @pytest.fixture
    def profiled(self, cfg, stubs, capsys):
        now = int(time.time())
        stubs.sacct_default(
            simple_job(4242, user=ME, submit=now - 3600, elapsed=290)
            + simple_job(4243, user=ME, submit=now - 3600)
        )
        os.makedirs(cfg.spool_dir)
        with open(os.path.join(cfg.spool_dir, "4242.c1.tsv"), "w") as handle:
            handle.write(series_text(steady(30), start=now - 3570))
        collect.harvest(cfg, now=now, log=lambda _m: None, ingest=profile.ingest_spool)
        capsys.readouterr()
        return cfg

    def test_job_without_a_profile(self, profiled, capsys):
        assert cli.main(["plot", "4243"]) == 3
        assert "has no profile" in capsys.readouterr().err
        assert cli.main(["plot", "999"]) == 3

    def test_missing_matplotlib_names_the_series_files(self, profiled, capsys, monkeypatch):
        monkeypatch.setattr(plotter, "plt", None)
        assert cli.main(["plot", "4242"]) == 1
        err = capsys.readouterr().err
        assert "needs matplotlib" in err and ".c1.tsv.gz" in err

    @needs_matplotlib
    def test_writes_the_png(self, profiled, capsys, tmp_path):
        target = tmp_path / "out.png"
        assert cli.main(["plot", "4242", "--out", str(target)]) == 0
        assert capsys.readouterr().out.strip() == str(target) and target.stat().st_size > 5000

    def test_job_view_points_to_the_plot(self, profiled, capsys):
        assert cli.main(["job", "--no-refresh", "4242"]) == 0
        out = capsys.readouterr().out
        assert "Profile (in-job sampler, cgroup-v2)" in out and "hpg plot 4242" in out
        assert "peak; profile" in out
