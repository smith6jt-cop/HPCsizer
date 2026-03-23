"""Tests for lib/plotter.py"""

import csv
import gzip
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.plotter import load_timeseries, plot_job


def _write_ts(path: str, rows: list) -> None:
    fieldnames = ["elapsed_sec", "rss_gb", "cpu_frac", "io_read_mb_s", "threads", "numa_miss_rate"]
    with gzip.open(path, "wt", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def sample_ts(tmp_path):
    rows = [
        {
            "elapsed_sec": i * 30,
            "rss_gb": 10.0 + i * 0.1,
            "cpu_frac": 0.6,
            "io_read_mb_s": 5.0,
            "threads": 4,
            "numa_miss_rate": 0.01,
        }
        for i in range(50)
    ]
    ts_path = str(tmp_path / "12345.csv.gz")
    _write_ts(ts_path, rows)
    return tmp_path, rows


class TestLoadTimeseries:
    def test_load_returns_correct_count(self, sample_ts):
        ts_dir, rows = sample_ts
        loaded = load_timeseries(str(ts_dir / "12345.csv.gz"))
        assert len(loaded) == len(rows)

    def test_load_parses_floats(self, sample_ts):
        ts_dir, _ = sample_ts
        loaded = load_timeseries(str(ts_dir / "12345.csv.gz"))
        assert isinstance(loaded[0]["rss_gb"], float)
        assert isinstance(loaded[0]["elapsed_sec"], float)

    def test_none_for_empty_values(self, tmp_path):
        rows = [
            {
                "elapsed_sec": 0,
                "rss_gb": "",
                "cpu_frac": 0.5,
                "io_read_mb_s": 0,
                "threads": 4,
                "numa_miss_rate": 0,
            }
        ]
        ts_path = str(tmp_path / "empty_val.csv.gz")
        _write_ts(ts_path, rows)
        loaded = load_timeseries(ts_path)
        assert loaded[0]["rss_gb"] is None


class TestPlotJob:
    def test_plot_creates_png(self, sample_ts):
        ts_dir, _ = sample_ts
        out_dir = str(ts_dir / "plots")
        result = plot_job("12345", str(ts_dir), out_dir)
        assert result is not None
        assert os.path.exists(result)
        assert result.endswith(".png")

    def test_plot_returns_none_when_no_ts(self, tmp_path):
        result = plot_job("99999", str(tmp_path), str(tmp_path / "plots"))
        assert result is None

    def test_plot_with_job_meta(self, sample_ts):
        ts_dir, _ = sample_ts
        out_dir = str(ts_dir / "plots2")
        meta = {"user": "alice", "job_name": "seurat_test", "req_mem_gb": 64.0}
        result = plot_job("12345", str(ts_dir), out_dir, job_meta=meta)
        assert result is not None
