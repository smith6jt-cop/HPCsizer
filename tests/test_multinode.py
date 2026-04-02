"""Tests for multi-node aggregation logic in finalize.py"""

import csv
import gzip
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from bin.finalize import (
    _compute_node_imbalance,
    _load_multinode_timeseries,
    _merge_multinode_timeseries,
)


def _write_node_ts(path: str, rows: list) -> None:
    fieldnames = [
        "elapsed_sec",
        "rss_gb",
        "hwm_gb",
        "swap_gb",
        "threads",
        "utime",
        "stime",
        "majflt",
        "io_read_mb_s",
        "io_write_mb_s",
        "pgmajfault",
        "cpu_frac",
        "numa_miss_rate",
        "lustre_read_mb_s",
        "lustre_write_mb_s",
        "lustre_metadata_ops_s",
        "lustre_open_count",
    ]
    with gzip.open(path, "wt", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _make_node_rows(n=20, cpu_frac=0.5, rss_gb=10.0):
    return [
        {
            "elapsed_sec": i * 30,
            "rss_gb": rss_gb,
            "hwm_gb": rss_gb + 1.0,
            "swap_gb": 0.0,
            "threads": 4,
            "utime": i * 100,
            "stime": i * 10,
            "majflt": 0,
            "io_read_mb_s": 5.0,
            "io_write_mb_s": 1.0,
            "pgmajfault": 0,
            "cpu_frac": cpu_frac,
            "numa_miss_rate": 0.01,
            "lustre_read_mb_s": 10.0,
            "lustre_write_mb_s": 2.0,
            "lustre_metadata_ops_s": 5.0,
            "lustre_open_count": i * 10,
        }
        for i in range(n)
    ]


class TestLoadMultinodeTimeseries:
    def test_finds_node_files(self, tmp_path):
        for hostname in ["c0101a-s1", "c0102a-s1"]:
            path = str(tmp_path / f"12345.node_{hostname}.csv.gz")
            _write_node_ts(path, _make_node_rows(n=5))
        result = _load_multinode_timeseries(str(tmp_path), "12345")
        assert len(result) == 2
        assert "c0101a-s1" in result
        assert "c0102a-s1" in result

    def test_returns_empty_when_no_files(self, tmp_path):
        result = _load_multinode_timeseries(str(tmp_path), "99999")
        assert result == {}

    def test_skips_corrupt_files(self, tmp_path):
        good_path = str(tmp_path / "12345.node_good.csv.gz")
        _write_node_ts(good_path, _make_node_rows(n=5))
        bad_path = str(tmp_path / "12345.node_bad.csv.gz")
        with open(bad_path, "w") as fh:
            fh.write("not a gzip file")
        result = _load_multinode_timeseries(str(tmp_path), "12345")
        assert "good" in result
        assert len(result) == 1


class TestComputeNodeImbalance:
    def test_balanced_nodes(self):
        node_data = {
            "node1": [{"cpu_frac": 0.5}] * 20,
            "node2": [{"cpu_frac": 0.5}] * 20,
            "node3": [{"cpu_frac": 0.5}] * 20,
        }
        result = _compute_node_imbalance(node_data)
        assert result["node_imbalance_cv"] == pytest.approx(0.0, abs=0.01)

    def test_imbalanced_nodes(self):
        node_data = {
            "node1": [{"cpu_frac": 0.9}] * 20,
            "node2": [{"cpu_frac": 0.0}] * 20,
            "node3": [{"cpu_frac": 0.0}] * 20,
        }
        result = _compute_node_imbalance(node_data)
        assert result["node_imbalance_cv"] is not None
        assert result["node_imbalance_cv"] > 1.0

    def test_single_node_returns_none(self):
        node_data = {"node1": [{"cpu_frac": 0.5}] * 20}
        result = _compute_node_imbalance(node_data)
        assert result["node_imbalance_cv"] is None


class TestMergeMultinodeTimeseries:
    def test_sums_rss_across_nodes(self):
        node_data = {
            "node1": _make_node_rows(n=5, rss_gb=10.0),
            "node2": _make_node_rows(n=5, rss_gb=20.0),
        }
        merged = _merge_multinode_timeseries(node_data)
        assert len(merged) == 5
        assert merged[0]["rss_gb"] == pytest.approx(30.0)

    def test_averages_cpu_frac(self):
        node_data = {
            "node1": _make_node_rows(n=5, cpu_frac=0.8),
            "node2": _make_node_rows(n=5, cpu_frac=0.2),
        }
        merged = _merge_multinode_timeseries(node_data)
        assert merged[0]["cpu_frac"] == pytest.approx(0.5)

    def test_single_node_passthrough(self):
        rows = _make_node_rows(n=5)
        node_data = {"node1": rows}
        merged = _merge_multinode_timeseries(node_data)
        assert len(merged) == 5
        assert merged[0]["rss_gb"] == rows[0]["rss_gb"]

    def test_empty_returns_empty(self):
        assert _merge_multinode_timeseries({}) == []
