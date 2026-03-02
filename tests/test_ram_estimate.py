"""Tests for haute._ram_estimate — RAM estimation and safe downsampling."""

from __future__ import annotations

from unittest.mock import patch

import polars as pl

from haute._ram_estimate import (
    _csv_row_count,
    _parquet_row_count,
    available_ram_bytes,
    available_vram_bytes,
    estimate_gpu_vram_bytes,
    estimate_safe_training_rows,
    estimate_source_rows,
)
from haute.graph_utils import GraphEdge, GraphNode, NodeData, PipelineGraph

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source_node(
    node_id: str = "src1",
    label: str = "quotes",
    node_type: str = "apiInput",
    config: dict | None = None,
) -> GraphNode:
    return GraphNode(
        id=node_id,
        type="custom",
        position={"x": 0, "y": 0},
        data=NodeData(
            label=label,
            nodeType=node_type,
            config=config or {},
        ),
    )


def _make_transform_node(
    node_id: str = "t1",
    label: str = "features",
    config: dict | None = None,
) -> GraphNode:
    return GraphNode(
        id=node_id,
        type="custom",
        position={"x": 0, "y": 100},
        data=NodeData(
            label=label,
            nodeType="transform",
            config=config or {},
        ),
    )


def _make_modelling_node(
    node_id: str = "m1",
    label: str = "model",
    config: dict | None = None,
) -> GraphNode:
    return GraphNode(
        id=node_id,
        type="custom",
        position={"x": 0, "y": 200},
        data=NodeData(
            label=label,
            nodeType="modelling",
            config=config or {},
        ),
    )


# ---------------------------------------------------------------------------
# available_ram_bytes
# ---------------------------------------------------------------------------


class TestAvailableRam:
    def test_returns_positive_int(self) -> None:
        ram = available_ram_bytes()
        assert isinstance(ram, int)
        assert ram > 0

    def test_returns_reasonable_value(self) -> None:
        """Should be at least 100 MB on any modern system."""
        ram = available_ram_bytes()
        assert ram > 100 * 1024 * 1024

    def test_fallback_when_proc_unavailable(self) -> None:
        """If /proc/meminfo is not readable, falls back gracefully."""
        with patch("builtins.open", side_effect=OSError):
            ram = available_ram_bytes()
            assert ram > 0


# ---------------------------------------------------------------------------
# Parquet/CSV row counts
# ---------------------------------------------------------------------------


class TestRowCounts:
    def test_parquet_row_count(self, tmp_path) -> None:
        path = tmp_path / "test.parquet"
        df = pl.DataFrame({"a": range(500), "b": range(500)})
        df.write_parquet(str(path))
        assert _parquet_row_count(str(path)) == 500

    def test_csv_row_count(self, tmp_path) -> None:
        path = tmp_path / "test.csv"
        df = pl.DataFrame({"x": range(123)})
        df.write_csv(str(path))
        assert _csv_row_count(str(path)) == 123


# ---------------------------------------------------------------------------
# estimate_source_rows
# ---------------------------------------------------------------------------


class TestEstimateSourceRows:
    def test_parquet_datasource(self, tmp_path) -> None:
        path = tmp_path / "data.parquet"
        pl.DataFrame({"a": range(1000)}).write_parquet(str(path))

        node = _make_source_node(
            node_type="dataSource",
            config={"path": str(path), "sourceType": "flat_file"},
        )
        graph = PipelineGraph(nodes=[node], edges=[])
        assert estimate_source_rows(graph) == 1000

    def test_returns_none_for_databricks(self) -> None:
        node = _make_source_node(
            node_type="dataSource",
            config={"sourceType": "databricks", "table": "cat.schema.tbl"},
        )
        graph = PipelineGraph(nodes=[node], edges=[])
        assert estimate_source_rows(graph) is None

    def test_returns_none_for_missing_file(self) -> None:
        node = _make_source_node(
            node_type="dataSource",
            config={"path": "/nonexistent/file.parquet", "sourceType": "flat_file"},
        )
        graph = PipelineGraph(nodes=[node], edges=[])
        assert estimate_source_rows(graph) is None

    def test_max_across_multiple_sources(self, tmp_path) -> None:
        p1 = tmp_path / "small.parquet"
        pl.DataFrame({"a": range(100)}).write_parquet(str(p1))
        p2 = tmp_path / "big.parquet"
        pl.DataFrame({"a": range(5000)}).write_parquet(str(p2))

        n1 = _make_source_node(
            node_id="s1", label="small",
            node_type="dataSource",
            config={"path": str(p1), "sourceType": "flat_file"},
        )
        n2 = _make_source_node(
            node_id="s2", label="big",
            node_type="dataSource",
            config={"path": str(p2), "sourceType": "flat_file"},
        )
        graph = PipelineGraph(nodes=[n1, n2], edges=[])
        assert estimate_source_rows(graph) == 5000


# ---------------------------------------------------------------------------
# estimate_safe_training_rows — integration
# ---------------------------------------------------------------------------


def _build_dummy_node_fn(node, *, source_names=None, row_limit=None,
                         node_map=None, orig_source_names=None,
                         preamble_ns=None, scenario="live", **_kwargs):
    """Minimal build_node_fn that creates a dummy source or passthrough."""
    label = node.data.label
    nt = node.data.nodeType

    if nt in ("dataSource", "apiInput"):
        n_rows = row_limit or 10_000

        def source_fn():
            return pl.LazyFrame({
                "a": range(n_rows),
                "b": [f"val_{i}" for i in range(n_rows)],
                "c": [float(i) for i in range(n_rows)],
            })

        return label, source_fn, True

    def transform_fn(*inputs):
        return inputs[0] if inputs else pl.LazyFrame()

    return label, transform_fn, False


class TestEstimateSafeTrainingRows:
    def _make_graph(self) -> PipelineGraph:
        src = _make_source_node(
            config={"path": "data/test.parquet", "sourceType": "flat_file"},
        )
        target = _make_modelling_node()
        edge = GraphEdge(id="e1", source=src.id, target=target.id)
        return PipelineGraph(nodes=[src, target], edges=[edge])

    def test_no_downsample_when_ram_sufficient(self, tmp_path) -> None:
        # Create a small parquet file
        path = tmp_path / "test.parquet"
        pl.DataFrame({"a": range(100)}).write_parquet(str(path))

        src = _make_source_node(
            node_type="dataSource",
            config={"path": str(path), "sourceType": "flat_file"},
        )
        target = _make_modelling_node()
        edge = GraphEdge(id="e1", source=src.id, target=target.id)
        graph = PipelineGraph(nodes=[src, target], edges=[edge])

        result = estimate_safe_training_rows(
            graph, target.id, _build_dummy_node_fn,
        )
        assert not result.was_downsampled
        assert result.safe_row_limit is None
        assert result.warning is None
        assert result.bytes_per_row > 0

    def test_downsample_when_ram_insufficient(self, tmp_path) -> None:
        # Create a source file claiming many rows
        path = tmp_path / "big.parquet"
        pl.DataFrame({"a": range(1000)}).write_parquet(str(path))

        src = _make_source_node(
            node_type="dataSource",
            config={"path": str(path), "sourceType": "flat_file"},
        )
        target = _make_modelling_node()
        edge = GraphEdge(id="e1", source=src.id, target=target.id)
        graph = PipelineGraph(nodes=[src, target], edges=[edge])

        # Pretend we only have 1 KB of RAM
        with patch("haute._ram_estimate.available_ram_bytes", return_value=1024):
            result = estimate_safe_training_rows(
                graph, target.id, _build_dummy_node_fn,
            )
        assert result.was_downsampled
        assert result.safe_row_limit is not None
        assert result.safe_row_limit < 1000
        assert result.warning is not None
        assert "downsampled" in result.warning.lower()
        assert result.total_rows == 1000

    def test_returns_no_limit_when_source_rows_unknown(self) -> None:
        # Databricks source — no row count available
        src = _make_source_node(
            node_type="dataSource",
            config={"sourceType": "databricks", "table": "cat.schema.tbl"},
        )
        target = _make_modelling_node()
        edge = GraphEdge(id="e1", source=src.id, target=target.id)
        graph = PipelineGraph(nodes=[src, target], edges=[edge])

        result = estimate_safe_training_rows(
            graph, target.id, _build_dummy_node_fn,
        )
        assert not result.was_downsampled
        assert result.safe_row_limit is None
        assert result.warning is None

    def test_warning_includes_row_counts(self, tmp_path) -> None:
        path = tmp_path / "medium.parquet"
        pl.DataFrame({"a": range(50_000)}).write_parquet(str(path))

        src = _make_source_node(
            node_type="dataSource",
            config={"path": str(path), "sourceType": "flat_file"},
        )
        target = _make_modelling_node()
        edge = GraphEdge(id="e1", source=src.id, target=target.id)
        graph = PipelineGraph(nodes=[src, target], edges=[edge])

        # Very low RAM to force downsampling
        with patch("haute._ram_estimate.available_ram_bytes", return_value=5000):
            result = estimate_safe_training_rows(
                graph, target.id, _build_dummy_node_fn,
            )
        assert result.was_downsampled
        assert "50,000" in result.warning
        assert result.total_rows == 50_000

    def test_safe_row_limit_respects_minimum(self, tmp_path) -> None:
        path = tmp_path / "tiny_ram.parquet"
        pl.DataFrame({"a": range(10_000)}).write_parquet(str(path))

        src = _make_source_node(
            node_type="dataSource",
            config={"path": str(path), "sourceType": "flat_file"},
        )
        target = _make_modelling_node()
        edge = GraphEdge(id="e1", source=src.id, target=target.id)
        graph = PipelineGraph(nodes=[src, target], edges=[edge])

        # Absurdly low RAM — should clamp to _MIN_PROBE_ROWS (500)
        with patch("haute._ram_estimate.available_ram_bytes", return_value=1):
            result = estimate_safe_training_rows(
                graph, target.id, _build_dummy_node_fn,
            )
        assert result.was_downsampled
        assert result.safe_row_limit >= 500

    def test_probe_columns_populated(self, tmp_path) -> None:
        """RamEstimate should include the probe column count."""
        path = tmp_path / "cols.parquet"
        pl.DataFrame({"a": range(100), "b": range(100)}).write_parquet(str(path))

        src = _make_source_node(
            node_type="dataSource",
            config={"path": str(path), "sourceType": "flat_file"},
        )
        target = _make_modelling_node()
        edge = GraphEdge(id="e1", source=src.id, target=target.id)
        graph = PipelineGraph(nodes=[src, target], edges=[edge])

        result = estimate_safe_training_rows(
            graph, target.id, _build_dummy_node_fn,
        )
        # Dummy source produces 3 columns (a, b, c)
        assert result.probe_columns == 3


# ---------------------------------------------------------------------------
# GPU VRAM estimation
# ---------------------------------------------------------------------------


class TestAvailableVram:
    def test_returns_int_or_none(self) -> None:
        result = available_vram_bytes()
        assert result is None or (isinstance(result, int) and result > 0)

    def test_returns_none_when_nvidia_smi_missing(self) -> None:
        with patch("subprocess.run", side_effect=FileNotFoundError):
            assert available_vram_bytes() is None


class TestEstimateGpuVram:
    def test_returns_positive_int(self) -> None:
        result = estimate_gpu_vram_bytes(1000, 50)
        assert isinstance(result, int)
        assert result > 0

    def test_scales_with_rows(self) -> None:
        # Use large row counts where data dominates over constant histogram cost
        small = estimate_gpu_vram_bytes(100_000, 100)
        large = estimate_gpu_vram_bytes(1_000_000, 100)
        assert large > small
        # Should scale roughly linearly with rows (histograms are constant)
        assert large / small > 5

    def test_scales_with_features(self) -> None:
        few = estimate_gpu_vram_bytes(10_000, 10)
        many = estimate_gpu_vram_bytes(10_000, 100)
        assert many > few

    def test_realistic_estimate_8gb_gpu(self) -> None:
        """10M rows × 100 features should exceed 8 GB VRAM."""
        estimate = estimate_gpu_vram_bytes(10_000_000, 100)
        eight_gb = 8 * 1024**3
        assert estimate > eight_gb, (
            f"Expected >8 GB for 10M×100, got {estimate / 1024**3:.1f} GB"
        )

    def test_small_dataset_fits_in_8gb(self) -> None:
        """100K rows × 50 features should fit in 8 GB."""
        estimate = estimate_gpu_vram_bytes(100_000, 50)
        eight_gb = 8 * 1024**3
        assert estimate < eight_gb, (
            f"Expected <8 GB for 100K×50, got {estimate / 1024**3:.1f} GB"
        )

    def test_depth_affects_estimate(self) -> None:
        shallow = estimate_gpu_vram_bytes(1_000_000, 100, depth=4)
        deep = estimate_gpu_vram_bytes(1_000_000, 100, depth=8)
        assert deep > shallow

    def test_border_count_affects_estimate(self) -> None:
        low = estimate_gpu_vram_bytes(1_000_000, 100, border_count=32)
        high = estimate_gpu_vram_bytes(1_000_000, 100, border_count=254)
        assert high > low
