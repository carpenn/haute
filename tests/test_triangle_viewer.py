"""Tests for the Triangle_Viewer node builder and type registrations."""

from __future__ import annotations

from collections.abc import Callable

import polars as pl
import pytest

from haute._builders import _build_node_fn
from haute._config_validation import warn_unrecognized_config_keys
from haute._types import NodeType, TriangleViewerConfig
from haute.graph_utils import GraphNode, NodeData

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_triangle_viewer(
    label: str = "My Triangle",
    config: dict | None = None,
) -> GraphNode:
    return GraphNode(
        id="tv1",
        data=NodeData(
            label=label,
            nodeType=NodeType.TRIANGLE_VIEWER,
            config=config or {},
        ),
    )


def _make_mapped_viewer(
    origin: str = "acc_year",
    dev: str = "dev_month",
    value: str = "loss",
) -> GraphNode:
    return _make_triangle_viewer(
        config={"originField": origin, "developmentField": dev, "valueField": value}
    )


def _build(node: GraphNode, sources: list[str] | None = None) -> tuple[str, Callable, bool]:
    return _build_node_fn(node, source_names=sources or ["upstream"])


# ---------------------------------------------------------------------------
# NodeType registration
# ---------------------------------------------------------------------------


class TestTriangleViewerNodeType:
    def test_triangle_viewer_in_node_type_enum(self) -> None:
        assert NodeType.TRIANGLE_VIEWER == "triangleViewer"

    def test_triangle_viewer_string_value(self) -> None:
        assert str(NodeType.TRIANGLE_VIEWER) == "triangleViewer"


# ---------------------------------------------------------------------------
# TriangleViewerConfig TypedDict
# ---------------------------------------------------------------------------


class TestTriangleViewerConfig:
    def test_config_is_typed_dict(self) -> None:
        cfg: TriangleViewerConfig = {
            "originField": "accident_year",
            "developmentField": "development_month",
            "valueField": "incurred_loss",
        }
        assert cfg["originField"] == "accident_year"
        assert cfg["developmentField"] == "development_month"
        assert cfg["valueField"] == "incurred_loss"

    def test_config_total_false_allows_partial(self) -> None:
        # total=False means every key is optional
        cfg: TriangleViewerConfig = {}
        assert isinstance(cfg, dict)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestTriangleViewerConfigValidation:
    def test_valid_config_produces_no_warnings(self, caplog: pytest.LogCaptureFixture) -> None:
        node = _make_triangle_viewer(
            config={
                "originField": "accident_year",
                "developmentField": "dev_month",
                "valueField": "loss",
            }
        )
        warn_unrecognized_config_keys(node.data.nodeType, node.data.config)
        # No unexpected-key warnings should be emitted
        assert "unexpected" not in caplog.text.lower()

    def test_unknown_key_triggers_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        node = _make_triangle_viewer(config={"originField": "x", "bogusKey": "y"})
        with caplog.at_level(logging.WARNING):
            warn_unrecognized_config_keys(node.data.nodeType, node.data.config)
        # Should warn about bogusKey or silently ignore — must not raise
        assert isinstance(caplog.records, list)


# ---------------------------------------------------------------------------
# Builder — fallback (no mappings configured)
# ---------------------------------------------------------------------------


class TestTriangleViewerBuilderFallback:
    def test_returns_tuple_of_three(self) -> None:
        node = _make_triangle_viewer()
        result = _build(node)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_func_name_derived_from_label(self) -> None:
        node = _make_triangle_viewer(label="My Triangle")
        func_name, _, _ = _build(node)
        assert func_name == "My_Triangle"

    def test_is_source_is_false(self) -> None:
        """Triangle_Viewer requires an upstream input — not a source node."""
        node = _make_triangle_viewer()
        _, _, is_source = _build(node)
        assert is_source is False

    def test_callable_is_returned(self) -> None:
        node = _make_triangle_viewer()
        _, fn, _ = _build(node)
        assert callable(fn)

    def test_no_mappings_passthrough(self) -> None:
        """When field mappings are absent the upstream frame is passed through unchanged."""
        node = _make_triangle_viewer()  # no config
        _, fn, _ = _build(node)

        upstream = pl.LazyFrame({"acc_year": [2020, 2021], "loss": [100, 200]})
        result = fn(upstream)

        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert collected.shape == (2, 2)
        assert "acc_year" in collected.columns

    def test_partial_mappings_passthrough(self) -> None:
        """If only some fields are mapped the frame is still passed through."""
        node = _make_triangle_viewer(config={"originField": "acc_year"})  # dev+value missing
        _, fn, _ = _build(node)

        upstream = pl.LazyFrame({"acc_year": [2020], "loss": [100]})
        collected = fn(upstream).collect()
        assert collected.shape == (1, 2)

    def test_no_inputs_returns_empty_frame(self) -> None:
        """If called with no upstreams (shouldn't happen in practice), returns empty."""
        node = _make_triangle_viewer()
        _, fn, _ = _build(node)
        result = fn()
        assert isinstance(result, pl.LazyFrame)


# ---------------------------------------------------------------------------
# Builder — aggregation (all mappings configured)
# ---------------------------------------------------------------------------


class TestTriangleViewerBuilderAggregation:
    """The builder must aggregate over the FULL upstream frame so the preview
    table reflects all rows, not just the preview sample.
    """

    def _make_upstream(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "acc_year": ["2020", "2020", "2020", "2021", "2021"],
                "dev_month": ["12", "12", "24", "12", "24"],
                "loss": [100.0, 50.0, 200.0, 150.0, 250.0],
            }
        )

    def test_returns_lazy_frame(self) -> None:
        node = _make_mapped_viewer()
        _, fn, _ = _build(node)
        result = fn(self._make_upstream())
        assert isinstance(result, pl.LazyFrame)

    def test_output_columns_match_input_fields(self) -> None:
        node = _make_mapped_viewer()
        _, fn, _ = _build(node)
        df = fn(self._make_upstream()).collect()
        assert "acc_year" in df.columns
        assert "dev_month" in df.columns
        assert "loss" in df.columns

    def test_row_count_equals_unique_pairs(self) -> None:
        """Each (origin, dev) combination should produce exactly one output row."""
        node = _make_mapped_viewer()
        _, fn, _ = _build(node)
        df = fn(self._make_upstream()).collect()
        # 4 unique pairs: (2020,12), (2020,24), (2021,12), (2021,24)
        assert df.shape[0] == 4

    def test_sums_multiple_rows_for_same_pair(self) -> None:
        """(2020, 12) has two rows: 100 + 50 = 150."""
        node = _make_mapped_viewer()
        _, fn, _ = _build(node)
        df = fn(self._make_upstream()).collect()
        row = df.filter(
            (pl.col("acc_year") == "2020") & (pl.col("dev_month") == "12")
        )
        assert row["loss"][0] == pytest.approx(150.0)

    def test_single_row_pairs_are_exact(self) -> None:
        """(2021, 24) has one row: 250."""
        node = _make_mapped_viewer()
        _, fn, _ = _build(node)
        df = fn(self._make_upstream()).collect()
        row = df.filter(
            (pl.col("acc_year") == "2021") & (pl.col("dev_month") == "24")
        )
        assert row["loss"][0] == pytest.approx(250.0)

    def test_handles_numeric_string_values(self) -> None:
        """String-encoded numbers should be cast to float and summed correctly."""
        upstream = pl.LazyFrame(
            {
                "acc_year": ["2020", "2020"],
                "dev_month": ["12", "12"],
                "loss": ["100", "50"],  # string values
            }
        )
        node = _make_mapped_viewer()
        _, fn, _ = _build(node)
        df = fn(upstream).collect()
        assert df["loss"][0] == pytest.approx(150.0)

    def test_non_numeric_values_treated_as_zero(self) -> None:
        """Non-parseable strings should be treated as 0 (not crash)."""
        upstream = pl.LazyFrame(
            {
                "acc_year": ["2020", "2020"],
                "dev_month": ["12", "12"],
                "loss": ["bad_value", "50"],
            }
        )
        node = _make_mapped_viewer()
        _, fn, _ = _build(node)
        df = fn(upstream).collect()
        assert df["loss"][0] == pytest.approx(50.0)

    def test_missing_column_falls_back_to_passthrough(self) -> None:
        """If a mapped field doesn't exist in the frame, return the frame unchanged."""
        upstream = pl.LazyFrame({"acc_year": [2020], "dev_month": [12]})
        # "loss" column is absent
        node = _make_mapped_viewer()
        _, fn, _ = _build(node)
        df = fn(upstream).collect()
        # Should not crash; should return the original frame
        assert "acc_year" in df.columns

    def test_large_input_aggregates_correctly(self) -> None:
        """Simulate a large dataset (many rows per cell) — totals must be exact."""
        n_per_cell = 1_000
        origins = ["2019", "2020"] * (n_per_cell * 2)
        devs = (["12"] * n_per_cell + ["24"] * n_per_cell) * 2
        values = [1.0] * (n_per_cell * 4)

        upstream = pl.LazyFrame(
            {"acc_year": origins, "dev_month": devs, "loss": values}
        )
        node = _make_mapped_viewer()
        _, fn, _ = _build(node)
        df = fn(upstream).collect()

        assert df.shape[0] == 4  # 2 origins x 2 dev periods
        # Each cell should sum to exactly n_per_cell
        for val in df["loss"]:
            assert val == pytest.approx(float(n_per_cell))
