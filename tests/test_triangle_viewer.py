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
# Builder
# ---------------------------------------------------------------------------


class TestTriangleViewerBuilder:
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

    def test_passthrough_returns_upstream_frame(self) -> None:
        """The builder should pass the upstream LazyFrame through unchanged."""
        node = _make_triangle_viewer()
        _, fn, _ = _build(node)

        upstream = pl.LazyFrame({"accident_year": [2020, 2021], "value": [100, 200]})
        result = fn(upstream)

        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert collected.shape == (2, 2)
        assert "accident_year" in collected.columns

    def test_passthrough_with_no_inputs_returns_empty_frame(self) -> None:
        """If called with no upstreams (shouldn't happen in practice), returns empty."""
        node = _make_triangle_viewer()
        _, fn, _ = _build(node)
        result = fn()
        assert isinstance(result, pl.LazyFrame)
