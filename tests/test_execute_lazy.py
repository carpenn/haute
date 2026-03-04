"""Comprehensive tests for haute._execute_lazy.

Covers:
  - _prune_live_switch_edges  — scenario-based edge pruning
  - _prepare_graph            — topo sort, parent building, id_to_name
  - _execute_lazy             — lazy execution path
  - _build_funcs              — function building for eager execution
  - _execute_eager_core       — eager execution with swallow_errors, timings, memory
  - EagerResult               — named tuple structure
"""

from __future__ import annotations

import polars as pl
import pytest

from haute._execute_lazy import (
    EagerResult,
    _build_funcs,
    _execute_eager_core,
    _execute_lazy,
    _extract_error_line,
    _prepare_graph,
    _prune_live_switch_edges,
)
from haute._types import (
    GraphEdge,
    GraphNode,
    NodeData,
    NodeType,
    PipelineGraph,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _e(src: str, tgt: str) -> GraphEdge:
    return GraphEdge(id=f"e_{src}_{tgt}", source=src, target=tgt)


def _source_node(nid: str, label: str | None = None) -> GraphNode:
    return GraphNode(
        id=nid,
        data=NodeData(label=label or nid, nodeType=NodeType.DATA_SOURCE),
    )


def _transform_node(nid: str, label: str | None = None, **extra_config) -> GraphNode:
    return GraphNode(
        id=nid,
        data=NodeData(label=label or nid, nodeType=NodeType.TRANSFORM, config=extra_config),
    )


def _live_switch_node(nid: str, ism: dict[str, str], inputs: list[str] | None = None) -> GraphNode:
    return GraphNode(
        id=nid,
        data=NodeData(
            label=nid,
            nodeType=NodeType.LIVE_SWITCH,
            config={"input_scenario_map": ism, "inputs": inputs or []},
        ),
    )


def _simple_build_fn(node: GraphNode, source_names=None, **kwargs):
    """Minimal build_node_fn for testing."""
    nid = node.id
    nt = node.data.nodeType

    if nt == NodeType.DATA_SOURCE:
        return nid, lambda: pl.DataFrame({"x": [1, 2, 3]}).lazy(), True
    else:
        return nid, lambda *dfs: dfs[0].with_columns(y=pl.col("x") * 2), False


# ===========================================================================
# _prune_live_switch_edges
# ===========================================================================


class TestPruneLiveSwitchEdges:
    def test_no_live_switch_no_change(self):
        edges = [_e("a", "b")]
        node_map = {"a": _source_node("a"), "b": _transform_node("b")}
        result = _prune_live_switch_edges(edges, node_map, "live")
        assert result == edges

    def test_prunes_inactive_branch(self):
        """Live switch with two inputs; only the active scenario's edge survives."""
        edges = [_e("live_input", "sw"), _e("batch_input", "sw")]
        node_map = {
            "live_input": _source_node("live_input"),
            "batch_input": _source_node("batch_input"),
            "sw": _live_switch_node("sw", {"live_input": "live", "batch_input": "test_batch"}),
        }
        result = _prune_live_switch_edges(edges, node_map, "live")
        sources = [e.source for e in result]
        assert "live_input" in sources
        assert "batch_input" not in sources

    def test_keeps_both_when_scenario_not_in_map(self):
        """If active scenario is not in any ISM value, keep all edges (fallback)."""
        edges = [_e("a", "sw"), _e("b", "sw")]
        node_map = {
            "a": _source_node("a"),
            "b": _source_node("b"),
            "sw": _live_switch_node("sw", {"a": "live", "b": "batch"}),
        }
        result = _prune_live_switch_edges(edges, node_map, "unknown_scenario")
        assert len(result) == 2

    def test_empty_ism_keeps_all(self):
        edges = [_e("a", "sw")]
        node_map = {
            "a": _source_node("a"),
            "sw": _live_switch_node("sw", {}),
        }
        result = _prune_live_switch_edges(edges, node_map, "live")
        assert len(result) == 1

    def test_non_switch_edges_unaffected(self):
        """Edges not targeting a live_switch are never pruned."""
        edges = [_e("a", "b"), _e("live_input", "sw")]
        node_map = {
            "a": _source_node("a"),
            "b": _transform_node("b"),
            "live_input": _source_node("live_input"),
            "sw": _live_switch_node("sw", {"live_input": "live"}),
        }
        result = _prune_live_switch_edges(edges, node_map, "live")
        non_sw_edges = [e for e in result if e.target != "sw"]
        assert len(non_sw_edges) == 1
        assert non_sw_edges[0].source == "a"


# ===========================================================================
# _prepare_graph
# ===========================================================================


class TestPrepareGraph:
    def test_basic_preparation(self):
        g = PipelineGraph(
            nodes=[_source_node("a"), _transform_node("b")],
            edges=[_e("a", "b")],
        )
        node_map, order, parents, id_to_name = _prepare_graph(g)
        assert set(order) == {"a", "b"}
        assert parents["b"] == ["a"]
        assert parents["a"] == []
        assert id_to_name["a"] == "a"
        assert id_to_name["b"] == "b"

    def test_target_filters_to_ancestors(self):
        g = PipelineGraph(
            nodes=[_source_node("a"), _transform_node("b"), _transform_node("c")],
            edges=[_e("a", "b"), _e("a", "c")],
        )
        _, order, _, _ = _prepare_graph(g, target_node_id="b")
        assert set(order) == {"a", "b"}
        assert "c" not in order

    def test_scenario_passed_to_prune(self):
        """Verify scenario is used for live_switch pruning."""
        g = PipelineGraph(
            nodes=[
                _source_node("live"),
                _source_node("batch"),
                _live_switch_node("sw", {"live": "live", "batch": "test_batch"}),
            ],
            edges=[_e("live", "sw"), _e("batch", "sw")],
        )
        _, order, parents, _ = _prepare_graph(g, scenario="live")
        # "batch" should not be a parent of "sw" in live scenario
        assert "batch" not in parents.get("sw", [])

    def test_id_to_name_sanitizes_labels(self):
        g = PipelineGraph(
            nodes=[
                GraphNode(id="n1", data=NodeData(label="My Node", nodeType=NodeType.DATA_SOURCE)),
            ],
            edges=[],
        )
        _, _, _, id_to_name = _prepare_graph(g)
        assert id_to_name["n1"] == "My_Node"


# ===========================================================================
# _execute_lazy
# ===========================================================================


class TestExecuteLazy:
    def test_basic_lazy_chain(self):
        g = PipelineGraph(
            nodes=[_source_node("src"), _transform_node("t")],
            edges=[_e("src", "t")],
        )
        outputs, order, parents, id_to_name = _execute_lazy(g, _simple_build_fn)
        assert isinstance(outputs["t"], pl.LazyFrame)
        df = outputs["t"].collect()
        assert "y" in df.columns
        assert df["y"].to_list() == [2, 4, 6]

    def test_dataframe_auto_converted_to_lazy(self):
        def build_fn(node, **kwargs):
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}), True
            return node.id, lambda *dfs: dfs[0], False

        g = PipelineGraph(
            nodes=[_source_node("src"), _transform_node("t")],
            edges=[_e("src", "t")],
        )
        outputs, _, _, _ = _execute_lazy(g, build_fn)
        assert isinstance(outputs["src"], pl.LazyFrame)

    def test_non_source_with_no_input_raises(self):
        def build_fn(node, **kwargs):
            return node.id, lambda *dfs: dfs[0], False

        g = PipelineGraph(
            nodes=[_transform_node("lonely")],
            edges=[],
        )
        with pytest.raises(ValueError, match="No input data available"):
            _execute_lazy(g, build_fn)

    def test_target_node_filters_execution(self):
        g = PipelineGraph(
            nodes=[_source_node("a"), _transform_node("b"), _transform_node("c")],
            edges=[_e("a", "b"), _e("b", "c")],
        )
        outputs, _, _, _ = _execute_lazy(g, _simple_build_fn, target_node_id="b")
        assert "b" in outputs
        assert "c" not in outputs

    def test_preamble_ns_forwarded(self):
        """preamble_ns should be passed through to build_node_fn."""
        captured = {}

        def build_fn(node, **kwargs):
            captured.update(kwargs)
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True
            return node.id, lambda *dfs: dfs[0], False

        g = PipelineGraph(
            nodes=[_source_node("s")],
            edges=[],
        )
        _execute_lazy(g, build_fn, preamble_ns={"helper": lambda x: x})
        assert "preamble_ns" in captured


# ===========================================================================
# EagerResult
# ===========================================================================


# ===========================================================================
# _build_funcs
# ===========================================================================


class TestBuildFuncs:
    def test_builds_funcs_for_all_nodes(self):
        node_map = {"a": _source_node("a"), "b": _transform_node("b")}
        order = ["a", "b"]
        parents_of = {"a": [], "b": ["a"]}
        id_to_name = {"a": "a", "b": "b"}
        all_parents = {"b": ["a"]}

        funcs = _build_funcs(
            order, node_map, parents_of, id_to_name, all_parents,
            _simple_build_fn,
        )
        assert "a" in funcs
        assert "b" in funcs
        fn_a, is_source_a = funcs["a"]
        fn_b, is_source_b = funcs["b"]
        assert is_source_a is True
        assert is_source_b is False

    def test_row_limit_forwarded(self):
        captured_kwargs = {}

        def build_fn(node, **kwargs):
            captured_kwargs[node.id] = kwargs
            return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

        node_map = {"a": _source_node("a")}
        _build_funcs(
            ["a"], node_map, {"a": []}, {"a": "a"}, {},
            build_fn, row_limit=100,
        )
        assert captured_kwargs["a"]["row_limit"] == 100

    def test_scenario_forwarded(self):
        captured_kwargs = {}

        def build_fn(node, **kwargs):
            captured_kwargs[node.id] = kwargs
            return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

        node_map = {"a": _source_node("a")}
        _build_funcs(
            ["a"], node_map, {"a": []}, {"a": "a"}, {},
            build_fn, scenario="test_batch",
        )
        assert captured_kwargs["a"]["scenario"] == "test_batch"


# ===========================================================================
# _execute_eager_core
# ===========================================================================


class TestExecuteEagerCore:
    def test_basic_eager_execution(self):
        g = PipelineGraph(
            nodes=[_source_node("src"), _transform_node("t")],
            edges=[_e("src", "t")],
        )
        result = _execute_eager_core(g, _simple_build_fn)
        assert isinstance(result, EagerResult)
        assert result.outputs["src"] is not None
        assert result.outputs["t"] is not None
        assert isinstance(result.outputs["t"], pl.DataFrame)
        assert "y" in result.outputs["t"].columns

    def test_timings_populated(self):
        g = PipelineGraph(
            nodes=[_source_node("src")],
            edges=[],
        )
        result = _execute_eager_core(g, _simple_build_fn)
        assert "src" in result.timings
        assert result.timings["src"] >= 0

    def test_memory_bytes_populated(self):
        g = PipelineGraph(
            nodes=[_source_node("src")],
            edges=[],
        )
        result = _execute_eager_core(g, _simple_build_fn)
        assert "src" in result.memory_bytes
        assert result.memory_bytes["src"] > 0

    def test_swallow_errors_true_captures_error(self):
        """With swallow_errors=True, errors are captured, not raised."""
        def build_fn(node, **kwargs):
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

            def failing_fn(*dfs):
                raise RuntimeError("intentional test error")

            return node.id, failing_fn, False

        g = PipelineGraph(
            nodes=[_source_node("src"), _transform_node("t")],
            edges=[_e("src", "t")],
        )
        result = _execute_eager_core(g, build_fn, swallow_errors=True)
        assert "t" in result.errors
        assert "intentional test error" in result.errors["t"]
        assert result.outputs["t"] is None

    def test_swallow_errors_false_raises(self):
        """With swallow_errors=False (default), errors are raised."""
        def build_fn(node, **kwargs):
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

            def failing_fn(*dfs):
                raise RuntimeError("boom")

            return node.id, failing_fn, False

        g = PipelineGraph(
            nodes=[_source_node("src"), _transform_node("t")],
            edges=[_e("src", "t")],
        )
        with pytest.raises(RuntimeError, match="boom"):
            _execute_eager_core(g, build_fn, swallow_errors=False)

    def test_row_limit_applied_to_lazy_source(self):
        """row_limit should head-truncate source LazyFrames."""
        def build_fn(node, **kwargs):
            return node.id, lambda: pl.DataFrame({"x": list(range(100))}).lazy(), True

        g = PipelineGraph(
            nodes=[_source_node("src")],
            edges=[],
        )
        result = _execute_eager_core(g, build_fn, row_limit=5)
        assert len(result.outputs["src"]) == 5

    def test_target_node_filters(self):
        g = PipelineGraph(
            nodes=[_source_node("a"), _transform_node("b"), _transform_node("c")],
            edges=[_e("a", "b"), _e("b", "c")],
        )
        result = _execute_eager_core(g, _simple_build_fn, target_node_id="b")
        assert "b" in result.outputs
        assert "c" not in result.outputs

    def test_non_source_no_input_raises_eagerly(self):
        def build_fn(node, **kwargs):
            return node.id, lambda *dfs: dfs[0], False

        g = PipelineGraph(
            nodes=[_transform_node("lonely")],
            edges=[],
        )
        with pytest.raises(ValueError, match="No input data available"):
            _execute_eager_core(g, build_fn)

    def test_eager_handles_dataframe_source(self):
        """A source that returns a DataFrame (not LazyFrame) should work."""
        def build_fn(node, **kwargs):
            return node.id, lambda: pl.DataFrame({"x": [1, 2]}), True

        g = PipelineGraph(
            nodes=[_source_node("src")],
            edges=[],
        )
        result = _execute_eager_core(g, build_fn)
        assert isinstance(result.outputs["src"], pl.DataFrame)
        assert len(result.outputs["src"]) == 2

    def test_scenario_forwarded_to_build_fn(self):
        captured = {}

        def build_fn(node, **kwargs):
            captured[node.id] = kwargs.get("scenario")
            return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

        g = PipelineGraph(nodes=[_source_node("s")], edges=[])
        _execute_eager_core(g, build_fn, scenario="test_batch")
        assert captured["s"] == "test_batch"

    def test_multiple_errors_captured_with_swallow(self):
        """Multiple node failures are all captured."""
        def build_fn(node, **kwargs):
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

            def fail(*dfs):
                raise ValueError(f"fail_{node.id}")

            return node.id, fail, False

        g = PipelineGraph(
            nodes=[
                _source_node("s"),
                _transform_node("t1"),
                _transform_node("t2"),
            ],
            edges=[_e("s", "t1"), _e("s", "t2")],
        )
        result = _execute_eager_core(g, build_fn, swallow_errors=True)
        assert "t1" in result.errors
        assert "t2" in result.errors


# ═══════════════════════════════════════════════════════════════════════════
# _extract_error_line
# ═══════════════════════════════════════════════════════════════════════════


class TestExtractErrorLine:
    """Tests for _extract_error_line helper."""

    def test_syntax_error_with_lineno(self):
        exc = SyntaxError("invalid syntax")
        exc.lineno = 5
        assert _extract_error_line(exc) == 5

    def test_syntax_error_without_lineno(self):
        exc = SyntaxError("unexpected EOF")
        exc.lineno = None
        assert _extract_error_line(exc) is None

    def test_runtime_error_with_line_in_message(self):
        exc = ValueError("name 'foo' is not defined (line 3)")
        assert _extract_error_line(exc) == 3

    def test_runtime_error_without_line_info(self):
        exc = TypeError("unsupported operand type")
        assert _extract_error_line(exc) is None

    def test_no_match_on_partial_word(self):
        """'inline' or 'pipeline' should not match."""
        exc = ValueError("inline processing failed")
        assert _extract_error_line(exc) is None

    def test_traceback_style_message(self):
        exc = RuntimeError("File \"<string>\", line 7, in <module>")
        assert _extract_error_line(exc) == 7


# ═══════════════════════════════════════════════════════════════════════════
# error_lines in _execute_eager_core
# ═══════════════════════════════════════════════════════════════════════════


class TestEagerCoreErrorLines:
    """Test that error_lines is populated in EagerResult."""

    def test_syntax_error_populates_error_lines(self):
        def build_fn(node, **kwargs):
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

            def bad_syntax(*dfs):
                exc = SyntaxError("bad syntax")
                exc.lineno = 3
                raise exc

            return node.id, bad_syntax, False

        g = PipelineGraph(
            nodes=[_source_node("src"), _transform_node("t")],
            edges=[_e("src", "t")],
        )
        result = _execute_eager_core(g, build_fn, swallow_errors=True)
        assert result.error_lines["t"] == 3

    def test_runtime_error_with_line_populates_error_lines(self):
        def build_fn(node, **kwargs):
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

            def bad_runtime(*dfs):
                raise ValueError("error on line 5")

            return node.id, bad_runtime, False

        g = PipelineGraph(
            nodes=[_source_node("src"), _transform_node("t")],
            edges=[_e("src", "t")],
        )
        result = _execute_eager_core(g, build_fn, swallow_errors=True)
        assert result.error_lines["t"] == 5

    def test_error_without_line_not_in_error_lines(self):
        def build_fn(node, **kwargs):
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

            def no_line(*dfs):
                raise TypeError("unsupported operand")

            return node.id, no_line, False

        g = PipelineGraph(
            nodes=[_source_node("src"), _transform_node("t")],
            edges=[_e("src", "t")],
        )
        result = _execute_eager_core(g, build_fn, swallow_errors=True)
        assert "t" not in result.error_lines

    def test_successful_node_not_in_error_lines(self):
        g = PipelineGraph(
            nodes=[_source_node("src")],
            edges=[],
        )
        result = _execute_eager_core(g, _simple_build_fn)
        assert result.error_lines == {}
