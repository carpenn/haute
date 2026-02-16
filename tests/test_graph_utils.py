"""Tests for haute.graph_utils - shared graph utilities."""

from __future__ import annotations

import polars as pl
import pytest

from haute.graph_utils import (
    _execute_lazy,
    _prepare_graph,
    _sanitize_func_name,
    ancestors,
    topo_sort_ids,
)


# ---------------------------------------------------------------------------
# _sanitize_func_name
# ---------------------------------------------------------------------------

class TestSanitizeFuncName:
    def test_simple_label(self):
        assert _sanitize_func_name("Load Data") == "Load_Data"

    def test_hyphens_become_underscores(self):
        assert _sanitize_func_name("my-node") == "my_node"

    def test_strips_special_chars(self):
        assert _sanitize_func_name("node@#!1") == "node1"

    def test_leading_digit_gets_prefix(self):
        assert _sanitize_func_name("123abc") == "node_123abc"

    def test_empty_label_returns_unnamed(self):
        assert _sanitize_func_name("") == "unnamed_node"

    def test_whitespace_only_returns_unnamed(self):
        assert _sanitize_func_name("   ") == "unnamed_node"

    def test_preserves_case(self):
        assert _sanitize_func_name("MyNode") == "MyNode"


# ---------------------------------------------------------------------------
# topo_sort_ids
# ---------------------------------------------------------------------------

class TestTopoSort:
    def test_linear_chain(self):
        ids = ["a", "b", "c"]
        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
        ]
        assert topo_sort_ids(ids, edges) == ["a", "b", "c"]

    def test_diamond(self):
        ids = ["a", "b", "c", "d"]
        edges = [
            {"source": "a", "target": "b"},
            {"source": "a", "target": "c"},
            {"source": "b", "target": "d"},
            {"source": "c", "target": "d"},
        ]
        result = topo_sort_ids(ids, edges)
        assert result[0] == "a"
        assert result[-1] == "d"
        assert set(result) == {"a", "b", "c", "d"}
        # Verify topological invariant
        idx = {nid: i for i, nid in enumerate(result)}
        for e in edges:
            assert idx[e["source"]] < idx[e["target"]]

    def test_single_node(self):
        assert topo_sort_ids(["x"], []) == ["x"]

    def test_no_edges_returns_sorted(self):
        result = topo_sort_ids(["c", "a", "b"], [])
        assert result == ["a", "b", "c"]

    def test_deterministic_ordering(self):
        """With equal in-degree, nodes should be sorted alphabetically."""
        ids = ["c", "b", "a"]
        edges = [{"source": "a", "target": "c"}, {"source": "b", "target": "c"}]
        result = topo_sort_ids(ids, edges)
        assert result == ["a", "b", "c"]
        # Verify topological invariant: every parent before its child
        idx = {nid: i for i, nid in enumerate(result)}
        for e in edges:
            assert idx[e["source"]] < idx[e["target"]], (
                f"{e['source']} should come before {e['target']}"
            )

    def test_cycle_drops_nodes(self):
        """Cycle nodes never reach in-degree 0, so they're silently dropped."""
        ids = ["a", "b", "c"]
        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
            {"source": "c", "target": "a"},
        ]
        result = topo_sort_ids(ids, edges)
        assert len(result) < len(ids)  # cycle members dropped

    def test_edges_referencing_unknown_nodes(self):
        """Edges referencing non-existent IDs should be ignored."""
        ids = ["a", "b"]
        edges = [{"source": "a", "target": "b"}, {"source": "x", "target": "y"}]
        result = topo_sort_ids(ids, edges)
        assert set(result) == {"a", "b"}

    def test_empty_input(self):
        assert topo_sort_ids([], []) == []


# ---------------------------------------------------------------------------
# ancestors
# ---------------------------------------------------------------------------

class TestAncestors:
    def test_includes_self(self):
        result = ancestors("a", [], {"a", "b"})
        assert "a" in result

    def test_finds_parents(self):
        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
        ]
        result = ancestors("c", edges, {"a", "b", "c"})
        assert result == {"a", "b", "c"}

    def test_excludes_unrelated(self):
        edges = [
            {"source": "a", "target": "b"},
            {"source": "x", "target": "y"},
        ]
        result = ancestors("b", edges, {"a", "b", "x", "y"})
        assert result == {"a", "b"}


# ---------------------------------------------------------------------------
# _prepare_graph
# ---------------------------------------------------------------------------

def _make_graph(nodes_data: list[tuple[str, str]], edges_data: list[tuple[str, str]]) -> dict:
    """Helper to build a minimal graph dict."""
    nodes = [
        {"id": nid, "data": {"label": label, "nodeType": "transform", "config": {}}}
        for nid, label in nodes_data
    ]
    edges = [
        {"id": f"e_{s}_{t}", "source": s, "target": t}
        for s, t in edges_data
    ]
    return {"nodes": nodes, "edges": edges}


class TestPrepareGraph:
    def test_returns_all_nodes_without_target(self):
        g = _make_graph([("a", "A"), ("b", "B")], [("a", "b")])
        node_map, order, parents, names = _prepare_graph(g)
        assert set(order) == {"a", "b"}

    def test_filters_to_ancestors_with_target(self):
        g = _make_graph(
            [("a", "A"), ("b", "B"), ("c", "C")],
            [("a", "b")],
        )
        _, order, _, _ = _prepare_graph(g, target_node_id="b")
        assert "c" not in order
        assert set(order) == {"a", "b"}

    def test_parents_of_correct(self):
        g = _make_graph(
            [("a", "A"), ("b", "B"), ("c", "C")],
            [("a", "c"), ("b", "c")],
        )
        _, _, parents, _ = _prepare_graph(g)
        assert set(parents["c"]) == {"a", "b"}
        assert parents["a"] == []


# ---------------------------------------------------------------------------
# _execute_lazy
# ---------------------------------------------------------------------------

class TestExecuteLazy:
    @staticmethod
    def _simple_build_fn(node: dict, source_names: list[str] | None = None):
        """Minimal build_node_fn for testing."""
        nid = node["id"]
        nt = node.get("data", {}).get("nodeType", "transform")
        name = node.get("data", {}).get("label", nid)

        if nt == "dataSource":
            def fn() -> pl.LazyFrame:
                return pl.DataFrame({"x": [1, 2, 3]}).lazy()
            return name, fn, True
        else:
            def fn(*dfs: pl.LazyFrame) -> pl.LazyFrame:
                return dfs[0].with_columns(y=pl.col("x") * 2)
            return name, fn, False

    def test_basic_execution(self):
        g = _make_graph(
            [("src", "Source"), ("t", "Transform")],
            [("src", "t")],
        )
        g["nodes"][0]["data"]["nodeType"] = "dataSource"

        outputs, order, _, _ = _execute_lazy(g, self._simple_build_fn)
        assert "src" in outputs
        assert "t" in outputs
        df = outputs["t"].collect()
        assert "y" in df.columns
        assert df["y"].to_list() == [2, 4, 6]

    def test_target_filters_execution(self):
        g = _make_graph(
            [("a", "A"), ("b", "B"), ("c", "C")],
            [("a", "b"), ("b", "c")],
        )
        g["nodes"][0]["data"]["nodeType"] = "dataSource"

        outputs, order, _, _ = _execute_lazy(g, self._simple_build_fn, target_node_id="b")
        assert "b" in outputs
        assert "c" not in outputs

    def test_dataframe_converted_to_lazy(self):
        """If a node fn returns a DataFrame, it should be auto-converted to LazyFrame."""
        def build_fn(node, source_names=None):
            if node["id"] == "src":
                return "src", lambda: pl.DataFrame({"x": [1]}), True
            return "t", lambda *dfs: dfs[0], False

        g = _make_graph([("src", "Src"), ("t", "T")], [("src", "t")])
        g["nodes"][0]["data"]["nodeType"] = "dataSource"

        outputs, _, _, _ = _execute_lazy(g, build_fn)
        assert isinstance(outputs["t"], pl.LazyFrame)

    def test_non_source_no_input_raises(self):
        """A non-source node with no parents and no prior outputs raises ValueError."""
        def build_fn(node, source_names=None):
            return node["id"], lambda *dfs: dfs[0], False

        g = _make_graph([("lonely", "Lonely")], [])
        with pytest.raises(ValueError, match="no input and is not a source"):
            _execute_lazy(g, build_fn)

    def test_fallback_to_last_output(self):
        """A non-source with no edges but prior outputs uses the last available frame."""
        call_order = []

        def build_fn(node, source_names=None):
            nid = node["id"]
            if nid == "src":
                def fn():
                    call_order.append("src")
                    return pl.DataFrame({"x": [1]}).lazy()
                return nid, fn, True
            else:
                def fn(*dfs):
                    call_order.append(nid)
                    return dfs[0].with_columns(y=pl.lit(99))
                return nid, fn, False

        # Two nodes, no edge - "t" should fallback to src's output
        g = _make_graph([("src", "Src"), ("t", "T")], [])
        g["nodes"][0]["data"]["nodeType"] = "dataSource"

        outputs, _, _, _ = _execute_lazy(g, build_fn)
        df = outputs["t"].collect()
        assert df["y"].to_list() == [99]
        assert call_order == ["src", "t"]

    def test_multi_input_node(self):
        """A node with two parents receives both LazyFrames."""
        def build_fn(node, source_names=None):
            nid = node["id"]
            if nid in ("a", "b"):
                data = {"x": [1]} if nid == "a" else {"y": [2]}
                return nid, lambda d=data: pl.DataFrame(d).lazy(), True
            else:
                def fn(*dfs):
                    return dfs[0].join(dfs[1], how="cross")
                return nid, fn, False

        g = _make_graph(
            [("a", "A"), ("b", "B"), ("c", "C")],
            [("a", "c"), ("b", "c")],
        )
        g["nodes"][0]["data"]["nodeType"] = "dataSource"
        g["nodes"][1]["data"]["nodeType"] = "dataSource"

        outputs, _, _, _ = _execute_lazy(g, build_fn)
        df = outputs["c"].collect()
        assert set(df.columns) == {"x", "y"}
