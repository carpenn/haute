"""Tests for runw.graph_utils — shared graph utilities."""

from __future__ import annotations

import polars as pl

from runw.graph_utils import (
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

    def test_single_node(self):
        assert topo_sort_ids(["x"], []) == ["x"]

    def test_no_edges_returns_sorted(self):
        result = topo_sort_ids(["c", "a", "b"], [])
        assert result == ["a", "b", "c"]

    def test_deterministic(self):
        ids = ["c", "b", "a"]
        edges = [{"source": "a", "target": "c"}, {"source": "b", "target": "c"}]
        r1 = topo_sort_ids(ids, edges)
        r2 = topo_sort_ids(ids, edges)
        assert r1 == r2


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
