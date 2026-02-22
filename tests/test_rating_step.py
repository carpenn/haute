"""Tests for the rating step node: executor, parser, and codegen."""

from __future__ import annotations

import polars as pl

from haute.graph_utils import GraphEdge, GraphNode, NodeData, PipelineGraph
from haute.codegen import graph_to_code
from haute.executor import _build_node_fn
from haute.parser import parse_pipeline_source
from tests.conftest import make_source_node as _source_node

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rating_node(
    nid: str,
    tables: list[dict] | None = None,
    operation: str = "multiply",
    combined_column: str = "",
) -> GraphNode:
    cfg: dict = {"tables": tables or []}
    if operation != "multiply":
        cfg["operation"] = operation
    if combined_column:
        cfg["combinedColumn"] = combined_column
    return GraphNode(
        id=nid,
        data=NodeData(
            label=nid,
            nodeType="ratingStep",
            config=cfg,
        ),
    )


# ---------------------------------------------------------------------------
# Executor: _apply_rating_table via _build_node_fn
# ---------------------------------------------------------------------------

class TestRatingStepExecutor:
    """Rating step executor applies lookup joins correctly."""

    def test_one_way_lookup(self):
        """Single-factor table: join on one column."""
        tables = [{
            "name": "Age Factor",
            "factors": ["age_band"],
            "outputColumn": "age_factor",
            "defaultValue": "1.0",
            "entries": [
                {"age_band": "young", "value": 1.3},
                {"age_band": "older", "value": 0.9},
            ],
        }]
        node = _rating_node("r1", tables)
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"age_band": ["young", "older", "unknown"]}).lazy()
        result = fn(lf).collect()
        assert result["age_factor"].to_list() == [1.3, 0.9, 1.0]

    def test_two_way_lookup(self):
        """Two-factor table: join on two columns."""
        tables = [{
            "name": "Age × Prop",
            "factors": ["age_band", "prop_band"],
            "outputColumn": "factor",
            "defaultValue": None,
            "entries": [
                {"age_band": "young", "prop_band": "House", "value": 1.2},
                {"age_band": "young", "prop_band": "Flat", "value": 1.5},
                {"age_band": "older", "prop_band": "House", "value": 0.9},
            ],
        }]
        node = _rating_node("r2", tables)
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({
            "age_band": ["young", "young", "older", "older"],
            "prop_band": ["House", "Flat", "House", "Flat"],
        }).lazy()
        result = fn(lf).collect()
        assert result["factor"].to_list() == [1.2, 1.5, 0.9, None]

    def test_three_way_lookup(self):
        """Three-factor table: join on three columns."""
        tables = [{
            "name": "3-way",
            "factors": ["a", "b", "c"],
            "outputColumn": "val",
            "defaultValue": "0.0",
            "entries": [
                {"a": "x", "b": "y", "c": "z", "value": 2.5},
            ],
        }]
        node = _rating_node("r3", tables)
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({
            "a": ["x", "x"],
            "b": ["y", "y"],
            "c": ["z", "w"],
        }).lazy()
        result = fn(lf).collect()
        assert result["val"].to_list() == [2.5, 0.0]

    def test_multiple_tables(self):
        """Multiple tables in one node, each produces its own output column."""
        tables = [
            {
                "name": "T1",
                "factors": ["band"],
                "outputColumn": "f1",
                "defaultValue": "1.0",
                "entries": [{"band": "A", "value": 2.0}],
            },
            {
                "name": "T2",
                "factors": ["band"],
                "outputColumn": "f2",
                "defaultValue": "1.0",
                "entries": [{"band": "A", "value": 3.0}],
            },
        ]
        node = _rating_node("r4", tables)
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"band": ["A", "B"]}).lazy()
        result = fn(lf).collect()
        assert result["f1"].to_list() == [2.0, 1.0]
        assert result["f2"].to_list() == [3.0, 1.0]

    def test_empty_tables_passthrough(self):
        """Rating node with no tables passes through unchanged."""
        node = _rating_node("r5", [])
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"x": [1, 2]}).lazy()
        result = fn(lf).collect()
        assert result.columns == ["x"]

    def test_incomplete_table_skipped(self):
        """Table with missing factors/entries/outputColumn is skipped."""
        tables = [
            {"name": "bad", "factors": [], "outputColumn": "out",
             "defaultValue": None, "entries": [{"value": 1.0}]},
        ]
        node = _rating_node("r6", tables)
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"x": [1]}).lazy()
        result = fn(lf).collect()
        assert result.columns == ["x"]

    def test_combine_multiply(self):
        """Two tables combined via multiply."""
        tables = [
            {"name": "T1", "factors": ["band"], "outputColumn": "f1",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 2.0}]},
            {"name": "T2", "factors": ["band"], "outputColumn": "f2",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 3.0}]},
        ]
        node = _rating_node("rc1", tables, combined_column="combined")
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"band": ["A", "B"]}).lazy()
        result = fn(lf).collect()
        assert result["f1"].to_list() == [2.0, 1.0]
        assert result["f2"].to_list() == [3.0, 1.0]
        assert result["combined"].to_list() == [6.0, 1.0]

    def test_combine_add(self):
        """Two tables combined via add."""
        tables = [
            {"name": "T1", "factors": ["band"], "outputColumn": "f1",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 2.0}]},
            {"name": "T2", "factors": ["band"], "outputColumn": "f2",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 3.0}]},
        ]
        node = _rating_node("rc2", tables, operation="add", combined_column="total")
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"band": ["A", "B"]}).lazy()
        result = fn(lf).collect()
        assert result["total"].to_list() == [5.0, 2.0]

    def test_combine_min(self):
        """Two tables combined via min."""
        tables = [
            {"name": "T1", "factors": ["band"], "outputColumn": "f1",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 2.0}]},
            {"name": "T2", "factors": ["band"], "outputColumn": "f2",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 3.0}]},
        ]
        node = _rating_node("rc3", tables, operation="min", combined_column="mn")
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"band": ["A", "B"]}).lazy()
        result = fn(lf).collect()
        assert result["mn"].to_list() == [2.0, 1.0]

    def test_combine_max(self):
        """Two tables combined via max."""
        tables = [
            {"name": "T1", "factors": ["band"], "outputColumn": "f1",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 2.0}]},
            {"name": "T2", "factors": ["band"], "outputColumn": "f2",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 3.0}]},
        ]
        node = _rating_node("rc4", tables, operation="max", combined_column="mx")
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"band": ["A", "B"]}).lazy()
        result = fn(lf).collect()
        assert result["mx"].to_list() == [3.0, 1.0]

    def test_no_combined_column_skips_combine(self):
        """Without combinedColumn, no combination column is created."""
        tables = [
            {"name": "T1", "factors": ["band"], "outputColumn": "f1",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 2.0}]},
            {"name": "T2", "factors": ["band"], "outputColumn": "f2",
             "defaultValue": "1.0", "entries": [{"band": "A", "value": 3.0}]},
        ]
        node = _rating_node("rc5", tables)
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"band": ["A"]}).lazy()
        result = fn(lf).collect()
        assert "combined" not in result.columns
        assert result.columns == ["band", "f1", "f2"]

    def test_string_factor_values_match(self):
        """Factor values are cast to Utf8 so string bands match."""
        tables = [{
            "name": "T",
            "factors": ["band"],
            "outputColumn": "out",
            "defaultValue": None,
            "entries": [{"band": "1", "value": 9.9}],
        }]
        node = _rating_node("r7", tables)
        _, fn, _ = _build_node_fn(node)
        # Source has integer column — should still match via Utf8 cast
        lf = pl.DataFrame({"band": [1, 2]}).lazy()
        result = fn(lf).collect()
        assert result["out"].to_list() == [9.9, None]


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class TestRatingStepParser:
    """Parser extracts tables config from decorated functions."""

    def test_parse_rating_step(self):
        code = '''
import polars as pl
from haute import pipeline

@pipeline.node(tables=[
    {"name": "T1", "factors": ["age"], "output_column": "af",
     "default_value": 1.0, "entries": [{"age": "young", "value": 1.5}]},
])
def rating(df: pl.LazyFrame) -> pl.LazyFrame:
    """Apply rating."""
    return df
'''
        parsed = parse_pipeline_source(code)
        assert len(parsed.nodes) == 1
        n = parsed.nodes[0]
        assert n.data.nodeType == "ratingStep"
        tables = n.data.config["tables"]
        assert len(tables) == 1
        assert tables[0]["factors"] == ["age"]
        assert tables[0]["outputColumn"] == "af"
        assert tables[0]["defaultValue"] == 1.0
        assert len(tables[0]["entries"]) == 1

    def test_parse_empty_tables(self):
        code = '''
import polars as pl
from haute import pipeline

@pipeline.node(tables=[])
def rating(df: pl.LazyFrame) -> pl.LazyFrame:
    """Empty."""
    return df
'''
        parsed = parse_pipeline_source(code)
        n = parsed.nodes[0]
        assert n.data.nodeType == "ratingStep"
        assert n.data.config["tables"] == []

    def test_parse_operation_and_combined(self):
        code = '''
import polars as pl
from haute import pipeline

@pipeline.node(tables=[
    {"name": "T1", "factors": ["band"], "output_column": "f1",
     "entries": [{"band": "A", "value": 1.0}]},
    {"name": "T2", "factors": ["band"], "output_column": "f2",
     "entries": [{"band": "A", "value": 2.0}]},
], operation="add", combined_column="total")
def rating(df: pl.LazyFrame) -> pl.LazyFrame:
    """Combined."""
    return df
'''
        parsed = parse_pipeline_source(code)
        n = parsed.nodes[0]
        assert n.data.config["operation"] == "add"
        assert n.data.config["combinedColumn"] == "total"


# ---------------------------------------------------------------------------
# Codegen
# ---------------------------------------------------------------------------

class TestRatingStepCodegen:
    """Codegen produces valid rating step decorators."""

    def test_codegen_rating_step(self):
        tables = [{
            "name": "T1",
            "factors": ["band"],
            "outputColumn": "factor",
            "defaultValue": 1.0,
            "entries": [{"band": "A", "value": 2.0}],
        }]
        node = _rating_node("rating", tables)
        src = _source_node("src")
        graph = PipelineGraph(
            nodes=[src, node],
            edges=[GraphEdge(id="e1", source="src", target="rating")],
        )
        code = graph_to_code(graph)
        assert "tables=" in code
        assert "output_column" in code

    def test_codegen_roundtrip(self):
        """Code generated from a graph can be parsed back."""
        tables = [{
            "name": "Age Factor",
            "factors": ["age_band"],
            "outputColumn": "age_factor",
            "defaultValue": 1.0,
            "entries": [
                {"age_band": "young", "value": 1.3},
                {"age_band": "older", "value": 0.9},
            ],
        }]
        node = _rating_node("rating", tables)
        src = _source_node("src")
        graph = PipelineGraph(
            nodes=[src, node],
            edges=[GraphEdge(id="e1", source="src", target="rating")],
        )
        code = graph_to_code(graph)

        parsed = parse_pipeline_source(code)
        rating_nodes = [
            n for n in parsed.nodes if n.data.nodeType == "ratingStep"
        ]
        assert len(rating_nodes) == 1
        rt = rating_nodes[0].data.config["tables"]
        assert len(rt) == 1
        assert rt[0]["factors"] == ["age_band"]
        assert rt[0]["outputColumn"] == "age_factor"
        assert len(rt[0]["entries"]) == 2

    def test_codegen_with_operation(self):
        """Codegen emits operation and combined_column when set."""
        tables = [
            {"name": "T1", "factors": ["b"], "outputColumn": "f1",
             "defaultValue": 1.0, "entries": [{"b": "A", "value": 2.0}]},
            {"name": "T2", "factors": ["b"], "outputColumn": "f2",
             "defaultValue": 1.0, "entries": [{"b": "A", "value": 3.0}]},
        ]
        node = _rating_node("rating", tables, operation="add", combined_column="total")
        src = _source_node("src")
        graph = PipelineGraph(
            nodes=[src, node],
            edges=[GraphEdge(id="e1", source="src", target="rating")],
        )
        code = graph_to_code(graph)
        assert "operation='add'" in code
        assert "combined_column='total'" in code

        # Roundtrip
        parsed = parse_pipeline_source(code)
        rn = [n for n in parsed.nodes if n.data.nodeType == "ratingStep"][0]
        assert rn.data.config["operation"] == "add"
        assert rn.data.config["combinedColumn"] == "total"

    def test_codegen_multiply_default_omitted(self):
        """Multiply (default) operation is not emitted in code."""
        tables = [
            {"name": "T1", "factors": ["b"], "outputColumn": "f1",
             "entries": [{"b": "A", "value": 2.0}]},
        ]
        node = _rating_node("rating", tables, combined_column="c")
        src = _source_node("src")
        graph = PipelineGraph(
            nodes=[src, node],
            edges=[GraphEdge(id="e1", source="src", target="rating")],
        )
        code = graph_to_code(graph)
        assert 'operation=' not in code
        assert "combined_column='c'" in code
