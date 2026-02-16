"""Tests for haute.trace - execution trace / data lineage."""

from __future__ import annotations

import pytest
import polars as pl

from haute.trace import (
    SchemaDiff,
    TraceResult,
    TraceStep,
    _collect_row,
    _compute_schema_diff,
    _jsonify_row,
    execute_trace,
    trace_result_to_dict,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _source_node(nid: str, path: str) -> dict:
    return {
        "id": nid,
        "data": {"label": nid, "nodeType": "dataSource", "config": {"path": path}},
    }


def _transform_node(nid: str, code: str = "") -> dict:
    return {
        "id": nid,
        "data": {"label": nid, "nodeType": "transform", "config": {"code": code}},
    }


def _edge(src: str, tgt: str) -> dict:
    return {"id": f"e_{src}_{tgt}", "source": src, "target": tgt}


# ---------------------------------------------------------------------------
# _jsonify_row
# ---------------------------------------------------------------------------

class TestJsonifyRow:
    def test_primitives_preserved(self):
        row = {"a": 1, "b": 2.5, "c": "hello", "d": True, "e": None}
        result = _jsonify_row(row)
        assert result == row

    def test_non_primitives_stringified(self):
        from datetime import date

        row = {"d": date(2025, 1, 1)}
        result = _jsonify_row(row)
        assert isinstance(result["d"], str)


# ---------------------------------------------------------------------------
# _compute_schema_diff
# ---------------------------------------------------------------------------

class TestComputeSchemaDiff:
    def test_source_node_all_added(self):
        diff = _compute_schema_diff(None, {"a": 1, "b": 2})
        assert diff.columns_added == ["a", "b"]
        assert diff.columns_removed == []
        assert diff.columns_modified == []

    def test_column_added(self):
        diff = _compute_schema_diff({"a": 1}, {"a": 1, "b": 2})
        assert diff.columns_added == ["b"]
        assert diff.columns_passed == ["a"]

    def test_column_removed(self):
        diff = _compute_schema_diff({"a": 1, "b": 2}, {"a": 1})
        assert diff.columns_removed == ["b"]

    def test_column_modified(self):
        diff = _compute_schema_diff({"a": 1}, {"a": 99})
        assert diff.columns_modified == ["a"]
        assert diff.columns_passed == []

    def test_nan_equals_nan(self):
        diff = _compute_schema_diff({"a": float("nan")}, {"a": float("nan")})
        assert diff.columns_passed == ["a"]
        assert diff.columns_modified == []


# ---------------------------------------------------------------------------
# _collect_row
# ---------------------------------------------------------------------------

class TestCollectRow:
    def test_collects_correct_row(self):
        lf = pl.DataFrame({"x": [10, 20, 30]}).lazy()
        row = _collect_row(lf, 1)
        assert row["x"] == 20

    def test_empty_on_out_of_range(self):
        lf = pl.DataFrame({"x": [1]}).lazy()
        row = _collect_row(lf, 5)
        assert row == {}


# ---------------------------------------------------------------------------
# execute_trace
# ---------------------------------------------------------------------------

class TestExecuteTrace:
    def test_basic_trace(self, tmp_path):
        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(p)

        graph = {
            "nodes": [
                _source_node("src", str(p)),
                _transform_node("t", ".with_columns(y=pl.col('x') * 10)"),
            ],
            "edges": [_edge("src", "t")],
        }
        result = execute_trace(graph, row_index=0)

        assert isinstance(result, TraceResult)
        assert result.nodes_in_trace == 2
        assert result.total_nodes_in_pipeline == 2
        assert len(result.steps) == 2

        # Source step should have all columns added
        src_step = result.steps[0]
        assert "x" in src_step.schema_diff.columns_added

        # Transform step should have y added
        t_step = result.steps[1]
        assert "y" in t_step.schema_diff.columns_added

    def test_trace_defaults_to_last_node(self, tmp_path):
        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(p)

        graph = {
            "nodes": [
                _source_node("src", str(p)),
                _transform_node("t"),
            ],
            "edges": [_edge("src", "t")],
        }
        result = execute_trace(graph)
        assert result.target_node_id == "t"

    def test_trace_with_column_filter(self, tmp_path):
        """Column filter should include only nodes where the column appears."""
        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": [1], "z": [99]}).write_parquet(p)

        graph = {
            "nodes": [
                _source_node("src", str(p)),
                # passthrough - doesn't touch 'y'
                _transform_node("mid"),
                # adds 'y' - should be included
                _transform_node("t", ".with_columns(y=pl.col('x') * 2)"),
            ],
            "edges": [_edge("src", "mid"), _edge("mid", "t")],
        }
        result_unfiltered = execute_trace(graph)
        result_filtered = execute_trace(graph, column="y")

        # Filtered should have fewer steps than unfiltered
        assert result_filtered.nodes_in_trace <= result_unfiltered.nodes_in_trace
        # The node that adds 'y' must be present
        filtered_ids = [s.node_id for s in result_filtered.steps]
        assert "t" in filtered_ids
        # 'y' should appear in the transform's schema_diff
        t_step = next(s for s in result_filtered.steps if s.node_id == "t")
        assert "y" in t_step.schema_diff.columns_added

    def test_empty_graph_raises(self):
        with pytest.raises(ValueError, match="Empty graph"):
            execute_trace({"nodes": [], "edges": []})

    def test_missing_target_raises(self, tmp_path):
        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(p)

        graph = {"nodes": [_source_node("src", str(p))], "edges": []}
        with pytest.raises(ValueError, match="not found"):
            execute_trace(graph, target_node_id="nonexistent")


# ---------------------------------------------------------------------------
# trace_result_to_dict
# ---------------------------------------------------------------------------

class TestTraceResultToDict:
    def test_serialises_correctly(self):
        result = TraceResult(
            target_node_id="t",
            row_index=0,
            column=None,
            output_value={"x": 1},
            steps=[
                TraceStep(
                    node_id="t",
                    node_name="Transform",
                    node_type="transform",
                    schema_diff=SchemaDiff(
                        columns_added=["x"],
                        columns_removed=[],
                        columns_modified=[],
                        columns_passed=[],
                    ),
                    input_values={},
                    output_values={"x": 1},
                    execution_ms=1.5,
                ),
            ],
            total_nodes_in_pipeline=1,
            nodes_in_trace=1,
            execution_ms=2.0,
        )
        d = trace_result_to_dict(result)
        assert d["target_node_id"] == "t"
        assert len(d["steps"]) == 1
        assert d["steps"][0]["schema_diff"]["columns_added"] == ["x"]
        assert d["execution_ms"] == 2.0
