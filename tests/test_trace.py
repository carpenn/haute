"""Tests for haute.trace - execution trace / data lineage."""

from __future__ import annotations

import pytest
import polars as pl

from haute.trace import (
    SchemaDiff,
    TraceResult,
    TraceStep,
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

    def test_trace_calculated_column_keeps_ancestors(self, tmp_path):
        """Calculated column keeps the creating node AND all its ancestors."""
        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": [1], "z": [99]}).write_parquet(p)

        graph = {
            "nodes": [
                _source_node("src", str(p)),
                # passthrough - doesn't have 'y' but feeds into t
                _transform_node("mid"),
                # adds 'y' - column_relevant, ancestors kept for calc path
                _transform_node("t", ".with_columns(y=pl.col('x') * 2)"),
            ],
            "edges": [_edge("src", "mid"), _edge("mid", "t")],
        }
        result = execute_trace(graph, column="y")

        # 'y' is created at t → t is column_relevant, src/mid are ancestors
        ids = [s.node_id for s in result.steps]
        assert ids == ["src", "mid", "t"]
        assert result.steps[2].column_relevant is True   # t: adds y
        assert result.steps[0].column_relevant is False   # src: ancestor
        assert result.steps[1].column_relevant is False   # mid: ancestor

    def test_trace_passthrough_prunes_unrelated_branches(self, tmp_path):
        """Pass-through column prunes source branches that don't carry it."""
        p1 = tmp_path / "a.parquet"
        p2 = tmp_path / "b.parquet"
        pl.DataFrame({"x": [1], "shared": [10]}).write_parquet(p1)
        pl.DataFrame({"y": [2], "shared": [10]}).write_parquet(p2)

        graph = {
            "nodes": [
                _source_node("a", str(p1)),   # has x
                _source_node("b", str(p2)),   # has y, not x
                _transform_node("join", "a.join(b, on='shared')"),
            ],
            "edges": [_edge("a", "join"), _edge("b", "join")],
        }
        result = execute_trace(graph, column="x")

        # 'x' comes from 'a' only — 'b' should be pruned
        ids = {s.node_id for s in result.steps}
        assert "a" in ids
        assert "join" in ids
        assert "b" not in ids

    def test_trace_column_passthrough_keeps_path(self, tmp_path):
        """A pass-through column traces back through all nodes that carry it."""
        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": [1], "z": [99]}).write_parquet(p)

        graph = {
            "nodes": [
                _source_node("src", str(p)),
                _transform_node("mid"),  # passes x through
                _transform_node("t", ".with_columns(y=pl.col('x') * 2)"),
            ],
            "edges": [_edge("src", "mid"), _edge("mid", "t")],
        }
        result = execute_trace(graph, column="x")

        # 'x' exists in all 3 nodes → all 3 in trace
        assert len(result.steps) == 3
        assert all(s.column_relevant for s in result.steps)

    def test_row_id_from_deploy_input(self, tmp_path):
        """Trace discovers row_id_column from deploy_input source and extracts its value."""
        p = tmp_path / "data.parquet"
        pl.DataFrame({"policy_id": [100, 200, 300], "x": [1, 2, 3]}).write_parquet(p)

        graph = {
            "nodes": [
                {
                    "id": "src",
                    "data": {
                        "label": "src",
                        "nodeType": "dataSource",
                        "config": {
                            "path": str(p),
                            "deploy_input": True,
                            "row_id_column": "policy_id",
                        },
                    },
                },
                _transform_node("t"),
            ],
            "edges": [_edge("src", "t")],
        }
        result = execute_trace(graph, row_index=1)
        assert result.row_id_column == "policy_id"
        assert result.row_id_value == 200

    def test_row_id_none_without_deploy_input(self, tmp_path):
        """Without deploy_input, row_id_column and row_id_value are None."""
        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(p)

        graph = {
            "nodes": [_source_node("src", str(p))],
            "edges": [],
        }
        result = execute_trace(graph, row_index=0)
        assert result.row_id_column is None
        assert result.row_id_value is None

    def test_cache_reuses_execution_for_different_rows(self, tmp_path):
        """Subsequent traces on same graph reuse cached DataFrames."""
        from haute.trace import _cache

        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": [10, 20, 30]}).write_parquet(p)

        graph = {
            "nodes": [_source_node("src", str(p)), _transform_node("t")],
            "edges": [_edge("src", "t")],
        }
        _cache.invalidate()

        r0 = execute_trace(graph, row_index=0)
        fp_after_first = _cache.fingerprint
        assert r0.output_value["x"] == 10

        r1 = execute_trace(graph, row_index=1)
        assert r1.output_value["x"] == 20
        # Cache fingerprint unchanged → was a cache hit
        assert _cache.fingerprint == fp_after_first

    def test_cache_invalidates_on_graph_change(self, tmp_path):
        """Changing graph code invalidates the cache."""
        from haute.trace import _cache

        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": [1, 2]}).write_parquet(p)

        graph1 = {
            "nodes": [_source_node("src", str(p)), _transform_node("t")],
            "edges": [_edge("src", "t")],
        }
        _cache.invalidate()
        execute_trace(graph1)
        fp1 = _cache.fingerprint

        graph2 = {
            "nodes": [
                _source_node("src", str(p)),
                _transform_node("t", ".with_columns(y=pl.col('x') * 2)"),
            ],
            "edges": [_edge("src", "t")],
        }
        execute_trace(graph2)
        assert _cache.fingerprint != fp1

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
