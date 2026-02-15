"""Tests for runw.executor — graph execution engine."""

from __future__ import annotations

import pytest
import polars as pl

from runw.executor import _build_node_fn, _exec_user_code, execute_graph, execute_sink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _source_node(nid: str, path: str) -> dict:
    return {
        "id": nid,
        "data": {
            "label": nid,
            "nodeType": "dataSource",
            "config": {"path": path},
        },
    }


def _transform_node(nid: str, code: str = "") -> dict:
    return {
        "id": nid,
        "data": {
            "label": nid,
            "nodeType": "transform",
            "config": {"code": code},
        },
    }


def _output_node(nid: str, fields: list[str] | None = None) -> dict:
    return {
        "id": nid,
        "data": {
            "label": nid,
            "nodeType": "output",
            "config": {"fields": fields or []},
        },
    }


def _edge(src: str, tgt: str) -> dict:
    return {"id": f"e_{src}_{tgt}", "source": src, "target": tgt}


# ---------------------------------------------------------------------------
# _exec_user_code
# ---------------------------------------------------------------------------

class TestExecUserCode:
    def test_chain_syntax(self):
        lf = pl.DataFrame({"x": [1, 2, 3]}).lazy()
        result = _exec_user_code(".with_columns(y=pl.col('x') * 2)", ["df"], (lf,))
        df = result.collect()
        assert "y" in df.columns
        assert df["y"].to_list() == [2, 4, 6]

    def test_full_expression(self):
        lf = pl.DataFrame({"x": [10]}).lazy()
        result = _exec_user_code("df.with_columns(y=pl.lit(42))", ["df"], (lf,))
        df = result.collect()
        assert df["y"].to_list() == [42]

    def test_assignment_style(self):
        lf = pl.DataFrame({"x": [1]}).lazy()
        result = _exec_user_code("df = df.with_columns(y=pl.lit(99))", ["df"], (lf,))
        df = result.collect()
        assert df["y"].to_list() == [99]

    def test_syntax_error_adjusts_line_number(self):
        lf = pl.DataFrame({"x": [1]}).lazy()
        with pytest.raises(SyntaxError):
            _exec_user_code(".invalid syntax here !!!", ["df"], (lf,))

    def test_eager_result_converted_to_lazy(self):
        lf = pl.DataFrame({"x": [1]}).lazy()
        result = _exec_user_code("df = df.collect()", ["df"], (lf,))
        assert isinstance(result, pl.LazyFrame)


# ---------------------------------------------------------------------------
# _build_node_fn
# ---------------------------------------------------------------------------

class TestBuildNodeFn:
    def test_data_source_parquet(self, tmp_path):
        p = tmp_path / "data.parquet"
        pl.DataFrame({"a": [1, 2]}).write_parquet(p)

        node = _source_node("src", str(p))
        name, fn, is_source = _build_node_fn(node)
        assert is_source is True
        df = fn().collect()
        assert df["a"].to_list() == [1, 2]

    def test_data_source_csv(self, tmp_path):
        p = tmp_path / "data.csv"
        pl.DataFrame({"b": [3, 4]}).write_csv(p)

        node = _source_node("src", str(p))
        name, fn, is_source = _build_node_fn(node)
        df = fn().collect()
        assert df["b"].to_list() == [3, 4]

    def test_transform_with_code(self):
        node = _transform_node("t", code=".with_columns(y=pl.col('x') + 1)")
        _, fn, is_source = _build_node_fn(node, source_names=["df"])
        assert is_source is False
        lf = pl.DataFrame({"x": [10]}).lazy()
        df = fn(lf).collect()
        assert df["y"].to_list() == [11]

    def test_transform_passthrough_without_code(self):
        node = _transform_node("t", code="")
        _, fn, is_source = _build_node_fn(node)
        lf = pl.DataFrame({"x": [5]}).lazy()
        df = fn(lf).collect()
        assert df["x"].to_list() == [5]

    def test_output_selects_fields(self):
        node = _output_node("out", fields=["a"])
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        df = fn(lf).collect()
        assert df.columns == ["a"]

    def test_sink_passthrough(self):
        node = {
            "id": "sink",
            "data": {"label": "sink", "nodeType": "dataSink", "config": {}},
        }
        _, fn, is_source = _build_node_fn(node)
        assert is_source is False
        lf = pl.DataFrame({"x": [1]}).lazy()
        assert fn(lf).collect().shape == (1, 1)


# ---------------------------------------------------------------------------
# execute_graph
# ---------------------------------------------------------------------------

class TestExecuteGraph:
    def test_simple_pipeline(self, tmp_path):
        p = tmp_path / "input.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(p)

        graph = {
            "nodes": [
                _source_node("src", str(p)),
                _transform_node("t", ".with_columns(y=pl.col('x') * 2)"),
            ],
            "edges": [_edge("src", "t")],
        }
        results = execute_graph(graph)
        assert results["src"]["status"] == "ok"
        assert results["t"]["status"] == "ok"
        assert results["t"]["row_count"] == 3
        assert any(c["name"] == "y" for c in results["t"]["columns"])

    def test_empty_graph(self):
        assert execute_graph({"nodes": [], "edges": []}) == {}

    def test_row_limit(self, tmp_path):
        p = tmp_path / "big.parquet"
        pl.DataFrame({"x": list(range(100))}).write_parquet(p)

        graph = {"nodes": [_source_node("s", str(p))], "edges": []}
        results = execute_graph(graph, row_limit=5)
        assert results["s"]["row_count"] == 5

    def test_target_node_id(self, tmp_path):
        p = tmp_path / "d.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(p)

        graph = {
            "nodes": [
                _source_node("a", str(p)),
                _transform_node("b"),
                _transform_node("c"),
            ],
            "edges": [_edge("a", "b"), _edge("b", "c")],
        }
        results = execute_graph(graph, target_node_id="b")
        assert "b" in results
        assert "c" not in results

    def test_error_node_captured(self, tmp_path):
        p = tmp_path / "d.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(p)

        graph = {
            "nodes": [
                _source_node("src", str(p)),
                # Select a column that doesn't exist — triggers ColumnNotFoundError at collect
                _transform_node("bad", code=".select('nonexistent_col')"),
            ],
            "edges": [_edge("src", "bad")],
        }
        results = execute_graph(graph)
        assert results["bad"]["status"] == "error"
        assert results["bad"]["error"]  # non-empty error message


# ---------------------------------------------------------------------------
# execute_sink
# ---------------------------------------------------------------------------

class TestExecuteSink:
    def test_writes_parquet(self, tmp_path):
        src_path = tmp_path / "in.parquet"
        out_path = tmp_path / "out.parquet"
        pl.DataFrame({"x": [1, 2]}).write_parquet(src_path)

        graph = {
            "nodes": [
                _source_node("src", str(src_path)),
                {
                    "id": "sink",
                    "data": {
                        "label": "sink",
                        "nodeType": "dataSink",
                        "config": {"path": str(out_path), "format": "parquet"},
                    },
                },
            ],
            "edges": [_edge("src", "sink")],
        }
        result = execute_sink(graph, sink_node_id="sink")
        assert result["status"] == "ok"
        assert result["row_count"] == 2
        assert out_path.exists()
        df = pl.read_parquet(out_path)
        assert len(df) == 2

    def test_writes_csv(self, tmp_path):
        src_path = tmp_path / "in.parquet"
        out_path = tmp_path / "out.csv"
        pl.DataFrame({"a": [10]}).write_parquet(src_path)

        graph = {
            "nodes": [
                _source_node("src", str(src_path)),
                {
                    "id": "sink",
                    "data": {
                        "label": "sink",
                        "nodeType": "dataSink",
                        "config": {"path": str(out_path), "format": "csv"},
                    },
                },
            ],
            "edges": [_edge("src", "sink")],
        }
        result = execute_sink(graph, sink_node_id="sink")
        assert result["status"] == "ok"
        assert out_path.exists()

    def test_missing_sink_raises(self):
        graph = {"nodes": [], "edges": []}
        with pytest.raises(ValueError, match="not found"):
            execute_sink(graph, sink_node_id="nope")

    def test_no_path_raises(self, tmp_path):
        src = tmp_path / "in.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(src)
        graph = {
            "nodes": [
                _source_node("src", str(src)),
                {
                    "id": "sink",
                    "data": {
                        "label": "sink",
                        "nodeType": "dataSink",
                        "config": {"path": "", "format": "parquet"},
                    },
                },
            ],
            "edges": [_edge("src", "sink")],
        }
        with pytest.raises(ValueError, match="no output path"):
            execute_sink(graph, sink_node_id="sink")
