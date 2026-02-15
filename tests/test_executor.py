"""Tests for haute.executor — graph execution engine."""

from __future__ import annotations

import pytest
import polars as pl

from haute.executor import _build_node_fn, _exec_user_code, execute_graph, execute_sink


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

    def test_multiple_named_sources(self):
        """Multiple source names are injected as named variables."""
        lf_a = pl.DataFrame({"x": [1]}).lazy()
        lf_b = pl.DataFrame({"y": [2]}).lazy()
        code = "df = a.join(b, how='cross')"
        result = _exec_user_code(code, ["a", "b"], (lf_a, lf_b))
        df = result.collect()
        assert set(df.columns) == {"x", "y"}

    def test_extra_ns_injects_object(self):
        """extra_ns should inject additional variables (e.g. external model obj)."""
        lf = pl.DataFrame({"x": [10]}).lazy()
        code = "df = df.with_columns(y=pl.lit(val))"
        result = _exec_user_code(code, ["df"], (lf,), extra_ns={"val": 42})
        df = result.collect()
        assert df["y"].to_list() == [42]

    def test_runtime_error_preserves_message(self):
        """Runtime errors in user code should propagate with useful messages."""
        lf = pl.DataFrame({"x": [1]}).lazy()
        with pytest.raises(Exception) as exc_info:
            _exec_user_code("df = 1 / 0", ["df"], (lf,))
        assert "division" in str(exc_info.value).lower()


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
        assert name == "src"
        df = fn().collect()
        assert df["a"].to_list() == [1, 2]

    def test_data_source_csv(self, tmp_path):
        p = tmp_path / "data.csv"
        pl.DataFrame({"b": [3, 4]}).write_csv(p)

        node = _source_node("src", str(p))
        name, fn, is_source = _build_node_fn(node)
        df = fn().collect()
        assert df["b"].to_list() == [3, 4]

    def test_data_source_json(self, tmp_path):
        p = tmp_path / "data.json"
        pl.DataFrame({"c": [5, 6]}).write_json(p)

        node = _source_node("src", str(p))
        _, fn, is_source = _build_node_fn(node)
        assert is_source is True
        result = fn()
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert df["c"].to_list() == [5, 6]

    def test_data_source_databricks_raises(self):
        node = {
            "id": "db",
            "data": {
                "label": "db",
                "nodeType": "dataSource",
                "config": {"sourceType": "databricks"},
            },
        }
        _, fn, is_source = _build_node_fn(node)
        assert is_source is True
        with pytest.raises(NotImplementedError, match="Databricks"):
            fn()

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

    def test_output_passthrough_without_fields(self):
        node = _output_node("out", fields=[])
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        df = fn(lf).collect()
        assert set(df.columns) == {"a", "b"}

    def test_sink_passthrough(self):
        node = {
            "id": "sink",
            "data": {"label": "sink", "nodeType": "dataSink", "config": {}},
        }
        _, fn, is_source = _build_node_fn(node)
        assert is_source is False
        lf = pl.DataFrame({"x": [1]}).lazy()
        df = fn(lf).collect()
        assert df["x"].to_list() == [1]

    def test_external_file_with_json(self, tmp_path):
        """externalFile with JSON file type loads and injects obj."""
        import json as _json
        p = tmp_path / "data.json"
        p.write_text(_json.dumps({"multiplier": 10}))

        node = {
            "id": "ext",
            "data": {
                "label": "ext",
                "nodeType": "externalFile",
                "config": {
                    "path": str(p),
                    "fileType": "json",
                    "code": "df = df.with_columns(y=pl.lit(obj['multiplier']))",
                },
            },
        }
        _, fn, is_source = _build_node_fn(node, source_names=["df"])
        assert is_source is False
        lf = pl.DataFrame({"x": [1]}).lazy()
        df = fn(lf).collect()
        assert df["y"].to_list() == [10]

    def test_external_file_with_pickle(self, tmp_path):
        """externalFile with pickle file type loads and injects obj."""
        import pickle
        p = tmp_path / "data.pkl"
        with open(p, "wb") as f:
            pickle.dump({"factor": 5}, f)

        node = {
            "id": "ext",
            "data": {
                "label": "ext",
                "nodeType": "externalFile",
                "config": {
                    "path": str(p),
                    "fileType": "pickle",
                    "code": "df = df.with_columns(y=pl.lit(obj['factor']))",
                },
            },
        }
        _, fn, _ = _build_node_fn(node, source_names=["df"])
        lf = pl.DataFrame({"x": [1]}).lazy()
        df = fn(lf).collect()
        assert df["y"].to_list() == [5]

    def test_external_file_passthrough_without_code(self):
        """externalFile without code acts as passthrough."""
        node = {
            "id": "ext",
            "data": {
                "label": "ext",
                "nodeType": "externalFile",
                "config": {"path": "model.pkl", "fileType": "pickle", "code": ""},
            },
        }
        _, fn, is_source = _build_node_fn(node)
        assert is_source is False
        lf = pl.DataFrame({"x": [7]}).lazy()
        df = fn(lf).collect()
        assert df["x"].to_list() == [7]

    def test_unknown_node_type_passthrough(self):
        """Unknown nodeType should act as passthrough."""
        node = {
            "id": "unk",
            "data": {"label": "unk", "nodeType": "unknownFutureType", "config": {}},
        }
        _, fn, is_source = _build_node_fn(node)
        assert is_source is False
        lf = pl.DataFrame({"x": [3]}).lazy()
        df = fn(lf).collect()
        assert df["x"].to_list() == [3]


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
        assert "nonexistent_col" in results["bad"]["error"].lower() or "not found" in results["bad"]["error"].lower(), (
            f"Expected column-not-found error, got: {results['bad']['error']}"
        )
        assert results["bad"]["row_count"] == 0
        assert results["bad"]["columns"] == []


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
