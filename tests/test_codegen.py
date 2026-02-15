"""Tests for runw.codegen — graph JSON → Python code generation."""

from __future__ import annotations

from runw.codegen import _build_params, _node_to_code, graph_to_code


# ---------------------------------------------------------------------------
# _build_params
# ---------------------------------------------------------------------------

class TestBuildParams:
    def test_no_sources(self):
        assert _build_params([]) == "df: pl.DataFrame"

    def test_single_source(self):
        assert _build_params(["load_data"]) == "load_data: pl.DataFrame"

    def test_multiple_sources(self):
        result = _build_params(["a", "b"])
        assert result == "a: pl.DataFrame, b: pl.DataFrame"


# ---------------------------------------------------------------------------
# _node_to_code
# ---------------------------------------------------------------------------

class TestNodeToCode:
    def test_data_source_parquet(self):
        node = {
            "id": "src",
            "data": {
                "label": "Load Data",
                "nodeType": "dataSource",
                "config": {"path": "data/input.parquet"},
            },
        }
        code = _node_to_code(node)
        assert "def Load_Data()" in code
        assert "scan_parquet" in code
        assert "data/input.parquet" in code

    def test_data_source_csv(self):
        node = {
            "id": "src",
            "data": {
                "label": "CSV Source",
                "nodeType": "dataSource",
                "config": {"path": "data/input.csv"},
            },
        }
        code = _node_to_code(node)
        assert "scan_csv" in code

    def test_transform_with_code(self):
        node = {
            "id": "t",
            "data": {
                "label": "Clean",
                "nodeType": "transform",
                "config": {"code": ".filter(pl.col('x') > 0)"},
            },
        }
        code = _node_to_code(node, source_names=["load_data"])
        assert "def Clean(load_data: pl.DataFrame)" in code
        assert "filter" in code

    def test_transform_without_code(self):
        node = {
            "id": "t",
            "data": {"label": "Pass", "nodeType": "transform", "config": {}},
        }
        code = _node_to_code(node)
        assert "def Pass(df: pl.DataFrame)" in code
        assert "return" in code

    def test_output_with_fields(self):
        node = {
            "id": "out",
            "data": {
                "label": "Output",
                "nodeType": "output",
                "config": {"fields": ["a", "b"]},
            },
        }
        code = _node_to_code(node, source_names=["transform"])
        assert "output=True" in code
        assert ".select(" in code

    def test_sink_parquet(self):
        node = {
            "id": "s",
            "data": {
                "label": "Write",
                "nodeType": "dataSink",
                "config": {"path": "out.parquet", "format": "parquet"},
            },
        }
        code = _node_to_code(node, source_names=["transform"])
        assert "write_parquet" in code

    def test_sink_csv(self):
        node = {
            "id": "s",
            "data": {
                "label": "Write CSV",
                "nodeType": "dataSink",
                "config": {"path": "out.csv", "format": "csv"},
            },
        }
        code = _node_to_code(node)
        assert "write_csv" in code


# ---------------------------------------------------------------------------
# graph_to_code
# ---------------------------------------------------------------------------

class TestGraphToCode:
    def test_generates_valid_python(self):
        graph = {
            "nodes": [
                {
                    "id": "src",
                    "data": {
                        "label": "Source",
                        "nodeType": "dataSource",
                        "config": {"path": "data.parquet"},
                    },
                },
                {
                    "id": "t",
                    "data": {
                        "label": "Transform",
                        "nodeType": "transform",
                        "config": {"code": ".with_columns(y=pl.col('x'))"},
                    },
                },
            ],
            "edges": [{"id": "e1", "source": "src", "target": "t"}],
        }
        code = graph_to_code(graph, pipeline_name="test_pipe")
        assert "import polars as pl" in code
        assert "import runw" in code
        assert 'Pipeline("test_pipe"' in code
        assert "def Source()" in code
        assert "def Transform(" in code
        assert 'pipeline.connect("Source", "Transform")' in code

        # Verify it's valid Python
        compile(code, "<test>", "exec")

    def test_preamble_included(self):
        graph = {"nodes": [], "edges": []}
        code = graph_to_code(graph, preamble="import numpy as np")
        assert "import numpy as np" in code

    def test_empty_graph(self):
        code = graph_to_code({"nodes": [], "edges": []})
        assert "import polars as pl" in code
        compile(code, "<test>", "exec")
