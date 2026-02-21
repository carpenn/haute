"""Tests for haute.codegen - graph JSON → Python code generation."""

from __future__ import annotations

from haute._types import GraphNode, PipelineGraph
from haute.codegen import _build_params, _node_to_code, graph_to_code


def _n(d: dict) -> GraphNode:
    """Shorthand: dict → GraphNode."""
    return GraphNode.model_validate(d)


def _g(d: dict) -> PipelineGraph:
    """Shorthand: dict → PipelineGraph."""
    return PipelineGraph.model_validate(d)


# ---------------------------------------------------------------------------
# _build_params
# ---------------------------------------------------------------------------

class TestBuildParams:
    def test_no_sources(self):
        assert _build_params([]) == "df: pl.LazyFrame"

    def test_single_source(self):
        assert _build_params(["load_data"]) == "load_data: pl.LazyFrame"

    def test_multiple_sources(self):
        result = _build_params(["a", "b"])
        assert result == "a: pl.LazyFrame, b: pl.LazyFrame"


# ---------------------------------------------------------------------------
# _node_to_code
# ---------------------------------------------------------------------------

def _compile_node_code(code: str) -> None:
    """Verify generated node code compiles inside a pipeline context."""
    wrapper = (
        "import polars as pl\nimport haute\n"
        "pipeline = haute.Pipeline('test')\n\n"
        f"{code}\n"
    )
    compile(wrapper, "<test>", "exec")


class TestNodeToCode:
    def test_data_source_parquet(self):
        node = _n({
            "id": "src",
            "data": {
                "label": "Load Data",
                "nodeType": "dataSource",
                "config": {"path": "data/input.parquet"},
            },
        })
        code = _node_to_code(node)
        assert "def Load_Data()" in code
        assert 'scan_parquet("data/input.parquet")' in code
        assert '@pipeline.node(path="data/input.parquet")' in code
        _compile_node_code(code)

    def test_data_source_csv(self):
        node = _n({
            "id": "src",
            "data": {
                "label": "CSV Source",
                "nodeType": "dataSource",
                "config": {"path": "data/input.csv"},
            },
        })
        code = _node_to_code(node)
        assert 'scan_csv("data/input.csv")' in code
        assert "def CSV_Source()" in code
        _compile_node_code(code)

    def test_data_source_databricks(self):
        node = _n({
            "id": "src",
            "data": {
                "label": "DB Source",
                "nodeType": "dataSource",
                "config": {"sourceType": "databricks", "table": "catalog.schema.tbl"},
            },
        })
        code = _node_to_code(node)
        assert "read_cached_table" in code
        assert "catalog.schema.tbl" in code
        _compile_node_code(code)

    def test_transform_with_code(self):
        node = _n({
            "id": "t",
            "data": {
                "label": "Clean",
                "nodeType": "transform",
                "config": {"code": ".filter(pl.col('x') > 0)"},
            },
        })
        code = _node_to_code(node, source_names=["load_data"])
        assert "def Clean(load_data: pl.LazyFrame)" in code
        assert "filter" in code
        assert "return df" in code
        _compile_node_code(code)

    def test_transform_without_code_uses_first_source(self):
        node = _n({
            "id": "t",
            "data": {"label": "Pass", "nodeType": "transform", "config": {}},
        })
        code = _node_to_code(node, source_names=["upstream"])
        assert "def Pass(upstream: pl.LazyFrame)" in code
        assert "return upstream" in code
        _compile_node_code(code)

    def test_transform_without_code_no_sources_returns_df(self):
        node = _n({
            "id": "t",
            "data": {"label": "Pass", "nodeType": "transform", "config": {}},
        })
        code = _node_to_code(node, source_names=[])
        assert "def Pass(df: pl.LazyFrame)" in code
        assert "return df" in code
        _compile_node_code(code)

    def test_output_with_fields(self):
        node = _n({
            "id": "out",
            "data": {
                "label": "Output",
                "nodeType": "output",
                "config": {"fields": ["a", "b"]},
            },
        })
        code = _node_to_code(node, source_names=["transform"])
        assert "output=True" in code
        assert 'fields=["a", "b"]' in code or "fields=['a', 'b']" in code
        assert "transform.select(" in code
        assert "def Output(transform: pl.LazyFrame)" in code
        _compile_node_code(code)

    def test_output_without_fields(self):
        node = _n({
            "id": "out",
            "data": {
                "label": "Final",
                "nodeType": "output",
                "config": {"fields": []},
            },
        })
        code = _node_to_code(node, source_names=["src"])
        assert "return src" in code
        assert ".select" not in code
        _compile_node_code(code)

    def test_sink_parquet(self):
        node = _n({
            "id": "s",
            "data": {
                "label": "Write",
                "nodeType": "dataSink",
                "config": {"path": "out.parquet", "format": "parquet"},
            },
        })
        code = _node_to_code(node, source_names=["transform"])
        assert 'write_parquet("out.parquet")' in code
        assert "def Write(transform: pl.LazyFrame)" in code
        _compile_node_code(code)

    def test_sink_csv(self):
        node = _n({
            "id": "s",
            "data": {
                "label": "Write CSV",
                "nodeType": "dataSink",
                "config": {"path": "out.csv", "format": "csv"},
            },
        })
        code = _node_to_code(node)
        assert 'write_csv("out.csv")' in code
        _compile_node_code(code)

    def test_model_score(self):
        node = _n({
            "id": "ms",
            "data": {
                "label": "Score",
                "nodeType": "modelScore",
                "config": {"model_uri": "models:/my_model/1"},
            },
        })
        code = _node_to_code(node)
        assert 'model_uri="models:/my_model/1"' in code
        assert "def Score(df: pl.LazyFrame)" in code
        _compile_node_code(code)

    def test_rating_step(self):
        node = _n({
            "id": "rs",
            "data": {
                "label": "Lookup",
                "nodeType": "ratingStep",
                "config": {"tables": [{
                    "name": "Region",
                    "factors": ["region"],
                    "outputColumn": "region_factor",
                    "defaultValue": 1.0,
                    "entries": [{"region": "North", "value": 1.1}],
                }]},
            },
        })
        code = _node_to_code(node)
        assert "tables=" in code
        assert "output_column" in code
        _compile_node_code(code)

    def test_external_file_pickle(self):
        node = _n({
            "id": "ext",
            "data": {
                "label": "Model",
                "nodeType": "externalFile",
                "config": {"path": "model.pkl", "fileType": "pickle", "code": "df = obj.predict(df)"},
            },
        })
        code = _node_to_code(node, source_names=["features"])
        assert 'external="model.pkl"' in code
        assert "load_external_object" in code
        assert "obj" in code
        _compile_node_code(code)

    def test_external_file_catboost(self):
        node = _n({
            "id": "ext",
            "data": {
                "label": "CB Model",
                "nodeType": "externalFile",
                "config": {
                    "path": "model.cbm", "fileType": "catboost",
                    "modelClass": "regressor", "code": "df = obj.predict(df)",
                },
            },
        })
        code = _node_to_code(node)
        assert "load_external_object" in code
        assert 'model_class="regressor"' in code
        _compile_node_code(code)


# ---------------------------------------------------------------------------
# graph_to_code
# ---------------------------------------------------------------------------

class TestGraphToCode:
    def test_generates_valid_python(self):
        graph = _g({
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
        })
        code = graph_to_code(graph, pipeline_name="test_pipe")
        assert "import polars as pl" in code
        assert "import haute" in code
        assert 'Pipeline("test_pipe"' in code
        assert "def Source()" in code
        assert "def Transform(Source: pl.LazyFrame)" in code
        assert 'pipeline.connect("Source", "Transform")' in code
        compile(code, "<test>", "exec")

    def test_preamble_positioned_before_pipeline_def(self):
        graph = _g({
            "nodes": [{"id": "s", "data": {"label": "S", "nodeType": "dataSource", "config": {"path": "d.parquet"}}}],
            "edges": [],
        })
        code = graph_to_code(graph, preamble="import numpy as np")
        lines = code.splitlines()
        preamble_idx = next(i for i, line in enumerate(lines) if "numpy" in line)
        pipeline_idx = next(i for i, line in enumerate(lines) if "haute.Pipeline(" in line)
        assert preamble_idx < pipeline_idx, "Preamble must appear before pipeline definition"
        compile(code, "<test>", "exec")

    def test_empty_graph(self):
        code = graph_to_code(_g({"nodes": [], "edges": []}))
        assert "import polars as pl" in code
        assert "import haute" in code
        assert "Pipeline" in code
        # No nodes, so no @pipeline.node or pipeline.connect
        assert "@pipeline.node" not in code
        assert "pipeline.connect" not in code
        compile(code, "<test>", "exec")

    def test_description_included(self):
        graph = _g({"nodes": [], "edges": []})
        code = graph_to_code(graph, pipeline_name="p", description="Motor pricing")
        assert 'description="Motor pricing"' in code

    def test_multi_node_pipeline_compiles(self):
        """Full 3-node graph with edges generates compilable code."""
        graph = _g({
            "nodes": [
                {"id": "a", "data": {"label": "Read", "nodeType": "dataSource", "config": {"path": "d.parquet"}}},
                {"id": "b", "data": {"label": "Clean", "nodeType": "transform", "config": {"code": ".drop_nulls()"}}},
                {"id": "c", "data": {"label": "Out", "nodeType": "output", "config": {"fields": ["x"]}}},
            ],
            "edges": [
                {"id": "e1", "source": "a", "target": "b"},
                {"id": "e2", "source": "b", "target": "c"},
            ],
        })
        code = graph_to_code(graph)
        compile(code, "<test>", "exec")
        # Verify edges are emitted
        assert 'pipeline.connect("Read", "Clean")' in code
        assert 'pipeline.connect("Clean", "Out")' in code
