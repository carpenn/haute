"""Tests for haute.codegen - graph JSON → Python code generation."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from haute.codegen import _build_params, _node_to_code, graph_to_code
from haute.parser import parse_pipeline_source
from tests.conftest import make_graph as _g, make_node as _n

PROJECT_ROOT = Path(__file__).resolve().parent.parent


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
    @pytest.mark.parametrize(
        "label, config, expected_strings",
        [
            pytest.param(
                "Load Data",
                {"path": "data/input.parquet"},
                ["def Load_Data()", 'scan_parquet("data/input.parquet")', '@pipeline.node(path="data/input.parquet")'],
                id="parquet",
            ),
            pytest.param(
                "CSV Source",
                {"path": "data/input.csv"},
                ['scan_csv("data/input.csv")', "def CSV_Source()"],
                id="csv",
            ),
            pytest.param(
                "DB Source",
                {"sourceType": "databricks", "table": "catalog.schema.tbl"},
                ["read_cached_table", "catalog.schema.tbl"],
                id="databricks",
            ),
        ],
    )
    def test_data_source(self, label, config, expected_strings):
        node = _n({
            "id": "src",
            "data": {"label": label, "nodeType": "dataSource", "config": config},
        })
        code = _node_to_code(node)
        for s in expected_strings:
            assert s in code, f"Expected {s!r} in generated code"
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
                "config": {
                    "sourceType": "run",
                    "run_id": "abc123",
                    "artifact_path": "model.cbm",
                    "task": "regression",
                    "output_column": "prediction",
                },
            },
        })
        code = _node_to_code(node)
        assert 'source_type="run"' in code
        assert 'run_id="abc123"' in code
        assert "load_mlflow_model" in code
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

    @pytest.mark.parametrize(
        "label, config, source_names, expected_strings",
        [
            pytest.param(
                "Model",
                {"path": "model.pkl", "fileType": "pickle", "code": "df = obj.predict(df)"},
                ["features"],
                ['external="model.pkl"', "load_external_object", "obj"],
                id="pickle",
            ),
            pytest.param(
                "CB Model",
                {"path": "model.cbm", "fileType": "catboost", "modelClass": "regressor", "code": "df = obj.predict(df)"},
                [],
                ["load_external_object", 'model_class="regressor"'],
                id="catboost",
            ),
        ],
    )
    def test_external_file(self, label, config, source_names, expected_strings):
        node = _n({
            "id": "ext",
            "data": {"label": label, "nodeType": "externalFile", "config": config},
        })
        code = _node_to_code(node, source_names=source_names) if source_names else _node_to_code(node)
        for s in expected_strings:
            assert s in code, f"Expected {s!r} in generated code"
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


# ---------------------------------------------------------------------------
# Live switch codegen
# ---------------------------------------------------------------------------


class TestLiveSwitchCodegen:
    def _switch_node(self, mode="live"):
        return _n({
            "id": "switch",
            "data": {
                "label": "Switch",
                "nodeType": "liveSwitch",
                "config": {"mode": mode, "inputs": ["live_src", "batch_src"]},
            },
        })

    def test_live_mode_emits_mode_kwarg(self):
        code = _node_to_code(self._switch_node("live"), source_names=["live_src", "batch_src"])
        assert 'mode="live"' in code
        assert "return live_src" in code
        _compile_node_code(code)

    def test_non_live_mode_emits_mode_kwarg(self):
        code = _node_to_code(self._switch_node("batch_src"), source_names=["live_src", "batch_src"])
        assert 'mode="batch_src"' in code
        assert "return batch_src" in code
        _compile_node_code(code)

    def test_round_trip_preserves_live_mode(self):
        """Codegen → parse round-trip must preserve mode='live'."""
        node = self._switch_node("live")
        code = _node_to_code(node, source_names=["live_src", "batch_src"])
        full_code = (
            "import polars as pl\nimport haute\n"
            'pipeline = haute.Pipeline("test")\n\n'
            '@pipeline.node(path="a.parquet")\n'
            "def live_src() -> pl.LazyFrame:\n"
            '    return pl.scan_parquet("a.parquet")\n\n'
            '@pipeline.node(path="b.parquet")\n'
            "def batch_src() -> pl.LazyFrame:\n"
            '    return pl.scan_parquet("b.parquet")\n\n'
            f"{code}\n"
            'pipeline.connect("live_src", "Switch")\n'
            'pipeline.connect("batch_src", "Switch")\n'
        )
        graph = parse_pipeline_source(full_code)
        switch_nodes = [n for n in graph.nodes if n.data.nodeType == "liveSwitch"]
        assert len(switch_nodes) == 1
        assert switch_nodes[0].data.config["mode"] == "live"

    def test_round_trip_preserves_non_live_mode(self):
        """Codegen → parse round-trip must preserve non-live mode."""
        node = self._switch_node("batch_src")
        code = _node_to_code(node, source_names=["live_src", "batch_src"])
        full_code = (
            "import polars as pl\nimport haute\n"
            'pipeline = haute.Pipeline("test")\n\n'
            '@pipeline.node(path="a.parquet")\n'
            "def live_src() -> pl.LazyFrame:\n"
            '    return pl.scan_parquet("a.parquet")\n\n'
            '@pipeline.node(path="b.parquet")\n'
            "def batch_src() -> pl.LazyFrame:\n"
            '    return pl.scan_parquet("b.parquet")\n\n'
            f"{code}\n"
            'pipeline.connect("live_src", "Switch")\n'
            'pipeline.connect("batch_src", "Switch")\n'
        )
        graph = parse_pipeline_source(full_code)
        switch_nodes = [n for n in graph.nodes if n.data.nodeType == "liveSwitch"]
        assert len(switch_nodes) == 1
        assert switch_nodes[0].data.config["mode"] == "batch_src"


# ---------------------------------------------------------------------------
# Safety net: committed pipeline files must have live switch set to "live"
# ---------------------------------------------------------------------------

def _find_pipeline_files() -> list[Path]:
    """Find .py files containing live_switch=True (excluding tests and venv)."""
    results = []
    for py_file in PROJECT_ROOT.rglob("*.py"):
        rel = py_file.relative_to(PROJECT_ROOT)
        if rel.parts[0] in (".venv", "tests"):
            continue
        try:
            text = py_file.read_text()
        except (OSError, UnicodeDecodeError):
            continue
        if re.search(r"live_switch\s*=\s*True", text):
            results.append(py_file)
    return results


class TestLiveSwitchSafety:
    """Ensure no pipeline file is accidentally committed with mode != 'live'.

    If this test fails, a liveSwitch node is pointing at a non-live data
    source. Change the mode back to 'live' before committing.
    """

    @pytest.mark.parametrize("pipeline_file", _find_pipeline_files(), ids=lambda p: str(p.relative_to(PROJECT_ROOT)))
    def test_live_switch_mode_is_live(self, pipeline_file: Path):
        graph = parse_pipeline_source(pipeline_file.read_text(), source_file=str(pipeline_file))
        for node in graph.nodes:
            if node.data.nodeType == "liveSwitch":
                mode = node.data.config.get("mode", "live")
                assert mode == "live", (
                    f"{pipeline_file.relative_to(PROJECT_ROOT)}: liveSwitch node "
                    f"'{node.data.label}' has mode='{mode}' — must be 'live' before committing."
                )
