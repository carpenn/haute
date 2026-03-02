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
                ["def Load_Data()", 'scan_parquet("data/input.parquet")', 'config="config/datasource/Load_Data.json"'],
                id="parquet",
            ),
            pytest.param(
                "CSV Source",
                {"path": "data/input.csv"},
                ['scan_csv("data/input.csv")', "def CSV_Source()", 'config="config/datasource/CSV_Source.json"'],
                id="csv",
            ),
            pytest.param(
                "DB Source",
                {"sourceType": "databricks", "table": "catalog.schema.tbl"},
                ["read_cached_table", "catalog.schema.tbl", 'config="config/datasource/DB_Source.json"'],
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
        assert 'config="config/output/Output.json"' in code
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
        assert 'config="config/model_score/Score.json"' in code
        # Body still contains the model loading logic
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
        assert 'config="config/tables/Lookup.json"' in code
        assert "def Lookup(" in code
        assert "return df" in code
        _compile_node_code(code)

    @pytest.mark.parametrize(
        "label, config, source_names, expected_strings",
        [
            pytest.param(
                "Model",
                {"path": "model.pkl", "fileType": "pickle", "code": "df = obj.predict(df)"},
                ["features"],
                ['config="config/external_model/Model.json"', "load_external_object", "obj"],
                id="pickle",
            ),
            pytest.param(
                "CB Model",
                {"path": "model.cbm", "fileType": "catboost", "modelClass": "regressor", "code": "df = obj.predict(df)"},
                [],
                ['config="config/external_model/CB_Model.json"', "load_external_object", '"catboost", "regressor"'],
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
    def _switch_node(self, scenario_map=None):
        if scenario_map is None:
            scenario_map = {"live_src": "live", "batch_src": "test_batch"}
        return _n({
            "id": "switch",
            "data": {
                "label": "Switch",
                "nodeType": "liveSwitch",
                "config": {"input_scenario_map": scenario_map, "inputs": ["live_src", "batch_src"]},
            },
        })

    def test_emits_config_ref_with_live_active(self):
        code = _node_to_code(self._switch_node(), source_names=["live_src", "batch_src"])
        assert 'config="config/live_switch/Switch.json"' in code
        assert "return live_src" in code
        _compile_node_code(code)

    def test_emits_config_ref_with_no_live_mapping(self):
        code = _node_to_code(
            self._switch_node({"live_src": "test_batch", "batch_src": "prod"}),
            source_names=["live_src", "batch_src"],
        )
        assert 'config="config/live_switch/Switch.json"' in code
        # Falls back to first param when no input mapped to "live"
        assert "return live_src" in code
        _compile_node_code(code)

    def test_round_trip_preserves_scenario_map(self, tmp_path):
        """Codegen → parse round-trip must preserve input_scenario_map."""
        import json

        scenario_map = {"live_src": "live", "batch_src": "test_batch"}
        node = self._switch_node(scenario_map)
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
        # Write config JSON files so the parser can resolve them
        cfg_dir = tmp_path / "config" / "live_switch"
        cfg_dir.mkdir(parents=True)
        (cfg_dir / "Switch.json").write_text(json.dumps({"input_scenario_map": scenario_map, "inputs": ["live_src", "batch_src"]}))
        for name in ("live_src", "batch_src"):
            ds_dir = tmp_path / "config" / "datasource"
            ds_dir.mkdir(parents=True, exist_ok=True)
            (ds_dir / f"{name}.json").write_text(json.dumps({"path": "a.parquet"}))

        py_file = tmp_path / "test.py"
        py_file.write_text(full_code)
        from haute.parser import parse_pipeline_file
        graph = parse_pipeline_file(py_file)
        switch_nodes = [n for n in graph.nodes if n.data.nodeType == "liveSwitch"]
        assert len(switch_nodes) == 1
        assert switch_nodes[0].data.config["input_scenario_map"] == scenario_map


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
    """Ensure the global active_scenario is 'live' before committing.

    The scenario is now a global setting stored in the .haute.json sidecar,
    not a per-node config value. If this test fails, someone left the
    active_scenario on a non-live value — reset it to 'live' before committing.
    """

    @pytest.mark.parametrize("pipeline_file", _find_pipeline_files(), ids=lambda p: str(p.relative_to(PROJECT_ROOT)))
    def test_active_scenario_is_live(self, pipeline_file: Path):
        import json
        sidecar = pipeline_file.with_suffix(".haute.json")
        if not sidecar.exists():
            return  # no sidecar → defaults to "live", nothing to check
        data = json.loads(sidecar.read_text())
        active = data.get("active_scenario", "live")
        assert active == "live", (
            f"{sidecar.relative_to(PROJECT_ROOT)}: active_scenario is "
            f"'{active}' — must be 'live' before committing."
        )
