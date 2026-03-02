"""Tests for MODEL_SCORE codegen and parser round-trip."""

from __future__ import annotations

import pytest

from haute.graph_utils import GraphNode, NodeData, PipelineGraph
from haute.codegen import graph_to_code
from haute._parser_helpers import _build_node_config, _infer_node_type
from tests.conftest import make_edge, make_graph


# ---------------------------------------------------------------------------
# Code generation
# ---------------------------------------------------------------------------


class TestModelScoreCodegen:
    def test_codegen_run_based(self):
        """Generates correct code for run-based model scoring."""
        graph = make_graph({
            "nodes": [
                {
                    "id": "source",
                    "data": {"label": "source", "nodeType": "dataSource", "config": {"path": "data.parquet"}},
                },
                {
                    "id": "score",
                    "data": {
                        "label": "scorer",
                        "nodeType": "modelScore",
                        "config": {
                            "sourceType": "run",
                            "run_id": "abc123",
                            "artifact_path": "model.cbm",
                            "task": "regression",
                            "output_column": "prediction",
                        },
                    },
                },
            ],
            "edges": [make_edge("source", "score").model_dump()],
        })
        code = graph_to_code(graph)

        assert 'source_type="run"' in code
        assert 'run_id="abc123"' in code
        assert 'artifact_path="model.cbm"' in code
        assert 'task="regression"' in code
        assert "load_mlflow_model" in code
        assert "model.predict" in code
        assert 'pl.Series("prediction"' in code
        # Generated code must be valid Python
        compile(code, "<test_run_based>", "exec")
        # Upstream is named "source", template must alias lf = source
        assert "lf = source" in code

    def test_codegen_registered(self):
        """Generates correct code for registered model scoring."""
        graph = make_graph({
            "nodes": [
                {
                    "id": "source",
                    "data": {"label": "source", "nodeType": "dataSource", "config": {"path": "data.parquet"}},
                },
                {
                    "id": "score",
                    "data": {
                        "label": "scorer",
                        "nodeType": "modelScore",
                        "config": {
                            "sourceType": "registered",
                            "registered_model": "my-model",
                            "version": "3",
                            "task": "regression",
                            "output_column": "pred",
                        },
                    },
                },
            ],
            "edges": [make_edge("source", "score").model_dump()],
        })
        code = graph_to_code(graph)

        assert 'source_type="registered"' in code
        assert 'registered_model="my-model"' in code
        assert 'version="3"' in code
        assert "load_mlflow_model" in code
        assert 'pl.Series("pred"' in code
        compile(code, "<test_registered>", "exec")

    def test_codegen_classification_includes_proba(self):
        """Classification task includes predict_proba code."""
        graph = make_graph({
            "nodes": [
                {
                    "id": "source",
                    "data": {"label": "source", "nodeType": "dataSource", "config": {"path": "data.parquet"}},
                },
                {
                    "id": "score",
                    "data": {
                        "label": "scorer",
                        "nodeType": "modelScore",
                        "config": {
                            "sourceType": "run",
                            "run_id": "abc",
                            "artifact_path": "model.cbm",
                            "task": "classification",
                            "output_column": "prediction",
                        },
                    },
                },
            ],
            "edges": [make_edge("source", "score").model_dump()],
        })
        code = graph_to_code(graph)

        assert "predict_proba" in code
        assert "prediction_proba" in code
        compile(code, "<test_classification>", "exec")

    def test_codegen_regression_no_proba(self):
        """Regression task does NOT include predict_proba code."""
        graph = make_graph({
            "nodes": [
                {
                    "id": "source",
                    "data": {"label": "source", "nodeType": "dataSource", "config": {"path": "data.parquet"}},
                },
                {
                    "id": "score",
                    "data": {
                        "label": "scorer",
                        "nodeType": "modelScore",
                        "config": {
                            "sourceType": "run",
                            "run_id": "abc",
                            "artifact_path": "model.cbm",
                            "task": "regression",
                            "output_column": "prediction",
                        },
                    },
                },
            ],
            "edges": [make_edge("source", "score").model_dump()],
        })
        code = graph_to_code(graph)

        assert "predict_proba" not in code
        compile(code, "<test_regression>", "exec")


# ---------------------------------------------------------------------------
# Parser helpers — _infer_node_type
# ---------------------------------------------------------------------------


class TestInferNodeType:
    def test_model_score_flag(self):
        """model_score=True decorator kwarg → MODEL_SCORE."""
        nt = _infer_node_type({"model_score": True}, n_params=1)
        assert nt == "modelScore"

    def test_registered_model_kwarg(self):
        """registered_model kwarg → MODEL_SCORE."""
        nt = _infer_node_type({"registered_model": "my-model"}, n_params=1)
        assert nt == "modelScore"

    def test_source_type_and_run_id(self):
        """source_type + run_id kwargs → MODEL_SCORE."""
        nt = _infer_node_type({"source_type": "run", "run_id": "abc"}, n_params=1)
        assert nt == "modelScore"


# ---------------------------------------------------------------------------
# Parser helpers — _build_node_config
# ---------------------------------------------------------------------------


class TestBuildNodeConfigModelScore:
    def test_build_config_run_based(self):
        """Builds correct config from run-based decorator kwargs."""
        kwargs = {
            "model_score": True,
            "source_type": "run",
            "run_id": "abc123",
            "artifact_path": "model.cbm",
            "task": "regression",
            "output_column": "prediction",
        }
        config = _build_node_config("modelScore", kwargs, "", ["df"])

        assert config["sourceType"] == "run"
        assert config["run_id"] == "abc123"
        assert config["artifact_path"] == "model.cbm"
        assert config["task"] == "regression"
        assert config["output_column"] == "prediction"

    def test_build_config_registered(self):
        """Builds correct config from registered model decorator kwargs."""
        kwargs = {
            "model_score": True,
            "source_type": "registered",
            "registered_model": "my-model",
            "version": "2",
            "task": "classification",
            "output_column": "pred",
        }
        config = _build_node_config("modelScore", kwargs, "", ["df"])

        assert config["sourceType"] == "registered"
        assert config["registered_model"] == "my-model"
        assert config["version"] == "2"
        assert config["task"] == "classification"
        assert config["output_column"] == "pred"


# ---------------------------------------------------------------------------
# Parser round-trip
# ---------------------------------------------------------------------------


class TestParserRoundTrip:
    def test_codegen_parses_back(self, tmp_path):
        """Generated code can be parsed back into a graph."""
        from haute._config_io import collect_node_configs
        from haute.parser import parse_pipeline_source

        graph = make_graph({
            "nodes": [
                {
                    "id": "source",
                    "data": {"label": "source", "nodeType": "dataSource", "config": {"path": "data.parquet"}},
                },
                {
                    "id": "scorer",
                    "data": {
                        "label": "scorer",
                        "nodeType": "modelScore",
                        "config": {
                            "sourceType": "run",
                            "run_id": "run123",
                            "artifact_path": "model.cbm",
                            "task": "regression",
                            "output_column": "prediction",
                        },
                    },
                },
            ],
            "edges": [make_edge("source", "scorer").model_dump()],
        })
        code = graph_to_code(graph)

        # Write config files so the parser can resolve them
        for rel_path, content in collect_node_configs(graph).items():
            cfg_file = tmp_path / rel_path
            cfg_file.parent.mkdir(parents=True, exist_ok=True)
            cfg_file.write_text(content)

        # Parse it back
        parsed = parse_pipeline_source(code, _base_dir=tmp_path)
        node_map = {n.data.label: n for n in parsed.nodes}

        assert "scorer" in node_map
        scorer = node_map["scorer"]
        assert scorer.data.nodeType == "modelScore"
        assert scorer.data.config.get("sourceType") == "run"
        assert scorer.data.config.get("run_id") == "run123"
        # Auto-generated scaffolding must NOT leak into config["code"]
        assert scorer.data.config.get("code", "") == ""

    def test_roundtrip_preserves_user_code(self):
        """User post-processing code survives codegen → parse round-trip."""
        from haute.parser import parse_pipeline_source

        graph = make_graph({
            "nodes": [
                {
                    "id": "source",
                    "data": {"label": "source", "nodeType": "dataSource", "config": {"path": "data.parquet"}},
                },
                {
                    "id": "scorer",
                    "data": {
                        "label": "scorer",
                        "nodeType": "modelScore",
                        "config": {
                            "sourceType": "run",
                            "run_id": "run123",
                            "artifact_path": "model.cbm",
                            "task": "regression",
                            "output_column": "prediction",
                            "code": 'df = df.with_columns(doubled=pl.col("prediction") * 2)',
                        },
                    },
                },
            ],
            "edges": [make_edge("source", "scorer").model_dump()],
        })
        code = graph_to_code(graph)

        parsed = parse_pipeline_source(code)
        node_map = {n.data.label: n for n in parsed.nodes}
        scorer = node_map["scorer"]
        assert 'doubled' in scorer.data.config.get("code", "")
