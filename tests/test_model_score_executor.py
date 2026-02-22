"""Tests for MODEL_SCORE executor node."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl
import pytest

from haute._types import GraphEdge, GraphNode, NodeData, PipelineGraph
from haute.executor import _build_node_fn, execute_graph
from tests.conftest import make_edge, make_graph


def _make_model_score_graph(
    *,
    source_type: str = "run",
    run_id: str = "abc123",
    artifact_path: str = "model.cbm",
    registered_model: str = "",
    version: str = "latest",
    task: str = "regression",
    output_column: str = "prediction",
    code: str = "",
    data_path: str = "",
) -> PipelineGraph:
    """Build a 2-node graph: dataSource → modelScore."""
    config = {
        "sourceType": source_type,
        "run_id": run_id,
        "artifact_path": artifact_path,
        "registered_model": registered_model,
        "version": version,
        "task": task,
        "output_column": output_column,
        "code": code,
    }
    return make_graph({
        "nodes": [
            {
                "id": "source",
                "data": {"label": "source", "nodeType": "dataSource", "config": {"path": data_path}},
            },
            {
                "id": "score",
                "data": {"label": "score", "nodeType": "modelScore", "config": config},
            },
        ],
        "edges": [make_edge("source", "score").model_dump()],
    })


@pytest.fixture()
def sample_data(tmp_path):
    """Create a small parquet file with features."""
    df = pl.DataFrame({
        "x1": [1.0, 2.0, 3.0, 4.0, 5.0],
        "x2": [10.0, 20.0, 30.0, 40.0, 50.0],
        "id": [1, 2, 3, 4, 5],
    })
    path = tmp_path / "data.parquet"
    df.write_parquet(path)
    return str(path)


def _make_mock_model(task: str = "regression", feature_names: list[str] | None = None):
    """Create a mock CatBoost model."""
    model = MagicMock()
    model.feature_names_ = feature_names or ["x1", "x2"]
    model.predict.return_value = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    if task == "classification":
        model.predict_proba.return_value = np.array([
            [0.9, 0.1],
            [0.8, 0.2],
            [0.3, 0.7],
            [0.6, 0.4],
            [0.2, 0.8],
        ])
    return model


# ---------------------------------------------------------------------------
# Auto-predict regression
# ---------------------------------------------------------------------------


class TestModelScoreAutoPredictRegression:
    def test_adds_prediction_column(self, sample_data):
        """Model score node adds prediction column to output."""
        mock_model = _make_mock_model("regression")

        graph = _make_model_score_graph(data_path=sample_data)
        with patch("haute._mlflow_io.load_mlflow_model", return_value=mock_model):
            results = execute_graph(graph, target_node_id="score", row_limit=100)

        assert results["score"]["status"] == "ok"
        cols = [c["name"] for c in results["score"]["columns"]]
        assert "prediction" in cols
        assert results["score"]["row_count"] == 5

    def test_custom_output_column(self, sample_data):
        """Custom output_column name is used."""
        mock_model = _make_mock_model("regression")

        graph = _make_model_score_graph(data_path=sample_data, output_column="my_pred")
        with patch("haute._mlflow_io.load_mlflow_model", return_value=mock_model):
            results = execute_graph(graph, target_node_id="score", row_limit=100)

        cols = [c["name"] for c in results["score"]["columns"]]
        assert "my_pred" in cols


# ---------------------------------------------------------------------------
# Classification with proba
# ---------------------------------------------------------------------------


class TestModelScoreClassification:
    def test_adds_proba_column(self, sample_data):
        """Classification model adds _proba column."""
        mock_model = _make_mock_model("classification")

        graph = _make_model_score_graph(
            data_path=sample_data, task="classification",
        )
        with patch("haute._mlflow_io.load_mlflow_model", return_value=mock_model):
            results = execute_graph(graph, target_node_id="score", row_limit=100)

        assert results["score"]["status"] == "ok"
        cols = [c["name"] for c in results["score"]["columns"]]
        assert "prediction" in cols
        assert "prediction_proba" in cols


# ---------------------------------------------------------------------------
# Passthrough when no config
# ---------------------------------------------------------------------------


class TestModelScorePassthrough:
    def test_empty_config_passthrough(self, sample_data):
        """Empty config (no sourceType) acts as passthrough."""
        graph = make_graph({
            "nodes": [
                {
                    "id": "source",
                    "data": {"label": "source", "nodeType": "dataSource", "config": {"path": sample_data}},
                },
                {
                    "id": "score",
                    "data": {"label": "score", "nodeType": "modelScore", "config": {}},
                },
            ],
            "edges": [make_edge("source", "score").model_dump()],
        })
        results = execute_graph(graph, target_node_id="score", row_limit=100)
        assert results["score"]["status"] == "ok"
        # Same columns as source (passthrough)
        cols = [c["name"] for c in results["score"]["columns"]]
        assert "x1" in cols
        assert "prediction" not in cols

    def test_run_without_run_id_passthrough(self, sample_data):
        """sourceType=run without run_id is a passthrough."""
        graph = _make_model_score_graph(
            data_path=sample_data, source_type="run", run_id="",
        )
        results = execute_graph(graph, target_node_id="score", row_limit=100)
        assert results["score"]["status"] == "ok"
        cols = [c["name"] for c in results["score"]["columns"]]
        assert "prediction" not in cols

    def test_registered_without_model_passthrough(self, sample_data):
        """sourceType=registered without registered_model is a passthrough."""
        graph = _make_model_score_graph(
            data_path=sample_data, source_type="registered",
            registered_model="", run_id="",
        )
        results = execute_graph(graph, target_node_id="score", row_limit=100)
        assert results["score"]["status"] == "ok"
        cols = [c["name"] for c in results["score"]["columns"]]
        assert "prediction" not in cols


# ---------------------------------------------------------------------------
# Missing features
# ---------------------------------------------------------------------------


class TestModelScoreMissingFeatures:
    def test_partial_features(self, sample_data):
        """Model features not in input are skipped; CatBoost gets available ones."""
        mock_model = _make_mock_model("regression", feature_names=["x1", "x2", "x_missing"])
        mock_model.predict.return_value = np.array([1.0, 2.0, 3.0, 4.0, 5.0])

        graph = _make_model_score_graph(data_path=sample_data)
        with patch("haute._mlflow_io.load_mlflow_model", return_value=mock_model):
            results = execute_graph(graph, target_node_id="score", row_limit=100)

        assert results["score"]["status"] == "ok"
        # Verify predict was called with only the available features
        call_args = mock_model.predict.call_args
        X = call_args[0][0]
        assert list(X.columns) == ["x1", "x2"]


# ---------------------------------------------------------------------------
# Post-processing code
# ---------------------------------------------------------------------------


class TestModelScorePostProcessing:
    def test_with_postprocessing_code(self, sample_data):
        """User code modifies output after prediction."""
        mock_model = _make_mock_model("regression")

        graph = _make_model_score_graph(
            data_path=sample_data,
            code='df = df.with_columns(doubled=pl.col("prediction") * 2)',
        )
        with patch("haute._mlflow_io.load_mlflow_model", return_value=mock_model):
            results = execute_graph(graph, target_node_id="score", row_limit=100)

        assert results["score"]["status"] == "ok"
        cols = [c["name"] for c in results["score"]["columns"]]
        assert "prediction" in cols
        assert "doubled" in cols

    def test_model_accessible_in_postprocessing_scope(self, sample_data):
        """User post-processing code can access the loaded model object."""
        mock_model = _make_mock_model("regression")

        graph = _make_model_score_graph(
            data_path=sample_data,
            code='df = df.with_columns(n_features=pl.lit(len(model.feature_names_)))',
        )
        with patch("haute._mlflow_io.load_mlflow_model", return_value=mock_model):
            results = execute_graph(graph, target_node_id="score", row_limit=100)

        assert results["score"]["status"] == "ok"
        cols = [c["name"] for c in results["score"]["columns"]]
        assert "n_features" in cols


# ---------------------------------------------------------------------------
# _build_node_fn unit tests
# ---------------------------------------------------------------------------


class TestBuildNodeFnModelScore:
    def test_build_node_fn_returns_passthrough_no_config(self):
        """_build_node_fn returns passthrough for empty modelScore config."""
        node = GraphNode(
            id="ms1",
            data=NodeData(label="scorer", nodeType="modelScore", config={}),
        )
        func_name, fn, is_source = _build_node_fn(node, source_names=["upstream"])
        assert func_name == "scorer"
        assert is_source is False
        # Passthrough returns input
        lf = pl.DataFrame({"a": [1]}).lazy()
        result = fn(lf)
        assert result.collect().to_dicts() == [{"a": 1}]

    def test_build_node_fn_returns_scoring_fn(self):
        """_build_node_fn returns a scoring function when config is present."""
        node = GraphNode(
            id="ms1",
            data=NodeData(
                label="scorer",
                nodeType="modelScore",
                config={
                    "sourceType": "run",
                    "run_id": "abc",
                    "artifact_path": "model.cbm",
                    "task": "regression",
                    "output_column": "pred",
                },
            ),
        )
        func_name, fn, is_source = _build_node_fn(node, source_names=["upstream"])
        assert func_name == "scorer"
        assert is_source is False
        # fn should be the model_score_fn (not passthrough) — callable
        assert callable(fn)
