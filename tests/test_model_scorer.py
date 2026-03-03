"""Comprehensive tests for haute._model_scorer.

Covers:
  - ModelScorer construction and defaults
  - ModelScorer.score — eager vs batched routing
  - ModelScorer._score_eager delegation
  - _sink_to_temp helper
  - _batch_score_to_parquet helper
  - Error cases: missing model features, empty input

All MLflow and CatBoost dependencies are mocked.
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl

from haute._model_scorer import (
    ModelScorer,
    _batch_score_to_parquet,
    _sink_to_temp,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_model(
    feature_names: list[str] | None = None,
    predictions: Any = None,
    probas: Any = None,
) -> MagicMock:
    """Create a mock model with predict / predict_proba / feature_names_."""
    model = MagicMock()
    model.feature_names_ = feature_names or ["a", "b"]
    if predictions is not None:
        model.predict.return_value = np.array(predictions)
    else:
        model.predict.return_value = np.array([0.5, 0.6, 0.7])
    if probas is not None:
        model.predict_proba.return_value = np.array(probas)
    else:
        # Default: no predict_proba
        del model.predict_proba
    return model


# ===========================================================================
# ModelScorer construction
# ===========================================================================


class TestModelScorerInit:
    def test_defaults(self):
        scorer = ModelScorer(source_type="run")
        assert scorer.source_type == "run"
        assert scorer.run_id == ""
        assert scorer.artifact_path == ""
        assert scorer.registered_model == ""
        assert scorer.version == "latest"
        assert scorer.task == "regression"
        assert scorer.output_col == "prediction"
        assert scorer.code == ""
        assert scorer.source_names == []
        assert scorer.scenario == "live"
        assert scorer.row_limit is None

    def test_custom_values(self):
        scorer = ModelScorer(
            source_type="registered",
            registered_model="my_model",
            version="3",
            task="classification",
            output_col="pred",
            code="x = 1",
            source_names=["df1", "df2"],
            scenario="test_batch",
            row_limit=100,
        )
        assert scorer.source_type == "registered"
        assert scorer.registered_model == "my_model"
        assert scorer.version == "3"
        assert scorer.task == "classification"
        assert scorer.source_names == ["df1", "df2"]
        assert scorer.row_limit == 100

    def test_source_names_none_becomes_empty_list(self):
        scorer = ModelScorer(source_type="run", source_names=None)
        assert scorer.source_names == []


# ===========================================================================
# ModelScorer.score — routing logic
# ===========================================================================


class TestModelScorerScore:
    @patch("haute._model_scorer.ModelScorer._score_eager")
    @patch("haute._mlflow_io.load_mlflow_model")
    def test_live_scenario_uses_eager(self, mock_load, mock_eager):
        model = _make_mock_model()
        mock_load.return_value = model
        mock_eager.return_value = pl.DataFrame({"x": [1], "prediction": [0.5]}).lazy()

        scorer = ModelScorer(source_type="run", run_id="abc", scenario="live")
        lf = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        scorer.score(lf)

        mock_eager.assert_called_once()

    @patch("haute._model_scorer.ModelScorer._score_batched")
    @patch("haute._mlflow_io.load_mlflow_model")
    def test_non_live_scenario_uses_batched(self, mock_load, mock_batched):
        model = _make_mock_model()
        mock_load.return_value = model
        mock_batched.return_value = pl.DataFrame({"x": [1]}).lazy()

        scorer = ModelScorer(source_type="run", run_id="abc", scenario="batch")
        lf = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        scorer.score(lf)

        mock_batched.assert_called_once()

    @patch("haute._model_scorer.ModelScorer._score_eager")
    @patch("haute._mlflow_io.load_mlflow_model")
    def test_row_limit_forces_eager(self, mock_load, mock_eager):
        """Even non-live scenario uses eager when row_limit is set."""
        model = _make_mock_model()
        mock_load.return_value = model
        mock_eager.return_value = pl.DataFrame({"x": [1]}).lazy()

        scorer = ModelScorer(source_type="run", run_id="abc", scenario="batch", row_limit=10)
        lf = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        scorer.score(lf)

        mock_eager.assert_called_once()

    @patch("haute._mlflow_io.load_mlflow_model")
    def test_feature_intersection(self, mock_load):
        """Only features present in both model and input are used."""
        model = _make_mock_model(feature_names=["a", "b", "missing_col"])
        mock_load.return_value = model

        scorer = ModelScorer(source_type="run", run_id="abc", scenario="live")

        with patch.object(scorer, "_score_eager") as mock_eager:
            mock_eager.return_value = pl.DataFrame({"a": [1], "prediction": [0.5]}).lazy()
            lf = pl.DataFrame({"a": [1], "b": [2]}).lazy()
            scorer.score(lf)

            # Features passed should be ["a", "b"] (intersection)
            call_args = mock_eager.call_args
            features = call_args[0][2]
            assert "a" in features
            assert "b" in features
            assert "missing_col" not in features

    @patch("haute._mlflow_io.load_mlflow_model")
    def test_empty_input_doesnt_crash(self, mock_load):
        """score() with no dfs should use an empty LazyFrame."""
        model = _make_mock_model(feature_names=[])
        model.predict.return_value = np.array([])
        mock_load.return_value = model

        scorer = ModelScorer(source_type="run", run_id="abc", scenario="live")

        with patch.object(scorer, "_score_eager") as mock_eager:
            mock_eager.return_value = pl.LazyFrame()
            scorer.score()  # no dfs passed
            mock_eager.assert_called_once()

    @patch("haute.executor._exec_user_code")
    @patch("haute._mlflow_io.load_mlflow_model")
    def test_user_code_applied_after_scoring(self, mock_load, mock_exec):
        """Post-processing user code should be applied after scoring."""
        model = _make_mock_model()
        mock_load.return_value = model
        mock_exec.return_value = pl.DataFrame({"result": [1]}).lazy()

        scorer = ModelScorer(
            source_type="run", run_id="abc", scenario="live",
            code="result = result * 2",
            source_names=["df"],
        )
        with patch.object(scorer, "_score_eager") as mock_eager:
            mock_eager.return_value = pl.DataFrame({"x": [1]}).lazy()
            scorer.score(pl.DataFrame({"a": [1], "b": [2]}).lazy())

        mock_exec.assert_called_once()
        # Verify model is in extra_ns
        call_kwargs = mock_exec.call_args
        assert "model" in call_kwargs[1]["extra_ns"]


# ===========================================================================
# _sink_to_temp
# ===========================================================================


class TestSinkToTemp:
    def test_basic_sink(self):
        lf = pl.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}).lazy()
        path = _sink_to_temp(lf)
        try:
            assert os.path.exists(path)
            assert path.endswith(".parquet")
            df = pl.read_parquet(path)
            assert len(df) == 3
            assert set(df.columns) == {"x", "y"}
        finally:
            os.unlink(path)

    def test_fallback_on_sink_failure(self):
        """If sink_parquet fails, fallback to collect+write_parquet."""
        lf = MagicMock(spec=pl.LazyFrame)
        lf.sink_parquet.side_effect = Exception("sink failed")

        # Set up the fallback path
        mock_df = pl.DataFrame({"x": [1]})
        lf.collect.return_value = mock_df

        path = _sink_to_temp(lf)
        try:
            assert os.path.exists(path)
            df = pl.read_parquet(path)
            assert len(df) == 1
        finally:
            os.unlink(path)


# ===========================================================================
# _batch_score_to_parquet
# ===========================================================================


class TestBatchScoreToParquet:
    def test_regression_scoring(self, tmp_path):
        """Batch scoring produces output parquet with prediction column."""
        input_path = str(tmp_path / "input.parquet")
        df = pl.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
        df.write_parquet(input_path)

        model = _make_mock_model(
            feature_names=["a", "b"],
            predictions=np.array([0.1, 0.2, 0.3]),
        )

        with patch("haute._mlflow_io._prepare_predict_frame") as mock_prep:
            mock_prep.return_value = df[["a", "b"]].to_pandas()
            out_path = _batch_score_to_parquet(
                model, input_path, ["a", "b"], "pred", "regression",
            )

        try:
            result = pl.read_parquet(out_path)
            assert "pred" in result.columns
            assert len(result) == 3
        finally:
            os.unlink(out_path)

    def test_classification_with_proba(self, tmp_path):
        """Classification with predict_proba produces both pred and pred_proba columns."""
        input_path = str(tmp_path / "input.parquet")
        df = pl.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        df.write_parquet(input_path)

        model = _make_mock_model(
            feature_names=["a", "b"],
            predictions=np.array([0, 1]),
            probas=np.array([[0.3, 0.7], [0.4, 0.6]]),
        )

        with patch("haute._mlflow_io._prepare_predict_frame") as mock_prep:
            mock_prep.return_value = df[["a", "b"]].to_pandas()
            out_path = _batch_score_to_parquet(
                model, input_path, ["a", "b"], "pred", "classification",
            )

        try:
            result = pl.read_parquet(out_path)
            assert "pred" in result.columns
            assert "pred_proba" in result.columns
            assert len(result) == 2
        finally:
            os.unlink(out_path)

    def test_classification_without_proba(self, tmp_path):
        """Classification model without predict_proba only produces pred column."""
        input_path = str(tmp_path / "input.parquet")
        df = pl.DataFrame({"a": [1.0], "b": [2.0]})
        df.write_parquet(input_path)

        model = _make_mock_model(
            feature_names=["a", "b"],
            predictions=np.array([1]),
        )
        # No predict_proba

        with patch("haute._mlflow_io._prepare_predict_frame") as mock_prep:
            mock_prep.return_value = df[["a", "b"]].to_pandas()
            out_path = _batch_score_to_parquet(
                model, input_path, ["a", "b"], "pred", "classification",
            )

        try:
            result = pl.read_parquet(out_path)
            assert "pred" in result.columns
            assert "pred_proba" not in result.columns
        finally:
            os.unlink(out_path)
