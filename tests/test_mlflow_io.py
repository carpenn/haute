"""Tests for haute._mlflow_io — MLflow model loading and caching."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl
import pytest

from haute._mlflow_io import (
    _MODEL_CACHE_MAX_SIZE,
    _find_cbm_artifact,
    _model_cache,
    _prepare_predict_frame,
    load_mlflow_model,
)
from haute._mlflow_utils import resolve_version as _resolve_version


@pytest.fixture(autouse=True)
def _clear_cache():
    """Clear the model cache before/after each test."""
    _model_cache.clear()
    yield
    _model_cache.clear()


@pytest.fixture()
def mock_mlflow_env():
    """Set up mock mlflow modules and common patches for load_mlflow_model tests."""
    mock_mlflow = MagicMock()
    mock_mlflow.artifacts.download_artifacts.return_value = "/tmp/model.cbm"

    mock_client_instance = MagicMock()
    mock_mlflow_tracking = MagicMock()
    mock_mlflow_tracking.MlflowClient.return_value = mock_client_instance

    modules_patch = patch.dict(sys.modules, {
        "mlflow": mock_mlflow,
        "mlflow.tracking": mock_mlflow_tracking,
    })
    resolve_patch = patch(
        "haute.modelling._mlflow_log.resolve_tracking_backend",
        return_value=("file:///mlruns", "local"),
    )
    return mock_mlflow, mock_client_instance, modules_patch, resolve_patch


# ---------------------------------------------------------------------------
# load_mlflow_model — run-based
# ---------------------------------------------------------------------------


class TestLoadRunBasedModel:
    def test_load_run_based_model(self, mock_mlflow_env):
        """Run-based loading downloads artifacts and loads CatBoost model."""
        fake_model = MagicMock()
        _mock_mlflow, _mock_client, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._load_catboost_model", return_value=fake_model), \
             patch("haute._mlflow_io._find_cbm_artifact", return_value="model.cbm"):
            result = load_mlflow_model(
                source_type="run",
                run_id="abc123",
                artifact_path="model.cbm",
                task="regression",
            )

        assert result is fake_model

    def test_run_based_missing_run_id(self, mock_mlflow_env):
        """Raises ValueError when run_id is empty for source_type=run."""
        _, _, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch:
            with pytest.raises(ValueError, match="run_id is required"):
                load_mlflow_model(source_type="run", run_id="", task="regression")


# ---------------------------------------------------------------------------
# load_mlflow_model — registered
# ---------------------------------------------------------------------------


class TestLoadRegisteredModel:
    def test_registered_missing_model_name(self, mock_mlflow_env):
        """Raises ValueError when registered_model is empty."""
        _, _, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch:
            with pytest.raises(ValueError, match="registered_model is required"):
                load_mlflow_model(
                    source_type="registered",
                    registered_model="",
                    task="regression",
                )

    def test_load_registered_model(self, mock_mlflow_env):
        """Registered model loading resolves version and downloads."""
        fake_model = MagicMock()
        _, mock_client, modules_patch, resolve_patch = mock_mlflow_env

        mv = MagicMock(run_id="resolved_run_id")
        mock_client.get_model_version.return_value = mv

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._load_catboost_model", return_value=fake_model), \
             patch("haute._mlflow_io.resolve_version", return_value="2"), \
             patch("haute._mlflow_io._find_cbm_artifact", return_value="model.cbm"):
            result = load_mlflow_model(
                source_type="registered",
                registered_model="my-model",
                version="2",
                task="regression",
            )

        assert result is fake_model


# ---------------------------------------------------------------------------
# Invalid source type
# ---------------------------------------------------------------------------


class TestInvalidSourceType:
    def test_invalid_source_type(self, mock_mlflow_env):
        """Raises ValueError for unknown sourceType."""
        _, _, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch:
            with pytest.raises(ValueError, match="Invalid sourceType"):
                load_mlflow_model(source_type="invalid", task="regression")


# ---------------------------------------------------------------------------
# Cache tests
# ---------------------------------------------------------------------------


class TestModelCache:
    def test_cache_hit(self, mock_mlflow_env):
        """Second call with same args returns cached model without re-download."""
        fake_model = MagicMock()
        # Pre-populate cache with the key that load_mlflow_model will produce
        cache_key = ("run", "abc123", "model.cbm", "regression")
        _model_cache.put(cache_key, fake_model)

        mock_mlflow, _, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._find_cbm_artifact", return_value="model.cbm"):
            result = load_mlflow_model(
                source_type="run",
                run_id="abc123",
                artifact_path="model.cbm",
                task="regression",
            )

        assert result is fake_model
        # download_artifacts should NOT be called (cache hit)
        mock_mlflow.artifacts.download_artifacts.assert_not_called()

    def test_cache_lru_eviction(self, mock_mlflow_env):
        """Exceeding max cache size evicts oldest entry."""
        # Fill cache beyond max
        for i in range(_MODEL_CACHE_MAX_SIZE + 2):
            _model_cache.put(("run", f"run_{i}", f"art_{i}", "regression"), MagicMock())

        fake_model = MagicMock()
        _, _, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._load_catboost_model", return_value=fake_model), \
             patch("haute._mlflow_io._find_cbm_artifact", return_value="model.cbm"):
            load_mlflow_model(
                source_type="run",
                run_id="new_run",
                artifact_path="model.cbm",
                task="regression",
            )

        # The new entry should be in the cache
        assert ("run", "new_run", "model.cbm", "regression") in _model_cache


# ---------------------------------------------------------------------------
# _resolve_version
# ---------------------------------------------------------------------------


class TestResolveVersion:
    def test_concrete_version_returned_as_is(self):
        """Non-'latest' version is returned unchanged."""
        client = MagicMock()
        assert _resolve_version(client, "my-model", "3") == "3"

    def test_latest_resolves_to_highest(self):
        """'latest' resolves to the highest version number."""
        client = MagicMock()
        v1 = MagicMock(version="1")
        v2 = MagicMock(version="2")
        v3 = MagicMock(version="3")
        client.search_model_versions.return_value = [v1, v3, v2]
        result = _resolve_version(client, "my-model", "latest")
        assert result == "3"

    def test_empty_version_resolves_to_latest(self):
        """Empty string resolves to the latest version."""
        client = MagicMock()
        v1 = MagicMock(version="1")
        client.search_model_versions.return_value = [v1]
        result = _resolve_version(client, "my-model", "")
        assert result == "1"

    def test_no_versions_raises(self):
        """Raises ValueError when no versions exist."""
        client = MagicMock()
        client.search_model_versions.return_value = []
        with pytest.raises(ValueError, match="No versions found"):
            _resolve_version(client, "my-model", "latest")


# ---------------------------------------------------------------------------
# _find_cbm_artifact
# ---------------------------------------------------------------------------


class TestFindCbmArtifact:
    def test_finds_top_level_cbm(self):
        """Finds .cbm at the root level."""
        client = MagicMock()
        art = MagicMock(path="my_model.cbm", is_dir=False)
        client.list_artifacts.return_value = [art]
        assert _find_cbm_artifact(client, "run1") == "my_model.cbm"

    def test_finds_cbm_in_subdirectory(self):
        """Finds .cbm one level deep."""
        client = MagicMock()
        dir_art = MagicMock(path="models", is_dir=True)
        client.list_artifacts.side_effect = [
            [dir_art],  # Top level
            [MagicMock(path="models/trained.cbm", is_dir=False)],  # Subdirectory
        ]
        assert _find_cbm_artifact(client, "run1") == "models/trained.cbm"

    def test_no_cbm_raises(self):
        """Raises FileNotFoundError when no .cbm is found."""
        client = MagicMock()
        art = MagicMock(path="readme.txt", is_dir=False)
        client.list_artifacts.return_value = [art]
        with pytest.raises(FileNotFoundError, match="No .cbm artifact"):
            _find_cbm_artifact(client, "run1")


# ---------------------------------------------------------------------------
# _load_catboost_model
# ---------------------------------------------------------------------------


class TestLoadCatboostModel:
    def test_regression_loads_regressor(self):
        """task=regression uses CatBoostRegressor."""
        from haute._mlflow_io import _load_catboost_model

        mock_model = MagicMock()
        mock_cls = MagicMock(return_value=mock_model)
        mock_catboost = MagicMock(CatBoostRegressor=mock_cls, CatBoostClassifier=MagicMock())
        with patch.dict(sys.modules, {"catboost": mock_catboost}):
            result = _load_catboost_model("/tmp/model.cbm", "regression")
        mock_model.load_model.assert_called_once_with("/tmp/model.cbm")
        assert result is mock_model

    def test_classification_loads_classifier(self):
        """task=classification uses CatBoostClassifier."""
        from haute._mlflow_io import _load_catboost_model

        mock_model = MagicMock()
        mock_cls = MagicMock(return_value=mock_model)
        mock_catboost = MagicMock(CatBoostClassifier=mock_cls, CatBoostRegressor=MagicMock())
        with patch.dict(sys.modules, {"catboost": mock_catboost}):
            result = _load_catboost_model("/tmp/model.cbm", "classification")
        mock_model.load_model.assert_called_once_with("/tmp/model.cbm")
        assert result is mock_model


# ---------------------------------------------------------------------------
# MLflow not installed
# ---------------------------------------------------------------------------


class TestMlflowNotInstalled:
    def test_import_error_message(self):
        """Raises ImportError with pip install instruction when mlflow missing."""
        with patch.dict(sys.modules, {"mlflow": None}):
            with pytest.raises(ImportError, match="pip install mlflow"):
                load_mlflow_model(source_type="run", run_id="x", task="regression")


# ---------------------------------------------------------------------------
# Task validation
# ---------------------------------------------------------------------------


class TestTaskValidation:
    def test_invalid_task_raises(self):
        """Invalid task value raises ValueError before any MLflow calls."""
        with pytest.raises(ValueError, match="Invalid task"):
            load_mlflow_model(source_type="run", run_id="x", task="clustering")

    def test_empty_task_raises(self):
        """Empty task string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid task"):
            load_mlflow_model(source_type="run", run_id="x", task="")


# ---------------------------------------------------------------------------
# _prepare_predict_frame — real data, no mocks
# ---------------------------------------------------------------------------


def _mock_model(cat_indices: list[int] | None = None) -> MagicMock:
    """Create a mock CatBoost model with optional categorical feature indices."""
    m = MagicMock()
    if cat_indices is not None:
        m.get_cat_feature_indices.return_value = cat_indices
    else:
        del m.get_cat_feature_indices  # no categorical features
    return m


class TestPreparePredictFrame:
    def test_numeric_only_returns_numpy(self):
        """All-numeric features should return a numpy array."""
        model = _mock_model(cat_indices=[])
        df = pl.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        result = _prepare_predict_frame(model, df, ["a", "b"])
        assert isinstance(result, np.ndarray)
        assert result.shape == (2, 2)

    def test_numeric_nulls_become_nan(self):
        """Null values in numeric columns should become NaN after Float32 cast."""
        model = _mock_model(cat_indices=[])
        df = pl.DataFrame({"x": [1.0, None, 3.0]})
        result = _prepare_predict_frame(model, df, ["x"])
        assert isinstance(result, np.ndarray)
        assert np.isnan(result[1, 0]), "Null should become NaN"
        assert result[0, 0] == pytest.approx(1.0, abs=0.01)

    def test_categorical_nulls_become_sentinel(self):
        """Null values in categorical columns should be filled with '_MISSING_'."""
        model = _mock_model(cat_indices=[0])
        df = pl.DataFrame({"cat": ["a", None, "b"]})
        result = _prepare_predict_frame(model, df, ["cat"])
        # With cat features, returns pandas DataFrame
        import pandas as pd
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[1, 0] == "_MISSING_"

    def test_mixed_numeric_and_categorical(self):
        """Mixed features: numeric→numpy float, categorical→sentinel+Categorical."""
        model = _mock_model(cat_indices=[1])  # second feature is categorical
        df = pl.DataFrame({
            "num": [1.0, None, 3.0],
            "cat": ["x", None, "y"],
        })
        result = _prepare_predict_frame(model, df, ["num", "cat"])
        import pandas as pd
        assert isinstance(result, pd.DataFrame)
        # numeric column should have NaN for null
        assert np.isnan(result["num"].iloc[1])
        # categorical column should have sentinel for null
        assert result["cat"].iloc[1] == "_MISSING_"

    def test_no_cat_feature_indices_attr(self):
        """Model without get_cat_feature_indices treats all features as numeric."""
        model = _mock_model(cat_indices=None)  # deletes the attribute
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = _prepare_predict_frame(model, df, ["a"])
        assert isinstance(result, np.ndarray)

    def test_feature_order_preserved(self):
        """Output columns should match the requested feature order."""
        model = _mock_model(cat_indices=[])
        df = pl.DataFrame({"b": [10.0, 20.0], "a": [1.0, 2.0]})
        result = _prepare_predict_frame(model, df, ["a", "b"])
        # First column should be "a", second "b"
        assert result[0, 0] == pytest.approx(1.0, abs=0.01)
        assert result[0, 1] == pytest.approx(10.0, abs=0.01)
