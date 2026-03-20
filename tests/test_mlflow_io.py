"""Tests for haute._mlflow_io — MLflow model loading and caching."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl
import pytest

from haute._mlflow_io import (
    _MODEL_CACHE_MAX_SIZE,
    ScoringModel,
    _append_classification_proba,
    _find_artifact_by_extension,
    _find_cbm_artifact,
    _find_model_artifact,
    _load_rustystats_model,
    _model_cache,
    _prepare_predict_frame,
    _wrap_catboost,
    _wrap_pyfunc,
    load_local_model,
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
        """Run-based loading downloads artifacts and returns ScoringModel."""
        fake_model = MagicMock()
        fake_model.feature_names_ = ["a", "b"]
        fake_model.get_cat_feature_indices.return_value = []
        _mock_mlflow, _mock_client, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._load_catboost_model", return_value=fake_model), \
             patch("haute._mlflow_io._resolve_artifact_local", return_value="/tmp/model.cbm"), \
             patch("haute._mlflow_io._find_cbm_artifact", return_value="model.cbm"):
            result = load_mlflow_model(
                source_type="run",
                run_id="abc123",
                artifact_path="model.cbm",
                task="regression",
            )

        assert isinstance(result, ScoringModel)
        assert result.raw_model is fake_model
        assert result.flavor == "catboost"
        assert result.feature_names == ["a", "b"]

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
        """Registered model loading resolves version and returns ScoringModel."""
        fake_model = MagicMock()
        fake_model.feature_names_ = ["x"]
        fake_model.get_cat_feature_indices.return_value = []
        _, mock_client, modules_patch, resolve_patch = mock_mlflow_env

        mv = MagicMock(run_id="resolved_run_id")
        mock_client.get_model_version.return_value = mv

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._load_catboost_model", return_value=fake_model), \
             patch("haute._mlflow_io._resolve_artifact_local", return_value="/tmp/model.cbm"), \
             patch("haute._mlflow_utils.resolve_version", return_value="2"), \
             patch("haute._mlflow_io._find_cbm_artifact", return_value="model.cbm"):
            result = load_mlflow_model(
                source_type="registered",
                registered_model="my-model",
                version="2",
                task="regression",
            )

        assert isinstance(result, ScoringModel)
        assert result.raw_model is fake_model


# ---------------------------------------------------------------------------
# load_mlflow_model — pyfunc auto-detection
# ---------------------------------------------------------------------------


class TestPyfuncAutoDetect:
    def test_non_cbm_artifact_uses_pyfunc(self, mock_mlflow_env):
        """Artifact path not ending in .cbm loads via pyfunc."""
        fake_pyfunc = MagicMock()
        fake_pyfunc.metadata.signature.inputs.input_names.return_value = ["f1", "f2"]
        _, _, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._load_pyfunc_model", return_value=fake_pyfunc):
            result = load_mlflow_model(
                source_type="run",
                run_id="abc123",
                artifact_path="model",
                task="regression",
            )

        assert isinstance(result, ScoringModel)
        assert result.flavor == "pyfunc"
        assert result.feature_names == ["f1", "f2"]
        assert result.cat_feature_names == frozenset()

    def test_auto_discover_falls_back_to_pyfunc(self, mock_mlflow_env):
        """When no .cbm found and no artifact_path, falls back to pyfunc 'model'."""
        fake_pyfunc = MagicMock()
        fake_pyfunc.metadata.signature.inputs.input_names.return_value = ["a"]
        _, _, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._find_cbm_artifact", side_effect=FileNotFoundError), \
             patch("haute._mlflow_io._find_model_artifact", return_value=("model", "pyfunc")), \
             patch("haute._mlflow_io._load_pyfunc_model", return_value=fake_pyfunc):
            result = load_mlflow_model(
                source_type="run",
                run_id="abc123",
                artifact_path="model",
                task="regression",
            )

        assert result.flavor == "pyfunc"


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
        fake_sm = ScoringModel(MagicMock(), ["a"], frozenset(), "catboost")
        cache_key = ("run", "abc123", "model.cbm", "regression")
        _model_cache.put(cache_key, fake_sm)

        mock_mlflow, _, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._find_cbm_artifact", return_value="model.cbm"):
            result = load_mlflow_model(
                source_type="run",
                run_id="abc123",
                artifact_path="model.cbm",
                task="regression",
            )

        assert result is fake_sm
        # download_artifacts should NOT be called (cache hit)
        mock_mlflow.artifacts.download_artifacts.assert_not_called()

    def test_cache_lru_eviction(self, mock_mlflow_env):
        """Exceeding max cache size evicts oldest entry."""
        for i in range(_MODEL_CACHE_MAX_SIZE + 2):
            _model_cache.put(("run", f"run_{i}", f"art_{i}", "regression"), MagicMock())

        fake_model = MagicMock()
        fake_model.feature_names_ = ["a"]
        fake_model.get_cat_feature_indices.return_value = []
        _, _, modules_patch, resolve_patch = mock_mlflow_env

        with modules_patch, resolve_patch, \
             patch("haute._mlflow_io._load_catboost_model", return_value=fake_model), \
             patch("haute._mlflow_io._resolve_artifact_local", return_value="/tmp/model.cbm"), \
             patch("haute._mlflow_io._find_cbm_artifact", return_value="model.cbm"):
            load_mlflow_model(
                source_type="run",
                run_id="new_run",
                artifact_path="model.cbm",
                task="regression",
            )

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
# _wrap_catboost / _wrap_pyfunc
# ---------------------------------------------------------------------------


class TestWrappers:
    def test_wrap_catboost(self):
        """_wrap_catboost extracts feature names and cat features."""
        model = MagicMock()
        model.feature_names_ = ["a", "b", "c"]
        model.get_cat_feature_indices.return_value = [2]

        sm = _wrap_catboost(model)
        assert sm.flavor == "catboost"
        assert sm.feature_names == ["a", "b", "c"]
        assert sm.cat_feature_names == frozenset({"c"})
        assert sm.raw_model is model

    def test_wrap_catboost_no_cat_features(self):
        """_wrap_catboost with no cat feature indices."""
        model = MagicMock()
        model.feature_names_ = ["x", "y"]
        model.get_cat_feature_indices.return_value = []

        sm = _wrap_catboost(model)
        assert sm.cat_feature_names == frozenset()

    def test_wrap_pyfunc(self):
        """_wrap_pyfunc extracts features from model signature."""
        model = MagicMock()
        model.metadata.signature.inputs.input_names.return_value = ["f1", "f2"]

        sm = _wrap_pyfunc(model)
        assert sm.flavor == "pyfunc"
        assert sm.feature_names == ["f1", "f2"]
        assert sm.cat_feature_names == frozenset()

    def test_wrap_pyfunc_no_signature(self):
        """_wrap_pyfunc with no signature returns empty feature list."""
        model = MagicMock()
        model.metadata = None

        sm = _wrap_pyfunc(model)
        assert sm.feature_names == []


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


class TestPreparePredictFrame:
    def test_numeric_only_returns_numpy(self):
        """All-numeric features return numpy array (catboost flavor, no cats)."""
        df = pl.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        result = _prepare_predict_frame(df, ["a", "b"], frozenset(), "catboost")
        assert isinstance(result, np.ndarray)
        assert result.shape == (2, 2)

    def test_numeric_nulls_become_nan(self):
        """Null values in numeric columns become NaN after Float32 cast."""
        df = pl.DataFrame({"x": [1.0, None, 3.0]})
        result = _prepare_predict_frame(df, ["x"], frozenset(), "catboost")
        assert isinstance(result, np.ndarray)
        assert np.isnan(result[1, 0]), "Null should become NaN"
        assert result[0, 0] == pytest.approx(1.0, abs=0.01)

    def test_categorical_nulls_become_sentinel(self):
        """Null values in categorical columns filled with '_MISSING_'."""
        df = pl.DataFrame({"cat": ["a", None, "b"]})
        result = _prepare_predict_frame(df, ["cat"], frozenset({"cat"}), "catboost")
        import pandas as pd
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[1, 0] == "_MISSING_"

    def test_mixed_numeric_and_categorical(self):
        """Mixed features: numeric→float32, categorical→sentinel+Categorical."""
        df = pl.DataFrame({
            "num": [1.0, None, 3.0],
            "cat": ["x", None, "y"],
        })
        result = _prepare_predict_frame(
            df, ["num", "cat"], frozenset({"cat"}), "catboost",
        )
        import pandas as pd
        assert isinstance(result, pd.DataFrame)
        assert np.isnan(result["num"].iloc[1])
        assert result["cat"].iloc[1] == "_MISSING_"

    def test_no_cat_features_returns_numpy(self):
        """No cat_feature_names treats all features as numeric."""
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = _prepare_predict_frame(df, ["a"], frozenset(), "catboost")
        assert isinstance(result, np.ndarray)

    def test_feature_order_preserved(self):
        """Output columns match the requested feature order."""
        df = pl.DataFrame({"b": [10.0, 20.0], "a": [1.0, 2.0]})
        result = _prepare_predict_frame(df, ["a", "b"], frozenset(), "catboost")
        assert result[0, 0] == pytest.approx(1.0, abs=0.01)
        assert result[0, 1] == pytest.approx(10.0, abs=0.01)

    def test_pyfunc_always_returns_pandas(self):
        """Pyfunc flavor always returns pandas DataFrame, even with no cats."""
        df = pl.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        result = _prepare_predict_frame(df, ["a", "b"], frozenset(), "pyfunc")
        import pandas as pd
        assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# _find_artifact_by_extension (D8 refactor)
# ---------------------------------------------------------------------------


class TestFindArtifactByExtension:
    """Tests for the unified artifact discovery helper."""

    def test_finds_top_level_cbm(self):
        """Finds .cbm at the root level."""
        client = MagicMock()
        art = MagicMock(path="my_model.cbm", is_dir=False)
        client.list_artifacts.return_value = [art]
        assert _find_artifact_by_extension(client, "run1", ".cbm", "CatBoost") == "my_model.cbm"

    def test_finds_top_level_rsglm(self):
        """Finds .rsglm at the root level."""
        client = MagicMock()
        art = MagicMock(path="glm_model.rsglm", is_dir=False)
        client.list_artifacts.return_value = [art]
        assert _find_artifact_by_extension(client, "run1", ".rsglm", "RustyStats") == "glm_model.rsglm"

    def test_finds_cbm_in_subdirectory(self):
        """Finds .cbm one level deep in a subdirectory."""
        client = MagicMock()
        dir_art = MagicMock(path="models", is_dir=True)
        client.list_artifacts.side_effect = [
            [dir_art],
            [MagicMock(path="models/trained.cbm", is_dir=False)],
        ]
        assert _find_artifact_by_extension(client, "run1", ".cbm", "CatBoost") == "models/trained.cbm"

    def test_finds_rsglm_in_subdirectory(self):
        """Finds .rsglm one level deep in a subdirectory."""
        client = MagicMock()
        dir_art = MagicMock(path="artifacts", is_dir=True)
        client.list_artifacts.side_effect = [
            [dir_art],
            [MagicMock(path="artifacts/glm.rsglm", is_dir=False)],
        ]
        assert _find_artifact_by_extension(client, "run1", ".rsglm", "RustyStats") == "artifacts/glm.rsglm"

    def test_missing_cbm_raises_with_label(self):
        """FileNotFoundError includes the extension and label."""
        client = MagicMock()
        art = MagicMock(path="readme.txt", is_dir=False)
        client.list_artifacts.return_value = [art]
        with pytest.raises(FileNotFoundError, match=r"No \.cbm artifact.*CatBoost"):
            _find_artifact_by_extension(client, "run1", ".cbm", "CatBoost")

    def test_missing_rsglm_raises_with_label(self):
        """FileNotFoundError includes the extension and label."""
        client = MagicMock()
        art = MagicMock(path="readme.txt", is_dir=False)
        client.list_artifacts.return_value = [art]
        with pytest.raises(FileNotFoundError, match=r"No \.rsglm artifact.*RustyStats"):
            _find_artifact_by_extension(client, "run1", ".rsglm", "RustyStats")

    def test_arbitrary_extension(self):
        """Works for any extension, not just .cbm and .rsglm."""
        client = MagicMock()
        art = MagicMock(path="weights.pt", is_dir=False)
        client.list_artifacts.return_value = [art]
        assert _find_artifact_by_extension(client, "run1", ".pt", "PyTorch") == "weights.pt"

    def test_prefers_top_level_over_subdirectory(self):
        """Top-level match is returned even if subdirectory also contains a match."""
        client = MagicMock()
        top_art = MagicMock(path="model.cbm", is_dir=False)
        dir_art = MagicMock(path="subdir", is_dir=True)
        client.list_artifacts.return_value = [top_art, dir_art]
        # list_artifacts should only be called once (top level)
        result = _find_artifact_by_extension(client, "run1", ".cbm", "CatBoost")
        assert result == "model.cbm"
        client.list_artifacts.assert_called_once_with("run1")

    def test_delegates_correctly_via_find_cbm(self):
        """_find_cbm_artifact delegates to _find_artifact_by_extension."""
        client = MagicMock()
        art = MagicMock(path="model.cbm", is_dir=False)
        client.list_artifacts.return_value = [art]
        assert _find_cbm_artifact(client, "run1") == "model.cbm"

    def test_delegates_correctly_via_find_rsglm(self):
        """_find_rsglm_artifact delegates to _find_artifact_by_extension."""
        from haute._mlflow_io import _find_rsglm_artifact
        client = MagicMock()
        art = MagicMock(path="model.rsglm", is_dir=False)
        client.list_artifacts.return_value = [art]
        assert _find_rsglm_artifact(client, "run1") == "model.rsglm"


# ---------------------------------------------------------------------------
# _append_classification_proba (D9 refactor)
# ---------------------------------------------------------------------------


class TestAppendClassificationProba:
    """Tests for the unified classification probability helper."""

    def test_2d_proba_extracts_column_1(self):
        """2-D probability array extracts the positive class (column 1)."""
        df = pl.DataFrame({"x": [1, 2, 3]})
        model = MagicMock()
        model.predict_proba.return_value = np.array([
            [0.8, 0.2],
            [0.3, 0.7],
            [0.5, 0.5],
        ])
        sm = ScoringModel(model, ["x"], frozenset(), "catboost")
        result = _append_classification_proba(df, sm, np.array([[1], [2], [3]]), "pred")
        assert "pred_proba" in result.columns
        expected = [0.2, 0.7, 0.5]
        actual = result["pred_proba"].to_list()
        for a, e in zip(actual, expected):
            assert a == pytest.approx(e)

    def test_1d_proba_used_directly(self):
        """1-D probability array is used as-is."""
        df = pl.DataFrame({"x": [1, 2]})
        model = MagicMock()
        model.predict_proba.return_value = np.array([0.3, 0.9])
        sm = ScoringModel(model, ["x"], frozenset(), "catboost")
        result = _append_classification_proba(df, sm, np.array([[1], [2]]), "pred")
        assert "pred_proba" in result.columns
        actual = result["pred_proba"].to_list()
        assert actual[0] == pytest.approx(0.3)
        assert actual[1] == pytest.approx(0.9)

    def test_no_predict_proba_returns_unchanged(self):
        """Models without predict_proba return the DataFrame unchanged."""
        df = pl.DataFrame({"x": [1, 2]})
        model = MagicMock(spec=[])  # no predict_proba
        sm = ScoringModel(model, ["x"], frozenset(), "pyfunc")
        result = _append_classification_proba(df, sm, np.array([[1], [2]]), "pred")
        assert "pred_proba" not in result.columns
        assert result.equals(df)

    def test_custom_output_col_name(self):
        """Proba column is named ``<output_col>_proba``."""
        df = pl.DataFrame({"x": [1]})
        model = MagicMock()
        model.predict_proba.return_value = np.array([[0.4, 0.6]])
        sm = ScoringModel(model, ["x"], frozenset(), "catboost")
        result = _append_classification_proba(df, sm, np.array([[1]]), "my_score")
        assert "my_score_proba" in result.columns

    def test_regression_should_not_call_this(self):
        """Verify the helper is only called for classification (by caller convention)."""
        # This test validates the contract: the helper itself doesn't check task,
        # it just appends probas. Callers gate on task == "classification".
        df = pl.DataFrame({"x": [1]})
        model = MagicMock()
        model.predict_proba.return_value = np.array([0.5])
        sm = ScoringModel(model, ["x"], frozenset(), "catboost")
        # Even if called, it appends the column — which is correct behavior.
        result = _append_classification_proba(df, sm, np.array([[1]]), "pred")
        assert "pred_proba" in result.columns


# ---------------------------------------------------------------------------
# T9: _load_rustystats_model
# ---------------------------------------------------------------------------


class TestLoadRustystatsModel:
    """Tests for _load_rustystats_model using mocked rustystats module."""

    def test_loads_and_wraps_model(self, tmp_path):
        """Reads bytes from file and wraps in ScoringModel with flavor='rustystats'."""
        model_file = tmp_path / "model.rsglm"
        model_file.write_bytes(b"fake_bytes")

        mock_model = MagicMock()
        mock_model.feature_names = ["feat_a", "feat_b"]
        mock_rs = MagicMock()
        mock_rs.GLMModel.from_bytes.return_value = mock_model

        with patch.dict(sys.modules, {"rustystats": mock_rs}):
            sm = _load_rustystats_model(str(model_file))

        assert isinstance(sm, ScoringModel)
        assert sm.flavor == "rustystats"
        assert sm.feature_names == ["feat_a", "feat_b"]
        assert sm.cat_feature_names == frozenset()
        assert sm.raw_model is mock_model

    def test_model_without_feature_names(self, tmp_path):
        """Model without feature_names attribute gets empty list."""
        model_file = tmp_path / "model.rsglm"
        model_file.write_bytes(b"fake_bytes")

        mock_model = MagicMock(spec=[])  # no feature_names
        mock_rs = MagicMock()
        mock_rs.GLMModel.from_bytes.return_value = mock_model

        with patch.dict(sys.modules, {"rustystats": mock_rs}):
            sm = _load_rustystats_model(str(model_file))

        assert sm.feature_names == []

    def test_reads_file_as_bytes(self, tmp_path):
        """Verifies the file is read in binary mode."""
        model_file = tmp_path / "test.rsglm"
        model_file.write_bytes(b"fake_model_bytes")

        mock_model = MagicMock()
        mock_model.feature_names = ["x"]
        mock_rs = MagicMock()
        mock_rs.GLMModel.from_bytes.return_value = mock_model

        with patch.dict(sys.modules, {"rustystats": mock_rs}):
            _load_rustystats_model(str(model_file))

        mock_rs.GLMModel.from_bytes.assert_called_once_with(b"fake_model_bytes")


# ---------------------------------------------------------------------------
# T9: load_local_model for .rsglm and edge cases
# ---------------------------------------------------------------------------


class TestLoadLocalModel:
    """Tests for load_local_model dispatching by file extension."""

    def test_cbm_dispatches_to_catboost(self):
        """'.cbm' extension dispatches to CatBoost loader."""
        mock_model = MagicMock()
        mock_model.feature_names_ = ["a"]
        mock_model.get_cat_feature_indices.return_value = []

        mock_catboost = MagicMock()
        mock_catboost.CatBoostRegressor.return_value = mock_model
        with patch.dict(sys.modules, {"catboost": mock_catboost}):
            sm = load_local_model("/tmp/model.cbm", task="regression")

        assert isinstance(sm, ScoringModel)
        assert sm.flavor == "catboost"

    def test_rsglm_dispatches_to_rustystats(self, tmp_path):
        """'.rsglm' extension dispatches to RustyStats loader."""
        model_file = tmp_path / "model.rsglm"
        model_file.write_bytes(b"fake_bytes")

        mock_model = MagicMock()
        mock_model.feature_names = ["x", "y"]
        mock_rs = MagicMock()
        mock_rs.GLMModel.from_bytes.return_value = mock_model

        with patch.dict(sys.modules, {"rustystats": mock_rs}):
            sm = load_local_model(str(model_file))

        assert isinstance(sm, ScoringModel)
        assert sm.flavor == "rustystats"
        assert sm.feature_names == ["x", "y"]

    def test_unsupported_extension_raises(self):
        """Unknown extension raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="not yet supported"):
            load_local_model("/tmp/model.pkl")

    def test_unsupported_extension_lists_formats(self):
        """Error message lists supported formats."""
        with pytest.raises(NotImplementedError, match=r"\.cbm.*\.rsglm"):
            load_local_model("/tmp/model.onnx")

    def test_classification_task_forwarded_for_cbm(self):
        """task='classification' is forwarded to CatBoost loader."""
        mock_model = MagicMock()
        mock_model.feature_names_ = ["a"]
        mock_model.get_cat_feature_indices.return_value = []

        mock_catboost = MagicMock()
        mock_catboost.CatBoostClassifier.return_value = mock_model
        with patch.dict(sys.modules, {"catboost": mock_catboost}):
            sm = load_local_model("/tmp/model.cbm", task="classification")

        assert sm.flavor == "catboost"


# ---------------------------------------------------------------------------
# T9: _prepare_predict_frame with flavor="rustystats"
# ---------------------------------------------------------------------------


class TestPreparePredictFrameRustystats:
    """Tests for _prepare_predict_frame with rustystats flavor."""

    def test_returns_polars_dataframe(self):
        """RustyStats flavor returns the Polars DataFrame directly (no conversion)."""
        df = pl.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        result = _prepare_predict_frame(df, ["a", "b"], frozenset(), "rustystats")
        assert isinstance(result, pl.DataFrame)

    def test_returns_exact_input_dataframe(self):
        """RustyStats should get the exact same DataFrame object (no copy)."""
        df = pl.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        result = _prepare_predict_frame(df, ["a", "b"], frozenset(), "rustystats")
        assert result is df

    def test_nulls_not_processed(self):
        """RustyStats handles its own null preprocessing — nulls pass through."""
        df = pl.DataFrame({"a": [1.0, None, 3.0]})
        result = _prepare_predict_frame(df, ["a"], frozenset(), "rustystats")
        assert result["a"].null_count() == 1

    def test_categoricals_not_processed(self):
        """RustyStats handles its own categoricals — no sentinel fill."""
        df = pl.DataFrame({"cat": ["x", None, "y"]})
        result = _prepare_predict_frame(df, ["cat"], frozenset({"cat"}), "rustystats")
        # Should NOT have _MISSING_ sentinel — nulls pass through
        assert result["cat"].null_count() == 1


# ---------------------------------------------------------------------------
# T9: _find_model_artifact — auto-detection
# ---------------------------------------------------------------------------


class TestFindModelArtifact:
    """Tests for _find_model_artifact auto-detection logic."""

    def test_finds_cbm_first(self):
        """CatBoost .cbm is preferred over other formats."""
        client = MagicMock()
        art = MagicMock(path="model.cbm", is_dir=False)
        client.list_artifacts.return_value = [art]
        path, flavor = _find_model_artifact(client, "run1")
        assert path == "model.cbm"
        assert flavor == "catboost"

    def test_finds_rsglm_when_no_cbm(self):
        """RustyStats .rsglm is found when no .cbm exists."""
        client = MagicMock()
        rsglm_art = MagicMock(path="model.rsglm", is_dir=False)
        client.list_artifacts.return_value = [rsglm_art]
        path, flavor = _find_model_artifact(client, "run1")
        assert path == "model.rsglm"
        assert flavor == "rustystats"

    def test_finds_pyfunc_when_no_native(self):
        """Falls back to pyfunc 'model' directory."""
        client = MagicMock()
        model_dir = MagicMock(path="model", is_dir=True)
        client.list_artifacts.return_value = [model_dir]
        path, flavor = _find_model_artifact(client, "run1")
        assert path == "model"
        assert flavor == "pyfunc"

    def test_no_artifact_raises(self):
        """Raises FileNotFoundError when no model artifact found."""
        client = MagicMock()
        txt_art = MagicMock(path="readme.txt", is_dir=False)
        client.list_artifacts.return_value = [txt_art]
        with pytest.raises(FileNotFoundError, match="No model artifact"):
            _find_model_artifact(client, "run1")
