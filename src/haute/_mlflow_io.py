"""MLflow model loading utilities for the MODEL_SCORE node.

Thread-safe LRU cache for models loaded from MLflow.
Supports CatBoost (native ``.cbm``) and any MLflow pyfunc model.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl

from haute._logging import get_logger
from haute._lru_cache import LRUCache
from haute._mlflow_utils import resolve_version

if TYPE_CHECKING:
    from catboost import CatBoostClassifier, CatBoostRegressor
    from mlflow.tracking import MlflowClient

logger = get_logger(component="mlflow_io")

_MODEL_CACHE_MAX_SIZE = 16
_model_cache: LRUCache[tuple[str, str, str, str], "ScoringModel"] = LRUCache(
    max_size=_MODEL_CACHE_MAX_SIZE,
)


# ---------------------------------------------------------------------------
# ScoringModel — uniform interface for all model flavors
# ---------------------------------------------------------------------------


class ScoringModel:
    """Uniform scoring interface wrapping any MLflow-loaded model.

    Provides a consistent API regardless of model flavor (CatBoost,
    pyfunc, etc.), abstracting away flavor-specific details like
    categorical feature handling and prediction output format.

    Attribute access is proxied to the underlying model for backward
    compatibility with code that accesses CatBoost-specific attributes
    (e.g. ``model.feature_names_``, ``model.get_cat_feature_indices()``).
    """

    __slots__ = ("_model", "feature_names", "cat_feature_names", "flavor")

    def __init__(
        self,
        model: Any,
        feature_names: list[str],
        cat_feature_names: frozenset[str] = frozenset(),
        flavor: str = "pyfunc",
    ) -> None:
        self._model = model
        self.feature_names = feature_names
        self.cat_feature_names = cat_feature_names
        self.flavor = flavor

    @property
    def raw_model(self) -> Any:
        """Access the underlying model object."""
        return self._model

    def predict(self, x_data: Any) -> np.ndarray:
        """Return 1-D array of predictions."""
        raw = self._model.predict(x_data)
        return np.asarray(raw).flatten()

    def predict_proba(self, x_data: Any) -> np.ndarray | None:
        """Return class probabilities, or ``None`` if unsupported."""
        fn = getattr(self._model, "predict_proba", None)
        if fn is None:
            return None
        return np.asarray(fn(x_data))

    def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to the underlying model for backward compat."""
        return getattr(self._model, name)


# ---------------------------------------------------------------------------
# CatBoost helpers
# ---------------------------------------------------------------------------


def _load_catboost_model(path: str, task: str) -> CatBoostRegressor | CatBoostClassifier:
    """Load a CatBoost model from a local file path."""
    if task == "classification":
        from catboost import CatBoostClassifier

        model = CatBoostClassifier()
    else:
        from catboost import CatBoostRegressor

        model = CatBoostRegressor()
    model.load_model(path)
    return model


def _wrap_catboost(model: CatBoostRegressor | CatBoostClassifier) -> ScoringModel:
    """Wrap a raw CatBoost model in a ``ScoringModel``."""
    feature_names = list(model.feature_names_)
    cat_idx = (
        set(model.get_cat_feature_indices())
        if hasattr(model, "get_cat_feature_indices") else set()
    )
    cat_names = frozenset(
        feature_names[i] for i in cat_idx if i < len(feature_names)
    )
    return ScoringModel(
        model=model,
        feature_names=feature_names,
        cat_feature_names=cat_names,
        flavor="catboost",
    )


def load_local_model(path: str, task: str = "regression") -> ScoringModel:
    """Load a model from a local file path (e.g. bundled deploy artifact).

    Auto-detects flavor from file extension:
    - ``.cbm`` → CatBoost native loader
    - Otherwise → not yet supported (pyfunc local loading planned)
    """
    if path.endswith(".cbm"):
        raw = _load_catboost_model(path, task)
        return _wrap_catboost(raw)
    raise NotImplementedError(
        f"Local model loading not yet supported for: {path!r}. "
        "Only .cbm (CatBoost) files are currently supported for bundled deploy."
    )


# ---------------------------------------------------------------------------
# Pyfunc helpers
# ---------------------------------------------------------------------------


def _load_pyfunc_model(mlflow_module: Any, run_id: str, artifact_path: str) -> Any:
    """Load a model via MLflow pyfunc flavor."""
    model_uri = f"runs:/{run_id}/{artifact_path}"
    return mlflow_module.pyfunc.load_model(model_uri)


def _wrap_pyfunc(model: Any) -> ScoringModel:
    """Wrap an MLflow pyfunc model in a ``ScoringModel``."""
    feature_names = _extract_pyfunc_features(model)
    return ScoringModel(
        model=model,
        feature_names=feature_names,
        cat_feature_names=frozenset(),
        flavor="pyfunc",
    )


def _extract_pyfunc_features(model: Any) -> list[str]:
    """Extract feature names from a pyfunc model's signature."""
    sig = getattr(getattr(model, "metadata", None), "signature", None)
    if sig is None:
        return []
    inputs = sig.inputs
    if inputs is None:
        return []
    if hasattr(inputs, "input_names"):
        return list(inputs.input_names())
    # Older MLflow versions expose inputs as a list of ColSpec
    return [col.name for col in inputs]


# ---------------------------------------------------------------------------
# Artifact discovery
# ---------------------------------------------------------------------------


def _find_cbm_artifact(client: MlflowClient, run_id: str) -> str:
    """Find the first ``.cbm`` artifact in a run's artifact list."""
    artifacts = client.list_artifacts(run_id)
    for art in artifacts:
        if art.path.endswith(".cbm"):
            return str(art.path)
    # Check one level deep (artifacts may be in subdirectories)
    for art in artifacts:
        if art.is_dir:
            sub_artifacts = client.list_artifacts(run_id, art.path)
            for sub in sub_artifacts:
                if sub.path.endswith(".cbm"):
                    return str(sub.path)
    raise FileNotFoundError(
        f"No .cbm artifact found in run '{run_id}'. "
        "Ensure the model was logged with mlflow.log_artifact()."
    )


def _find_model_artifact(client: MlflowClient, run_id: str) -> tuple[str, str]:
    """Find the model artifact in a run, returning ``(path, flavor)``.

    Checks for CatBoost (``.cbm``) first, then falls back to a pyfunc
    model directory.
    """
    try:
        return _find_cbm_artifact(client, run_id), "catboost"
    except FileNotFoundError:
        pass

    # Look for a pyfunc model directory (contains MLmodel file)
    artifacts = client.list_artifacts(run_id)
    for art in artifacts:
        if art.path == "model" and art.is_dir:
            return "model", "pyfunc"
    # Check one level deep
    for art in artifacts:
        if art.is_dir:
            sub = client.list_artifacts(run_id, art.path)
            for s in sub:
                if s.path.endswith("/MLmodel") or s.path == "MLmodel":
                    return art.path, "pyfunc"

    raise FileNotFoundError(
        f"No model artifact found in run '{run_id}'. "
        "Expected .cbm file (CatBoost) or model directory (pyfunc)."
    )


def _resolve_artifact_local(
    mlflow: Any, run_id: str, artifact_path: str,
) -> str:
    """Return a local path to the model artifact, downloading only if needed.

    Saves downloaded artifacts under ``.cache/models/<run_id>/`` so they
    survive server restarts without re-downloading from remote tracking
    servers (saves ~30 s+ for Databricks-hosted artifacts).
    """
    from pathlib import Path

    cache_dir = Path.cwd() / ".cache" / "models" / run_id
    local_path = cache_dir / Path(artifact_path).name

    if local_path.is_file():
        logger.info(
            "mlflow_artifact_disk_cache_hit",
            path=str(local_path),
        )
        return str(local_path)

    # Cache miss — download from tracking server
    logger.info(
        "mlflow_artifact_downloading",
        run_id=run_id,
        artifact=artifact_path,
    )
    downloaded = mlflow.artifacts.download_artifacts(
        f"runs:/{run_id}/{artifact_path}",
        dst_path=str(cache_dir),
    )
    # download_artifacts may nest inside a subdirectory; prefer exact path
    if local_path.is_file():
        return str(local_path)
    return str(downloaded)


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------


def load_mlflow_model(
    *,
    source_type: str,
    run_id: str = "",
    artifact_path: str = "",
    registered_model: str = "",
    version: str = "",
    task: str = "regression",
    tracking_uri: str = "",
) -> ScoringModel:
    """Load a model from MLflow, auto-detecting CatBoost vs pyfunc.

    CatBoost models (``.cbm`` artifacts) get the optimized native loader
    with categorical feature support.  All other models are loaded via
    MLflow's pyfunc flavor.

    Cached by ``(source_type, identifier, version/artifact, task)``.

    Args:
        source_type: ``"run"`` to load from a specific run, or ``"registered"``
            to load from a registered model version.
        run_id: MLflow run ID (required when *source_type* is ``"run"``).
        artifact_path: Artifact path within the run (e.g. ``"model.cbm"``).
            If empty, auto-discovers: tries ``.cbm`` first, then pyfunc ``model/``.
        registered_model: Registered model name (required when *source_type* is
            ``"registered"``).
        version: Model version string (``"1"``, ``"2"``, or ``"latest"``).
        task: ``"regression"`` or ``"classification"`` — determines which
            CatBoost class to use for loading (ignored for pyfunc).
        tracking_uri: Override tracking URI; auto-detected if empty.

    Returns:
        A ``ScoringModel`` wrapping the loaded model with a uniform interface.
    """
    valid_tasks = ("regression", "classification")
    if task not in valid_tasks:
        raise ValueError(
            f"Invalid task {task!r}. Expected one of: {', '.join(valid_tasks)}"
        )

    try:
        import mlflow  # noqa: F401
    except ImportError:
        raise ImportError(
            "mlflow is not installed. Install it with: pip install mlflow"
        ) from None

    from mlflow.tracking import MlflowClient

    from haute.modelling._mlflow_log import resolve_tracking_backend

    if not tracking_uri:
        tracking_uri, _backend = resolve_tracking_backend()
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient(tracking_uri=tracking_uri)

    # Resolve concrete run_id and artifact_path depending on source_type
    resolved_run_id = run_id
    resolved_artifact = artifact_path
    resolved_version = version

    if source_type == "registered":
        if not registered_model:
            raise ValueError("registered_model is required when sourceType is 'registered'")
        resolved_version = resolve_version(client, registered_model, version)
        mv = client.get_model_version(registered_model, resolved_version)
        resolved_run_id = mv.run_id or ""
    elif source_type == "run":
        if not resolved_run_id:
            raise ValueError("run_id is required when sourceType is 'run'")
    else:
        raise ValueError(f"Invalid sourceType: {source_type!r}. Expected 'run' or 'registered'.")

    # Auto-discover artifact if not specified
    if not resolved_artifact:
        resolved_artifact, _flavor = _find_model_artifact(client, resolved_run_id)
    # else: detect from the artifact path extension

    # Detect flavor from artifact path
    flavor = "catboost" if resolved_artifact.endswith(".cbm") else "pyfunc"

    cache_key = (source_type, resolved_run_id, resolved_version or resolved_artifact, task)

    cached = _model_cache.get(cache_key)
    if cached is not None:
        logger.info("mlflow_model_cache_hit", key=str(cache_key))
        return cached

    # Load model based on detected flavor
    if flavor == "catboost":
        local_path = _resolve_artifact_local(
            mlflow, resolved_run_id, resolved_artifact,
        )
        raw_model = _load_catboost_model(local_path, task)
        scoring_model = _wrap_catboost(raw_model)
    else:
        raw_model = _load_pyfunc_model(mlflow, resolved_run_id, resolved_artifact)
        scoring_model = _wrap_pyfunc(raw_model)

    _model_cache.put(cache_key, scoring_model)

    logger.info(
        "mlflow_model_loaded",
        source_type=source_type,
        run_id=resolved_run_id,
        artifact=resolved_artifact,
        task=task,
        flavor=flavor,
    )
    return scoring_model


# ---------------------------------------------------------------------------
# Shared scoring helpers (used by executor + deploy scorer)
# ---------------------------------------------------------------------------


def _prepare_predict_frame(
    df_eager: pl.DataFrame,
    features: list[str],
    cat_feature_names: frozenset[str] = frozenset(),
    flavor: str = "pyfunc",
) -> Any:
    """Prepare a Polars DataFrame for model prediction.

    Handles null values: float32 cast for numerics (null→NaN),
    sentinel fill + Categorical cast for categorical features.

    Returns numpy array or pandas DataFrame depending on model needs:
    - CatBoost with no categoricals: numpy array (fastest)
    - CatBoost with categoricals: pandas DataFrame (CatBoost requirement)
    - Pyfunc: always pandas DataFrame
    """
    numeric_cols = [c for c in features if c not in cat_feature_names]
    cat_cols = [c for c in features if c in cat_feature_names]
    selected = df_eager.select(features)
    if numeric_cols:
        selected = selected.with_columns(
            [pl.col(c).cast(pl.Float32) for c in numeric_cols]
        )
    if cat_cols:
        selected = selected.with_columns(
            [pl.col(c).fill_null("_MISSING_").cast(pl.Categorical) for c in cat_cols]
        )
    # Pyfunc always needs pandas; CatBoost can use numpy when no cats
    if flavor == "pyfunc" or cat_cols:
        return selected.to_pandas()
    return selected.to_numpy()


def _score_eager(
    scoring_model: ScoringModel,
    lf: pl.LazyFrame,
    features: list[str],
    output_col: str = "prediction",
    task: str = "regression",
) -> pl.LazyFrame:
    """Collect a LazyFrame and score in-memory. Returns a LazyFrame.

    Shared between the dev executor and the deploy scorer.
    """
    df_eager = lf.collect()
    x_data = _prepare_predict_frame(
        df_eager, features,
        cat_feature_names=scoring_model.cat_feature_names,
        flavor=scoring_model.flavor,
    )
    preds = scoring_model.predict(x_data)
    df_eager = df_eager.with_columns(
        pl.Series(output_col, preds),
    )
    if task == "classification":
        probas = scoring_model.predict_proba(x_data)
        if probas is not None:
            if probas.ndim == 2:
                probas = probas[:, 1]
            df_eager = df_eager.with_columns(
                pl.Series(f"{output_col}_proba", np.asarray(probas).flatten()),
            )
    return df_eager.lazy()
