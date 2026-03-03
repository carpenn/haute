"""MLflow model loading utilities for the MODEL_SCORE node.

Thread-safe LRU cache for CatBoost models loaded from MLflow,
analogous to ``_io.py``'s ``load_external_object()``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import polars as pl

from haute._logging import get_logger
from haute._lru_cache import LRUCache

if TYPE_CHECKING:
    from catboost import CatBoostClassifier, CatBoostRegressor
    from mlflow.tracking import MlflowClient

logger = get_logger(component="mlflow_io")

_MODEL_CACHE_MAX_SIZE = 16
_model_cache: LRUCache[tuple[str, str, str, str], object] = LRUCache(
    max_size=_MODEL_CACHE_MAX_SIZE,
)


def load_mlflow_model(
    *,
    source_type: str,
    run_id: str = "",
    artifact_path: str = "",
    registered_model: str = "",
    version: str = "",
    task: str = "regression",
    tracking_uri: str = "",
) -> CatBoostRegressor | CatBoostClassifier:
    """Load a CatBoost model from MLflow.  Cached by (source_type, identifier, version, task).

    Args:
        source_type: ``"run"`` to load from a specific run, or ``"registered"``
            to load from a registered model version.
        run_id: MLflow run ID (required when *source_type* is ``"run"``).
        artifact_path: Artifact path within the run (e.g. ``"model.cbm"``).
        registered_model: Registered model name (required when *source_type* is
            ``"registered"``).
        version: Model version string (``"1"``, ``"2"``, or ``"latest"``).
        task: ``"regression"`` or ``"classification"`` — determines which
            CatBoost class to use for loading.
        tracking_uri: Override tracking URI; auto-detected if empty.

    Returns:
        A loaded CatBoost model (``CatBoostRegressor`` or ``CatBoostClassifier``).

    Raises:
        ImportError: If MLflow is not installed.
        FileNotFoundError: If the model artifact cannot be found.
        ValueError: If configuration is invalid.
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
        resolved_version = _resolve_version(client, registered_model, version)
        mv = client.get_model_version(registered_model, resolved_version)
        resolved_run_id = mv.run_id or ""
        if not resolved_artifact:
            resolved_artifact = _find_cbm_artifact(client, resolved_run_id)
    elif source_type == "run":
        if not resolved_run_id:
            raise ValueError("run_id is required when sourceType is 'run'")
        if not resolved_artifact:
            resolved_artifact = _find_cbm_artifact(client, resolved_run_id)
    else:
        raise ValueError(f"Invalid sourceType: {source_type!r}. Expected 'run' or 'registered'.")

    cache_key = (source_type, resolved_run_id, resolved_version or resolved_artifact, task)

    cached = _model_cache.get(cache_key)
    if cached is not None:
        logger.info("mlflow_model_cache_hit", key=str(cache_key))
        return cached

    # Persistent local artifact cache — avoids re-downloading from remote
    # tracking servers (e.g. Databricks) on every server restart.
    local_path = _resolve_artifact_local(
        mlflow, resolved_run_id, resolved_artifact,
    )

    model = _load_catboost_model(local_path, task)
    _model_cache.put(cache_key, model)

    logger.info(
        "mlflow_model_loaded",
        source_type=source_type,
        run_id=resolved_run_id,
        artifact=resolved_artifact,
        task=task,
    )
    return model


def _resolve_version(
    client: MlflowClient, model_name: str, version: str,
) -> str:
    """Resolve ``"latest"`` or empty version to a concrete version number."""
    if version and version != "latest":
        return version

    safe_name = model_name.replace("'", "\\'")
    versions = client.search_model_versions(f"name='{safe_name}'")
    if not versions:
        raise ValueError(
            f"No versions found for registered model '{model_name}'. "
            "Train and register a model first."
        )
    sorted_versions = sorted(versions, key=lambda v: int(v.version), reverse=True)
    return sorted_versions[0].version


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


# ---------------------------------------------------------------------------
# Shared CatBoost scoring helpers (used by executor + deploy scorer)
# ---------------------------------------------------------------------------


def _prepare_predict_frame(
    model: CatBoostRegressor | CatBoostClassifier,
    df_eager: pl.DataFrame,
    features: list[str],
) -> Any:
    """Prepare a Polars DataFrame for CatBoost predict.

    Handles null values: float32 cast for numerics (null→NaN),
    sentinel fill + Categorical cast for categorical features.

    Returns numpy array or pandas DataFrame (depending on whether
    categorical features are present), so the return type is ``Any``.
    """

    cat_idx = (
        set(model.get_cat_feature_indices())
        if hasattr(model, "get_cat_feature_indices") else set()
    )
    cat_names = {features[i] for i in cat_idx if i < len(features)}
    numeric_cols = [c for c in features if c not in cat_names]
    cat_cols = [c for c in features if c in cat_names]
    selected = df_eager.select(features)
    if numeric_cols:
        selected = selected.with_columns(
            [pl.col(c).cast(pl.Float32) for c in numeric_cols]
        )
    if cat_cols:
        selected = selected.with_columns(
            [pl.col(c).fill_null("_MISSING_").cast(pl.Categorical) for c in cat_cols]
        )
    return selected.to_pandas() if cat_cols else selected.to_numpy()


def _score_eager(
    model: CatBoostRegressor | CatBoostClassifier,
    lf: pl.LazyFrame,
    features: list[str],
    output_col: str = "prediction",
    task: str = "regression",
) -> pl.LazyFrame:
    """Collect a LazyFrame and score in-memory. Returns a LazyFrame.

    Shared between the dev executor and the deploy scorer.
    """
    df_eager = lf.collect()
    x_data = _prepare_predict_frame(model, df_eager, features)
    preds = model.predict(x_data).flatten()
    df_eager = df_eager.with_columns(
        pl.Series(output_col, preds),
    )
    if task == "classification" and hasattr(model, "predict_proba"):
        probas = model.predict_proba(x_data)
        if probas.ndim == 2:
            probas = probas[:, 1]
        df_eager = df_eager.with_columns(
            pl.Series(f"{output_col}_proba", probas),
        )
    return df_eager.lazy()
