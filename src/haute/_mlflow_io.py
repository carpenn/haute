"""MLflow model loading utilities for the MODEL_SCORE node.

Thread-safe LRU cache for CatBoost models loaded from MLflow,
analogous to ``_io.py``'s ``load_external_object()``.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import TYPE_CHECKING

from haute._logging import get_logger

if TYPE_CHECKING:
    from catboost import CatBoostClassifier, CatBoostRegressor
    from mlflow.tracking import MlflowClient

logger = get_logger(component="mlflow_io")

_MODEL_CACHE_MAX_SIZE = 16
_model_cache: OrderedDict[tuple[str, str, str, str], object] = OrderedDict()
_model_cache_lock = threading.Lock()


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
        resolved_run_id = mv.run_id
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

    with _model_cache_lock:
        cached = _model_cache.get(cache_key)
        if cached is not None:
            _model_cache.move_to_end(cache_key)
            logger.info("mlflow_model_cache_hit", key=str(cache_key))
            return cached

    local_path = mlflow.artifacts.download_artifacts(
        f"runs:/{resolved_run_id}/{resolved_artifact}"
    )

    model = _load_catboost_model(local_path, task)

    with _model_cache_lock:
        _model_cache[cache_key] = model
        if len(_model_cache) > _MODEL_CACHE_MAX_SIZE:
            _model_cache.popitem(last=False)

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
            return art.path
    # Check one level deep (artifacts may be in subdirectories)
    for art in artifacts:
        if art.is_dir:
            sub_artifacts = client.list_artifacts(run_id, art.path)
            for sub in sub_artifacts:
                if sub.path.endswith(".cbm"):
                    return sub.path
    raise FileNotFoundError(
        f"No .cbm artifact found in run '{run_id}'. "
        "Ensure the model was logged with mlflow.log_artifact()."
    )


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
