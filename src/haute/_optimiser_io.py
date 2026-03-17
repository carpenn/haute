"""Optimiser artifact loading utilities for the OPTIMISER_APPLY node.

Thread-safe mtime-aware cache for optimiser artifacts (saved JSON files
containing lambdas, factor tables, and version metadata).

Supports two resolution paths:
  - **File**: Load directly from a local JSON path.
  - **MLflow**: Download ``optimiser_result.json`` from an MLflow run
    (by run ID or registered model + version), then load as JSON.

Analogous to ``_mlflow_io.py`` for MLflow models and ``_io.py`` for
external objects.
"""

from __future__ import annotations

import json
import os
from typing import Any

from haute._logging import get_logger
from haute._lru_cache import LRUCache
from haute._mlflow_utils import resolve_version

logger = get_logger(component="optimiser_io")

_ARTIFACT_CACHE_MAX_SIZE = 8
_artifact_cache: LRUCache[tuple[str, float], dict[str, Any]] = LRUCache(
    max_size=_ARTIFACT_CACHE_MAX_SIZE,
)


def load_optimiser_artifact(path: str) -> dict[str, Any]:
    """Load an optimiser artifact JSON file with mtime-based caching.

    The cache key is ``(path, mtime)`` so edits to the file on disk are
    picked up automatically.  Bounded to ``_ARTIFACT_CACHE_MAX_SIZE``
    entries with oldest-eviction.

    Returns the full parsed JSON dict (mode, lambdas, constraints,
    factor_tables, version, etc.).
    """
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        mtime = 0.0

    key = (path, mtime)

    if key in _artifact_cache:
        logger.debug("optimiser_artifact_cache_hit", path=path)
        cached = _artifact_cache.get(key)
        assert cached is not None  # guaranteed by __contains__ check above
        return cached

    with open(path) as f:
        artifact: dict[str, Any] = json.load(f)

    _artifact_cache.put(key, artifact)
    logger.info("optimiser_artifact_loaded", path=path, mode=artifact.get("mode"))
    return artifact


# ---------------------------------------------------------------------------
# MLflow resolution
# ---------------------------------------------------------------------------

_MLFLOW_ARTIFACT_NAME = "optimiser_result.json"

# Separate LRU cache for MLflow-sourced artifacts (keyed by run_id or
# model+version, not file path).
_mlflow_cache: LRUCache[tuple[str, str, str], dict[str, Any]] = LRUCache(
    max_size=_ARTIFACT_CACHE_MAX_SIZE,
)


def load_mlflow_optimiser_artifact(
    *,
    source_type: str,
    run_id: str = "",
    registered_model: str = "",
    version: str = "",
    tracking_uri: str = "",
) -> dict[str, Any]:
    """Download and cache an optimiser artifact from MLflow.

    Args:
        source_type: ``"run"`` or ``"registered"``.
        run_id: MLflow run ID (required when *source_type* is ``"run"``).
        registered_model: Registered model name (required when
            *source_type* is ``"registered"``).
        version: Model version (``"1"``, ``"latest"``, etc.).
        tracking_uri: Override tracking URI; auto-detected if empty.

    Returns:
        Parsed artifact dict (same shape as ``load_optimiser_artifact``).
    """
    try:
        import mlflow
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

    resolved_run_id = run_id
    resolved_version = version

    if source_type == "registered":
        if not registered_model:
            raise ValueError(
                "registered_model is required when sourceType is 'registered'"
            )
        resolved_version = resolve_version(client, registered_model, version)
        mv = client.get_model_version(registered_model, resolved_version)
        resolved_run_id = mv.run_id or ""
    elif source_type == "run":
        if not resolved_run_id:
            raise ValueError("run_id is required when sourceType is 'run'")
    else:
        raise ValueError(
            f"Invalid sourceType: {source_type!r}. Expected 'run' or 'registered'."
        )

    cache_key = (source_type, resolved_run_id, resolved_version)
    if cache_key in _mlflow_cache:
        logger.debug("mlflow_optimiser_cache_hit", key=str(cache_key))
        cached = _mlflow_cache.get(cache_key)
        assert cached is not None  # guaranteed by __contains__ check above
        return cached

    local_path = mlflow.artifacts.download_artifacts(
        f"runs:/{resolved_run_id}/{_MLFLOW_ARTIFACT_NAME}"
    )

    with open(local_path) as f:
        artifact: dict[str, Any] = json.load(f)

    _mlflow_cache.put(cache_key, artifact)
    logger.info(
        "mlflow_optimiser_artifact_loaded",
        source_type=source_type,
        run_id=resolved_run_id,
        mode=artifact.get("mode"),
    )
    return artifact


