"""File I/O utilities — data source reading and external object loading."""

from __future__ import annotations

import threading
from collections import OrderedDict

import polars as pl

from haute._logging import get_logger

logger = get_logger(component="io")

_OBJECT_CACHE_MAX_SIZE = 32


def read_source(path: str) -> pl.LazyFrame:
    """Read a data file into a LazyFrame, dispatching on file extension.

    Centralises the csv/json/parquet dispatch that was previously duplicated
    across the executor, scorer, schema inference, and server modules.

    Raises:
        ValueError: If the file extension is not supported.
    """
    if path.endswith(".csv"):
        return pl.scan_csv(path)
    if path.endswith(".json"):
        return pl.read_json(path).lazy()
    if path.endswith(".jsonl"):
        return pl.scan_ndjson(path)
    if path.endswith(".parquet"):
        return pl.scan_parquet(path)
    suffix = path.rsplit(".", 1)[-1] if "." in path else ""
    logger.error("unsupported_file_type", path=path, suffix=suffix)
    raise ValueError(f"Unsupported file type: .{suffix}")


_object_cache: OrderedDict[tuple[str, float, str, str], object] = OrderedDict()
_object_cache_lock = threading.Lock()


def load_external_object(path: str, file_type: str, model_class: str = "classifier") -> object:
    """Load an external file (model, JSON, pickle, joblib) and return the object.

    Shared by the development executor and the deploy scoring engine.

    Results are cached by ``(path, mtime, file_type, model_class)`` so
    repeated calls (preview clicks, API scoring requests) skip disk I/O.
    The cache auto-invalidates when the file is modified on disk.
    Bounded to ``_OBJECT_CACHE_MAX_SIZE`` entries (LRU eviction).

    All paths are validated to be within the project root before loading.
    Pickle files are deserialized with a restricted unpickler that only
    allows known-safe classes.
    """
    import os

    from haute._sandbox import validate_project_path

    validate_project_path(path)

    try:
        mtime = os.path.getmtime(path)
    except OSError:
        mtime = 0.0
    key = (path, mtime, file_type, model_class)

    with _object_cache_lock:
        cached = _object_cache.get(key)
        if cached is not None:
            _object_cache.move_to_end(key)
            return cached

    obj = _load_external_object_uncached(path, file_type, model_class)

    with _object_cache_lock:
        _object_cache[key] = obj
        if len(_object_cache) > _OBJECT_CACHE_MAX_SIZE:
            _object_cache.popitem(last=False)

    return obj


def _load_external_object_uncached(
    path: str, file_type: str, model_class: str,
) -> object:
    """Deserialize an external file from disk (no caching)."""
    if file_type == "json":
        import json as _json

        with open(path) as f:
            return _json.load(f)
    elif file_type == "joblib":
        import joblib

        return joblib.load(path)
    elif file_type == "catboost":
        if model_class == "regressor":
            from catboost import CatBoostRegressor

            m = CatBoostRegressor()
        else:
            from catboost import CatBoostClassifier

            m = CatBoostClassifier()
        m.load_model(path)
        return m
    else:  # pickle
        from haute._sandbox import safe_unpickle

        return safe_unpickle(path)
