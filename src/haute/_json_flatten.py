"""Schema-aware JSON flattening for tabular data.

Provides tools to:

- **Infer** a flatten schema from sample JSON data
- **Flatten** nested JSON dicts into single-row tabular dicts
- **Load** samples from ``.json`` (single object or array) and ``.jsonl``

The schema describes the expected structure of the JSON data, including
nested objects and arrays with a maximum item count.  The :func:`flatten`
function walks the *schema* (not the data) to produce a consistent column
set regardless of what data is present.

Column names use dot-separated paths (e.g. ``proposer.licence.licence_type``).
Array indices are **1-based** (e.g. ``additional_drivers.1.first_name``).
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

from haute._logging import get_logger

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

logger = get_logger(component="json_flatten")

# ---------------------------------------------------------------------------
# Large-file threshold, streaming constants & progress tracking
# ---------------------------------------------------------------------------

_LARGE_FILE_THRESHOLD = 50 * 1024 * 1024  # 50 MB
_SCHEMA_SAMPLE_SIZE = 10_000  # rows sampled for streaming schema inference
_FLATTEN_CHUNK_SIZE = 50_000  # rows per parquet row-group

# Thread-safe progress tracking for active cache builds, keyed by data_path.
_flatten_progress: dict[str, dict[str, object]] = {}
_flatten_lock = threading.Lock()


# -- Test helpers for flatten progress state ---------------------------------

def _set_flatten_progress(data_path: str, data: dict[str, object]) -> None:
    """Set flatten progress for *data_path* (test helper)."""
    with _flatten_lock:
        _flatten_progress[data_path] = data


def _clear_flatten_progress() -> None:
    """Clear all flatten progress entries (test helper)."""
    with _flatten_lock:
        _flatten_progress.clear()

# ---------------------------------------------------------------------------
# Type inference helpers
# ---------------------------------------------------------------------------

_TYPE_PRIORITY: dict[str, int] = {"bool": 0, "int": 1, "float": 2, "str": 3}


def _infer_type(value: Any) -> str:
    """Return the schema type name for a scalar value."""
    # bool must be checked before int — bool is a subclass of int
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "str"


def _wider_type(a: str, b: str) -> str:
    """Return the wider of two scalar types (int < float < str)."""
    return a if _TYPE_PRIORITY.get(a, 3) >= _TYPE_PRIORITY.get(b, 3) else b


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------


def _infer_schema_node(value: Any) -> dict[str, Any] | str:
    """Build a schema node from a single JSON value."""
    if value is None:
        return "str"
    if isinstance(value, dict):
        return {k: _infer_schema_node(v) for k, v in value.items()}
    if isinstance(value, list):
        items: dict[str, Any] | str = {}
        for item in value:
            items = _merge_schema_nodes(items, _infer_schema_node(item))
        return {"$max": len(value), "$items": items if items != {} else {}}
    return _infer_type(value)


def _merge_schema_nodes(
    a: dict[str, Any] | str,
    b: dict[str, Any] | str,
) -> dict[str, Any] | str:
    """Merge two schema nodes, widening types and unioning fields."""
    # Identity: empty dict means "no schema yet"
    if a == {}:
        return b
    if b == {}:
        return a

    # Both scalars — widen
    if isinstance(a, str) and isinstance(b, str):
        return _wider_type(a, b)

    # Both dicts
    if isinstance(a, dict) and isinstance(b, dict):
        a_is_array = "$max" in a
        b_is_array = "$max" in b

        if a_is_array and b_is_array:
            return {
                "$max": max(a["$max"], b["$max"]),
                "$items": _merge_schema_nodes(
                    a.get("$items", {}), b.get("$items", {}),
                ),
            }
        if not a_is_array and not b_is_array:
            merged = dict(a)
            for k, v in b.items():
                merged[k] = _merge_schema_nodes(merged[k], v) if k in merged else v
            return merged

    # Type conflict (scalar vs dict) — prefer the richer structure
    return a if isinstance(a, dict) else b


def infer_schema(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Infer a flatten schema from one or more sample JSON dicts.

    Each sample should be a single JSON object (e.g. one quote).
    The returned schema merges all fields (union) and uses the maximum
    observed array length for ``$max``.
    """
    if not samples:
        return {}
    schema: dict[str, Any] | str = {}
    for sample in samples:
        schema = _merge_schema_nodes(schema, _infer_schema_node(sample))
    return schema if isinstance(schema, dict) else {}


# ---------------------------------------------------------------------------
# Schema-aware flattening
# ---------------------------------------------------------------------------


def flatten(
    data: dict[str, Any] | None,
    schema: dict[str, Any],
    *,
    _prefix: str = "",
) -> dict[str, Any]:
    """Flatten a nested JSON dict using a schema.

    Walks the *schema* tree (not the data) to produce a consistent column
    set.  Missing data becomes ``None``.

    Keys use ``"."`` as separator; array indices are **1-based**.
    """
    if data is None:
        data = {}
    result: dict[str, Any] = {}

    for key, spec in schema.items():
        full_key = f"{_prefix}.{key}" if _prefix else key
        value = data.get(key) if isinstance(data, dict) else None

        if isinstance(spec, str):
            # Leaf — emit value or None
            result[full_key] = value

        elif "$max" in spec:
            # Array
            max_items: int = spec["$max"]
            items_schema = spec.get("$items", {})
            if max_items == 0 or not items_schema:
                continue
            raw_list = value if isinstance(value, list) else []
            for i in range(max_items):
                idx_key = f"{full_key}.{i + 1}"
                element = raw_list[i] if i < len(raw_list) else None
                if isinstance(items_schema, str):
                    # Array of scalars
                    result[idx_key] = element
                else:
                    # Array of objects — recurse
                    child = element if isinstance(element, dict) else None
                    result.update(flatten(child, items_schema, _prefix=idx_key))

        else:
            # Nested object — recurse
            child = value if isinstance(value, dict) else None
            result.update(flatten(child, spec, _prefix=full_key))

    return result


def schema_columns(
    schema: dict[str, Any],
    *,
    _prefix: str = "",
) -> list[str]:
    """Return the flat column names a schema produces, in traversal order."""
    cols: list[str] = []
    for key, spec in schema.items():
        full_key = f"{_prefix}.{key}" if _prefix else key
        if isinstance(spec, str):
            cols.append(full_key)
        elif "$max" in spec:
            max_items: int = spec["$max"]
            items_schema = spec.get("$items", {})
            if max_items == 0 or not items_schema:
                continue
            for i in range(max_items):
                idx_key = f"{full_key}.{i + 1}"
                if isinstance(items_schema, str):
                    cols.append(idx_key)
                else:
                    cols.extend(schema_columns(items_schema, _prefix=idx_key))
        else:
            cols.extend(schema_columns(spec, _prefix=full_key))
    return cols


# ---------------------------------------------------------------------------
# Streaming record iterator
# ---------------------------------------------------------------------------


def _iter_json_records(path: Path) -> Iterator[dict[str, Any]]:
    """Yield JSON dicts one at a time from a ``.json`` or ``.jsonl`` file.

    For ``.jsonl`` files this is truly streaming — only one line is held in
    memory at a time.  For ``.json`` files the full parse is unavoidable
    (standard JSON requires it), but records are *yielded* so downstream
    processing can still be chunked.
    """
    if path.suffix == ".jsonl":
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    obj = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    yield obj
    else:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item
        elif isinstance(data, dict):
            yield data


def _infer_schema_streaming(
    path: Path,
    max_samples: int = _SCHEMA_SAMPLE_SIZE,
) -> dict[str, Any]:
    """Infer a flatten schema by sampling the first *max_samples* records.

    Unlike :func:`infer_schema` this never holds the full file in memory —
    it streams records and merges schemas on the fly, stopping after
    *max_samples* rows.
    """
    schema: dict[str, Any] | str = {}
    count = 0
    for record in _iter_json_records(path):
        schema = _merge_schema_nodes(schema, _infer_schema_node(record))
        count += 1
        if count >= max_samples:
            break
    logger.info("schema_inferred_streaming", path=str(path), samples=count)
    return schema if isinstance(schema, dict) else {}


# ---------------------------------------------------------------------------
# Arrow schema helpers
# ---------------------------------------------------------------------------


def _schema_leaf_types(
    schema: dict[str, Any],
    *,
    _prefix: str = "",
) -> list[tuple[str, str]]:
    """Return ``[(column_name, type_str), ...]`` from a flatten schema."""
    result: list[tuple[str, str]] = []
    for key, spec in schema.items():
        full_key = f"{_prefix}.{key}" if _prefix else key
        if isinstance(spec, str):
            result.append((full_key, spec))
        elif "$max" in spec:
            max_items: int = spec["$max"]
            items_schema = spec.get("$items", {})
            if max_items == 0 or not items_schema:
                continue
            for i in range(max_items):
                idx_key = f"{full_key}.{i + 1}"
                if isinstance(items_schema, str):
                    result.append((idx_key, items_schema))
                else:
                    result.extend(
                        _schema_leaf_types(items_schema, _prefix=idx_key),
                    )
        else:
            result.extend(_schema_leaf_types(spec, _prefix=full_key))
    return result


def _arrow_schema_from_flatten(flatten_schema: dict[str, Any]) -> pa.Schema:
    """Build a PyArrow ``Schema`` from a flatten schema.

    JSON numbers are inherently ambiguous (``1`` could be int or float),
    so both ``"int"`` and ``"float"`` map to ``float64`` for safety.  This
    avoids mid-stream cast failures when a field inferred as int from the
    sample turns out to contain floats in later records.
    """
    import pyarrow as pa

    _dtype_map: dict[str, pa.DataType] = {
        "bool": pa.bool_(),
        "int": pa.float64(),
        "float": pa.float64(),
        "str": pa.string(),
    }
    return pa.schema([
        pa.field(name, _dtype_map.get(dtype, pa.string()), nullable=True)
        for name, dtype in _schema_leaf_types(flatten_schema)
    ])


# ---------------------------------------------------------------------------
# Chunked streaming flatten → parquet writer (fallback for Polars failures)
# ---------------------------------------------------------------------------


def _chunked(
    iterator: Iterator[dict[str, Any]],
    size: int,
) -> Iterator[list[dict[str, Any]]]:
    """Yield lists of up to *size* items from *iterator*."""
    chunk: list[dict[str, Any]] = []
    for item in iterator:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _coerce_to_arrow(values: list[Any], target_type: pa.DataType) -> pa.Array:
    """Build an Arrow array, coercing values that don't match *target_type*.

    Fast path: ``pa.array(values, type=target_type)`` — handles the vast
    majority of data.  Slow path: per-value coercion for mixed-type edges.
    """
    import pyarrow as pa

    try:
        return pa.array(values, type=target_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError,
            TypeError, ValueError):
        pass

    if target_type == pa.float64():
        coerced: list[float | None] = []
        for v in values:
            if v is None:
                coerced.append(None)
            elif isinstance(v, (int, float)):
                coerced.append(float(v))
            else:
                try:
                    coerced.append(float(v))
                except (ValueError, TypeError):
                    coerced.append(None)
        return pa.array(coerced, type=pa.float64())

    if target_type == pa.bool_():
        return pa.array(
            [v if isinstance(v, bool) else None for v in values],
            type=pa.bool_(),
        )

    # String fallback — always succeeds
    return pa.array(
        [str(v) if v is not None else None for v in values],
        type=pa.string(),
    )


def _rows_to_batch(
    rows: list[dict[str, Any]],
    arrow_schema: pa.Schema,
) -> pa.RecordBatch:
    """Convert flattened row-dicts into an Arrow ``RecordBatch``."""
    import pyarrow as pa

    arrays = [
        _coerce_to_arrow(
            [row.get(field.name) for row in rows],
            field.type,
        )
        for field in arrow_schema
    ]
    return pa.record_batch(arrays, schema=arrow_schema)


# -- Core streaming writer (fallback) ----------------------------------------

def _flatten_and_write_streaming(
    records_iter: Iterator[dict[str, Any]],
    flatten_schema: dict[str, Any],
    cache_path: Path,
    *,
    chunk_size: int = _FLATTEN_CHUNK_SIZE,
    progress_key: str | None = None,
    t0: float | None = None,
) -> int:
    """Stream JSON records through flatten → PyArrow ParquetWriter in chunks.

    This is the **fallback** path used when the Polars-native path fails
    (e.g. malformed JSONL, schema inconsistencies).  Single-threaded but
    memory-bounded: only ``chunk_size`` rows are held at a time.

    Returns the total number of rows written.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cache_path.with_suffix(".parquet.tmp")
    arrow_schema = _arrow_schema_from_flatten(flatten_schema)

    writer: pq.ParquetWriter | None = None
    total_rows = 0

    def _update_progress() -> None:
        if progress_key is not None and t0 is not None:
            with _flatten_lock:
                _flatten_progress[progress_key] = {
                    "rows": total_rows,
                    "elapsed": round(time.monotonic() - t0, 1),
                }

    def _write_rows(rows: list[dict[str, Any]]) -> None:
        nonlocal writer, total_rows
        if not rows:
            return
        batch = _rows_to_batch(rows, arrow_schema)
        if writer is None:
            writer = pq.ParquetWriter(
                str(tmp_path), arrow_schema, compression="zstd",
            )
        writer.write_batch(batch)
        total_rows += len(rows)
        _update_progress()

    try:
        for chunk_records in _chunked(records_iter, chunk_size):
            rows = [flatten(d, flatten_schema) for d in chunk_records]
            _write_rows(rows)

        if writer is not None:
            writer.close()
            writer = None
            tmp_path.rename(cache_path)
        else:
            empty = pa.table(
                {f.name: pa.array([], type=f.type) for f in arrow_schema},
                schema=arrow_schema,
            )
            pq.write_table(empty, str(tmp_path), compression="zstd")
            tmp_path.rename(cache_path)

        logger.info(
            "json_cache_written",
            path=str(cache_path),
            rows=total_rows,
            size_bytes=cache_path.stat().st_size,
        )
        return total_rows

    except BaseException:
        if writer is not None:
            writer.close()
        tmp_path.unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Polars-native flatten → parquet (primary path)
# ---------------------------------------------------------------------------

_NDJSON_SCHEMA_SAMPLE = 1_000  # rows Polars samples for NDJSON schema inference


def _struct_field_dtype(dtype: Any, name: str) -> Any:
    """Return the dtype of *name* within a Polars ``Struct``, or ``None``.

    Works across Polars versions by duck-typing on the ``fields`` attribute.
    """
    fields = getattr(dtype, "fields", None)
    if fields is None:
        return None
    for f in fields:
        if f.name == name:
            return f.dtype
    return None


def _build_polars_exprs(
    flatten_schema: dict[str, Any],
    lf_schema: dict[str, Any],
) -> list[pl.Expr]:
    """Translate a flatten schema into Polars ``select()`` expressions.

    Walks the flatten schema tree and builds one expression per leaf column.
    For each leaf, the expression chains ``.struct.field()`` and
    ``.list.get()`` calls to navigate from the top-level Polars column down
    to the target value.  Missing columns or struct fields produce
    ``pl.lit(None)`` so the resulting DataFrame always has the full column
    set regardless of data completeness.

    Parameters
    ----------
    flatten_schema:
        The flatten schema (same format as :func:`infer_schema`).
    lf_schema:
        Mapping of ``{column_name: polars_dtype}`` from the LazyFrame's
        ``collect_schema()``.

    Returns
    -------
    list[pl.Expr]
        One aliased expression per leaf column in schema traversal order.
    """
    import polars as pl

    exprs: list[pl.Expr] = []

    def _walk(
        spec: dict[str, Any] | str,
        alias: str,
        expr: pl.Expr | None,
        dtype: Any,
    ) -> None:
        """Recursively descend the schema tree, emitting leaf expressions."""
        if isinstance(spec, str):
            # Leaf — emit expression or null literal
            exprs.append(
                expr.alias(alias) if expr is not None else pl.lit(None).alias(alias),
            )

        elif "$max" in spec:
            # Array — iterate up to $max elements
            max_items: int = spec["$max"]
            items_schema = spec.get("$items", {})
            if not max_items or not items_schema:
                return
            inner = getattr(dtype, "inner", None) if dtype is not None else None
            for i in range(max_items):
                elem_expr = (
                    expr.list.get(i, null_on_oob=True) if expr is not None else None
                )
                _walk(items_schema, f"{alias}.{i + 1}", elem_expr, inner)

        else:
            # Object — recurse into each field
            for key, child_spec in spec.items():
                child_alias = f"{alias}.{key}" if alias else key
                child_dtype = _struct_field_dtype(dtype, key)
                if expr is not None and child_dtype is not None:
                    child_expr = expr.struct.field(key)
                else:
                    child_expr = None
                    child_dtype = None
                _walk(child_spec, child_alias, child_expr, child_dtype)

    for key, spec in flatten_schema.items():
        col_dtype = lf_schema.get(key)
        col_expr = pl.col(key) if col_dtype is not None else None
        _walk(spec, key, col_expr, col_dtype)

    return exprs


def _flatten_with_polars(
    data_path: Path,
    flatten_schema: dict[str, Any],
    cache_path: Path,
    *,
    progress_key: str | None = None,
    t0: float | None = None,
) -> int:
    """Flatten JSON/JSONL to parquet using Polars-native operations.

    Pipeline: ``scan_ndjson`` (Rust multi-threaded reader) →
    expression-based flatten (``.struct.field()``, ``.list.get()``) →
    ``sink_parquet`` (streaming write).

    **No Python-level data processing loop** — all heavy lifting happens
    in Polars' Rust core, which parallelises automatically across CPU cores.

    Falls back to ``collect(engine="streaming") → write_parquet`` if
    ``sink_parquet`` doesn't support the expression plan.

    Returns the total number of rows written.
    Raises on failure (caller should fall back to streaming Python path).
    """
    import polars as pl
    import pyarrow.parquet as pq

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cache_path.with_suffix(".parquet.tmp")

    # --- Read with Polars ---------------------------------------------------
    if data_path.suffix == ".jsonl":
        lf = pl.scan_ndjson(data_path, infer_schema_length=_NDJSON_SCHEMA_SAMPLE)
    else:
        lf = pl.read_json(data_path).lazy()

    # --- Build flatten expressions ------------------------------------------
    schema_map = dict(lf.collect_schema())
    exprs = _build_polars_exprs(flatten_schema, schema_map)

    if not exprs:
        # Empty flatten schema — write an empty parquet file
        pl.DataFrame().write_parquet(str(tmp_path), compression="zstd")
        tmp_path.rename(cache_path)
        return 0

    result_lf = lf.select(exprs)

    # --- Write to parquet ---------------------------------------------------
    try:
        result_lf.sink_parquet(str(tmp_path), compression="zstd")
    except Exception:
        # Streaming sink may not support all expressions — fall back to
        # collect with streaming engine + eager write (same pattern as
        # executor.py sink output).
        logger.info("polars_sink_fallback", path=str(data_path))
        df = result_lf.collect(engine="streaming")
        df.write_parquet(str(tmp_path), compression="zstd")

    tmp_path.rename(cache_path)

    # --- Report result ------------------------------------------------------
    meta = pq.read_metadata(str(cache_path))
    total_rows = meta.num_rows

    if progress_key is not None and t0 is not None:
        with _flatten_lock:
            _flatten_progress[progress_key] = {
                "rows": total_rows,
                "elapsed": round(time.monotonic() - t0, 1),
            }

    logger.info(
        "json_cache_written_polars",
        path=str(cache_path),
        rows=total_rows,
        size_bytes=cache_path.stat().st_size,
    )
    return total_rows


# ---------------------------------------------------------------------------
# Sample loading (kept for backward compat — UI schema preview, tests)
# ---------------------------------------------------------------------------


def load_samples(path: str | Path) -> list[dict[str, Any]]:
    """Load sample data from a ``.json`` or ``.jsonl`` file.

    - ``.json``:  a single object ``{…}`` or an array of objects ``[{…}, …]``.
    - ``.jsonl``: one JSON object per line.

    .. note::
       For large files prefer :func:`_iter_json_records` which streams
       records without holding the full file in memory.
    """
    p = Path(path)
    text = p.read_text(encoding="utf-8")

    if p.suffix == ".jsonl":
        samples: list[dict[str, Any]] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped:
                obj = json.loads(stripped)
                if isinstance(obj, dict):
                    samples.append(obj)
        logger.info("samples_loaded", path=str(p), count=len(samples))
        return samples

    data = json.loads(text)
    if isinstance(data, list):
        result = [d for d in data if isinstance(d, dict)]
    elif isinstance(data, dict):
        result = [data]
    else:
        result = []

    logger.info("samples_loaded", path=str(p), count=len(result))
    return result


# ---------------------------------------------------------------------------
# Convenience: flatten → LazyFrame
# ---------------------------------------------------------------------------


def flatten_to_frame(
    data: dict[str, Any] | list[dict[str, Any]],
    schema: dict[str, Any],
) -> pl.LazyFrame:
    """Flatten one or more JSON dicts and return a Polars ``LazyFrame``."""
    import polars as pl

    if isinstance(data, dict):
        data = [data]
    rows = [flatten(d, schema) for d in data]
    return pl.from_dicts(rows).lazy()


# ---------------------------------------------------------------------------
# Parquet cache for flattened JSON — avoids re-flattening on every run
# ---------------------------------------------------------------------------

_CACHE_DIR = ".haute_cache"


def _json_cache_path(data_path: str | Path) -> Path:
    """Compute the parquet cache path for a JSON data file."""
    safe = str(data_path).replace("/", "_").replace("\\", "_").replace(".", "_")
    return Path.cwd() / _CACHE_DIR / f"json_{safe}.parquet"


def _is_cache_valid(cache_path: Path, *source_paths: Path) -> bool:
    """Return True if cache exists and is newer than all source files."""
    if not cache_path.exists():
        return False
    cache_mtime = cache_path.stat().st_mtime
    for src in source_paths:
        if src.exists() and src.stat().st_mtime > cache_mtime:
            return False
    return True


def _flatten_and_write(
    samples: list[dict[str, Any]],
    schema: dict[str, Any],
    cache_path: Path,
) -> None:
    """Flatten samples and write to a parquet cache file.

    Writes to a temporary file first and atomically renames on success
    so a failed flatten never leaves a corrupt cache behind.
    """
    import polars as pl

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cache_path.with_suffix(".parquet.tmp")

    try:
        rows = [flatten(d, schema) for d in samples]
        df = pl.from_dicts(rows) if rows else pl.DataFrame()
        df.write_parquet(tmp_path, compression="zstd")
        tmp_path.rename(cache_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    logger.info(
        "json_cache_written",
        path=str(cache_path),
        rows=len(samples),
        size_bytes=cache_path.stat().st_size,
    )


def _resolve_flatten_schema(
    data_path: Path,
    schema: dict[str, Any] | None,
    config_path: str | Path | None,
) -> dict[str, Any]:
    """Resolve the flatten schema from explicit arg, config file, or inference."""
    if schema is not None:
        return schema

    if config_path is not None:
        cp = Path(config_path)
        if cp.exists():
            cfg = json.loads(cp.read_text(encoding="utf-8"))
            from_config = cfg.get("flattenSchema")
            if from_config is not None:
                return from_config

    inferred = _infer_schema_streaming(data_path)
    logger.info(
        "schema_inferred", path=str(data_path),
        columns=len(schema_columns(inferred)),
    )
    return inferred


def read_json_flat(
    data_path: str | Path,
    *,
    schema: dict[str, Any] | None = None,
    config_path: str | Path | None = None,
) -> pl.LazyFrame:
    """Load JSON/JSONL, flatten to tabular, return a Polars ``LazyFrame``.

    On the first call, flattens the data and writes a parquet cache under
    ``.haute_cache/``.  Subsequent calls return ``pl.scan_parquet()`` directly
    — truly lazy with predicate pushdown.  The cache auto-invalidates when
    the source data file or config file is modified.

    Primary path uses Polars-native ``scan_ndjson`` + expression-based
    flatten + ``sink_parquet`` — all in Rust, multi-threaded.  Falls back to
    Python streaming if the Polars path fails.
    """
    import polars as pl

    p = Path(data_path)
    cache_path = _json_cache_path(data_path)

    source_paths = [p]
    if config_path is not None:
        source_paths.append(Path(config_path))

    if _is_cache_valid(cache_path, *source_paths):
        logger.info("json_cache_hit", path=str(data_path), cache=str(cache_path))
        return pl.scan_parquet(cache_path)

    resolved = _resolve_flatten_schema(p, schema, config_path)

    try:
        _flatten_with_polars(p, resolved, cache_path)
    except Exception:
        logger.info("polars_flatten_fallback", path=str(data_path))
        _flatten_and_write_streaming(_iter_json_records(p), resolved, cache_path)

    return pl.scan_parquet(cache_path)


# ---------------------------------------------------------------------------
# Explicit cache management (mirrors _databricks_io cache helpers)
# ---------------------------------------------------------------------------


def is_large_json(data_path: str | Path) -> bool:
    """Return True if the file size is >= the large-file threshold (50 MB)."""
    p = Path(data_path)
    return p.exists() and p.stat().st_size >= _LARGE_FILE_THRESHOLD


def flatten_progress(data_path: str) -> dict[str, object] | None:
    """Return current flatten progress for *data_path*, or ``None`` if not active."""
    with _flatten_lock:
        return _flatten_progress.get(data_path)


class JsonCacheInfoDict(TypedDict):
    path: str
    data_path: str
    row_count: int
    column_count: int
    columns: dict[str, str]
    size_bytes: int
    cached_at: float


def json_cache_info(data_path: str | Path) -> JsonCacheInfoDict | None:
    """Return metadata about a cached JSON file, or ``None`` if not cached."""
    import pyarrow.parquet as pq

    cache_path = _json_cache_path(data_path)
    if not cache_path.exists():
        return None
    stat = cache_path.stat()
    meta = pq.read_metadata(str(cache_path))
    arrow_schema = pq.read_schema(str(cache_path))
    columns = {name: str(arrow_schema.field(name).type) for name in arrow_schema.names}
    return {
        "path": str(cache_path),
        "data_path": str(data_path),
        "row_count": meta.num_rows,
        "column_count": meta.num_columns,
        "columns": columns,
        "size_bytes": stat.st_size,
        "cached_at": stat.st_mtime,
    }


def clear_json_cache(data_path: str | Path) -> bool:
    """Delete the cached parquet file for a JSON data file. Returns True if deleted."""
    cache_path = _json_cache_path(data_path)
    if cache_path.exists():
        cache_path.unlink()
        return True
    return False


class JsonBuildResultDict(JsonCacheInfoDict):
    cache_seconds: float


def build_json_cache(
    data_path: str | Path,
    schema: dict[str, Any] | None = None,
    config_path: str | Path | None = None,
) -> JsonBuildResultDict:
    """Build the parquet cache for a JSON/JSONL file with progress tracking.

    This is the explicit entry point for the "Cache as Parquet" button.
    Mirrors :func:`_databricks_io.fetch_and_cache`.

    Primary path uses Polars-native ``scan_ndjson`` + expression-based
    flatten + ``sink_parquet`` — all heavy lifting in Rust, automatically
    parallelised across CPU cores.  Falls back to Python streaming
    (single-threaded, chunked PyArrow writes) if the Polars path fails.
    """
    p = Path(data_path)
    data_path_str = str(data_path)
    cache_path = _json_cache_path(data_path)

    t0 = time.monotonic()
    with _flatten_lock:
        _flatten_progress[data_path_str] = {"rows": 0, "elapsed": 0.0}

    try:
        resolved = _resolve_flatten_schema(p, schema, config_path)
        try:
            _flatten_with_polars(
                p, resolved, cache_path,
                progress_key=data_path_str,
                t0=t0,
            )
        except Exception:
            logger.info("polars_flatten_fallback", path=str(data_path))
            _flatten_and_write_streaming(
                _iter_json_records(p),
                resolved,
                cache_path,
                progress_key=data_path_str,
                t0=t0,
            )
    finally:
        with _flatten_lock:
            _flatten_progress.pop(data_path_str, None)

    elapsed = time.monotonic() - t0

    import pyarrow.parquet as pq

    stat = cache_path.stat()
    meta = pq.read_metadata(str(cache_path))
    arrow_schema = pq.read_schema(str(cache_path))
    columns_map = {
        name: str(arrow_schema.field(name).type) for name in arrow_schema.names
    }

    return {
        "path": str(cache_path),
        "data_path": data_path_str,
        "row_count": meta.num_rows,
        "column_count": meta.num_columns,
        "columns": columns_map,
        "size_bytes": stat.st_size,
        "cached_at": stat.st_mtime,
        "cache_seconds": round(elapsed, 2),
    }
