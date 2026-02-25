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
from pathlib import Path
from typing import TYPE_CHECKING, Any

from haute._logging import get_logger

if TYPE_CHECKING:
    import polars as pl

logger = get_logger(component="json_flatten")

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
# Sample loading
# ---------------------------------------------------------------------------


def load_samples(path: str | Path) -> list[dict[str, Any]]:
    """Load sample data from a ``.json`` or ``.jsonl`` file.

    - ``.json``:  a single object ``{…}`` or an array of objects ``[{…}, …]``.
    - ``.jsonl``: one JSON object per line.
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


def read_json_flat(
    data_path: str | Path,
    *,
    schema: dict[str, Any] | None = None,
    config_path: str | Path | None = None,
) -> pl.LazyFrame:
    """Load JSON/JSONL, flatten to tabular, return a Polars ``LazyFrame``.

    If *schema* is provided it is used directly.  Otherwise, if *config_path*
    points to a config JSON with a ``flattenSchema`` key, that schema is used.
    As a final fallback the schema is inferred from the data.
    """
    samples = load_samples(data_path)

    if schema is None and config_path is not None:
        cp = Path(config_path)
        if cp.exists():
            cfg = json.loads(cp.read_text(encoding="utf-8"))
            schema = cfg.get("flattenSchema")

    if schema is None:
        schema = infer_schema(samples)
        logger.info("schema_inferred", path=str(data_path), columns=len(schema_columns(schema)))

    return flatten_to_frame(samples, schema)
