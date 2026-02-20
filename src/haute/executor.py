"""Graph executor: run a pipeline graph JSON dynamically.

Takes a React Flow graph (nodes + edges) and executes it as a real
Polars pipeline, without needing a saved .py file.

Node functions produce LazyFrames.  Preview and trace use eager
single-pass execution with per-graph caching so repeated clicks
don't re-execute the pipeline.  Source nodes are capped at
row_limit rows.  Sink and CLI paths use lazy execution so Polars
can optimise the full plan end-to-end.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from typing import Any

import polars as pl

from haute.graph_utils import (
    GraphNode,
    PipelineGraph,
    _execute_lazy,
    _Frame,
    _prepare_graph,
    _sanitize_func_name,
    build_instance_mapping,
    graph_fingerprint,
    load_external_object,
    resolve_orig_source_names,
)

logger = logging.getLogger("uvicorn.error")


def _exec_user_code(
    code: str,
    src_names: list[str],
    dfs: tuple[_Frame, ...],
    extra_ns: dict[str, Any] | None = None,
    orig_source_names: list[str] | None = None,
    input_mapping: dict[str, str] | None = None,
) -> _Frame:
    """Wrap, execute, and return the result of user-provided code.

    Shared by transform and externalFile node types.
    - Injects ``pl``, input DataFrames by name, and ``df`` (first input).
    - Optionally merges *extra_ns* into the local namespace (e.g. ``obj``).
    - Handles the ``.chain`` / bare-expression wrapping and adjusts line
      numbers in error messages so they match the editor.
    """
    local_ns: dict[str, Any] = {"pl": pl}
    for i, d in enumerate(dfs):
        if i < len(src_names):
            local_ns[src_names[i]] = d
    # Instance alias injection: bind original source names so the original's
    # code can reference variables by their original upstream labels.
    if orig_source_names:
        mapping = build_instance_mapping(orig_source_names, src_names, input_mapping)
        for orig, inst in mapping.items():
            if orig not in local_ns and inst in local_ns:
                local_ns[orig] = local_ns[inst]
    if dfs:
        local_ns["df"] = dfs[0]
    if extra_ns:
        local_ns.update(extra_ns)

    exec_code = code
    line_offset = 0
    if code.startswith("."):
        first = src_names[0] if src_names else "df"
        exec_code = f"df = (\n    {first}\n    {code}\n)"
        line_offset = 2
    elif "df =" not in code and "df=" not in code:
        exec_code = f"df = (\n    {code}\n)"
        line_offset = 1

    try:
        exec(exec_code, {"pl": pl, **(extra_ns or {})}, local_ns)
    except SyntaxError as exc:
        if exc.lineno is not None:
            exc.lineno = max(1, exc.lineno - line_offset)
        raise
    except Exception as exc:
        msg = str(exc)
        if line_offset and re.search(r"line \d+", msg):
            msg = re.sub(
                r"line (\d+)",
                lambda m: f"line {max(1, int(m.group(1)) - line_offset)}",
                msg,
            )
            raise type(exc)(msg) from None
        raise

    result = local_ns.get("df", dfs[0] if dfs else pl.LazyFrame())
    if isinstance(result, pl.DataFrame):
        result = result.lazy()
    return result


def resolve_instance_node(node: GraphNode, node_map: dict[str, GraphNode]) -> GraphNode:
    """If *node* is an instance, return a merged node with the original's config.

    The returned node keeps the instance's own id, label, and position but
    uses the original node's ``nodeType`` and ``config`` (minus the
    ``instanceOf`` key itself).  If the original cannot be found the
    instance is returned unchanged.
    """
    config = node.get("data", {}).get("config", {})
    ref = config.get("instanceOf")
    if not ref or ref not in node_map:
        return node
    original = node_map[ref]
    orig_data = original.get("data", {})
    orig_config = {k: v for k, v in orig_data.get("config", {}).items() if k != "instanceOf"}
    # Preserve instance-specific keys (inputMapping) that the UI sets
    inst_config = node.get("data", {}).get("config", {})
    instance_keys = {k: v for k, v in inst_config.items() if k in ("inputMapping",)}
    merged_data = {
        **node.get("data", {}),
        "nodeType": orig_data.get("nodeType", "transform"),
        "config": {**orig_config, "instanceOf": ref, **instance_keys},
    }
    return {**node, "data": merged_data}


def _build_node_fn(
    node: GraphNode,
    source_names: list[str] | None = None,
    row_limit: int | None = None,
    node_map: dict[str, GraphNode] | None = None,
    orig_source_names: list[str] | None = None,
) -> tuple[str, Callable, bool]:
    """Build an executable function from a graph node dict.

    Returns (func_name, fn, is_source).
    source_names: sanitized names of upstream nodes (used as variable names).
    row_limit: if set, Databricks sources push this into SQL LIMIT so the
        full table is never fetched during preview/trace.
    node_map: full graph node_map — used to resolve ``instanceOf`` references.
    """
    # Resolve instance → use original's config/nodeType
    if node_map:
        node = resolve_instance_node(node, node_map)

    data = node.get("data", {})
    node_type = data.get("nodeType", "transform")
    config = data.get("config", {})
    label = data.get("label", "Unnamed")
    func_name = _sanitize_func_name(label)

    if source_names is None:
        source_names = []

    if node_type == "apiInput":
        path = config.get("path", "")

        def api_source_fn() -> _Frame:
            if path.endswith(".jsonl"):
                return pl.scan_ndjson(path)
            else:
                # .json — single object or array
                return pl.read_json(path).lazy()

        return func_name, api_source_fn, True

    if node_type == "dataSource":
        path = config.get("path", "")
        source_type = config.get("sourceType", "flat_file")

        if source_type == "databricks":
            table = config.get("table", "")

            def source_fn(_table: str = table) -> _Frame:
                from haute._databricks_io import read_cached_table

                return read_cached_table(_table)

            return func_name, source_fn, True

        def source_fn() -> _Frame:
            if path.endswith(".csv"):
                return pl.scan_csv(path)
            elif path.endswith(".json"):
                return pl.read_json(path).lazy()
            else:
                return pl.scan_parquet(path)

        return func_name, source_fn, True

    elif node_type == "liveSwitch":
        mode = config.get("mode", "live")
        input_names = list(source_names)
        param_names = config.get("inputs", [])
        live_name = param_names[0] if param_names else None

        def switch_fn(*dfs: _Frame) -> _Frame:
            target = live_name if mode == "live" else mode
            if target:
                for i, name in enumerate(input_names):
                    if name == target:
                        return dfs[i]
            return dfs[0]

        return func_name, switch_fn, False

    elif node_type == "dataSink":
        # During normal run/preview, dataSink is a pass-through.
        # Actual writing happens via execute_sink() on explicit user action.
        def sink_passthrough(*dfs: _Frame) -> _Frame:
            return dfs[0] if dfs else pl.LazyFrame()

        return func_name, sink_passthrough, False

    elif node_type == "externalFile":
        code = config.get("code", "").strip()
        path = config.get("path", "")
        file_type = config.get("fileType", "pickle")
        model_class = config.get("modelClass", "classifier")
        _src_names = list(source_names)

        _orig_src = list(orig_source_names) if orig_source_names else None
        _in_map = dict(config.get("inputMapping", {})) or None
        if code:

            def external_fn(*dfs: _Frame) -> _Frame:
                obj = load_external_object(path, file_type, model_class)
                return _exec_user_code(
                    code, _src_names, dfs,
                    extra_ns={"obj": obj},
                    orig_source_names=_orig_src,
                    input_mapping=_in_map,
                )

            return func_name, external_fn, False
        else:

            def external_passthrough(*dfs: _Frame) -> _Frame:
                return dfs[0] if dfs else pl.LazyFrame()

            return func_name, external_passthrough, False

    elif node_type == "output":
        fields = config.get("fields", []) or []

        def output_fn(*dfs: _Frame) -> _Frame:
            lf = dfs[0] if dfs else pl.LazyFrame()
            if fields:
                lf = lf.select(fields)
            return lf

        return func_name, output_fn, False

    elif node_type in ("transform", "modelScore", "ratingStep"):
        code = config.get("code", "").strip()
        _src_names = list(source_names)
        _orig_src = list(orig_source_names) if orig_source_names else None
        _in_map = dict(config.get("inputMapping", {})) or None

        if code:

            def transform_fn(*dfs: _Frame) -> _Frame:
                return _exec_user_code(
                    code, _src_names, dfs,
                    orig_source_names=_orig_src,
                    input_mapping=_in_map,
                )

            return func_name, transform_fn, False
        else:

            def passthrough(*dfs: _Frame) -> _Frame:
                return dfs[0] if dfs else pl.LazyFrame()

            return func_name, passthrough, False

    else:

        def default_passthrough(*dfs: _Frame) -> _Frame:
            return dfs[0] if dfs else pl.LazyFrame()

        return func_name, default_passthrough, False


# ---------------------------------------------------------------------------
# Preview cache — same principle as the trace cache in trace.py.
# The pipeline doesn't change between node clicks — only the target node
# changes.  Cache the materialized DataFrames so clicking different nodes
# is instant instead of re-executing model scoring on 678K rows each time.
# ---------------------------------------------------------------------------


class _PreviewCache:
    """Single-entry cache for the most recent pipeline execution."""

    __slots__ = ("fingerprint", "eager_outputs", "errors", "order", "timings")

    def __init__(self) -> None:
        self.fingerprint: str | None = None
        self.eager_outputs: dict[str, pl.DataFrame] = {}
        self.errors: dict[str, str] = {}
        self.order: list[str] = []
        self.timings: dict[str, float] = {}

    def invalidate(self) -> None:
        self.fingerprint = None
        self.eager_outputs.clear()
        self.errors.clear()
        self.timings.clear()


_preview_cache = _PreviewCache()


def execute_graph(
    graph: PipelineGraph,
    target_node_id: str | None = None,
    row_limit: int | None = None,
    max_preview_rows: int = 100,
) -> dict[str, dict]:
    """Execute a graph and return per-node results.

    Uses eager single-pass execution with a single-entry cache so
    clicking different nodes doesn't re-execute the full pipeline.

    Args:
        graph: React Flow graph with "nodes" and "edges".
        target_node_id: If set, only execute nodes up to (and including) this node.
        row_limit: If set, apply .head(row_limit) to source nodes so only
                   that many rows flow through the pipeline.
        max_preview_rows: Max rows to include in the JSON preview payload.

    Returns:
        Dict mapping node_id → {
            "status": "ok" | "error",
            "row_count": int,
            "columns": [...],
            "preview": [...],
            "error": str | None,
        }
    """
    if not graph.get("nodes"):
        return {}

    fp = graph_fingerprint(graph, str(row_limit))

    errors: dict[str, str] = {}

    # Check if we can extend the cache (same graph, new target is a superset)
    if fp == _preview_cache.fingerprint and _preview_cache.eager_outputs:
        cached = _preview_cache.eager_outputs
        if target_node_id is None or target_node_id in cached:
            eager_outputs = cached
            order = _preview_cache.order
            errors = _preview_cache.errors
            timings = _preview_cache.timings
        else:
            eager_outputs, order, errors, timings = _eager_execute(
                graph, target_node_id, row_limit,
            )
            merged = {**cached, **eager_outputs}
            _preview_cache.eager_outputs = merged
            _preview_cache.errors = {**_preview_cache.errors, **errors}
            _preview_cache.timings = {**_preview_cache.timings, **timings}
            _preview_cache.order = list(
                dict.fromkeys(_preview_cache.order + order),
            )
            eager_outputs = merged
            errors = _preview_cache.errors
            timings = _preview_cache.timings
            order = _preview_cache.order
    else:
        eager_outputs, order, errors, timings = _eager_execute(
            graph, target_node_id, row_limit,
        )
        _preview_cache.fingerprint = fp
        _preview_cache.eager_outputs = eager_outputs
        _preview_cache.errors = errors
        _preview_cache.timings = timings
        _preview_cache.order = order

    # Pre-compute schema warnings for instance nodes by comparing the
    # columns available at the instance's inputs vs the original's inputs.
    nodes_list = graph.get("nodes", [])
    edges_list = graph.get("edges", [])
    node_map = {n["id"]: n for n in nodes_list}
    parents_of: dict[str, list[str]] = {}
    for e in edges_list:
        parents_of.setdefault(e["target"], []).append(e["source"])

    schema_warnings: dict[str, list[dict]] = {}
    for nid in order:
        ref = node_map.get(nid, {}).get("data", {}).get("config", {}).get("instanceOf")
        if not ref or ref not in node_map:
            continue
        # Columns feeding into the original node
        orig_input_cols: set[str] = set()
        for pid in parents_of.get(ref, []):
            df = eager_outputs.get(pid)
            if df is not None:
                orig_input_cols.update(df.columns)
        # Columns feeding into the instance node
        inst_input_cols: set[str] = set()
        for pid in parents_of.get(nid, []):
            df = eager_outputs.get(pid)
            if df is not None:
                inst_input_cols.update(df.columns)
        missing = orig_input_cols - inst_input_cols
        if missing:
            schema_warnings[nid] = [
                {"column": c, "status": "missing"} for c in sorted(missing)
            ]

    results: dict[str, dict] = {}
    for nid in order:
        if nid in errors:
            results[nid] = {
                "status": "error",
                "row_count": 0,
                "column_count": 0,
                "columns": [],
                "preview": [],
                "error": errors[nid],
                "timing_ms": timings.get(nid, 0),
                "schema_warnings": schema_warnings.get(nid, []),
            }
            continue
        df = eager_outputs.get(nid)
        if df is None:
            results[nid] = {
                "status": "error",
                "row_count": 0,
                "column_count": 0,
                "columns": [],
                "preview": [],
                "error": "No output",
                "timing_ms": timings.get(nid, 0),
                "schema_warnings": [],
            }
            continue
        columns = [
            {"name": c, "dtype": str(df[c].dtype)} for c in df.columns
        ]
        results[nid] = {
            "status": "ok",
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": columns,
            "preview": df.head(max_preview_rows).to_dicts(),
            "error": None,
            "timing_ms": timings.get(nid, 0),
            "schema_warnings": schema_warnings.get(nid, []),
        }

    return results


def _eager_execute(
    graph: PipelineGraph,
    target_node_id: str | None,
    row_limit: int | None,
) -> tuple[dict[str, pl.DataFrame | None], list[str], dict[str, str], dict[str, float]]:
    """Execute the graph eagerly in topo order.

    Returns (outputs, order, errors, timings) where errors maps
    node_id → message for nodes that failed, and timings maps
    node_id → execution milliseconds.
    """
    node_map, order, parents_of, id_to_name = _prepare_graph(
        graph, target_node_id,
    )

    # Full parent lookup from ALL edges for instance resolution
    all_parents: dict[str, list[str]] = {}
    for e in graph.get("edges", []):
        all_parents.setdefault(e["target"], []).append(e["source"])

    funcs: dict[str, tuple[Callable, bool]] = {}
    for nid in order:
        src_names = [
            id_to_name[pid]
            for pid in parents_of.get(nid, [])
            if pid in id_to_name
        ]
        orig_src_names = resolve_orig_source_names(
            node_map[nid], node_map, all_parents, id_to_name,
        )
        _, fn, is_source = _build_node_fn(
            node_map[nid], source_names=src_names, row_limit=row_limit,
            node_map=node_map, orig_source_names=orig_src_names,
        )
        funcs[nid] = (fn, is_source)

    eager_outputs: dict[str, pl.DataFrame | None] = {}
    errors: dict[str, str] = {}
    timings: dict[str, float] = {}

    for nid in order:
        fn, is_source = funcs[nid]
        t0 = time.perf_counter()
        try:
            if is_source:
                result = fn()
                if row_limit and isinstance(result, pl.LazyFrame):
                    result = result.head(row_limit)
            else:
                input_ids = parents_of.get(nid, [])
                input_lfs = [
                    eager_outputs[pid].lazy()
                    for pid in input_ids
                    if pid in eager_outputs and eager_outputs[pid] is not None
                ]
                if not input_lfs:
                    raise ValueError("No valid input data available")
                result = fn(*input_lfs)

            df = result.collect() if isinstance(result, pl.LazyFrame) else result
            eager_outputs[nid] = df
        except Exception as exc:
            logger.warning("Node %s failed: %s", nid, exc)
            eager_outputs[nid] = None
            errors[nid] = str(exc)
        timings[nid] = round((time.perf_counter() - t0) * 1000, 1)

    return eager_outputs, order, errors, timings


def execute_sink(graph: PipelineGraph, sink_node_id: str) -> dict:
    """Execute the pipeline up to a sink node and write its input to disk.

    This is called on-demand (not during normal run/preview).
    Returns a status dict with row count and output path.
    """
    from pathlib import Path

    nodes = graph.get("nodes", [])
    node_map = {n["id"]: n for n in nodes}

    sink_node = node_map.get(sink_node_id)
    if not sink_node:
        raise ValueError(f"Sink node '{sink_node_id}' not found")

    data = sink_node["data"]
    config = data.get("config", {})
    path = config.get("path", "")
    fmt = config.get("format", "parquet")

    if not path:
        raise ValueError("Sink node has no output path configured")

    lazy_outputs, _order, _parents, _names = _execute_lazy(
        graph,
        _build_node_fn,
        target_node_id=sink_node_id,
    )

    lf = lazy_outputs.get(sink_node_id)
    if lf is None:
        raise RuntimeError("Failed to compute sink input")

    df = lf.collect()

    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.write_csv(out)
    else:
        df.write_parquet(out)

    return {
        "status": "ok",
        "message": f"Wrote {len(df):,} rows to {path}",
        "row_count": len(df),
        "path": path,
        "format": fmt,
    }
