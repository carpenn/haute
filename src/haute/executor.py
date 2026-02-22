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

import re
import threading
from collections.abc import Callable
from typing import Any

import polars as pl

from haute._logging import get_logger
from haute._rating import (
    _apply_banding,
    _apply_rating_table,
    _combine_rating_columns,
    _normalise_banding_factors,
)
from haute._sandbox import safe_globals, validate_user_code
from haute.graph_utils import (
    GraphNode,
    NodeType,
    PipelineGraph,
    _execute_eager_core,
    _execute_lazy,
    _Frame,
    _sanitize_func_name,
    build_instance_mapping,
    graph_fingerprint,
    load_external_object,
    read_source,
)
from haute.schemas import ColumnInfo, NodeResult, SchemaWarning, SinkResponse

logger = get_logger(component="executor")


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

    # Validate the original user code at the AST level before exec().
    # This blocks dunder access, imports, getattr, class defs, etc.
    # at the structural level — a stronger layer than restricted builtins.
    validate_user_code(code)

    try:
        exec(exec_code, safe_globals(pl=pl, **(extra_ns or {})), local_ns)
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
    return result  # type: ignore[no-any-return]


def resolve_instance_node(node: GraphNode, node_map: dict[str, GraphNode]) -> GraphNode:
    """If *node* is an instance, return a merged node with the original's config.

    The returned node keeps the instance's own id, label, and position but
    uses the original node's ``nodeType`` and ``config`` (minus the
    ``instanceOf`` key itself).  If the original cannot be found the
    instance is returned unchanged.
    """
    config = node.data.config
    ref = config.get("instanceOf")
    if not ref or ref not in node_map:
        return node
    original = node_map[ref]
    orig_config = {k: v for k, v in original.data.config.items() if k != "instanceOf"}
    # Preserve instance-specific keys (inputMapping) that the UI sets
    instance_keys = {k: v for k, v in config.items() if k in ("inputMapping",)}
    merged_config = {**orig_config, "instanceOf": ref, **instance_keys}
    merged_data = node.data.model_copy(update={
        "nodeType": original.data.nodeType,
        "config": merged_config,
    })
    return node.model_copy(update={"data": merged_data})


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

    data = node.data
    node_type = data.nodeType
    config = data.config
    label = data.label
    func_name = _sanitize_func_name(label)

    if source_names is None:
        source_names = []

    if node_type == NodeType.API_INPUT:
        path = config.get("path", "")

        def api_source_fn() -> _Frame:
            return read_source(path)

        return func_name, api_source_fn, True

    if node_type == NodeType.DATA_SOURCE:
        path = config.get("path", "")
        source_type = config.get("sourceType", "flat_file")

        if source_type == "databricks":
            table = config.get("table", "")

            def _databricks_source(_table: str = table) -> _Frame:
                from haute._databricks_io import read_cached_table

                return read_cached_table(_table)

            return func_name, _databricks_source, True

        def source_fn() -> _Frame:
            return read_source(path)

        return func_name, source_fn, True

    elif node_type == NodeType.LIVE_SWITCH:
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

    elif node_type == NodeType.DATA_SINK:
        # During normal run/preview, dataSink is a pass-through.
        # Actual writing happens via execute_sink() on explicit user action.
        def sink_passthrough(*dfs: _Frame) -> _Frame:
            return dfs[0] if dfs else pl.LazyFrame()

        return func_name, sink_passthrough, False

    elif node_type == NodeType.EXTERNAL_FILE:
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

    elif node_type == NodeType.OUTPUT:
        fields = config.get("fields", []) or []

        def output_fn(*dfs: _Frame) -> _Frame:
            lf = dfs[0] if dfs else pl.LazyFrame()
            if fields:
                lf = lf.select(fields)
            return lf

        return func_name, output_fn, False

    elif node_type == NodeType.BANDING:
        factors = _normalise_banding_factors(config)

        def banding_fn(*dfs: _Frame, _factors: list = list(factors)) -> _Frame:
            lf = dfs[0] if dfs else pl.LazyFrame()
            for f in _factors:
                col = f.get("column", "")
                out = f.get("outputColumn", "")
                rules = f.get("rules", []) or []
                if not col or not out or not rules:
                    continue
                lf = _apply_banding(
                    lf, col, out, f.get("banding", "continuous"),
                    rules, f.get("default"),
                )
            return lf

        return func_name, banding_fn, False

    elif node_type == NodeType.RATING_STEP:
        tables: list[dict[str, Any]] = config.get("tables", []) or []
        # GUI config may send None for these fields, so `or` ensures a usable default
        _rs_operation: str = config.get("operation", "multiply") or "multiply"
        _rs_combined: str = config.get("combinedColumn", "") or ""

        def rating_fn(
            *dfs: _Frame,
            _tables: list = list(tables),
            _op: str = _rs_operation,
            _combined: str = _rs_combined,
        ) -> _Frame:
            lf = dfs[0] if dfs else pl.LazyFrame()
            out_cols: list[str] = []
            for t in _tables:
                lf = _apply_rating_table(lf, t)
                oc = t.get("outputColumn", "")
                if oc:
                    out_cols.append(oc)
            if _combined and len(out_cols) >= 2:
                logger.info(
                    "combining_rating_columns",
                    columns=out_cols,
                    operation=_op,
                    output=_combined,
                )
                lf = _combine_rating_columns(lf, out_cols, _op, _combined)
            return lf

        return func_name, rating_fn, False

    elif node_type == NodeType.MODELLING:
        # Pass-through in preview mode. Training happens via /api/modelling/train.
        def modelling_passthrough(*dfs: _Frame) -> _Frame:
            return dfs[0] if dfs else pl.LazyFrame()

        return func_name, modelling_passthrough, False

    elif node_type == NodeType.MODEL_SCORE:
        source_type = config.get("sourceType", "")
        _run_id = config.get("run_id", "")
        _artifact_path = config.get("artifact_path", "")
        _registered_model = config.get("registered_model", "")
        _version = config.get("version", "latest")
        _task = config.get("task", "regression")
        _output_col = config.get("output_column", "prediction")
        code = config.get("code", "").strip()
        _src_names = list(source_names)

        # If no model source configured, passthrough
        if not source_type or (source_type == "run" and not _run_id) or (
            source_type == "registered" and not _registered_model
        ):

            def model_score_passthrough(*dfs: _Frame) -> _Frame:
                return dfs[0] if dfs else pl.LazyFrame()

            return func_name, model_score_passthrough, False

        def model_score_fn(*dfs: _Frame) -> _Frame:
            from haute._mlflow_io import load_mlflow_model

            model = load_mlflow_model(
                source_type=source_type,
                run_id=_run_id,
                artifact_path=_artifact_path,
                registered_model=_registered_model,
                version=_version,
                task=_task,
            )
            lf = dfs[0] if dfs else pl.LazyFrame()
            # Determine feature intersection before collecting so Polars
            # can (in theory) apply projection pushdown on upstream scans.
            available_cols = set(lf.collect_schema().names())
            features = [f for f in model.feature_names_ if f in available_cols]
            # CatBoost requires numpy arrays; collect → predict → lazy is the minimum conversion
            df_eager = lf.collect()
            x_data = df_eager.select(features).to_pandas()
            preds = model.predict(x_data).flatten()
            df_eager = df_eager.with_columns(pl.Series(_output_col, preds))

            if _task == "classification" and hasattr(model, "predict_proba"):
                probas = model.predict_proba(x_data)
                if probas.ndim == 2:
                    probas = probas[:, 1]
                df_eager = df_eager.with_columns(
                    pl.Series(f"{_output_col}_proba", probas)
                )

            result_lf = df_eager.lazy()
            if code:
                result_lf = _exec_user_code(
                    code, _src_names, (result_lf,),
                    extra_ns={"model": model},
                )
            return result_lf

        return func_name, model_score_fn, False

    elif node_type == NodeType.TRANSFORM:
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
    """Thread-safe single-entry cache for the most recent pipeline execution."""

    __slots__ = ("fingerprint", "eager_outputs", "errors", "order", "timings", "_lock")

    def __init__(self) -> None:
        self.fingerprint: str | None = None
        self.eager_outputs: dict[str, pl.DataFrame] = {}
        self.errors: dict[str, str] = {}
        self.order: list[str] = []
        self.timings: dict[str, float] = {}
        self._lock = threading.Lock()

    def invalidate(self) -> None:
        with self._lock:
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
) -> dict[str, NodeResult]:
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
    if not graph.nodes:
        return {}

    fp = graph_fingerprint(graph, str(row_limit))

    errors: dict[str, str] = {}

    # Check if we can extend the cache (same graph, new target is a superset)
    with _preview_cache._lock:
        if fp == _preview_cache.fingerprint and _preview_cache.eager_outputs:
            cached = _preview_cache.eager_outputs
            if target_node_id is None or target_node_id in cached:
                eager_outputs = cached
                order = _preview_cache.order
                errors = _preview_cache.errors
                timings = _preview_cache.timings
            else:
                raw_outputs, order, errors, timings = _eager_execute(
                    graph, target_node_id, row_limit,
                )
                eager_outputs = {k: v for k, v in raw_outputs.items() if v is not None}
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
            raw_outputs, order, errors, timings = _eager_execute(
                graph, target_node_id, row_limit,
            )
            eager_outputs = {k: v for k, v in raw_outputs.items() if v is not None}
            _preview_cache.fingerprint = fp
            _preview_cache.eager_outputs = eager_outputs
            _preview_cache.errors = errors
            _preview_cache.timings = timings
            _preview_cache.order = order

    # Pre-compute schema warnings for instance nodes by comparing the
    # columns available at the instance's inputs vs the original's inputs.
    node_map = graph.node_map
    parents_of = graph.parents_of

    schema_warnings: dict[str, list[SchemaWarning]] = {}
    for nid in order:
        ref = node_map[nid].data.config.get("instanceOf")
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
                SchemaWarning(column=c, status="missing") for c in sorted(missing)
            ]

    results: dict[str, NodeResult] = {}
    for nid in order:
        if nid in errors:
            results[nid] = NodeResult(
                status="error",
                error=errors[nid],
                timing_ms=timings.get(nid, 0),
                schema_warnings=schema_warnings.get(nid, []),
            )
            continue
        df = eager_outputs.get(nid)
        if df is None:
            results[nid] = NodeResult(
                status="error",
                error="No output",
                timing_ms=timings.get(nid, 0),
            )
            continue
        columns = [
            ColumnInfo(name=c, dtype=str(df[c].dtype)) for c in df.columns
        ]
        results[nid] = NodeResult(
            status="ok",
            row_count=len(df),
            column_count=len(df.columns),
            columns=columns,
            preview=df.head(max_preview_rows).to_dicts(),
            timing_ms=timings.get(nid, 0),
            schema_warnings=schema_warnings.get(nid, []),
        )

    error_count = sum(1 for r in results.values() if r.status == "error")
    logger.info(
        "graph_executed",
        node_count=len(results),
        error_count=error_count,
        target=target_node_id,
    )
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
    result = _execute_eager_core(
        graph,
        _build_node_fn,
        target_node_id=target_node_id,
        row_limit=row_limit,
        swallow_errors=True,
    )
    return result.outputs, result.order, result.errors, result.timings


def execute_sink(graph: PipelineGraph, sink_node_id: str) -> SinkResponse:
    """Execute the pipeline up to a sink node and write its input to disk.

    Uses Polars streaming sinks (``sink_parquet`` / ``sink_csv``) so the
    full dataset is never materialised in memory at once.  Falls back to
    ``collect(engine="streaming")`` + eager write if the streaming sink raises
    (e.g. when the plan contains an operation that doesn't support the
    streaming engine).

    This is called on-demand (not during normal run/preview).
    Returns a ``SinkResponse`` with row count and output path.
    """
    from pathlib import Path

    sink_node = graph.node_map.get(sink_node_id)
    if not sink_node:
        raise ValueError(f"Sink node '{sink_node_id}' not found")

    config = sink_node.data.config
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

    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    try:
        if fmt == "csv":
            lf.sink_csv(out)
        else:
            lf.sink_parquet(out)
    except Exception:
        # Fallback: collect with streaming hint, then write eagerly.
        logger.info("sink_streaming_fallback", path=path, format=fmt)
        df = lf.collect(engine="streaming")
        if fmt == "csv":
            df.write_csv(out)
        else:
            df.write_parquet(out)

    # Read back row count cheaply from file metadata.
    if fmt == "csv":
        row_count = pl.scan_csv(out).select(pl.len()).collect().item()
    else:
        row_count = pl.scan_parquet(out).select(pl.len()).collect().item()

    return SinkResponse(
        status="ok",
        message=f"Wrote {row_count:,} rows to {path}",
        row_count=row_count,
        path=path,
        format=fmt,
    )
