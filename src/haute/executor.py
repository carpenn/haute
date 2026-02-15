"""Graph executor: run a pipeline graph JSON dynamically.

Takes a React Flow graph (nodes + edges) and executes it as a real
Polars pipeline, without needing a saved .py file.

Uses LazyFrames throughout so Polars can push predicates and limits
down into scans.  Preview mode slaps a .head(row_limit) before
.collect() — the query optimiser folds this into the scan.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

import polars as pl

from haute.graph_utils import _execute_lazy, _Frame, _sanitize_func_name, load_external_object


def _exec_user_code(
    code: str,
    src_names: list[str],
    dfs: tuple[_Frame, ...],
    extra_ns: dict[str, Any] | None = None,
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
        if line_offset and re.search(r'line \d+', msg):
            msg = re.sub(
                r'line (\d+)',
                lambda m: f'line {max(1, int(m.group(1)) - line_offset)}',
                msg,
            )
            raise type(exc)(msg) from None
        raise

    result = local_ns.get("df", dfs[0] if dfs else pl.LazyFrame())
    if isinstance(result, pl.DataFrame):
        result = result.lazy()
    return result


def _build_node_fn(node: dict, source_names: list[str] | None = None) -> tuple[str, Callable, bool]:
    """Build an executable function from a graph node dict.

    Returns (func_name, fn, is_source).
    source_names: sanitized names of upstream nodes (used as variable names).
    """
    data = node.get("data", {})
    node_type = data.get("nodeType", "transform")
    config = data.get("config", {})
    label = data.get("label", "Unnamed")
    func_name = _sanitize_func_name(label)

    if source_names is None:
        source_names = []

    if node_type == "dataSource":
        path = config.get("path", "")
        source_type = config.get("sourceType", "flat_file")

        if source_type == "databricks":
            def source_fn() -> _Frame:
                raise NotImplementedError("Databricks source not yet implemented")
            return func_name, source_fn, True

        def source_fn() -> _Frame:
            if path.endswith(".csv"):
                return pl.scan_csv(path)
            elif path.endswith(".json"):
                # json has no scan — read eagerly then make lazy
                return pl.read_json(path).lazy()
            else:
                return pl.scan_parquet(path)

        return func_name, source_fn, True

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

        if code:
            def external_fn(*dfs: _Frame) -> _Frame:
                obj = load_external_object(path, file_type, model_class)
                return _exec_user_code(code, _src_names, dfs, extra_ns={"obj": obj})
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

        if code:
            def transform_fn(*dfs: _Frame) -> _Frame:
                return _exec_user_code(code, _src_names, dfs)
            return func_name, transform_fn, False
        else:
            def passthrough(*dfs: _Frame) -> _Frame:
                return dfs[0] if dfs else pl.LazyFrame()
            return func_name, passthrough, False

    else:
        def default_passthrough(*dfs: _Frame) -> _Frame:
            return dfs[0] if dfs else pl.LazyFrame()
        return func_name, default_passthrough, False




def execute_graph(
    graph: dict,
    target_node_id: str | None = None,
    row_limit: int | None = None,
    max_preview_rows: int = 100,
) -> dict[str, dict]:
    """Execute a graph (lazy) and return per-node results.

    Args:
        graph: React Flow graph with "nodes" and "edges".
        target_node_id: If set, only execute nodes up to (and including) this node.
        row_limit: If set, apply .head(row_limit) before .collect() so Polars
                   pushes the limit into the scan.  None means collect everything.
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

    lazy_outputs, order, _parents, _names = _execute_lazy(
        graph, _build_node_fn, target_node_id,
    )

    results: dict[str, dict] = {}
    for nid in order:
        lf = lazy_outputs.get(nid)
        if lf is None:
            results[nid] = {
                "status": "error", "row_count": 0, "column_count": 0,
                "columns": [], "preview": [], "error": "No output",
            }
            continue
        try:
            df = lf.head(row_limit).collect() if row_limit is not None else lf.collect()
            columns = [{"name": c, "dtype": str(df[c].dtype)} for c in df.columns]
            results[nid] = {
                "status": "ok",
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": columns,
                "preview": df.head(max_preview_rows).to_dicts(),
                "error": None,
            }
        except Exception as exc:
            results[nid] = {
                "status": "error", "row_count": 0, "column_count": 0,
                "columns": [], "preview": [], "error": str(exc),
            }

    return results


def execute_sink(graph: dict, sink_node_id: str) -> dict:
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
        graph, _build_node_fn, target_node_id=sink_node_id,
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
