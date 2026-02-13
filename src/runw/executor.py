"""Graph executor: run a pipeline graph JSON dynamically.

Takes a React Flow graph (nodes + edges) and executes it as a real
Polars pipeline, without needing a saved .py file.

Uses LazyFrames throughout so Polars can push predicates and limits
down into scans.  Preview mode slaps a .head(row_limit) before
.collect() — the query optimiser folds this into the scan.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from runw.graph_utils import _sanitize_func_name, topo_sort_ids, ancestors

# Type alias — nodes pass lazy frames between each other
_Frame = pl.LazyFrame


def _build_node_fn(node: dict, source_names: list[str] | None = None) -> tuple[str, callable, bool]:
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

    elif node_type in ("transform", "modelScore", "ratingStep", "output"):
        code = config.get("code", "").strip()
        _src_names = list(source_names)

        if code:
            def transform_fn(*dfs: _Frame) -> _Frame:
                local_ns: dict[str, Any] = {"pl": pl}
                for i, d in enumerate(dfs):
                    if i < len(_src_names):
                        local_ns[_src_names[i]] = d
                if dfs:
                    local_ns["df"] = dfs[0]

                exec_code = code
                if code.startswith("."):
                    first = _src_names[0] if _src_names else "df"
                    exec_code = f"df = (\n    {first}\n    {code}\n)"
                elif "df =" not in code and "df=" not in code:
                    exec_code = f"df = (\n    {code}\n)"

                exec(exec_code, {"pl": pl}, local_ns)
                result = local_ns.get("df", dfs[0] if dfs else pl.LazyFrame())
                # If user code collected to a DataFrame, make it lazy again
                if isinstance(result, pl.DataFrame):
                    result = result.lazy()
                return result
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
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    if not nodes:
        return {}

    node_map = {n["id"]: n for n in nodes}
    all_ids = set(node_map.keys())

    # Determine which nodes to execute
    if target_node_id:
        needed = ancestors(target_node_id, edges, all_ids)
    else:
        needed = all_ids

    # Filter edges to only relevant ones
    relevant_edges = [e for e in edges if e["source"] in needed and e["target"] in needed]

    # Topo sort the needed nodes
    order = topo_sort_ids([nid for nid in all_ids if nid in needed], relevant_edges)

    # Build parent lookup (node_id → list of parent node_ids)
    parents_of: dict[str, list[str]] = {nid: [] for nid in order}
    for e in relevant_edges:
        if e["target"] in parents_of:
            parents_of[e["target"]].append(e["source"])

    # Map node_id → sanitized func name (for source_names)
    id_to_name: dict[str, str] = {}
    for nid in order:
        label = node_map[nid].get("data", {}).get("label", "Unnamed")
        id_to_name[nid] = _sanitize_func_name(label)

    # Build functions with source names so variables match upstream node names
    funcs: dict[str, tuple[callable, bool]] = {}
    for nid in order:
        source_names = [id_to_name[pid] for pid in parents_of.get(nid, []) if pid in id_to_name]
        _, fn, is_source = _build_node_fn(node_map[nid], source_names=source_names)
        funcs[nid] = (fn, is_source)

    # Execute — all intermediate results stay lazy
    lazy_outputs: dict[str, _Frame] = {}
    results: dict[str, dict] = {}

    for nid in order:
        fn, is_source = funcs[nid]
        try:
            if is_source:
                lf = fn()
            else:
                input_ids = parents_of.get(nid, [])
                if input_ids:
                    input_lfs = [lazy_outputs[pid] for pid in input_ids if pid in lazy_outputs]
                    if len(input_lfs) == 0:
                        raise ValueError(f"No input data available for node '{nid}'")
                    lf = fn(*input_lfs)
                else:
                    last_lfs = list(lazy_outputs.values())
                    if last_lfs:
                        lf = fn(last_lfs[-1])
                    else:
                        raise ValueError(f"Node '{nid}' has no input and is not a source")

            # Ensure it's lazy
            if isinstance(lf, pl.DataFrame):
                lf = lf.lazy()

            lazy_outputs[nid] = lf

            # Collect — with optional row limit pushed into the query plan
            if row_limit is not None:
                df = lf.head(row_limit).collect()
            else:
                df = lf.collect()

            columns = [
                {"name": col, "dtype": str(df[col].dtype)}
                for col in df.columns
            ]

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
                "status": "error",
                "row_count": 0,
                "column_count": 0,
                "columns": [],
                "preview": [],
                "error": str(exc),
            }

    return results
