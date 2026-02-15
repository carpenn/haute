"""Shared graph utilities used by both codegen and executor."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable

import polars as pl

# Type alias — nodes pass lazy frames between each other
_Frame = pl.LazyFrame


def _sanitize_func_name(label: str) -> str:
    """Convert a human label to a valid Python function name (preserves casing)."""
    name = label.strip()
    name = name.replace(" ", "_").replace("-", "_")
    name = "".join(c for c in name if c.isalnum() or c == "_")
    if name and name[0].isdigit():
        name = f"node_{name}"
    return name or "unnamed_node"


def topo_sort_ids(node_ids: list[str], edges: list[dict]) -> list[str]:
    """Topological sort of node IDs based on edges (Kahn's algorithm)."""
    in_degree: dict[str, int] = {nid: 0 for nid in node_ids}
    children: dict[str, list[str]] = {nid: [] for nid in node_ids}

    for e in edges:
        src, tgt = e["source"], e["target"]
        if tgt in in_degree:
            in_degree[tgt] += 1
        if src in children:
            children[src].append(tgt)

    queue = deque(sorted(nid for nid, deg in in_degree.items() if deg == 0))
    result: list[str] = []

    while queue:
        nid = queue.popleft()
        result.append(nid)
        for child in children.get(nid, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)
        # Re-sort for deterministic ordering (queue is small)
        queue = deque(sorted(queue))

    return result


def ancestors(target_id: str, edges: list[dict], all_ids: set[str]) -> set[str]:
    """Get all ancestor node IDs of target (inclusive)."""
    parents: dict[str, list[str]] = {nid: [] for nid in all_ids}
    for e in edges:
        if e["target"] in parents:
            parents[e["target"]].append(e["source"])

    visited: set[str] = set()

    def walk(nid: str) -> None:
        if nid in visited:
            return
        visited.add(nid)
        for p in parents.get(nid, []):
            walk(p)

    walk(target_id)
    return visited


def _prepare_graph(
    graph: dict,
    target_node_id: str | None = None,
) -> tuple[
    dict[str, dict],       # node_map
    list[str],             # order (topo-sorted node IDs)
    dict[str, list[str]],  # parents_of
    dict[str, str],        # id_to_name
]:
    """Shared graph preparation: filter, topo-sort, and build lookups.

    Returns (node_map, order, parents_of, id_to_name).
    """
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    node_map = {n["id"]: n for n in nodes}
    all_ids = set(node_map.keys())

    if target_node_id:
        needed = ancestors(target_node_id, edges, all_ids)
    else:
        needed = all_ids

    relevant_edges = [e for e in edges if e["source"] in needed and e["target"] in needed]
    order = topo_sort_ids([nid for nid in all_ids if nid in needed], relevant_edges)

    parents_of: dict[str, list[str]] = {nid: [] for nid in order}
    for e in relevant_edges:
        if e["target"] in parents_of:
            parents_of[e["target"]].append(e["source"])

    id_to_name: dict[str, str] = {}
    for nid in order:
        label = node_map[nid].get("data", {}).get("label", "Unnamed")
        id_to_name[nid] = _sanitize_func_name(label)

    return node_map, order, parents_of, id_to_name


def _execute_lazy(
    graph: dict,
    build_node_fn: Callable,
    target_node_id: str | None = None,
) -> tuple[dict[str, _Frame], list[str], dict[str, list[str]], dict[str, str]]:
    """Execute a graph lazily and return per-node LazyFrames.

    This is the single execution core shared by execute_graph,
    execute_sink, and execute_trace.

    Args:
        graph: React Flow graph with "nodes" and "edges".
        build_node_fn: Function (node_dict, source_names) -> (name, fn, is_source).
        target_node_id: If set, only execute ancestors of this node.

    Returns:
        (lazy_outputs, order, parents_of, id_to_name)
    """
    node_map, order, parents_of, id_to_name = _prepare_graph(graph, target_node_id)

    # Build executable functions
    funcs: dict[str, tuple[Callable, bool]] = {}
    for nid in order:
        source_names = [id_to_name[pid] for pid in parents_of.get(nid, []) if pid in id_to_name]
        _, fn, is_source = build_node_fn(node_map[nid], source_names=source_names)
        funcs[nid] = (fn, is_source)

    # Execute — all intermediate results stay lazy
    lazy_outputs: dict[str, _Frame] = {}

    for nid in order:
        fn, is_source = funcs[nid]
        if is_source:
            lf = fn()
        else:
            input_ids = parents_of.get(nid, [])
            if input_ids:
                input_lfs = [lazy_outputs[pid] for pid in input_ids if pid in lazy_outputs]
                if not input_lfs:
                    raise ValueError(f"No input data available for node '{nid}'")
                lf = fn(*input_lfs)
            else:
                last_lfs = list(lazy_outputs.values())
                if last_lfs:
                    lf = fn(last_lfs[-1])
                else:
                    raise ValueError(f"Node '{nid}' has no input and is not a source")

        if isinstance(lf, pl.DataFrame):
            lf = lf.lazy()
        lazy_outputs[nid] = lf

    return lazy_outputs, order, parents_of, id_to_name
