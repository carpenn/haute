"""Shared graph utilities used by both codegen and executor."""

from __future__ import annotations

import hashlib
from collections import deque
from collections.abc import Callable
from typing import Any, TypedDict

import polars as pl

# Type alias - nodes pass lazy frames between each other
_Frame = pl.LazyFrame


class NodeData(TypedDict, total=False):
    """Data payload for a single pipeline node."""

    label: str
    description: str
    nodeType: str
    config: dict[str, Any]


class GraphNode(TypedDict, total=False):
    """A single node in the React Flow graph."""

    id: str
    type: str
    position: dict[str, float]
    data: NodeData


class GraphEdge(TypedDict):
    """A single edge in the React Flow graph."""

    id: str
    source: str
    target: str


class PipelineGraph(TypedDict, total=False):
    """React Flow graph structure used throughout Haute.

    This is the canonical type for the graph dict passed between
    parser, executor, codegen, deploy, and the server API layer.
    """

    nodes: list[GraphNode]
    edges: list[GraphEdge]
    pipeline_name: str
    pipeline_description: str
    preamble: str
    source_file: str


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


def graph_fingerprint(graph: dict, *extra_keys: str) -> str:
    """Deterministic hash of graph structure for cache invalidation.

    *extra_keys* are prepended (e.g. target_node_id, row_limit) so the
    same graph with different execution parameters gets a different hash.
    Used by both the trace cache (trace.py) and preview cache (executor.py).
    """
    parts: list[str] = list(extra_keys)
    for n in sorted(graph.get("nodes", []), key=lambda n: n["id"]):
        d = n.get("data", {})
        c = d.get("config", {})
        parts.append(
            f"{n['id']}|{d.get('nodeType')}|{c.get('code', '')}|{c.get('path', '')}"
            f"|{c.get('table', '')}|{c.get('query', '')}",
        )
    for e in sorted(
        graph.get("edges", []),
        key=lambda e: (e["source"], e["target"]),
    ):
        parts.append(f"{e['source']}->{e['target']}")
    return hashlib.md5("\n".join(parts).encode()).hexdigest()


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
    dict[str, dict],  # node_map
    list[str],  # order (topo-sorted node IDs)
    dict[str, list[str]],  # parents_of
    dict[str, str],  # id_to_name
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


_object_cache: dict[tuple[str, float, str, str], object] = {}


def load_external_object(path: str, file_type: str, model_class: str = "classifier") -> object:
    """Load an external file (model, JSON, pickle, joblib) and return the object.

    Shared by the development executor and the deploy scoring engine.

    Results are cached by ``(path, mtime, file_type, model_class)`` so
    repeated calls (preview clicks, API scoring requests) skip disk I/O.
    The cache auto-invalidates when the file is modified on disk.
    """
    import os

    try:
        mtime = os.path.getmtime(path)
    except OSError:
        mtime = 0.0
    key = (path, mtime, file_type, model_class)
    cached = _object_cache.get(key)
    if cached is not None:
        return cached

    obj = _load_external_object_uncached(path, file_type, model_class)
    _object_cache[key] = obj
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
        import pickle

        with open(path, "rb") as f:
            return pickle.load(f)


def flatten_graph(graph: dict) -> dict:
    """Dissolve all submodel nodes into a flat graph for execution.

    Replaces each ``submodel`` node with its child nodes (stored in the
    ``submodels`` metadata) and rewires boundary edges so they point to
    the actual internal nodes.

    If the graph has no submodels, it is returned unchanged.
    """
    submodels = graph.get("submodels")
    if not submodels:
        return graph

    nodes = list(graph.get("nodes", []))
    edges = list(graph.get("edges", []))

    # Remove submodel placeholder nodes
    submodel_node_ids = {f"submodel__{name}" for name in submodels}
    nodes = [n for n in nodes if n["id"] not in submodel_node_ids]

    # Inline child nodes and internal edges from each submodel
    for sm_name, sm_meta in submodels.items():
        sm_graph = sm_meta.get("graph", {})
        nodes.extend(sm_graph.get("nodes", []))
        edges_to_add = sm_graph.get("edges", [])
        edges.extend(edges_to_add)

    # Rewire boundary edges: submodel handles → actual child nodes
    rewired_edges: list[dict] = []
    for edge in edges:
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        source_handle = edge.get("sourceHandle", "")
        target_handle = edge.get("targetHandle", "")

        new_edge = dict(edge)

        if src in submodel_node_ids and source_handle:
            # e.g. sourceHandle="out__frequency_model" → source="frequency_model"
            actual_src = source_handle.removeprefix("out__")
            new_edge["source"] = actual_src
            new_edge["id"] = f"e_{actual_src}_{tgt}"
            new_edge.pop("sourceHandle", None)

        if tgt in submodel_node_ids and target_handle:
            # e.g. targetHandle="in__frequency_model" → target="frequency_model"
            actual_tgt = target_handle.removeprefix("in__")
            new_edge["target"] = actual_tgt
            new_edge["id"] = f"e_{src}_{actual_tgt}"
            new_edge.pop("targetHandle", None)

        # Skip edges that still reference a submodel node (shouldn't happen)
        if new_edge["source"] in submodel_node_ids or new_edge["target"] in submodel_node_ids:
            continue

        rewired_edges.append(new_edge)

    # Deduplicate edges by (source, target)
    seen: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for e in rewired_edges:
        key = (e["source"], e["target"])
        if key not in seen:
            seen.add(key)
            deduped.append(e)

    result = {**graph, "nodes": nodes, "edges": deduped}
    result.pop("submodels", None)
    return result


def _execute_lazy(
    graph: dict,
    build_node_fn: Callable,
    target_node_id: str | None = None,
) -> tuple[dict[str, _Frame], list[str], dict[str, list[str]], dict[str, str]]:
    """Execute a graph lazily and return per-node LazyFrames.

    Used by execute_sink (batch writes) and score_graph (deploy scoring)
    where Polars can optimise the full lazy plan end-to-end.
    Interactive paths (preview, trace) use eager execution with caching
    instead — see executor._eager_execute and trace.execute_trace.

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

    # Execute - all intermediate results stay lazy
    lazy_outputs: dict[str, _Frame] = {}

    for nid in order:
        fn, is_source = funcs[nid]
        if is_source:
            lf = fn()
        else:
            input_ids = parents_of.get(nid, [])
            input_lfs = [lazy_outputs[pid] for pid in input_ids if pid in lazy_outputs]
            if not input_lfs:
                raise ValueError(f"No input data available for node '{nid}'")
            lf = fn(*input_lfs)

        if isinstance(lf, pl.DataFrame):
            lf = lf.lazy()
        lazy_outputs[nid] = lf

    return lazy_outputs, order, parents_of, id_to_name
