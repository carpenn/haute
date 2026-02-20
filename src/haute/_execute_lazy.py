"""Lazy graph execution — shared by executor and scorer."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from haute._logging import get_logger
from haute._topo import ancestors, topo_sort_ids
from haute._types import (
    GraphNode,
    PipelineGraph,
    _Frame,
    _sanitize_func_name,
    resolve_orig_source_names,
)

logger = get_logger(component="execute_lazy")


def _prepare_graph(
    graph: PipelineGraph,
    target_node_id: str | None = None,
) -> tuple[
    dict[str, GraphNode],  # node_map
    list[str],  # order (topo-sorted node IDs)
    dict[str, list[str]],  # parents_of
    dict[str, str],  # id_to_name
]:
    """Shared graph preparation: filter, topo-sort, and build lookups.

    Returns (node_map, order, parents_of, id_to_name).
    """
    nodes = graph.nodes
    edges = graph.edges

    node_map = {n.id: n for n in nodes}
    all_ids = set(node_map.keys())

    if target_node_id:
        needed = ancestors(target_node_id, edges, all_ids)
    else:
        needed = all_ids

    relevant_edges = [e for e in edges if e.source in needed and e.target in needed]
    order = topo_sort_ids([nid for nid in all_ids if nid in needed], relevant_edges)

    parents_of: dict[str, list[str]] = {nid: [] for nid in order}
    for e in relevant_edges:
        if e.target in parents_of:
            parents_of[e.target].append(e.source)

    id_to_name: dict[str, str] = {}
    for nid in order:
        label = node_map[nid].data.label
        id_to_name[nid] = _sanitize_func_name(label)

    return node_map, order, parents_of, id_to_name


def _execute_lazy(
    graph: PipelineGraph,
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

    # Full parent lookup from ALL edges for instance resolution
    all_parents: dict[str, list[str]] = {}
    for e in graph.edges:
        all_parents.setdefault(e.target, []).append(e.source)

    # Build executable functions
    funcs: dict[str, tuple[Callable, bool]] = {}
    for nid in order:
        source_names = [id_to_name[pid] for pid in parents_of.get(nid, []) if pid in id_to_name]
        orig_src_names = resolve_orig_source_names(
            node_map[nid], node_map, all_parents, id_to_name,
        )
        kwargs: dict[str, Any] = {"source_names": source_names}
        if orig_src_names is not None:
            kwargs["node_map"] = node_map
            kwargs["orig_source_names"] = orig_src_names
        _, fn, is_source = build_node_fn(node_map[nid], **kwargs)
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
