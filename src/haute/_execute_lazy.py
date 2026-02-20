"""Lazy and eager graph execution — shared by executor, trace, and scorer."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, NamedTuple

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

logger = get_logger(component="execute")


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


# ---------------------------------------------------------------------------
# Eager execution core — shared by executor (preview) and trace
# ---------------------------------------------------------------------------


def _build_funcs(
    order: list[str],
    node_map: dict[str, GraphNode],
    parents_of: dict[str, list[str]],
    id_to_name: dict[str, str],
    all_parents: dict[str, list[str]],
    build_node_fn: Callable,
    *,
    row_limit: int | None = None,
) -> dict[str, tuple[Callable, bool]]:
    """Build per-node executable functions from the graph.

    Shared between eager and lazy paths.  ``row_limit`` is forwarded to
    ``build_node_fn`` so Databricks sources can push LIMIT into SQL.
    """
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
        _, fn, is_source = build_node_fn(
            node_map[nid], source_names=src_names, row_limit=row_limit,
            node_map=node_map, orig_source_names=orig_src_names,
        )
        funcs[nid] = (fn, is_source)
    return funcs


class EagerResult(NamedTuple):
    """Result of eager graph execution."""

    outputs: dict[str, pl.DataFrame | None]
    order: list[str]
    parents_of: dict[str, list[str]]
    node_map: dict[str, GraphNode]
    id_to_name: dict[str, str]
    errors: dict[str, str]
    timings: dict[str, float]


def _execute_eager_core(
    graph: PipelineGraph,
    build_node_fn: Callable,
    target_node_id: str | None = None,
    row_limit: int | None = None,
    swallow_errors: bool = False,
) -> EagerResult:
    """Execute the graph eagerly in topo order and collect DataFrames.

    Shared core for the preview executor and the trace engine.

    Args:
        graph: React Flow graph.
        build_node_fn: ``(node, source_names=..., ...) -> (name, fn, is_source)``.
        target_node_id: If set, only execute ancestors of this node.
        row_limit: Cap source-node output to this many rows.
        swallow_errors: If ``True``, record per-node errors and continue
            (preview behaviour).  If ``False``, raise immediately (trace).

    Returns:
        An ``EagerResult`` with named fields for outputs, order,
        parents_of, node_map, id_to_name, errors, and timings.
    """
    node_map, order, parents_of, id_to_name = _prepare_graph(
        graph, target_node_id,
    )

    # Full parent lookup from ALL edges for instance resolution
    all_parents: dict[str, list[str]] = {}
    for e in graph.edges:
        all_parents.setdefault(e.target, []).append(e.source)

    funcs = _build_funcs(
        order, node_map, parents_of, id_to_name, all_parents,
        build_node_fn, row_limit=row_limit,
    )

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
                    raise ValueError(
                        f"No input data available for node '{nid}'",
                    )
                result = fn(*input_lfs)

            df = result.collect() if isinstance(result, pl.LazyFrame) else result
            eager_outputs[nid] = df
        except Exception as exc:
            if not swallow_errors:
                raise
            logger.warning("node_failed", node_id=nid, error=str(exc))
            eager_outputs[nid] = None
            errors[nid] = str(exc)
        timings[nid] = round((time.perf_counter() - t0) * 1000, 1)

    return EagerResult(eager_outputs, order, parents_of, node_map, id_to_name, errors, timings)
