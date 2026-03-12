"""Lazy and eager graph execution — shared by executor, trace, and scorer."""

from __future__ import annotations

import gc
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

import polars as pl

from haute._logging import get_logger
from haute._polars_utils import _malloc_trim, safe_sink
from haute._topo import ancestors, topo_sort_ids
from haute._types import (
    GraphEdge,
    GraphNode,
    NodeType,
    PipelineGraph,
    _Frame,
    _sanitize_func_name,
    resolve_orig_source_names,
)

logger = get_logger(component="execute")


def _prune_live_switch_edges(
    edges: list[GraphEdge],
    node_map: dict[str, GraphNode],
    scenario: str,
) -> list[GraphEdge]:
    """Remove edges to live_switch nodes from inputs inactive for *scenario*.

    A live_switch node's config contains ``input_scenario_map`` which maps
    each input name to the scenario it serves.  Only edges from inputs
    matching the active scenario are kept; the unused branch is pruned so
    it is neither executed nor shown in profilers.
    """
    switch_nodes = {
        nid: node for nid, node in node_map.items()
        if node.data.nodeType == NodeType.LIVE_SWITCH
    }
    if not switch_nodes:
        return edges

    exclude: set[tuple[str, str]] = set()
    for nid, node in switch_nodes.items():
        ism: dict[str, str] = node.data.config.get(
            "input_scenario_map", {},
        )
        if not ism:
            continue
        # If no input matches the active scenario, keep all edges
        # so the runtime fallback in switch_fn still works.
        if scenario not in ism.values():
            continue
        # For each direct parent edge, check if its name maps to a
        # different scenario — if so, exclude the edge.
        for e in edges:
            if e.target != nid:
                continue
            parent = node_map.get(e.source)
            if parent is None:
                continue
            parent_name = _sanitize_func_name(parent.data.label)
            mapped = ism.get(parent_name)
            if mapped is not None and mapped != scenario:
                exclude.add((e.source, nid))

    if not exclude:
        return edges
    return [
        e for e in edges
        if (e.source, e.target) not in exclude
    ]


def _prepare_graph(
    graph: PipelineGraph,
    target_node_id: str | None = None,
    scenario: str = "live",
) -> tuple[
    dict[str, GraphNode],  # node_map
    list[str],  # order (topo-sorted node IDs)
    dict[str, list[str]],  # parents_of
    dict[str, str],  # id_to_name
]:
    """Shared graph preparation: filter, topo-sort, and build lookups.

    Returns (node_map, order, parents_of, id_to_name).
    """
    node_map = graph.node_map
    edges = _prune_live_switch_edges(graph.edges, node_map, scenario)
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
    preamble_ns: dict | None = None,
    scenario: str = "live",
    checkpoint_dir: Path | None = None,
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
        scenario: Active execution scenario (``"live"`` = eager scoring).
        checkpoint_dir: If set, multi-input nodes (joins) and fan-out
            nodes (>1 downstream consumer) are checkpointed to parquet
            files in this directory and replaced with ``scan_parquet``
            references.  This breaks both chained-join memory
            accumulation and plan duplication across branches
            (GitHub pola-rs/polars#24206).

    Returns:
        (lazy_outputs, order, parents_of, id_to_name)
    """
    node_map, order, parents_of, id_to_name = _prepare_graph(
        graph, target_node_id, scenario=scenario,
    )

    # Full parent lookup from ALL edges for instance resolution
    all_parents = graph.parents_of

    # Build executable functions
    funcs: dict[str, tuple[Callable, bool]] = {}
    for nid in order:
        source_names = [id_to_name[pid] for pid in parents_of.get(nid, []) if pid in id_to_name]
        orig_src_names = resolve_orig_source_names(
            node_map[nid], node_map, all_parents, id_to_name,
        )
        kwargs: dict[str, Any] = {"source_names": source_names, "scenario": scenario}
        if orig_src_names is not None:
            kwargs["node_map"] = node_map
            kwargs["orig_source_names"] = orig_src_names
        if preamble_ns:
            kwargs["preamble_ns"] = preamble_ns
        _, fn, is_source = build_node_fn(node_map[nid], **kwargs)
        funcs[nid] = (fn, is_source)

    # Execute - all intermediate results stay lazy
    lazy_outputs: dict[str, _Frame] = {}

    # Count downstream consumers per node so we can checkpoint fan-out
    # points (nodes whose output feeds >1 consumer).  Without this,
    # Polars duplicates the entire upstream plan for each branch —
    # e.g. a 38 GB JSONL scan runs twice when two siblings share a parent.
    children_count: dict[str, int] = {nid: 0 for nid in order}
    children_of: dict[str, list[str]] = {nid: [] for nid in order}
    for nid, pids in parents_of.items():
        for pid in pids:
            if pid in children_count:
                children_count[pid] += 1
                children_of[pid].append(nid)

    # Separate mutable counter for tracking remaining downstream consumers.
    # Decremented at checkpoint time so we know when a parent's LazyFrame
    # can be safely deleted (freeing Polars/Rust Arrow buffers).
    remaining: dict[str, int] = dict(children_count)

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

        # Apply selected_columns filter for downstream propagation
        sel_cols = node_map[nid].data.config.get("selected_columns")
        if sel_cols:
            schema_names = lf.collect_schema().names()
            valid = [c for c in sel_cols if c in schema_names]
            if valid and len(valid) < len(schema_names):
                lf = lf.select(valid)

        # Checkpoint to break Polars plan duplication and chained-join
        # memory accumulation (pola-rs/polars#24206).  Three triggers:
        #   1. Multi-input nodes (joins) — each join materialises both
        #      sides; checkpointing isolates each join step.
        #   2. Fan-out nodes (>1 downstream consumer) — without a
        #      checkpoint Polars re-executes the full upstream plan once
        #      per consumer branch, duplicating I/O and memory.
        #   3. Nodes that feed into a join — when a compute-heavy node
        #      (e.g. model scoring) feeds a join, the join's sink would
        #      re-execute the full upstream plan.  Checkpointing the
        #      feeder ensures the join reads from parquet instead.
        # Sink to a temp parquet file and replace with scan_parquet so
        # Polars sees an independent query plan per segment.
        n_parents = len(parents_of.get(nid, []))
        n_children = children_count.get(nid, 0)
        feeds_join = any(
            len(parents_of.get(cid, [])) > 1
            for cid in children_of.get(nid, [])
        )
        if (
            checkpoint_dir is not None
            and not is_source
            and (n_parents > 1 or n_children > 1 or feeds_join)
        ):
            tmp = checkpoint_dir / f"{nid}.parquet"
            safe_sink(lf, tmp)

            # Drop the old LazyFrame (and any cached Arrow buffers it
            # holds) before replacing with a fresh scan reference.
            del lf
            # Drop parent LazyFrame refs that have no remaining consumers
            # downstream — lets Polars/Rust release the backing buffers.
            # Source nodes are kept: they hold cheap scan_* references and
            # callers may need them (e.g. optimiser extracting banding factors).
            for pid in parents_of.get(nid, []):
                remaining[pid] -= 1
                _, pid_is_source = funcs.get(pid, (None, False))
                if remaining[pid] <= 0 and pid in lazy_outputs and not pid_is_source:
                    del lazy_outputs[pid]
            gc.collect()
            _malloc_trim()

            lf = pl.scan_parquet(tmp)
            logger.info("checkpoint_written", node_id=nid, path=str(tmp))

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
    preamble_ns: dict | None = None,
    scenario: str = "live",
) -> dict[str, tuple[Callable, bool]]:
    """Build per-node executable functions from the graph.

    Shared between eager and lazy paths.  ``row_limit`` is forwarded to
    ``build_node_fn`` so Databricks sources can push LIMIT into SQL.
    ``preamble_ns`` is a compiled namespace of user-defined helpers from
    the pipeline file's preamble section.
    ``scenario`` is the active execution scenario forwarded to build_node_fn.
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
            preamble_ns=preamble_ns, scenario=scenario,
        )
        funcs[nid] = (fn, is_source)
    return funcs


def _extract_error_line(exc: Exception) -> int | None:
    """Extract user-code line number from an exception, if available.

    - SyntaxError: use .lineno (already adjusted by _exec_user_code).
    - _user_code_line attr: set by _exec_user_code from the traceback
      for runtime errors like NameError that don't embed line info
      in their message string.
    - Fallback: parse 'line N' from the error message
      (already adjusted by _exec_user_code's regex substitution).
    - Returns None when no line info is available.
    """
    if isinstance(exc, SyntaxError) and exc.lineno is not None:
        return exc.lineno
    user_line = getattr(exc, "_user_code_line", None)
    if user_line is not None:
        return user_line
    match = re.search(r"\bline (\d+)\b", str(exc))
    if match:
        return int(match.group(1))
    return None


class EagerResult(NamedTuple):
    """Result of eager graph execution."""

    outputs: dict[str, pl.DataFrame | None]
    order: list[str]
    parents_of: dict[str, list[str]]
    node_map: dict[str, GraphNode]
    id_to_name: dict[str, str]
    errors: dict[str, str]
    timings: dict[str, float]
    memory_bytes: dict[str, int]
    error_lines: dict[str, int]
    available_columns: dict[str, list[tuple[str, str]]]


def _execute_eager_core(
    graph: PipelineGraph,
    build_node_fn: Callable,
    target_node_id: str | None = None,
    row_limit: int | None = None,
    swallow_errors: bool = False,
    preamble_ns: dict | None = None,
    scenario: str = "live",
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
        scenario: Active execution scenario (``"live"`` = eager scoring).

    Returns:
        An ``EagerResult`` with named fields for outputs, order,
        parents_of, node_map, id_to_name, errors, timings, and
        memory_bytes.
    """
    node_map, order, parents_of, id_to_name = _prepare_graph(
        graph, target_node_id, scenario=scenario,
    )

    # Full parent lookup from ALL edges for instance resolution
    all_parents = graph.parents_of

    funcs = _build_funcs(
        order, node_map, parents_of, id_to_name, all_parents,
        build_node_fn, row_limit=row_limit, preamble_ns=preamble_ns,
        scenario=scenario,
    )

    eager_outputs: dict[str, pl.DataFrame | None] = {}
    errors: dict[str, str] = {}
    error_lines: dict[str, int] = {}
    timings: dict[str, float] = {}
    memory_bytes: dict[str, int] = {}
    available_columns: dict[str, list[tuple[str, str]]] = {}

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
                    df.lazy()
                    for pid in input_ids
                    if pid in eager_outputs and (df := eager_outputs[pid]) is not None
                ]
                if not input_lfs:
                    raise ValueError(
                        f"No input data available for node '{nid}'",
                    )
                result = fn(*input_lfs)

            df = result.collect(engine="streaming") if isinstance(result, pl.LazyFrame) else result

            # Capture full column set before selected_columns filtering
            available_columns[nid] = [
                (c, str(df[c].dtype)) for c in df.columns
            ]

            # Apply selected_columns filter for downstream propagation
            sel_cols = node_map[nid].data.config.get("selected_columns")
            if sel_cols:
                valid = [c for c in sel_cols if c in df.columns]
                if valid and len(valid) < len(df.columns):
                    df = df.select(valid)

            eager_outputs[nid] = df
            memory_bytes[nid] = df.estimated_size("b")
        except Exception as exc:
            if not swallow_errors:
                raise
            logger.warning("node_failed", node_id=nid, error=str(exc))
            eager_outputs[nid] = None
            errors[nid] = str(exc)
            error_line = _extract_error_line(exc)
            if error_line is not None:
                error_lines[nid] = error_line
        timings[nid] = round((time.perf_counter() - t0) * 1000, 1)

    return EagerResult(
        eager_outputs, order, parents_of, node_map,
        id_to_name, errors, timings, memory_bytes, error_lines,
        available_columns,
    )
