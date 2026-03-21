"""Execution trace: single-row instrumented pipeline execution.

Runs a pipeline graph on a single row and captures per-node snapshots
(input schema, output schema, row values, schema diffs).  This is the
foundation for the data-lineage / explainability feature described in
ARCHITECTURE.md §9.3.

Phase A - what's here now:
  • execute_trace()  - run graph, collect 1-row snapshots at every node
  • SchemaDiff       - classify columns as added/removed/modified/passed
  • TraceStep / TraceResult dataclasses

TODO (future phases):
  • Column provenance via Polars expression parsing
  • Human-readable expression generation per node type
  • JoinInfo / AggregationInfo for cardinality-changing nodes
  • Compare-trace (two rows side-by-side)
  • Row-identity tracking via __trace_row_id for filters/joins
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any

from haute._fingerprint_cache import FingerprintCache
from haute._logging import get_logger
from haute.executor import _build_node_fn, _compile_preamble, _preview_cache
from haute.graph_utils import (
    NodeType,
    PipelineGraph,
    _execute_eager_core,
    _prepare_graph,
    graph_fingerprint,
    topo_sort_ids,
)

logger = get_logger(component="trace")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class SchemaDiff:
    """Column-level diff between a node's input and output."""

    columns_added: list[str]
    columns_removed: list[str]
    columns_modified: list[str]
    columns_passed: list[str]


@dataclass
class TraceStep:
    """One node's contribution to the trace."""

    node_id: str
    node_name: str
    node_type: str

    # Schema changes
    schema_diff: SchemaDiff

    # Single-row snapshots (column → value)
    input_values: dict[str, Any]
    output_values: dict[str, Any]

    # True if this node adds/modifies/passes the traced column
    column_relevant: bool = True

    # Execution time for this node (ms)
    execution_ms: float = 0.0


@dataclass
class TraceResult:
    """Full trace for one row through the pipeline."""

    target_node_id: str
    row_index: int
    column: str | None
    output_value: Any

    steps: list[TraceStep]

    # Row identity (from apiInput node's row_id_column config)
    row_id_column: str | None = None
    row_id_value: Any = None

    # Summary counts
    total_nodes_in_pipeline: int = 0
    nodes_in_trace: int = 0
    execution_ms: float = 0.0


# ---------------------------------------------------------------------------
# Schema diff
# ---------------------------------------------------------------------------


def _compute_schema_diff(
    input_row: dict[str, Any] | None,
    output_row: dict[str, Any],
) -> SchemaDiff:
    """Compare input and output row dicts to classify columns."""
    if input_row is None:
        # Source node - everything is "added"
        return SchemaDiff(
            columns_added=list(output_row.keys()),
            columns_removed=[],
            columns_modified=[],
            columns_passed=[],
        )

    in_cols = set(input_row.keys())
    out_cols = set(output_row.keys())

    added = sorted(out_cols - in_cols)
    removed = sorted(in_cols - out_cols)

    modified = []
    passed = []
    for col in sorted(in_cols & out_cols):
        in_val = input_row[col]
        out_val = output_row[col]
        # Treat NaN == NaN as equal
        if in_val != out_val and not (_is_nan(in_val) and _is_nan(out_val)):
            modified.append(col)
        else:
            passed.append(col)

    return SchemaDiff(
        columns_added=added,
        columns_removed=removed,
        columns_modified=modified,
        columns_passed=passed,
    )


def _is_nan(v: Any) -> bool:
    return isinstance(v, float) and math.isnan(v)


def _is_non_finite(v: Any) -> bool:
    """Return True if *v* is a float that is NaN, +Inf, or -Inf."""
    return isinstance(v, float) and (math.isnan(v) or math.isinf(v))


def _jsonify_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert Polars row values to JSON-serialisable Python types.

    NaN, +Inf, and -Inf are replaced with ``None`` because they are not
    valid JSON values and would cause frontend parsing errors.
    """
    clean: dict[str, Any] = {}
    for k, v in row.items():
        if v is None:
            clean[k] = None
        elif _is_non_finite(v):
            clean[k] = None
        elif isinstance(v, (int, float, str, bool)):
            clean[k] = v
        else:
            # date, datetime, duration, list, struct → str fallback
            clean[k] = str(v)
    return clean


# ---------------------------------------------------------------------------
# Execution cache — avoids re-running the full pipeline on every trace click.
# The graph structure (node IDs, types, code, paths, edges) is hashed into a
# fingerprint.  When only row_index or column changes, the cached per-node
# DataFrames are reused and we just extract a different row — sub-millisecond.
# ---------------------------------------------------------------------------


_cache = FingerprintCache(
    slots=("eager_outputs", "order", "parents_of", "node_map", "source_ids"),
)


# ---------------------------------------------------------------------------
# Main trace executor
# ---------------------------------------------------------------------------


def execute_trace(
    graph: PipelineGraph,
    row_index: int = 0,
    target_node_id: str | None = None,
    column: str | None = None,
    row_limit: int = 1000,
    source: str = "live",
) -> TraceResult:
    """Execute a pipeline graph and return a single-row trace.

    Args:
        graph: React Flow graph with "nodes" and "edges".
        row_index: Which row in the target node's output to trace (0-indexed).
        target_node_id: Node to trace from. Defaults to the last node in topo order.
        column: Optional column name - if set, only include nodes that touch it.
        row_limit: Max rows to process per source node (matches the preview limit
                   so the trace operates on the same data the user sees).
        source: Active execution source (``"live"`` = API path).

    Returns:
        TraceResult with per-node steps showing how the row was produced.
    """
    t_start = time.perf_counter()

    nodes = graph.nodes
    edges = graph.edges

    if not nodes:
        raise ValueError("Empty graph - nothing to trace")

    # Resolve target before _prepare_graph filters to ancestors
    if target_node_id is None:
        all_ids = {n.id for n in nodes}
        target_node_id = topo_sort_ids(list(all_ids), edges)[-1]
    if not any(n.id == target_node_id for n in nodes):
        raise ValueError(f"Target node '{target_node_id}' not found in graph")

    # ---------- Eager execution with single-entry cache ----------
    # Model-scoring nodes can take ~1s on large datasets (678K rows).
    # The pipeline structure doesn't change between trace clicks — only the
    # row_index and column change.  Cache the materialized DataFrames and
    # reuse them: first click ~1.7s, subsequent clicks <10ms.
    fp = graph_fingerprint(graph, target_node_id, f"{row_limit}:{source}")

    cached = _cache.try_get(fp)
    if cached is not None:
        cache_hit = True
        logger.debug(
            "trace_cache_hit",
            fingerprint=fp[:8],
            target=target_node_id,
            cached_nodes=len(cached["eager_outputs"]),
        )
        eager_outputs = cached["eager_outputs"]
        order = cached["order"]
        parents_of = cached["parents_of"]
        node_map = cached["node_map"]
        source_ids = cached["source_ids"]
    else:
        cache_hit = False
        logger.debug(
            "trace_cache_miss",
            fingerprint=fp[:8],
            target=target_node_id,
            prev_fingerprint=(_cache.fingerprint or "")[:8],
        )

        # --- Try to reuse outputs from the preview cache ----------------
        # Preview uses fingerprint f"{row_limit}:{scenario}" (no target),
        # so compute that separately and check if we can skip execution.
        preview_fp = graph_fingerprint(graph, f"{row_limit}:{source}")
        preview_data = _preview_cache.try_get(preview_fp)
        reused_preview = False

        if preview_data is not None:
            prev_outputs = preview_data["eager_outputs"]
            # Preview uses swallow_errors=True, so some outputs may be
            # None on error.  Only reuse if target node has a real value.
            if target_node_id in prev_outputs and prev_outputs[target_node_id] is not None:
                # Graph-structure metadata still needs computing for
                # the trace-specific fields (parents_of, node_map, etc.)
                node_map, order, parents_of, _id_to_name = _prepare_graph(
                    graph,
                    target_node_id,
                    source=source,
                )
                # Verify all nodes in the topo order have non-None outputs
                if all(nid in prev_outputs and prev_outputs[nid] is not None for nid in order):
                    eager_outputs = {nid: prev_outputs[nid] for nid in order}
                    source_ids = {nid for nid in order if not parents_of.get(nid)}
                    reused_preview = True
                    logger.debug(
                        "trace_reused_preview_cache",
                        fingerprint=fp[:8],
                        preview_fingerprint=preview_fp[:8],
                        target=target_node_id,
                        reused_nodes=len(eager_outputs),
                    )

        if not reused_preview:
            # Cache miss — execute eagerly via shared core (raises on error)
            preamble_ns = _compile_preamble(graph.preamble or "")
            result = _execute_eager_core(
                graph,
                _build_node_fn,
                target_node_id=target_node_id,
                row_limit=row_limit,
                swallow_errors=False,
                preamble_ns=preamble_ns or None,
                source=source,
            )
            # Trace never swallows errors so all values are DataFrames here.
            eager_outputs = {nid: df for nid, df in result.outputs.items() if df is not None}
            order = result.order
            parents_of = result.parents_of
            node_map = result.node_map
            source_ids = {nid for nid in order if not parents_of.get(nid)}

        # Populate cache
        _cache.store(
            fp,
            eager_outputs=eager_outputs,
            order=order,
            parents_of=parents_of,
            node_map=node_map,
            source_ids=source_ids,
        )

    # Extract single row from each node's cached DataFrame
    cached_rows: dict[str, dict[str, Any]] = {}
    for nid in order:
        df = eager_outputs[nid]
        if row_index < len(df):
            cached_rows[nid] = _jsonify_row(df.row(row_index, named=True))
        else:
            cached_rows[nid] = {}

    # ---------- Build trace steps from cached rows ----------
    steps: list[TraceStep] = []

    for nid in order:
        is_source = nid in source_ids
        node_data = node_map[nid].data
        node_name = node_data.label
        node_type = node_data.nodeType

        output_row = cached_rows[nid]

        input_row: dict[str, Any] | None
        if is_source:
            input_row = None
        else:
            input_ids = parents_of.get(nid, [])
            if input_ids:
                input_row = {}
                for pid in input_ids:
                    input_row.update(cached_rows[pid])
            else:
                input_row = {}

        schema_diff = _compute_schema_diff(input_row, output_row)

        steps.append(
            TraceStep(
                node_id=nid,
                node_name=node_name,
                node_type=node_type,
                schema_diff=schema_diff,
                input_values=input_row if input_row is not None else {},
                output_values=output_row,
            )
        )

    # Free full DataFrames — only cached single rows are needed from here
    del eager_outputs

    # ---------- Column relevance: tag then prune irrelevant ancestors ----------
    #
    # Two cases:
    #   1. Pass-through column (e.g. VehGas): exists in multiple nodes' output.
    #      Keep only nodes whose output contains the column — this prunes
    #      unrelated source branches (e.g. claims/exposure when tracing VehGas
    #      which only comes from policies).
    #   2. Calculated column (e.g. premium): only exists at the node that creates
    #      it (columns_added).  ALL ancestors of that node feed the calculation,
    #      so they must stay in the trace even though they don't carry the column
    #      in their output.  Without this, calculated-field traces collapse to a
    #      single node with no edges.
    if column:
        _tag_column_relevance(steps, column)

        # Find nodes where the column is first created
        origin_ids = {s.node_id for s in steps if column in s.schema_diff.columns_added}
        # Collect all ancestors of origin nodes — they contribute to the calc
        ancestor_ids: set[str] = set()
        if origin_ids:
            queue = list(origin_ids)
            while queue:
                nid = queue.pop()
                for pid in parents_of.get(nid, []):
                    if pid not in ancestor_ids:
                        ancestor_ids.add(pid)
                        queue.append(pid)

        steps = [s for s in steps if s.column_relevant or s.node_id in ancestor_ids]

    # ---------- Output value (already in cache from batch collect) ----------
    target_row = cached_rows[target_node_id]
    output_value = target_row.get(column) if column else target_row

    # ---------- Row identity from apiInput node ----------
    row_id_column: str | None = None
    row_id_value: Any = None
    for n in nodes:
        if n.data.nodeType == NodeType.API_INPUT and n.data.config.get("row_id_column"):
            row_id_column = n.data.config["row_id_column"]
            row_id_value = target_row.get(row_id_column)
            break

    total_ms = round((time.perf_counter() - t_start) * 1000, 2)

    logger.info(
        "trace_executed",
        target=target_node_id,
        row_index=row_index,
        column=column,
        steps=len(steps),
        cache_hit=cache_hit,
        duration_ms=total_ms,
    )

    return TraceResult(
        target_node_id=target_node_id,
        row_index=row_index,
        column=column,
        output_value=output_value,
        steps=steps,
        row_id_column=row_id_column,
        row_id_value=row_id_value,
        total_nodes_in_pipeline=len(nodes),
        nodes_in_trace=len(steps),
        execution_ms=total_ms,
    )


# ---------------------------------------------------------------------------
# Column relevance tagging
# ---------------------------------------------------------------------------


def _tag_column_relevance(steps: list[TraceStep], column: str) -> None:
    """Tag each step with whether its output contains the target column.

    After tagging, the caller filters steps — see execute_trace() for the
    two-case logic (pass-through vs calculated columns).
    """
    for step in steps:
        sd = step.schema_diff
        step.column_relevant = (
            column in sd.columns_added
            or column in sd.columns_modified
            or column in sd.columns_passed
            or column in step.output_values
        )


# ---------------------------------------------------------------------------
# Serialisation - TraceResult → JSON-safe dict
# ---------------------------------------------------------------------------


def trace_result_to_dict(result: TraceResult) -> dict[str, Any]:
    """Convert a TraceResult to a JSON-serialisable dict for the API."""
    return {
        "target_node_id": result.target_node_id,
        "row_index": result.row_index,
        "column": result.column,
        "output_value": result.output_value,
        "steps": [
            {
                "node_id": s.node_id,
                "node_name": s.node_name,
                "node_type": s.node_type,
                "schema_diff": {
                    "columns_added": s.schema_diff.columns_added,
                    "columns_removed": s.schema_diff.columns_removed,
                    "columns_modified": s.schema_diff.columns_modified,
                    "columns_passed": s.schema_diff.columns_passed,
                },
                "input_values": s.input_values,
                "output_values": s.output_values,
                "column_relevant": s.column_relevant,
                "execution_ms": s.execution_ms,
            }
            for s in result.steps
        ],
        "row_id_column": result.row_id_column,
        "row_id_value": result.row_id_value,
        "total_nodes_in_pipeline": result.total_nodes_in_pipeline,
        "nodes_in_trace": result.nodes_in_trace,
        "execution_ms": result.execution_ms,
    }
