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

import time
from dataclasses import dataclass
from typing import Any

from haute.executor import _build_node_fn
from haute.graph_utils import _execute_lazy, _Frame, topo_sort_ids

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

    # Summary counts
    total_nodes_in_pipeline: int
    nodes_in_trace: int
    execution_ms: float


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
    try:
        import math
        return isinstance(v, float) and math.isnan(v)
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Collect a single row from a LazyFrame, with JSON-safe values
# ---------------------------------------------------------------------------

def _collect_row(lf: _Frame, row_index: int) -> dict[str, Any]:
    """Collect one row from a LazyFrame and return as a dict with JSON-safe values."""
    # Slice to just the target row (pushes into query plan)
    df = lf.slice(row_index, 1).collect()

    if df.is_empty():
        return {}

    row = df.row(0, named=True)
    return _jsonify_row(row)


def _jsonify_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert Polars row values to JSON-serialisable Python types."""
    clean: dict[str, Any] = {}
    for k, v in row.items():
        if v is None:
            clean[k] = None
        elif isinstance(v, (int, float, str, bool)):
            clean[k] = v
        else:
            # date, datetime, duration, list, struct → str fallback
            clean[k] = str(v)
    return clean


# ---------------------------------------------------------------------------
# Main trace executor
# ---------------------------------------------------------------------------

def execute_trace(
    graph: dict,
    row_index: int = 0,
    target_node_id: str | None = None,
    column: str | None = None,
) -> TraceResult:
    """Execute a pipeline graph and return a single-row trace.

    Args:
        graph: React Flow graph with "nodes" and "edges".
        row_index: Which row in the target node's output to trace (0-indexed).
        target_node_id: Node to trace from. Defaults to the last node in topo order.
        column: Optional column name - if set, only include nodes that touch it.

    Returns:
        TraceResult with per-node steps showing how the row was produced.
    """
    t_start = time.perf_counter()

    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    if not nodes:
        raise ValueError("Empty graph - nothing to trace")

    node_map = {n["id"]: n for n in nodes}
    all_ids = set(node_map.keys())

    # Default target: last node in topo order
    full_order = topo_sort_ids(list(all_ids), edges)
    if target_node_id is None:
        target_node_id = full_order[-1]
    if target_node_id not in node_map:
        raise ValueError(f"Target node '{target_node_id}' not found in graph")

    # Execute lazily via shared core
    lazy_outputs, order, parents_of, _id_to_name = _execute_lazy(
        graph, _build_node_fn, target_node_id,
    )

    # Determine which nodes are sources
    source_ids = {nid for nid in order if not parents_of.get(nid)}

    # ---------- Build trace steps ----------
    steps: list[TraceStep] = []

    for nid in order:
        t_node = time.perf_counter()

        is_source = nid in source_ids
        node_data = node_map[nid].get("data", {})
        node_name = node_data.get("label", nid)
        node_type = node_data.get("nodeType", "transform")

        output_row = _collect_row(lazy_outputs[nid], row_index)

        # Collect input row (merge parents for multi-input nodes)
        if is_source:
            input_row = None
        else:
            input_ids = parents_of.get(nid, [])
            if input_ids:
                # Merge parent output rows into one dict (left-to-right)
                input_row: dict[str, Any] = {}
                for pid in input_ids:
                    parent_row = _collect_row(lazy_outputs[pid], row_index)
                    input_row.update(parent_row)
            else:
                input_row = {}

        schema_diff = _compute_schema_diff(input_row, output_row)

        steps.append(TraceStep(
            node_id=nid,
            node_name=node_name,
            node_type=node_type,
            schema_diff=schema_diff,
            input_values=input_row if input_row is not None else {},
            output_values=output_row,
            execution_ms=round((time.perf_counter() - t_node) * 1000, 2),
        ))

    # ---------- Column filter ----------
    if column:
        steps = _filter_steps_by_column(steps, column)

    # ---------- Output value ----------
    target_row = _collect_row(lazy_outputs[target_node_id], row_index)
    output_value = target_row.get(column) if column else target_row

    total_ms = round((time.perf_counter() - t_start) * 1000, 2)

    return TraceResult(
        target_node_id=target_node_id,
        row_index=row_index,
        column=column,
        output_value=output_value,
        steps=steps,
        total_nodes_in_pipeline=len(nodes),
        nodes_in_trace=len(steps),
        execution_ms=total_ms,
    )


# ---------------------------------------------------------------------------
# Column filtering - keep only nodes that touch the target column
# ---------------------------------------------------------------------------

def _filter_steps_by_column(steps: list[TraceStep], column: str) -> list[TraceStep]:
    """Keep only trace steps where the column appears in the schema diff."""
    filtered: list[TraceStep] = []
    for step in steps:
        sd = step.schema_diff
        if (column in sd.columns_added
                or column in sd.columns_modified
                or column in sd.columns_passed
                or column in step.output_values):
            filtered.append(step)
    return filtered


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
                "execution_ms": s.execution_ms,
            }
            for s in result.steps
        ],
        "total_nodes_in_pipeline": result.total_nodes_in_pipeline,
        "nodes_in_trace": result.nodes_in_trace,
        "execution_ms": result.execution_ms,
    }
