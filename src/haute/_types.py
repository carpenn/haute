"""Core type definitions for Haute graph structures.

These Pydantic models are the **single source of truth** for the graph
data that flows between the parser, executor, codegen, deploy, and
server API layers.  ``schemas.py`` re-exports the graph models for
FastAPI endpoint validation.
"""

from __future__ import annotations

from typing import Any

import polars as pl
from pydantic import BaseModel, Field

# Type alias - nodes pass lazy frames between each other
_Frame = pl.LazyFrame


class NodeData(BaseModel):
    """Data payload for a single pipeline node."""

    label: str = "Unnamed"
    description: str = ""
    nodeType: str = "transform"  # noqa: N815 — matches React Flow frontend convention
    config: dict[str, Any] = Field(default_factory=dict)


class GraphNode(BaseModel):
    """A single node in the React Flow graph."""

    id: str
    type: str = "pipelineNode"
    position: dict[str, float] = Field(default_factory=lambda: {"x": 0, "y": 0})
    data: NodeData = Field(default_factory=NodeData)


class GraphEdge(BaseModel):
    """A single edge in the React Flow graph."""

    id: str
    source: str
    target: str
    sourceHandle: str | None = None  # noqa: N815 — matches React Flow frontend convention
    targetHandle: str | None = None  # noqa: N815 — matches React Flow frontend convention


class PipelineGraph(BaseModel):
    """React Flow graph structure used throughout Haute.

    This is the canonical type for the graph dict passed between
    parser, executor, codegen, deploy, and the server API layer.
    """

    nodes: list[GraphNode] = Field(default_factory=list)
    edges: list[GraphEdge] = Field(default_factory=list)
    pipeline_name: str | None = None
    pipeline_description: str | None = None
    preamble: str | None = None
    source_file: str | None = None
    submodels: dict[str, Any] | None = None
    warning: str | None = None


def _sanitize_func_name(label: str) -> str:
    """Convert a human label to a valid Python function name (preserves casing)."""
    name = label.strip()
    name = name.replace(" ", "_").replace("-", "_")
    name = "".join(c for c in name if c.isalnum() or c == "_")
    if name and name[0].isdigit():
        name = f"node_{name}"
    return name or "unnamed_node"


def build_instance_mapping(
    orig_names: list[str],
    inst_names: list[str],
    explicit: dict[str, str] | None = None,
) -> dict[str, str]:
    """Map original input parameter names to instance input names.

    Priority: explicit mapping → exact name match → substring match → positional.
    Used by the executor (alias injection) and codegen (kwarg generation).
    The frontend mirrors this algorithm in NodePanel.tsx (InstanceConfig auto-mapping).
    """
    mapping: dict[str, str] = {}
    if explicit:
        mapping = {k: v for k, v in explicit.items() if v}

    used: set[int] = set()
    for v in mapping.values():
        for i, inst in enumerate(inst_names):
            if inst == v and i not in used:
                used.add(i)
                break
    # Pass 1: exact match
    for orig in orig_names:
        if orig in mapping:
            continue
        for i, inst in enumerate(inst_names):
            if i not in used and inst == orig:
                mapping[orig] = inst
                used.add(i)
                break
    # Pass 2: substring match (e.g. "claims_aggregate" in "claims_aggregate_instance")
    for orig in orig_names:
        if orig in mapping:
            continue
        for i, inst in enumerate(inst_names):
            if i not in used and orig in inst:
                mapping[orig] = inst
                used.add(i)
                break
    # Pass 3: positional fallback for remaining
    unused = [i for i in range(len(inst_names)) if i not in used]
    unmatched = [o for o in orig_names if o not in mapping]
    for orig, i in zip(unmatched, unused):
        mapping[orig] = inst_names[i]

    return mapping


def resolve_orig_source_names(
    node: GraphNode,
    node_map: dict[str, GraphNode],
    all_parents: dict[str, list[str]],
    id_to_name: dict[str, str],
) -> list[str] | None:
    """For an instance node, return the sanitized names of the original's upstream inputs.

    Uses *all_parents* (built from the full edge list, not filtered by
    ``target_node_id``) so this works even when the original node isn't
    in the current execution subgraph.

    Returns ``None`` for non-instance nodes.
    """
    ref = node.data.config.get("instanceOf")
    if not ref or ref not in node_map:
        return None
    result: list[str] = []
    for pid in all_parents.get(ref, []):
        if pid in id_to_name:
            result.append(id_to_name[pid])
        else:
            n = node_map.get(pid)
            result.append(_sanitize_func_name(n.data.label) if n else pid)
    return result
