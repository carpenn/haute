"""Core type definitions for Haute graph structures.

These Pydantic models are the **single source of truth** for the graph
data that flows between the parser, executor, codegen, deploy, and
server API layers.  ``schemas.py`` re-exports the graph models for
FastAPI endpoint validation.
"""

from __future__ import annotations

from enum import StrEnum
from functools import cached_property
from typing import Any, TypedDict

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

# Type alias - nodes pass lazy frames between each other
_Frame = pl.LazyFrame


class HauteError(Exception):
    """Base exception for all Haute-specific errors."""


class NodeType(StrEnum):
    """Canonical node-type identifiers shared with the React Flow frontend.

    Inherits from ``StrEnum`` so ``NodeType.API_INPUT == "apiInput"`` is ``True``
    and JSON serialization produces the plain string value.
    """

    API_INPUT = "apiInput"
    DATA_SOURCE = "dataSource"
    TRANSFORM = "transform"
    MODEL_SCORE = "modelScore"
    BANDING = "banding"
    RATING_STEP = "ratingStep"
    OUTPUT = "output"
    DATA_SINK = "dataSink"
    EXTERNAL_FILE = "externalFile"
    LIVE_SWITCH = "liveSwitch"
    MODELLING = "modelling"
    SUBMODEL = "submodel"
    SUBMODEL_PORT = "submodelPort"


# ---------------------------------------------------------------------------
# Typed config shapes (documentation + IDE autocomplete, no runtime change)
# ---------------------------------------------------------------------------


class ApiInputConfig(TypedDict, total=False):
    """Config for apiInput nodes."""

    path: str
    row_id_column: str


class DataSourceConfig(TypedDict, total=False):
    """Config for dataSource nodes."""

    path: str
    sourceType: str  # "flat_file" | "databricks"
    table: str
    http_path: str
    query: str


class TransformConfig(TypedDict, total=False):
    """Config for transform nodes."""

    code: str
    instanceOf: str
    inputMapping: dict[str, str]


class ModelScoreConfig(TypedDict, total=False):
    """Config for modelScore nodes."""

    sourceType: str          # "run" | "registered"
    # run-based selection
    experiment_name: str     # UI-only: display name for panel re-open
    experiment_id: str       # UI-only: MLflow experiment ID for API calls
    run_id: str
    run_name: str            # UI-only: display name for panel re-open
    artifact_path: str       # e.g. "model.cbm"
    # registered model selection
    registered_model: str    # e.g. "catalog.schema.model" or "my-model"
    version: str             # "1", "2", etc. or "latest"
    # common
    task: str                # "regression" | "classification"
    output_column: str       # prediction column name, default "prediction"
    code: str                # optional post-processing code
    instanceOf: str
    inputMapping: dict[str, str]


class BandingFactor(TypedDict, total=False):
    """A single factor in a banding node config."""

    banding: str  # "continuous" | "discrete"
    column: str
    outputColumn: str
    rules: list[dict[str, Any]]
    default: str | None


class BandingConfig(TypedDict, total=False):
    """Config for banding nodes."""

    factors: list[BandingFactor]


class RatingTableEntry(TypedDict, total=False):
    """A single entry (row) in a rating table."""

    # Keys are dynamic factor names; values are strings/numbers


class RatingTable(TypedDict, total=False):
    """A single table in a ratingStep config."""

    name: str
    factors: list[str]
    outputColumn: str
    defaultValue: str | None
    entries: list[dict[str, Any]]


class RatingStepConfig(TypedDict, total=False):
    """Config for ratingStep nodes."""

    tables: list[RatingTable]
    operation: str  # "multiply" | "add" | "subtract" | "divide"
    combinedColumn: str


class OutputConfig(TypedDict, total=False):
    """Config for output nodes."""

    fields: list[str]


class DataSinkConfig(TypedDict, total=False):
    """Config for dataSink nodes."""

    path: str
    format: str  # "parquet" | "csv"


class ExternalFileConfig(TypedDict, total=False):
    """Config for externalFile nodes."""

    path: str
    fileType: str  # "pickle" | "json" | "joblib" | "catboost"
    modelClass: str  # "classifier" | "regressor" (catboost only)
    code: str


class LiveSwitchConfig(TypedDict, total=False):
    """Config for liveSwitch nodes."""

    mode: str  # "live" | <input_name>
    inputs: list[str]


class ModellingConfig(TypedDict, total=False):
    """Config for modelling (model training) nodes."""

    name: str
    target: str
    weight: str
    exclude: list[str]
    algorithm: str  # "catboost"
    task: str  # "regression" | "classification"
    params: dict[str, Any]
    split: dict[str, Any]
    metrics: list[str]
    mlflow_experiment: str
    model_name: str
    output_dir: str


class SubmodelConfig(TypedDict, total=False):
    """Config for submodel placeholder nodes."""

    file: str
    childNodeIds: list[str]
    inputPorts: list[str]
    outputPorts: list[str]


class NodeData(BaseModel):
    """Data payload for a single pipeline node."""

    label: str = "Unnamed"
    description: str = ""
    nodeType: NodeType = NodeType.TRANSFORM  # noqa: N815 — matches React Flow frontend convention
    config: dict[str, Any] = Field(default_factory=dict)


class GraphNode(BaseModel):
    """A single node in the React Flow graph."""

    id: str
    type: str = "pipelineNode"
    position: dict[str, float] = Field(default_factory=lambda: {"x": 0.0, "y": 0.0})
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

    model_config = ConfigDict(ignored_types=(cached_property,))

    nodes: list[GraphNode] = Field(default_factory=list)
    edges: list[GraphEdge] = Field(default_factory=list)
    pipeline_name: str | None = None
    pipeline_description: str | None = None
    preamble: str | None = None
    preserved_blocks: list[str] = Field(default_factory=list)
    source_file: str | None = None
    submodels: dict[str, Any] | None = None
    warning: str | None = None

    @cached_property
    def node_map(self) -> dict[str, GraphNode]:
        """Map node ID to node, cached for repeated access."""
        return {n.id: n for n in self.nodes}

    @cached_property
    def parents_of(self) -> dict[str, list[str]]:
        """Map each node to its parent node IDs (built from edges)."""
        result: dict[str, list[str]] = {}
        for e in self.edges:
            result.setdefault(e.target, []).append(e.source)
        return result


def _sanitize_func_name(label: str) -> str:
    """Convert a human label to a valid Python function name (preserves casing).

    Uses ASCII-only matching to stay in sync with the frontend implementation
    in frontend/src/utils/sanitizeName.ts.
    """
    name = label.strip()
    name = name.replace(" ", "_").replace("-", "_")
    name = "".join(c for c in name if c.isascii() and (c.isalnum() or c == "_"))
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
