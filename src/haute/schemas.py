"""Pydantic models for API request/response validation."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Shared sub-models
# ---------------------------------------------------------------------------

class GraphEdge(BaseModel):
    id: str
    source: str
    target: str


class GraphNodeData(BaseModel):
    label: str = "Unnamed"
    description: str = ""
    nodeType: str = "transform"
    config: dict[str, Any] = Field(default_factory=dict)


class GraphNode(BaseModel):
    id: str
    type: str = "pipelineNode"
    position: dict[str, float] = Field(default_factory=lambda: {"x": 0, "y": 0})
    data: GraphNodeData = Field(default_factory=GraphNodeData)


class Graph(BaseModel):
    nodes: list[GraphNode] = Field(default_factory=list)
    edges: list[GraphEdge] = Field(default_factory=list)
    pipeline_name: str | None = None
    pipeline_description: str | None = None
    preamble: str | None = None


class ColumnInfo(BaseModel):
    name: str
    dtype: str


# ---------------------------------------------------------------------------
# /api/pipeline/save
# ---------------------------------------------------------------------------

class SavePipelineRequest(BaseModel):
    name: str = "main"
    description: str = ""
    graph: Graph = Field(default_factory=Graph)
    preamble: str = ""
    source_file: str = ""


class SavePipelineResponse(BaseModel):
    status: str = "saved"
    file: str
    pipeline_name: str


# ---------------------------------------------------------------------------
# /api/pipeline/run
# ---------------------------------------------------------------------------

class RunPipelineRequest(BaseModel):
    graph: Graph


class NodeResult(BaseModel):
    status: str
    row_count: int = 0
    column_count: int = 0
    columns: list[ColumnInfo] = Field(default_factory=list)
    preview: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None


class RunPipelineResponse(BaseModel):
    status: str
    results: dict[str, NodeResult] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# /api/pipeline/preview
# ---------------------------------------------------------------------------

class PreviewNodeRequest(BaseModel):
    graph: Graph
    nodeId: str
    rowLimit: int = 1000


class PreviewNodeResponse(BaseModel):
    nodeId: str
    status: str
    row_count: int = 0
    column_count: int = 0
    columns: list[ColumnInfo] = Field(default_factory=list)
    preview: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None


# ---------------------------------------------------------------------------
# /api/pipeline/trace
# ---------------------------------------------------------------------------

class TraceRequest(BaseModel):
    graph: Graph
    rowIndex: int = 0
    targetNodeId: str | None = None
    column: str | None = None


class SchemaDiffResponse(BaseModel):
    columns_added: list[str] = Field(default_factory=list)
    columns_removed: list[str] = Field(default_factory=list)
    columns_modified: list[str] = Field(default_factory=list)
    columns_passed: list[str] = Field(default_factory=list)


class TraceStepResponse(BaseModel):
    node_id: str
    node_name: str
    node_type: str
    schema_diff: SchemaDiffResponse
    input_values: dict[str, Any] = Field(default_factory=dict)
    output_values: dict[str, Any] = Field(default_factory=dict)
    execution_ms: float = 0.0


class TraceResultResponse(BaseModel):
    target_node_id: str
    row_index: int
    column: str | None = None
    output_value: Any = None
    steps: list[TraceStepResponse] = Field(default_factory=list)
    total_nodes_in_pipeline: int = 0
    nodes_in_trace: int = 0
    execution_ms: float = 0.0


class TraceResponse(BaseModel):
    status: str
    trace: TraceResultResponse


# ---------------------------------------------------------------------------
# /api/pipeline/sink
# ---------------------------------------------------------------------------

class SinkRequest(BaseModel):
    graph: Graph
    nodeId: str


class SinkResponse(BaseModel):
    status: str
    message: str = ""
    row_count: int = 0
    path: str = ""
    format: str = "parquet"


# ---------------------------------------------------------------------------
# /api/files
# ---------------------------------------------------------------------------

class FileItem(BaseModel):
    name: str
    path: str
    type: str
    size: int | None = None


class BrowseFilesResponse(BaseModel):
    dir: str
    items: list[FileItem]


# ---------------------------------------------------------------------------
# /api/schema
# ---------------------------------------------------------------------------

class SchemaResponse(BaseModel):
    path: str
    columns: list[ColumnInfo]
    row_count: int
    column_count: int
    preview: list[dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# /api/pipelines (list)
# ---------------------------------------------------------------------------

class PipelineSummary(BaseModel):
    name: str
    description: str = ""
    file: str
    node_count: int = 0
    error: str | None = None
