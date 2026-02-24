"""Pydantic models for API request/response validation.

The canonical graph types (``GraphEdge``, ``NodeData``, ``GraphNode``,
``PipelineGraph``) are defined in ``haute._types`` and re-exported here
with API-friendly aliases so that FastAPI endpoint signatures stay clean.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from haute._types import GraphEdge as GraphEdge  # noqa: F401
from haute._types import GraphNode as GraphNode  # noqa: F401
from haute._types import NodeData as GraphNodeData  # noqa: F401
from haute._types import PipelineGraph as Graph  # noqa: F401


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
    preserved_blocks: list[str] = Field(default_factory=list)
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
    row_limit: int = 100_000


class NodeResult(BaseModel):
    status: str
    row_count: int = 0
    column_count: int = 0
    columns: list[ColumnInfo] = Field(default_factory=list)
    preview: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None
    timing_ms: float = 0
    schema_warnings: list[SchemaWarning] = Field(default_factory=list)


class RunPipelineResponse(BaseModel):
    status: str
    results: dict[str, NodeResult] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# /api/pipeline/preview
# ---------------------------------------------------------------------------


class PreviewNodeRequest(BaseModel):
    graph: Graph
    node_id: str
    row_limit: int = 1000


class NodeTimingInfo(BaseModel):
    node_id: str
    label: str
    timing_ms: float


class SchemaWarning(BaseModel):
    column: str
    status: str


class PreviewNodeResponse(BaseModel):
    node_id: str
    status: str
    row_count: int = 0
    column_count: int = 0
    columns: list[ColumnInfo] = Field(default_factory=list)
    preview: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None
    timing_ms: float = 0
    timings: list[NodeTimingInfo] = Field(default_factory=list)
    schema_warnings: list[SchemaWarning] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# /api/pipeline/trace
# ---------------------------------------------------------------------------


class TraceRequest(BaseModel):
    graph: Graph
    row_index: int = 0
    target_node_id: str | None = None
    column: str | None = None
    row_limit: int = 1000


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
    column_relevant: bool = True
    execution_ms: float = 0.0


class TraceResultResponse(BaseModel):
    target_node_id: str
    row_index: int
    column: str | None = None
    output_value: Any = None
    steps: list[TraceStepResponse] = Field(default_factory=list)
    row_id_column: str | None = None
    row_id_value: Any = None
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
    node_id: str


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


# ---------------------------------------------------------------------------
# /api/databricks/*
# ---------------------------------------------------------------------------


class WarehouseItem(BaseModel):
    id: str
    name: str
    http_path: str
    state: str
    size: str = ""


class WarehouseListResponse(BaseModel):
    warehouses: list[WarehouseItem]


class CatalogItem(BaseModel):
    name: str
    comment: str = ""


class CatalogListResponse(BaseModel):
    catalogs: list[CatalogItem]


class SchemaItem(BaseModel):
    name: str
    comment: str = ""


class SchemaListResponse(BaseModel):
    schemas: list[SchemaItem]


class TableItem(BaseModel):
    name: str
    full_name: str
    table_type: str = ""
    comment: str = ""


class TableListResponse(BaseModel):
    tables: list[TableItem]


class FetchTableRequest(BaseModel):
    table: str
    http_path: str | None = None
    query: str | None = None


class FetchTableResponse(BaseModel):
    path: str
    table: str
    row_count: int
    column_count: int
    columns: dict[str, str]
    size_bytes: int
    fetched_at: float
    fetch_seconds: float


class FetchProgressResponse(BaseModel):
    active: bool
    rows: int = 0
    batches: int = 0
    elapsed: float = 0.0


class CacheStatusResponse(BaseModel):
    cached: bool
    path: str | None = None
    table: str = ""
    row_count: int = 0
    column_count: int = 0
    columns: dict[str, str] = Field(default_factory=dict)
    size_bytes: int = 0
    fetched_at: float = 0


# ---------------------------------------------------------------------------
# /api/submodel/*
# ---------------------------------------------------------------------------


class CreateSubmodelRequest(BaseModel):
    name: str
    node_ids: list[str]
    graph: Graph
    preamble: str = ""
    source_file: str = ""
    pipeline_name: str = "main"


class CreateSubmodelResponse(BaseModel):
    status: str = "ok"
    submodel_file: str = ""
    parent_file: str = ""
    graph: Graph = Field(default_factory=Graph)


class DissolveSubmodelRequest(BaseModel):
    submodel_name: str
    graph: Graph
    preamble: str = ""
    source_file: str = ""
    pipeline_name: str = "main"


class DissolveSubmodelResponse(BaseModel):
    status: str = "ok"
    graph: Graph = Field(default_factory=Graph)


class SubmodelGraphResponse(BaseModel):
    status: str = "ok"
    submodel_name: str = ""
    graph: Graph = Field(default_factory=Graph)


# ---------------------------------------------------------------------------
# /api/modelling/*
# ---------------------------------------------------------------------------


class TrainRequest(BaseModel):
    graph: Graph
    node_id: str


class TrainResponse(BaseModel):
    status: str  # "started" | "completed" | "error"
    job_id: str | None = None
    metrics: dict[str, float] = Field(default_factory=dict)
    feature_importance: list[dict[str, Any]] = Field(default_factory=list)
    model_path: str = ""
    train_rows: int = 0
    test_rows: int = 0
    error: str | None = None
    best_iteration: int | None = None
    loss_history: list[dict[str, float]] = Field(default_factory=list)
    double_lift: list[dict[str, Any]] = Field(default_factory=list)
    shap_summary: list[dict[str, Any]] = Field(default_factory=list)
    feature_importance_loss: list[dict[str, Any]] = Field(default_factory=list)
    cv_results: dict[str, Any] | None = None
    ave_per_feature: list[dict[str, Any]] = Field(default_factory=list)


class TrainStatusResponse(BaseModel):
    status: str  # "running" | "completed" | "error"
    progress: float = 0.0
    message: str = ""
    iteration: int = 0
    total_iterations: int = 0
    train_loss: dict[str, float] = Field(default_factory=dict)
    elapsed_seconds: float = 0.0
    result: TrainResponse | None = None


class ExportScriptRequest(BaseModel):
    node_id: str
    graph: Graph
    data_path: str = ""


class ExportScriptResponse(BaseModel):
    script: str
    filename: str


class LogExperimentRequest(BaseModel):
    job_id: str
    experiment_name: str | None = None
    model_name: str | None = None


class LogExperimentResponse(BaseModel):
    status: str  # "ok" | "error"
    backend: str = ""
    experiment_name: str = ""
    run_id: str = ""
    run_url: str | None = None
    tracking_uri: str = ""
    error: str | None = None


class MlflowCheckResponse(BaseModel):
    mlflow_installed: bool
    backend: str = ""
    databricks_host: str = ""


# ---------------------------------------------------------------------------
# /api/mlflow/* (discovery for Model Score node)
# ---------------------------------------------------------------------------


class MlflowExperimentSummary(BaseModel):
    experiment_id: str
    name: str


class MlflowRunSummary(BaseModel):
    run_id: str
    run_name: str
    status: str
    start_time: int | None = None
    metrics: dict[str, float] = Field(default_factory=dict)
    params: dict[str, str] = Field(default_factory=dict)
    artifacts: list[str] = Field(default_factory=list)


class MlflowVersionBrief(BaseModel):
    version: str
    status: str
    run_id: str


class MlflowModelSummary(BaseModel):
    name: str
    latest_versions: list[MlflowVersionBrief] = Field(default_factory=list)


class MlflowModelVersionSummary(BaseModel):
    version: str
    run_id: str
    status: str
    creation_timestamp: int | None = None
    description: str = ""


# ---------------------------------------------------------------------------
# /api/optimiser/*
# ---------------------------------------------------------------------------


class OptimiserSolveRequest(BaseModel):
    graph: Graph
    node_id: str


class OptimiserSolveResponse(BaseModel):
    status: str  # "started" | "error"
    job_id: str | None = None
    error: str | None = None


class OptimiserStatusResponse(BaseModel):
    status: str  # "running" | "completed" | "error"
    progress: float = 0.0
    message: str = ""
    elapsed_seconds: float = 0.0
    result: dict[str, Any] | None = None


class OptimiserApplyRequest(BaseModel):
    job_id: str


class OptimiserApplyResponse(BaseModel):
    status: str
    total_objective: float = 0.0
    constraints: dict[str, float] = Field(default_factory=dict)
    preview: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    error: str | None = None


class OptimiserSaveRequest(BaseModel):
    job_id: str
    output_path: str


class OptimiserSaveResponse(BaseModel):
    status: str
    path: str | None = None
    message: str = ""


class OptimiserMlflowLogRequest(BaseModel):
    job_id: str
    experiment_name: str = "/optimisation"
    model_name: str | None = None


class OptimiserMlflowLogResponse(BaseModel):
    status: str
    backend: str = ""
    experiment_name: str = ""
    run_id: str | None = None
    run_url: str | None = None
    tracking_uri: str = ""
    error: str | None = None
