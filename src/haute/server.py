"""FastAPI backend for haute."""

import asyncio
import json as _json
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from haute._logging import configure_logging, get_logger
from haute.graph_utils import PipelineGraph
from haute.schemas import (
    BrowseFilesResponse,
    CacheStatusResponse,
    CatalogListResponse,
    CreateSubmodelRequest,
    CreateSubmodelResponse,
    DissolveSubmodelRequest,
    DissolveSubmodelResponse,
    FetchProgressResponse,
    FetchTableRequest,
    FetchTableResponse,
    FileItem,
    PipelineSummary,
    PreviewNodeRequest,
    PreviewNodeResponse,
    RunPipelineRequest,
    RunPipelineResponse,
    SavePipelineRequest,
    SavePipelineResponse,
    SchemaListResponse,
    SchemaResponse,
    SinkRequest,
    SinkResponse,
    SubmodelGraphResponse,
    TableListResponse,
    TraceRequest,
    TraceResponse,
    WarehouseListResponse,
)

STATIC_DIR = Path(__file__).parent / "static"
logger = get_logger(component="server")

_watcher_task: asyncio.Task | None = None


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    from haute.deploy._config import _load_env

    configure_logging()
    _load_env(Path.cwd())

    global _watcher_task
    _watcher_task = asyncio.create_task(_file_watcher())
    yield
    if _watcher_task:
        _watcher_task.cancel()


app = FastAPI(title="Haute", version="0.1.0", lifespan=_lifespan)


class _RequestIdMiddleware(BaseHTTPMiddleware):
    """Bind a unique request_id to structlog context for every HTTP request."""

    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("x-request-id", uuid.uuid4().hex[:12])
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=rid)
        response = await call_next(request)
        response.headers["x-request-id"] = rid
        return response


app.add_middleware(_RequestIdMiddleware)

# ---------------------------------------------------------------------------
# Self-write tracking (avoid file-watcher feedback loops)
# ---------------------------------------------------------------------------
_last_self_write: float = 0.0
_SELF_WRITE_COOLDOWN = 1.0  # seconds


def _mark_self_write() -> None:
    global _last_self_write
    _last_self_write = time.monotonic()


def _is_self_write() -> bool:
    return (time.monotonic() - _last_self_write) < _SELF_WRITE_COOLDOWN


# ---------------------------------------------------------------------------
# WebSocket connections for live sync
# ---------------------------------------------------------------------------
_ws_clients: set[WebSocket] = set()


async def _broadcast(data: dict) -> None:
    """Push a message to all connected WebSocket clients."""
    payload = _json.dumps(data)
    dead: list[WebSocket] = []
    for ws in _ws_clients:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)


@app.websocket("/ws/sync")
async def ws_sync(websocket: WebSocket) -> None:
    """WebSocket endpoint for live code ↔ GUI sync."""
    await websocket.accept()
    _ws_clients.add(websocket)
    logger.info("ws_connected", total_clients=len(_ws_clients))
    try:
        while True:
            await websocket.receive_text()  # keep-alive / client messages
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
        logger.info("ws_disconnected", remaining_clients=len(_ws_clients))


def _discover_pipelines() -> list[Path]:
    """Find pipeline .py files in the project root that contain ``haute.Pipeline``."""
    from haute.discovery import discover_pipelines

    return discover_pipelines()


def _load_sidecar_positions(py_path: Path) -> dict[str, dict[str, float]]:
    """Load node positions from the sidecar .haute.json file."""
    sidecar = py_path.with_suffix(".haute.json")
    if sidecar.exists():
        try:
            data = _json.loads(sidecar.read_text())
            return data.get("positions", {})
        except Exception as e:
            logger.warning("corrupt_sidecar", file=sidecar.name, error=str(e))
    return {}


def _save_sidecar(py_path: Path, graph: PipelineGraph) -> None:
    """Write node positions to the sidecar .haute.json file."""
    positions = {node.id: node.position for node in graph.nodes}

    sidecar = py_path.with_suffix(".haute.json")
    sidecar.write_text(_json.dumps({"positions": positions}, indent=2) + "\n")


def _parse_pipeline_to_graph(py_path: Path) -> PipelineGraph:
    """Parse a .py file and merge with sidecar positions."""
    from haute.parser import parse_pipeline_file

    graph = parse_pipeline_file(py_path)
    positions = _load_sidecar_positions(py_path)

    # Apply saved positions if available
    for node in graph.nodes:
        if node.id in positions:
            node.position = positions[node.id]

    return graph


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------


@app.get("/api/pipelines", response_model=list[PipelineSummary])
async def list_pipelines() -> list[PipelineSummary]:
    """List all discovered pipelines."""
    from haute.parser import parse_pipeline_file

    files = _discover_pipelines()
    result: list[PipelineSummary] = []
    for f in files:
        try:
            graph = parse_pipeline_file(f)
            result.append(
                PipelineSummary(
                    name=graph.pipeline_name or f.stem,
                    description=graph.pipeline_description or "",
                    file=str(f.relative_to(Path.cwd())),
                    node_count=len(graph.nodes),
                )
            )
        except Exception as e:
            result.append(
                PipelineSummary(
                    name=f.stem,
                    file=str(f),
                    error=str(e),
                )
            )
    return result


@app.get("/api/pipeline/{name}")
async def get_pipeline(name: str) -> dict:
    """Return the graph for a specific pipeline."""
    for f in _discover_pipelines():
        try:
            graph = _parse_pipeline_to_graph(f)
            if graph.pipeline_name == name:
                return graph.model_dump()
        except Exception as e:
            logger.warning("parse_failed", file=f.name, error=str(e))
            continue
    raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")


@app.get("/api/pipeline")
async def get_first_pipeline() -> dict:
    """Return the graph for the active pipeline, or an empty canvas.

    Python file is the source of truth. Sidecar .haute.json provides positions.
    """
    cwd = Path.cwd()

    for f in _discover_pipelines():
        try:
            graph = _parse_pipeline_to_graph(f)
            if graph.nodes:
                graph.source_file = str(f.relative_to(cwd))
                return graph.model_dump()
        except Exception as e:
            logger.warning("parse_failed", file=f.name, error=str(e))
            continue

    return PipelineGraph().model_dump()


@app.post("/api/pipeline/save", response_model=SavePipelineResponse)
async def save_pipeline(body: SavePipelineRequest) -> SavePipelineResponse:
    """Save a graph: .py (source of truth) + .haute.json (positions).

    When the graph contains submodels, multiple files are written via
    ``graph_to_code_multi``.
    """
    from haute.codegen import graph_to_code, graph_to_code_multi

    graph = body.graph

    # Validate singleton node types (max 1 each)
    singletons = [("apiInput", "API Input"), ("output", "Output"), ("liveSwitch", "Live Switch")]
    for singleton_type, label in singletons:
        count = sum(1 for n in graph.nodes if n.data.nodeType == singleton_type)
        if count > 1:
            raise HTTPException(
                status_code=400,
                detail=f"Only one {label} node is allowed per pipeline (found {count}).",
            )

    cwd = Path.cwd()

    # Determine main pipeline .py path
    if not body.source_file:
        raise HTTPException(
            status_code=400,
            detail="source_file is required — the frontend must track"
            " and send the original pipeline file path",
        )
    py_path = (cwd / body.source_file).resolve()
    if not py_path.is_relative_to(cwd):
        raise HTTPException(
            status_code=400,
            detail="source_file must be within the project directory",
        )

    _mark_self_write()

    if graph.submodels:
        # Multi-file write: main .py + submodel .py files
        files = graph_to_code_multi(
            graph,
            pipeline_name=body.name,
            description=body.description,
            preamble=body.preamble,
            source_file=body.source_file,
        )
        for rel_path, code in files.items():
            out_path = (cwd / rel_path).resolve()
            if not out_path.is_relative_to(cwd):
                continue
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(code)
    else:
        code = graph_to_code(
            graph,
            pipeline_name=body.name,
            description=body.description,
            preamble=body.preamble,
        )
        py_path.write_text(code)

    # Write sidecar .haute.json (node positions for the GUI)
    _save_sidecar(py_path, graph)

    return SavePipelineResponse(
        file=str(py_path.relative_to(cwd)),
        pipeline_name=body.name,
    )


@app.post("/api/pipeline/run", response_model=RunPipelineResponse)
async def run_pipeline(body: RunPipelineRequest) -> RunPipelineResponse:
    """Execute the full pipeline graph and return per-node results."""
    from haute.executor import execute_graph
    from haute.graph_utils import flatten_graph

    graph = flatten_graph(body.graph)
    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        results = await asyncio.to_thread(execute_graph, graph)
        return RunPipelineResponse(status="ok", results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/trace", response_model=TraceResponse)
async def trace_row(body: TraceRequest) -> TraceResponse:
    """Trace a single row through the pipeline, returning per-node snapshots."""
    from haute.graph_utils import flatten_graph
    from haute.trace import execute_trace, trace_result_to_dict

    graph = flatten_graph(body.graph)
    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        result = await asyncio.to_thread(
            execute_trace,
            graph,
            row_index=body.rowIndex,
            target_node_id=body.targetNodeId,
            column=body.column,
            row_limit=body.rowLimit,
        )
        return TraceResponse(
            status="ok",
            trace=trace_result_to_dict(result),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/preview", response_model=PreviewNodeResponse)
async def preview_node(body: PreviewNodeRequest) -> PreviewNodeResponse:
    """Run pipeline up to a specific node and return its output.

    Accepts an optional ``rowLimit`` (default 1000) that is pushed into
    the Polars lazy query plan so only that many rows are scanned.
    """
    from haute.executor import execute_graph
    from haute.graph_utils import flatten_graph

    graph = flatten_graph(body.graph)
    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        results = await asyncio.to_thread(
            execute_graph,
            graph,
            target_node_id=body.nodeId,
            row_limit=body.rowLimit,
        )
        node_result = results.get(body.nodeId)
        if not node_result:
            raise HTTPException(
                status_code=404,
                detail=f"Node '{body.nodeId}' not found in results",
            )

        node_map = {n.id: n for n in graph.nodes}
        timings = [
            {
                "nodeId": nid,
                "label": node_map[nid].data.label,
                "timing_ms": r.get("timing_ms", 0),
            }
            for nid, r in results.items()
            if nid in node_map
        ]

        return PreviewNodeResponse(nodeId=body.nodeId, timings=timings, **node_result)
    except HTTPException:
        raise
    except Exception as e:
        return PreviewNodeResponse(
            nodeId=body.nodeId,
            status="error",
            error=str(e),
        )


@app.post("/api/pipeline/sink", response_model=SinkResponse)
async def execute_sink_node(body: SinkRequest) -> SinkResponse:
    """Execute the pipeline up to a sink node and write output to disk.

    Only called on explicit user action (Write button), not during normal run/preview.
    """
    from haute.executor import execute_sink
    from haute.graph_utils import flatten_graph

    graph = flatten_graph(body.graph)
    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        result = await asyncio.to_thread(
            execute_sink, graph, sink_node_id=body.nodeId,
        )
        return SinkResponse(**result)
    except Exception as e:
        return SinkResponse(status="error", message=str(e))


@app.get("/api/files", response_model=BrowseFilesResponse)
async def browse_files(
    dir: str = ".",
    extensions: str = ".parquet,.csv,.json,.xml",
) -> BrowseFilesResponse:
    """Browse files on disk for the file picker UI."""
    base = Path.cwd()
    target = (base / dir).resolve()

    if not str(target).startswith(str(base)):
        raise HTTPException(status_code=403, detail="Cannot browse outside project root")
    if not target.is_dir():
        raise HTTPException(status_code=404, detail=f"Directory not found: {dir}")

    ext_list = [e.strip() for e in extensions.split(",")]
    items: list[FileItem] = []

    for entry in sorted(target.iterdir()):
        rel = str(entry.relative_to(base))
        if entry.name.startswith("."):
            continue
        if entry.is_dir():
            items.append(FileItem(name=entry.name, path=rel, type="directory"))
        elif any(entry.name.endswith(ext) for ext in ext_list):
            items.append(
                FileItem(
                    name=entry.name,
                    path=rel,
                    type="file",
                    size=entry.stat().st_size,
                )
            )

    return BrowseFilesResponse(
        dir=str(target.relative_to(base)),
        items=items,
    )


@app.get("/api/schema", response_model=SchemaResponse)
async def get_schema(path: str) -> SchemaResponse:
    """Read a data file and return its schema + preview."""
    import polars as pl

    base = Path.cwd()
    target = (base / path).resolve()

    if not str(target).startswith(str(base)):
        raise HTTPException(status_code=403, detail="Cannot read outside project root")
    if not target.is_file():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

    try:
        from haute.graph_utils import read_source

        lf = read_source(str(target))

        schema = lf.collect_schema()
        columns = [{"name": c, "dtype": str(d)} for c, d in schema.items()]
        preview_df = lf.head(5).collect()
        row_count = lf.select(pl.len()).collect().item()

        return SchemaResponse(
            path=path,
            columns=columns,
            row_count=row_count,
            column_count=len(columns),
            preview=preview_df.to_dicts(),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _get_databricks_client() -> Any:
    """Return a Databricks WorkspaceClient using data credentials from .env."""
    import os

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        raise HTTPException(
            status_code=400,
            detail="databricks-sdk is not installed. "
            "Install with: pip install haute[databricks]",
        )

    host = os.getenv("DATABRICKS_DATA_HOST") or os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_DATA_TOKEN") or os.getenv("DATABRICKS_TOKEN", "")

    if not host or not token:
        raise HTTPException(
            status_code=400,
            detail="DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env",
        )

    return WorkspaceClient(host=host, token=token)


@app.get("/api/databricks/warehouses", response_model=WarehouseListResponse)
async def list_databricks_warehouses() -> WarehouseListResponse:
    """List available Databricks SQL Warehouses."""
    try:
        w = _get_databricks_client()
        warehouses = []
        for wh in w.warehouses.list():
            warehouses.append({
                "id": wh.id,
                "name": wh.name,
                "http_path": f"/sql/1.0/warehouses/{wh.id}",
                "state": wh.state.value if wh.state else "UNKNOWN",
                "size": wh.cluster_size or "",
            })
        return {"warehouses": warehouses}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/databricks/catalogs", response_model=CatalogListResponse)
async def list_databricks_catalogs() -> CatalogListResponse:
    """List Unity Catalog catalogs."""
    try:
        w = _get_databricks_client()
        catalogs = [
            {"name": c.name, "comment": c.comment or ""}
            for c in w.catalogs.list()
            if c.name
        ]
        return {"catalogs": catalogs}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/databricks/schemas", response_model=SchemaListResponse)
async def list_databricks_schemas(catalog: str) -> SchemaListResponse:
    """List schemas within a Unity Catalog catalog."""
    try:
        w = _get_databricks_client()
        schemas = [
            {"name": s.name, "comment": s.comment or ""}
            for s in w.schemas.list(catalog_name=catalog)
            if s.name
        ]
        return {"schemas": schemas}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/databricks/tables", response_model=TableListResponse)
async def list_databricks_tables(catalog: str, schema: str) -> TableListResponse:
    """List tables within a Unity Catalog schema."""
    try:
        w = _get_databricks_client()
        tables = [
            {
                "name": t.name,
                "full_name": t.full_name or f"{catalog}.{schema}.{t.name}",
                "table_type": t.table_type.value if t.table_type else "",
                "comment": t.comment or "",
            }
            for t in w.tables.list(catalog_name=catalog, schema_name=schema)
            if t.name
        ]
        return {"tables": tables}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/databricks/fetch", response_model=FetchTableResponse)
async def fetch_databricks_table(body: FetchTableRequest) -> FetchTableResponse:
    """Fetch a Databricks table and cache it locally as parquet."""
    try:
        import asyncio

        from haute._databricks_io import fetch_and_cache

        result = await asyncio.to_thread(
            fetch_and_cache,
            table=body.table,
            http_path=body.http_path,
            query=body.query,
        )
        return FetchTableResponse(**result)
    except ImportError:
        raise HTTPException(
            status_code=400,
            detail="databricks-sql-connector is not installed. "
            "Install with: pip install haute[databricks]",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/databricks/fetch/progress", response_model=FetchProgressResponse)
async def get_fetch_progress(table: str) -> FetchProgressResponse:
    """Poll fetch progress for a table currently being downloaded."""
    from haute._databricks_io import fetch_progress

    progress = fetch_progress(table)
    if progress is None:
        return FetchProgressResponse(active=False)
    return FetchProgressResponse(active=True, **progress)


@app.get("/api/databricks/cache", response_model=CacheStatusResponse)
async def get_databricks_cache_status(table: str) -> CacheStatusResponse:
    """Check whether a Databricks table has been fetched and cached locally."""
    from haute._databricks_io import cache_info

    info = cache_info(table)
    if info is None:
        return CacheStatusResponse(cached=False, table=table)
    return CacheStatusResponse(cached=True, **info)


@app.delete("/api/databricks/cache", response_model=CacheStatusResponse)
async def delete_databricks_cache(table: str) -> CacheStatusResponse:
    """Delete the local parquet cache for a Databricks table."""
    from haute._databricks_io import clear_cache

    clear_cache(table)
    return CacheStatusResponse(cached=False, table=table)


@app.get("/api/schema/databricks", response_model=SchemaResponse)
async def get_databricks_schema(table: str) -> SchemaResponse:
    """Return schema + preview from the local parquet cache of a Databricks table."""
    import polars as pl

    from haute._databricks_io import cached_path

    p = cached_path(table)
    if p is None:
        raise HTTPException(
            status_code=404,
            detail=f'Table "{table}" has not been fetched yet. '
            f"Click Fetch Data on the data source node to download it.",
        )

    try:
        import pyarrow.parquet as pq

        df = pl.scan_parquet(p).head(1000).collect()
        columns = [{"name": c, "dtype": str(df[c].dtype)} for c in df.columns]
        preview_df = df.head(5)
        row_count = pq.read_metadata(str(p)).num_rows

        return SchemaResponse(
            path=table,
            columns=columns,
            row_count=row_count,
            column_count=len(columns),
            preview=preview_df.to_dicts(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Submodel endpoints
# ---------------------------------------------------------------------------


@app.post("/api/submodel/create", response_model=CreateSubmodelResponse)
async def create_submodel(body: CreateSubmodelRequest) -> CreateSubmodelResponse:
    """Group selected nodes into a submodel.

    Creates a new ``modules/<name>.py`` file, updates the main pipeline file,
    and returns the updated parent graph with the submodel node.
    """
    from haute._types import GraphEdge as _GEdge
    from haute._types import GraphNode as _GNode
    from haute._types import NodeData as _NData
    from haute.codegen import graph_to_code_multi
    from haute.graph_utils import _sanitize_func_name

    graph = body.graph
    cwd = Path.cwd()
    sm_name = _sanitize_func_name(body.name)
    sm_file = f"modules/{sm_name}.py"
    selected_ids = set(body.node_ids)

    if len(selected_ids) < 2:
        raise HTTPException(
            status_code=400,
            detail="A submodel must contain at least 2 nodes.",
        )

    nodes = graph.nodes
    edges = graph.edges

    # Separate child vs parent nodes
    child_nodes = [n for n in nodes if n.id in selected_ids]
    parent_nodes = [n for n in nodes if n.id not in selected_ids]
    child_node_ids = {n.id for n in child_nodes}

    # Separate edges: internal (both ends inside), cross-boundary, external
    internal_edges = [
        e for e in edges
        if e.source in child_node_ids and e.target in child_node_ids
    ]
    cross_edges = [
        e for e in edges
        if (e.source in child_node_ids) != (e.target in child_node_ids)
    ]
    external_edges = [
        e for e in edges
        if e.source not in child_node_ids
        and e.target not in child_node_ids
    ]

    # Determine input/output ports
    input_ports: list[str] = []
    output_ports: list[str] = []
    for e in cross_edges:
        if e.target in child_node_ids and e.source not in child_node_ids:
            if e.target not in input_ports:
                input_ports.append(e.target)
        if e.source in child_node_ids and e.target not in child_node_ids:
            if e.source not in output_ports:
                output_ports.append(e.source)

    # Build the submodel internal graph (dict — stored in submodels metadata)
    sm_graph = {
        "nodes": [n.model_dump() for n in child_nodes],
        "edges": [e.model_dump() for e in internal_edges],
        "submodel_name": sm_name,
        "submodel_description": "",
        "source_file": sm_file,
    }

    # Build the submodel placeholder node
    sm_node_id = f"submodel__{sm_name}"
    sm_node = _GNode(
        id=sm_node_id,
        type="submodel",
        position={"x": 0, "y": 0},
        data=_NData(
            label=sm_name,
            description="",
            nodeType="submodel",
            config={
                "file": sm_file,
                "childNodeIds": list(child_node_ids),
                "inputPorts": input_ports,
                "outputPorts": output_ports,
            },
        ),
    )

    # Rewire cross-boundary edges to point to/from the submodel node
    rewired_cross: list[_GEdge] = []
    for e in cross_edges:
        if e.target in child_node_ids:
            rewired_cross.append(_GEdge(
                id=f"e_{e.source}_{sm_node_id}__{e.target}",
                source=e.source,
                target=sm_node_id,
                targetHandle=f"in__{e.target}",
            ))
        elif e.source in child_node_ids:
            rewired_cross.append(_GEdge(
                id=f"e_{sm_node_id}_{e.target}__{e.source}",
                source=sm_node_id,
                sourceHandle=f"out__{e.source}",
                target=e.target,
            ))

    # Assemble the new parent graph (preserve existing submodels)
    existing_submodels = dict(graph.submodels or {})
    existing_submodels[sm_name] = {
        "file": sm_file,
        "childNodeIds": list(child_node_ids),
        "inputPorts": input_ports,
        "outputPorts": output_ports,
        "graph": sm_graph,
    }
    new_graph = graph.model_copy(update={
        "nodes": parent_nodes + [sm_node],
        "edges": external_edges + rewired_cross,
        "submodels": existing_submodels,
    })

    # Write files to disk
    _mark_self_write()
    files = graph_to_code_multi(
        new_graph,
        pipeline_name=body.pipeline_name,
        preamble=body.preamble,
        source_file=body.source_file,
    )
    for rel_path, code in files.items():
        out_path = (cwd / rel_path).resolve()
        if out_path.is_relative_to(cwd):
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(code)

    # Determine the main file path written
    if not body.source_file:
        raise HTTPException(
            status_code=400,
            detail="source_file is required — the frontend must track"
            " and send the original pipeline file path",
        )
    parent_file = body.source_file

    # Save sidecar
    py_path = (cwd / parent_file).resolve()
    if py_path.is_relative_to(cwd):
        _save_sidecar(py_path, new_graph)

    return CreateSubmodelResponse(
        status="ok",
        submodel_file=sm_file,
        parent_file=parent_file,
        graph=new_graph.model_dump(),
    )


@app.get("/api/submodel/{name}", response_model=SubmodelGraphResponse)
async def get_submodel(name: str) -> SubmodelGraphResponse:
    """Return the internal graph of a submodel for drill-down view."""
    from haute.parser import parse_submodel_file

    cwd = Path.cwd()
    sm_path = cwd / "modules" / f"{name}.py"
    if not sm_path.is_file():
        raise HTTPException(status_code=404, detail=f"Submodel '{name}' not found")

    sm_graph = parse_submodel_file(sm_path)

    # Load sidecar positions if available
    positions = _load_sidecar_positions(sm_path)
    for node in sm_graph.get("nodes", []):
        if node["id"] in positions:
            node["position"] = positions[node["id"]]

    return SubmodelGraphResponse(
        status="ok",
        submodel_name=sm_graph.get("submodel_name", name),
        graph=sm_graph,
    )


@app.post("/api/submodel/dissolve", response_model=DissolveSubmodelResponse)
async def dissolve_submodel(body: DissolveSubmodelRequest) -> DissolveSubmodelResponse:
    """Ungroup a submodel back into the parent pipeline.

    Inlines the submodel's nodes into the parent graph and deletes
    the submodel .py file.
    """
    from haute.graph_utils import flatten_graph

    graph = body.graph
    sm_name = body.submodel_name
    submodels = graph.submodels or {}

    if sm_name not in submodels:
        raise HTTPException(
            status_code=404,
            detail=f"Submodel '{sm_name}' not found in graph",
        )

    # Flatten just the target submodel
    sm_meta = submodels[sm_name]
    sm_file = sm_meta.get("file", "")

    # Remove the submodel from the graph metadata and flatten
    flat = flatten_graph(graph)

    # Write the updated main file
    from haute.codegen import graph_to_code

    cwd = Path.cwd()
    _mark_self_write()

    if not body.source_file:
        raise HTTPException(
            status_code=400,
            detail="source_file is required — the frontend must track"
            " and send the original pipeline file path",
        )
    py_path = (cwd / body.source_file).resolve()

    code = graph_to_code(
        flat,
        pipeline_name=body.pipeline_name,
        preamble=body.preamble,
    )
    py_path.write_text(code)
    _save_sidecar(py_path, flat)

    # Delete the submodel file
    if sm_file:
        sm_path = (cwd / sm_file).resolve()
        if sm_path.is_file() and sm_path.is_relative_to(cwd):
            sm_path.unlink()

    return DissolveSubmodelResponse(status="ok", graph=flat.model_dump())


# ---------------------------------------------------------------------------
# File watcher - live sync from .py edits to GUI
# ---------------------------------------------------------------------------


async def _file_watcher() -> None:
    """Watch pipeline directories for .py changes and broadcast to GUI."""
    try:
        from watchfiles import Change, awatch
    except ImportError:
        logger.warning("watchfiles_missing", msg="live sync disabled")
        return

    cwd = Path.cwd()
    watch_dirs = [cwd]
    modules_dir = cwd / "modules"
    if modules_dir.is_dir():
        watch_dirs.append(modules_dir)

    logger.info("file_watcher_started", watch_dirs=[str(d) for d in watch_dirs])

    async for changes in awatch(*watch_dirs, recursive=False):
        if _is_self_write():
            continue

        # Collect changed files
        changed_files: list[Path] = []
        has_module_change = False
        for change_type, changed_path in changes:
            p = Path(changed_path)
            if p.suffix != ".py" or p.name.startswith("__"):
                continue
            if change_type not in (Change.modified, Change.added):
                continue
            changed_files.append(p)
            if p.parent == modules_dir:
                has_module_change = True

        # If a module file changed, re-parse the parent pipeline instead
        if has_module_change:
            for f in _discover_pipelines():
                changed_files.append(f)

        # Deduplicate and parse
        seen: set[str] = set()
        for p in changed_files:
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)

            logger.info("file_changed", file=p.name)
            try:
                graph = _parse_pipeline_to_graph(p)
                await _broadcast(
                    {
                        "type": "graph_update",
                        "graph": graph.model_dump(),
                        "source_file": str(p),
                    }
                )
                n_nodes = len(graph.nodes)
                logger.info(
                    "graph_broadcast",
                    clients=len(_ws_clients),
                    nodes=n_nodes,
                )
            except Exception as e:
                logger.error("parse_error", file=p.name, error=str(e))
                await _broadcast(
                    {
                        "type": "parse_error",
                        "error": str(e),
                        "source_file": str(p),
                    }
                )


# ---------------------------------------------------------------------------
# Static file serving (built React frontend)
# ---------------------------------------------------------------------------

if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the React SPA - all non-API routes return index.html."""
        file_path = STATIC_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(STATIC_DIR / "index.html")
