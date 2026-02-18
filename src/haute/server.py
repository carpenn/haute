"""FastAPI backend for haute."""

import asyncio
import json as _json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from haute.schemas import (
    BrowseFilesResponse,
    CacheStatusResponse,
    CatalogListResponse,
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
    TableListResponse,
    TraceRequest,
    TraceResponse,
    WarehouseListResponse,
)

STATIC_DIR = Path(__file__).parent / "static"
logger = logging.getLogger("uvicorn.error")

_watcher_task: asyncio.Task | None = None


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    from haute.deploy._config import _load_env

    _load_env(Path.cwd())

    global _watcher_task
    _watcher_task = asyncio.create_task(_file_watcher())
    yield
    if _watcher_task:
        _watcher_task.cancel()


app = FastAPI(title="Haute", version="0.1.0", lifespan=_lifespan)

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
    logger.info("WebSocket client connected (%d total)", len(_ws_clients))
    try:
        while True:
            await websocket.receive_text()  # keep-alive / client messages
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
        logger.info("WebSocket client disconnected (%d remaining)", len(_ws_clients))


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
            logger.warning("Corrupt sidecar %s: %s", sidecar.name, e)
    return {}


def _save_sidecar(py_path: Path, graph: dict) -> None:
    """Write node positions to the sidecar .haute.json file."""
    positions = {}
    for node in graph.get("nodes", []):
        positions[node["id"]] = node.get("position", {"x": 0, "y": 0})

    sidecar = py_path.with_suffix(".haute.json")
    sidecar.write_text(_json.dumps({"positions": positions}, indent=2) + "\n")


def _parse_pipeline_to_graph(py_path: Path) -> dict:
    """Parse a .py file and merge with sidecar positions."""
    from haute.parser import parse_pipeline_file

    graph = parse_pipeline_file(py_path)
    positions = _load_sidecar_positions(py_path)

    # Apply saved positions if available
    for node in graph.get("nodes", []):
        if node["id"] in positions:
            node["position"] = positions[node["id"]]

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
                    name=graph.get("pipeline_name", f.stem),
                    description=graph.get("pipeline_description", ""),
                    file=str(f.relative_to(Path.cwd())),
                    node_count=len(graph.get("nodes", [])),
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
            if graph.get("pipeline_name") == name:
                return graph
        except Exception as e:
            logger.warning("Failed to parse %s: %s", f.name, e)
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
            if graph.get("nodes"):
                graph["source_file"] = str(f.relative_to(cwd))
                return graph
        except Exception as e:
            logger.warning("Failed to parse %s: %s", f.name, e)
            continue

    return {"nodes": [], "edges": []}


@app.post("/api/pipeline/save", response_model=SavePipelineResponse)
async def save_pipeline(body: SavePipelineRequest) -> SavePipelineResponse:
    """Save a graph: .py (source of truth) + .haute.json (positions)."""
    from haute.codegen import graph_to_code

    graph = body.graph.model_dump()

    cwd = Path.cwd()

    # Write .py (source of truth - runnable code)
    if body.source_file:
        py_path = (cwd / body.source_file).resolve()
        if not py_path.is_relative_to(cwd):
            raise HTTPException(
                status_code=400,
                detail="source_file must be within the project directory",
            )
    else:
        safe_name = body.name.lower().replace(" ", "_").replace("-", "_")
        py_path = cwd / f"{safe_name}.py"
    code = graph_to_code(
        graph,
        pipeline_name=body.name,
        description=body.description,
        preamble=body.preamble,
    )
    _mark_self_write()
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

    graph = body.graph.model_dump()
    if not graph.get("nodes"):
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        results = execute_graph(graph)
        return RunPipelineResponse(status="ok", results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/trace", response_model=TraceResponse)
async def trace_row(body: TraceRequest) -> TraceResponse:
    """Trace a single row through the pipeline, returning per-node snapshots."""
    from haute.trace import execute_trace, trace_result_to_dict

    graph = body.graph.model_dump()
    if not graph.get("nodes"):
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        result = execute_trace(
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

    graph = body.graph.model_dump()
    if not graph.get("nodes"):
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        results = execute_graph(
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
        return PreviewNodeResponse(nodeId=body.nodeId, **node_result)
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

    graph = body.graph.model_dump()
    if not graph.get("nodes"):
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        result = execute_sink(graph, sink_node_id=body.nodeId)
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
        if target.suffix == ".parquet":
            lf = pl.scan_parquet(target)
        elif target.suffix == ".csv":
            lf = pl.scan_csv(target)
        elif target.suffix == ".json":
            # JSON has no scan API — read eagerly (JSON files are small)
            lf = pl.read_json(target).lazy()
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {target.suffix}",
            )

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
        from haute._databricks_io import fetch_and_cache

        result = fetch_and_cache(
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


@app.get("/api/schema/databricks")
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
# File watcher - live sync from .py edits to GUI
# ---------------------------------------------------------------------------


async def _file_watcher() -> None:
    """Watch pipeline directories for .py changes and broadcast to GUI."""
    try:
        from watchfiles import Change, awatch
    except ImportError:
        logger.warning("watchfiles not installed - live sync disabled")
        return

    cwd = Path.cwd()
    watch_dirs = [cwd]
    modules_dir = cwd / "modules"
    if modules_dir.is_dir():
        watch_dirs.append(modules_dir)

    logger.info("File watcher started, watching: %s", [str(d) for d in watch_dirs])

    async for changes in awatch(*watch_dirs, recursive=False):
        if _is_self_write():
            continue

        for change_type, changed_path in changes:
            p = Path(changed_path)
            if p.suffix != ".py" or p.name.startswith("__"):
                continue
            if change_type not in (Change.modified, Change.added):
                continue

            logger.info("File changed: %s - re-parsing", p.name)
            try:
                graph = _parse_pipeline_to_graph(p)
                await _broadcast(
                    {
                        "type": "graph_update",
                        "graph": graph,
                        "source_file": str(p),
                    }
                )
                n_nodes = len(graph.get("nodes", []))
                logger.info(
                    "Broadcast graph_update to %d clients (%d nodes)",
                    len(_ws_clients),
                    n_nodes,
                )
            except Exception as e:
                logger.error("Parse error for %s: %s", p.name, e)
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
