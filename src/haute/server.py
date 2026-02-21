"""FastAPI backend for haute.

App factory, middleware, WebSocket sync, file watcher, and static serving.
Route handlers live in ``haute.routes.*`` — see:
  - ``routes/pipeline.py``  — pipeline CRUD, run, preview, trace, sink
  - ``routes/databricks.py``— Unity Catalog browsing, data fetching
  - ``routes/files.py``     — file browsing, schema inspection
  - ``routes/submodel.py``  — submodel create, get, dissolve
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from haute._logging import configure_logging, get_logger
from haute.routes._helpers import (
    broadcast,
    discover_pipelines,
    is_self_write,
    parse_pipeline_to_graph,
    ws_clients,
)
from haute.routes.databricks import router as databricks_router
from haute.routes.files import router as files_router
from haute.routes.modelling import router as modelling_router
from haute.routes.pipeline import router as pipeline_router
from haute.routes.submodel import router as submodel_router

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

# CORS for dev mode — Vite dev server (port 5173) talks to FastAPI (port 8000)
if not STATIC_DIR.exists():
    from fastapi.middleware.cors import CORSMiddleware

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# ---------------------------------------------------------------------------
# Include route modules
# ---------------------------------------------------------------------------
app.include_router(pipeline_router)
app.include_router(databricks_router)
app.include_router(files_router)
app.include_router(submodel_router)
app.include_router(modelling_router)


# ---------------------------------------------------------------------------
# WebSocket endpoint for live code ↔ GUI sync
# ---------------------------------------------------------------------------


@app.websocket("/ws/sync")
async def ws_sync(websocket: WebSocket) -> None:
    """WebSocket endpoint for live code ↔ GUI sync."""
    await websocket.accept()
    ws_clients.add(websocket)
    logger.info("ws_connected", total_clients=len(ws_clients))
    try:
        while True:
            await websocket.receive_text()  # keep-alive / client messages
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(websocket)
        logger.info("ws_disconnected", remaining_clients=len(ws_clients))


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
        if is_self_write():
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
            for f in discover_pipelines():
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
                graph = parse_pipeline_to_graph(p)
                await broadcast(
                    {
                        "type": "graph_update",
                        "graph": graph.model_dump(),
                        "source_file": str(p),
                    }
                )
                n_nodes = len(graph.nodes)
                logger.info(
                    "graph_broadcast",
                    clients=len(ws_clients),
                    nodes=n_nodes,
                )
            except Exception as e:
                logger.error("parse_error", file=p.name, error=str(e))
                await broadcast(
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
