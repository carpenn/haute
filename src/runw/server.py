"""FastAPI backend for runw."""

import asyncio
import json as _json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

STATIC_DIR = Path(__file__).parent / "static"
logger = logging.getLogger("uvicorn.error")

_watcher_task: asyncio.Task | None = None


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _watcher_task
    _watcher_task = asyncio.create_task(_file_watcher())
    yield
    if _watcher_task:
        _watcher_task.cancel()


app = FastAPI(title="Runway", version="0.1.0", lifespan=_lifespan)

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
    """Find pipeline .py files in the project."""
    cwd = Path.cwd()
    locations = [cwd / "pipelines", cwd / "examples"]
    found = []
    for loc in locations:
        if loc.is_dir():
            found.extend(sorted(loc.glob("*.py")))
    return [f for f in found if f.name != "__init__.py" and f.name != "create_sample_data.py"]


def _load_sidecar_positions(py_path: Path) -> dict[str, dict[str, float]]:
    """Load node positions from the sidecar .runw.json file."""
    sidecar = py_path.with_suffix(".runw.json")
    if sidecar.exists():
        try:
            data = _json.loads(sidecar.read_text())
            return data.get("positions", {})
        except Exception:
            pass
    return {}


def _save_sidecar(py_path: Path, graph: dict) -> None:
    """Write node positions to the sidecar .runw.json file."""
    positions = {}
    for node in graph.get("nodes", []):
        positions[node["id"]] = node.get("position", {"x": 0, "y": 0})

    sidecar = py_path.with_suffix(".runw.json")
    sidecar.write_text(_json.dumps({"positions": positions}, indent=2) + "\n")


def _parse_pipeline_to_graph(py_path: Path) -> dict:
    """Parse a .py file and merge with sidecar positions."""
    from runw.parser import parse_pipeline_file

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

@app.get("/api/pipelines")
async def list_pipelines():
    """List all discovered pipelines."""
    from runw.parser import parse_pipeline_file

    files = _discover_pipelines()
    result = []
    for f in files:
        try:
            graph = parse_pipeline_file(f)
            result.append({
                "name": graph.get("pipeline_name", f.stem),
                "description": graph.get("pipeline_description", ""),
                "file": str(f.relative_to(Path.cwd())),
                "node_count": len(graph.get("nodes", [])),
            })
        except Exception as e:
            result.append({"name": f.stem, "file": str(f), "error": str(e)})
    return result


@app.get("/api/pipeline/{name}")
async def get_pipeline(name: str):
    """Return the graph for a specific pipeline."""
    for f in _discover_pipelines():
        try:
            graph = _parse_pipeline_to_graph(f)
            if graph.get("pipeline_name") == name:
                return graph
        except Exception:
            continue
    raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")


@app.get("/api/pipeline")
async def get_first_pipeline():
    """Return the graph for the active pipeline, or an empty canvas.

    Python file is the source of truth. Sidecar .runw.json provides positions.
    """
    cwd = Path.cwd()
    pipelines_dir = cwd / "pipelines"

    # Parse .py files from pipelines/ directory
    if pipelines_dir.is_dir():
        for f in sorted(pipelines_dir.glob("*.py")):
            if f.name == "__init__.py":
                continue
            try:
                graph = _parse_pipeline_to_graph(f)
                if graph.get("nodes"):
                    return graph
            except Exception:
                continue

    # Fall back to examples/
    examples_dir = cwd / "examples"
    if examples_dir.is_dir():
        for f in sorted(examples_dir.glob("*.py")):
            if f.name == "__init__.py":
                continue
            try:
                graph = _parse_pipeline_to_graph(f)
                if graph.get("nodes"):
                    return graph
            except Exception:
                continue

    return {"nodes": [], "edges": []}


@app.post("/api/pipeline/save")
async def save_pipeline(body: dict):
    """Save a graph: .py (source of truth) + .runw.json (positions)."""
    from runw.codegen import graph_to_code

    name = body.get("name", "my_pipeline")
    description = body.get("description", "")
    graph = body.get("graph", {})
    preamble = body.get("preamble", "")

    cwd = Path.cwd()
    pipelines_dir = cwd / "pipelines"
    pipelines_dir.mkdir(exist_ok=True)

    safe_name = name.lower().replace(" ", "_").replace("-", "_")

    # Write .py (source of truth — runnable code)
    py_path = pipelines_dir / f"{safe_name}.py"
    code = graph_to_code(graph, pipeline_name=name, description=description, preamble=preamble)
    _mark_self_write()
    py_path.write_text(code)

    # Write sidecar .runw.json (node positions for the GUI)
    _save_sidecar(py_path, graph)

    return {
        "status": "saved",
        "file": str(py_path.relative_to(cwd)),
        "pipeline_name": name,
    }


@app.post("/api/pipeline/run")
async def run_pipeline(body: dict):
    """Execute the full pipeline graph and return per-node results."""
    from runw.executor import execute_graph

    graph = body.get("graph", {})
    if not graph.get("nodes"):
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        results = execute_graph(graph)
        return {"status": "ok", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/trace")
async def trace_row(body: dict):
    """Trace a single row through the pipeline, returning per-node snapshots.

    Request body:
        graph: React Flow graph JSON
        rowIndex: 0-indexed row to trace (default 0)
        targetNodeId: node to trace from (default: last node)
        column: optional column name to filter the trace to
    """
    from runw.trace import execute_trace, trace_result_to_dict

    graph = body.get("graph", {})
    row_index = body.get("rowIndex", 0)
    target_node_id = body.get("targetNodeId")
    column = body.get("column")

    if not graph.get("nodes"):
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        result = execute_trace(
            graph,
            row_index=int(row_index),
            target_node_id=target_node_id,
            column=column,
        )
        return {"status": "ok", "trace": trace_result_to_dict(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/preview")
async def preview_node(body: dict):
    """Run pipeline up to a specific node and return its output.

    Accepts an optional ``rowLimit`` (default 1000) that is pushed into
    the Polars lazy query plan so only that many rows are scanned.
    """
    from runw.executor import execute_graph

    graph = body.get("graph", {})
    node_id = body.get("nodeId")
    row_limit = body.get("rowLimit", 1000)

    if not node_id:
        raise HTTPException(status_code=400, detail="nodeId is required")
    if not graph.get("nodes"):
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        results = execute_graph(graph, target_node_id=node_id, row_limit=int(row_limit))
        node_result = results.get(node_id)
        if not node_result:
            raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found in results")
        return {"nodeId": node_id, **node_result}
    except HTTPException:
        raise
    except Exception as e:
        return {"nodeId": node_id, "status": "error", "error": str(e),
                "row_count": 0, "column_count": 0, "columns": [], "preview": []}


@app.post("/api/pipeline/sink")
async def execute_sink_node(body: dict):
    """Execute the pipeline up to a sink node and write output to disk.

    Only called on explicit user action (Write button), not during normal run/preview.
    """
    from runw.executor import execute_sink

    graph = body.get("graph", {})
    node_id = body.get("nodeId")

    if not node_id:
        raise HTTPException(status_code=400, detail="nodeId is required")
    if not graph.get("nodes"):
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        result = execute_sink(graph, sink_node_id=node_id)
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/files")
async def browse_files(dir: str = ".", extensions: str = ".parquet,.csv,.json,.xml"):
    """Browse files on disk for the file picker UI."""
    base = Path.cwd()
    target = (base / dir).resolve()

    if not str(target).startswith(str(base)):
        raise HTTPException(status_code=403, detail="Cannot browse outside project root")
    if not target.is_dir():
        raise HTTPException(status_code=404, detail=f"Directory not found: {dir}")

    ext_list = [e.strip() for e in extensions.split(",")]
    items = []

    for entry in sorted(target.iterdir()):
        rel = str(entry.relative_to(base))
        if entry.name.startswith("."):
            continue
        if entry.is_dir():
            items.append({"name": entry.name, "path": rel, "type": "directory"})
        elif any(entry.name.endswith(ext) for ext in ext_list):
            items.append({
                "name": entry.name,
                "path": rel,
                "type": "file",
                "size": entry.stat().st_size,
            })

    return {"dir": str(target.relative_to(base)), "items": items}


@app.get("/api/schema")
async def get_schema(path: str):
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
            df = pl.read_parquet(target)
        elif target.suffix == ".csv":
            df = pl.read_csv(target)
        elif target.suffix == ".json":
            df = pl.read_json(target)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {target.suffix}")

        columns = [
            {"name": col, "dtype": str(df[col].dtype)}
            for col in df.columns
        ]

        preview = df.head(5).to_dicts()

        return {
            "path": path,
            "columns": columns,
            "row_count": len(df),
            "column_count": len(df.columns),
            "preview": preview,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# File watcher — live sync from .py edits to GUI
# ---------------------------------------------------------------------------

async def _file_watcher() -> None:
    """Watch pipeline directories for .py changes and broadcast to GUI."""
    try:
        from watchfiles import Change, awatch
    except ImportError:
        logger.warning("watchfiles not installed — live sync disabled")
        return

    cwd = Path.cwd()
    watch_dirs = [d for d in [cwd / "pipelines", cwd / "examples"] if d.is_dir()]
    if not watch_dirs:
        logger.info("No pipelines/ or examples/ directory — file watcher idle")
        return

    logger.info("File watcher started, watching: %s", [str(d) for d in watch_dirs])

    async for changes in awatch(*watch_dirs):
        if _is_self_write():
            continue

        for change_type, changed_path in changes:
            p = Path(changed_path)
            if p.suffix != ".py" or p.name.startswith("__"):
                continue
            if change_type not in (Change.modified, Change.added):
                continue

            logger.info("File changed: %s — re-parsing", p.name)
            try:
                graph = _parse_pipeline_to_graph(p)
                await _broadcast({
                    "type": "graph_update",
                    "graph": graph,
                    "source_file": str(p),
                })
                n_nodes = len(graph.get("nodes", []))
                logger.info(
                    "Broadcast graph_update to %d clients (%d nodes)",
                    len(_ws_clients), n_nodes,
                )
            except Exception as e:
                logger.error("Parse error for %s: %s", p.name, e)
                await _broadcast({
                    "type": "parse_error",
                    "error": str(e),
                    "source_file": str(p),
                })


# ---------------------------------------------------------------------------
# Static file serving (built React frontend)
# ---------------------------------------------------------------------------

if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the React SPA — all non-API routes return index.html."""
        file_path = STATIC_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(STATIC_DIR / "index.html")
