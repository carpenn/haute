"""FastAPI backend for runw."""

import importlib.util
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from runw.pipeline import Pipeline

app = FastAPI(title="runw", version="0.1.0")

STATIC_DIR = Path(__file__).parent / "static"


def _load_pipeline(filepath: Path) -> Pipeline:
    """Import a .py file and find the Pipeline instance in it."""
    spec = importlib.util.spec_from_file_location(filepath.stem, filepath)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot load {filepath}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    for attr in dir(module):
        obj = getattr(module, attr)
        if isinstance(obj, Pipeline):
            return obj
    raise ValueError(f"No Pipeline instance found in {filepath}")


def _discover_pipelines() -> list[Path]:
    """Find pipeline .py files in the project."""
    cwd = Path.cwd()
    locations = [cwd / "pipelines", cwd / "examples"]
    found = []
    for loc in locations:
        if loc.is_dir():
            found.extend(sorted(loc.glob("*.py")))
    return [f for f in found if f.name != "__init__.py" and f.name != "create_sample_data.py"]


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/pipelines")
async def list_pipelines():
    """List all discovered pipelines."""
    files = _discover_pipelines()
    result = []
    for f in files:
        try:
            p = _load_pipeline(f)
            result.append({
                "name": p.name,
                "description": p.description,
                "file": str(f.relative_to(Path.cwd())),
                "node_count": len(p.nodes),
            })
        except Exception as e:
            result.append({"name": f.stem, "file": str(f), "error": str(e)})
    return result


@app.get("/api/pipeline/{name}")
async def get_pipeline(name: str):
    """Return the graph for a specific pipeline."""
    for f in _discover_pipelines():
        try:
            p = _load_pipeline(f)
            if p.name == name:
                return p.to_graph()
        except Exception:
            continue
    raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")


@app.get("/api/pipeline")
async def get_first_pipeline():
    """Return the graph for the active pipeline, or an empty canvas."""
    import json as _json

    cwd = Path.cwd()
    pipelines_dir = cwd / "pipelines"

    # First, check for a saved graph .json (preserves exact positions/config)
    if pipelines_dir.is_dir():
        for jf in sorted(pipelines_dir.glob("*.json")):
            try:
                data = _json.loads(jf.read_text())
                if "nodes" in data and "edges" in data:
                    return data
            except Exception:
                continue

    # Fall back to loading .py pipeline files from pipelines/
    if pipelines_dir.is_dir():
        for f in sorted(pipelines_dir.glob("*.py")):
            if f.name == "__init__.py":
                continue
            try:
                p = _load_pipeline(f)
                return p.to_graph()
            except Exception:
                continue

    # No pipelines found — return blank canvas
    return {"nodes": [], "edges": []}


@app.post("/api/pipeline/save")
async def save_pipeline(body: dict):
    """Save a graph as both a .py pipeline file and a .json graph state file."""
    import json as _json

    from runw.codegen import graph_to_code

    name = body.get("name", "my_pipeline")
    description = body.get("description", "")
    graph = body.get("graph", {})

    cwd = Path.cwd()
    pipelines_dir = cwd / "pipelines"
    pipelines_dir.mkdir(exist_ok=True)

    safe_name = name.lower().replace(" ", "_").replace("-", "_")

    # Write .py (runnable code)
    py_path = pipelines_dir / f"{safe_name}.py"
    code = graph_to_code(graph, pipeline_name=name, description=description)
    py_path.write_text(code)

    # Write .json (full graph state with positions for the GUI)
    json_path = pipelines_dir / f"{safe_name}.json"
    json_path.write_text(_json.dumps(graph, indent=2))

    return {
        "status": "saved",
        "file": str(py_path.relative_to(cwd)),
        "graph_file": str(json_path.relative_to(cwd)),
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
