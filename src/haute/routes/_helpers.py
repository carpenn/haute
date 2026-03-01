"""Shared helpers for route modules — self-write tracking, WebSocket broadcast, pipeline parsing."""

from __future__ import annotations

import json as _json
import time
from pathlib import Path
from typing import Any

from fastapi import WebSocket

from haute._logging import get_logger
from haute.graph_utils import PipelineGraph, _sanitize_func_name

logger = get_logger(component="server")

# ---------------------------------------------------------------------------
# Self-write tracking (avoid file-watcher feedback loops)
# ---------------------------------------------------------------------------
_last_self_write: float = 0.0
_SELF_WRITE_COOLDOWN = 2.0  # seconds (must exceed save duration + watcher debounce)


def mark_self_write() -> None:
    """Record that we just wrote a pipeline file ourselves."""
    global _last_self_write
    _last_self_write = time.monotonic()


def is_self_write() -> bool:
    """Return True if a self-write happened within the cooldown window."""
    return (time.monotonic() - _last_self_write) < _SELF_WRITE_COOLDOWN


# ---------------------------------------------------------------------------
# WebSocket connections for live sync
# ---------------------------------------------------------------------------
ws_clients: set[WebSocket] = set()


async def broadcast(data: dict[str, Any]) -> None:
    """Push a message to all connected WebSocket clients."""
    payload = _json.dumps(data)
    dead: list[WebSocket] = []
    for ws in ws_clients:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.discard(ws)


# ---------------------------------------------------------------------------
# Pipeline helpers shared across routes
# ---------------------------------------------------------------------------

# Lightweight index: pipeline_name → file_path.
# Built lazily on first lookup, invalidated by ``invalidate_pipeline_index()``.
_pipeline_index: dict[str, Path] | None = None


def invalidate_pipeline_index() -> None:
    """Clear the cached pipeline name→path index (called by file watcher)."""
    global _pipeline_index, _module_deps
    _pipeline_index = None
    _module_deps = None


def _ensure_pipeline_index() -> dict[str, Path]:
    """Build or return the cached pipeline name→path index."""
    global _pipeline_index
    if _pipeline_index is not None:
        return _pipeline_index

    from haute.discovery import discover_pipelines as _discover
    from haute.parser import parse_pipeline_file

    index: dict[str, Path] = {}
    for f in _discover():
        try:
            graph = parse_pipeline_file(f)
            name = graph.pipeline_name or f.stem
            index[name] = f
        except Exception:
            index[f.stem] = f
    _pipeline_index = index
    return _pipeline_index


def discover_pipelines() -> list[Path]:
    """Find pipeline .py files in the project root that contain ``haute.Pipeline``."""
    from haute.discovery import discover_pipelines as _discover

    return _discover()


# ---------------------------------------------------------------------------
# Module dependency map: module_stem → set of pipeline Paths that import it
# ---------------------------------------------------------------------------
_module_deps: dict[str, set[Path]] | None = None


def _ensure_module_deps() -> dict[str, set[Path]]:
    """Build or return the cached module → pipeline dependency map.

    Scans each pipeline source for ``pipeline.submodel("...")`` calls and
    maps the module file stem to the set of pipeline files that reference it.
    """
    global _module_deps
    if _module_deps is not None:
        return _module_deps

    import ast

    deps: dict[str, set[Path]] = {}
    for f in discover_pipelines():
        try:
            source = f.read_text()
            tree = ast.parse(source)
        except Exception:
            continue

        from haute._parser_submodels import extract_submodel_calls

        for rel_path in extract_submodel_calls(tree):
            module_stem = Path(rel_path).stem
            deps.setdefault(module_stem, set()).add(f)

    _module_deps = deps
    return _module_deps


def pipelines_importing_module(module_stem: str) -> list[Path]:
    """Return the pipeline files that import a given module (by stem name)."""
    deps = _ensure_module_deps()
    return list(deps.get(module_stem, []))


def lookup_pipeline_by_name(name: str) -> Path | None:
    """O(1) lookup of a pipeline file by name, using the cached index."""
    index = _ensure_pipeline_index()
    return index.get(name)


def load_sidecar_positions(py_path: Path) -> dict[str, dict[str, float]]:
    """Load node positions from the sidecar .haute.json file."""
    sidecar = py_path.with_suffix(".haute.json")
    if sidecar.exists():
        try:
            data = _json.loads(sidecar.read_text())
            positions: dict[str, dict[str, float]] = data.get("positions", {})
            return positions
        except Exception as e:
            logger.warning("corrupt_sidecar", file=sidecar.name, error=str(e))
    return {}


def save_sidecar(py_path: Path, graph: PipelineGraph) -> None:
    """Write node positions to the sidecar .haute.json file.

    Keys are the sanitised function names (which the parser uses as node IDs
    on re-parse), so positions survive label renames.
    """
    positions = {
        _sanitize_func_name(node.data.label): node.position
        for node in graph.nodes
    }
    sidecar = py_path.with_suffix(".haute.json")
    sidecar.write_text(_json.dumps({"positions": positions}, indent=2) + "\n")


def parse_pipeline_to_graph(py_path: Path) -> PipelineGraph:
    """Parse a .py file and merge with sidecar positions."""
    from haute.parser import parse_pipeline_file

    graph = parse_pipeline_file(py_path)
    positions = load_sidecar_positions(py_path)

    for node in graph.nodes:
        if node.id in positions:
            node.position = positions[node.id]

    return graph
