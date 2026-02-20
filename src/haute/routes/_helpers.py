"""Shared helpers for route modules — self-write tracking, WebSocket broadcast, pipeline parsing."""

from __future__ import annotations

import json as _json
import time
from pathlib import Path
from typing import Any

from fastapi import WebSocket

from haute._logging import get_logger
from haute.graph_utils import PipelineGraph

logger = get_logger(component="server")

# ---------------------------------------------------------------------------
# Self-write tracking (avoid file-watcher feedback loops)
# ---------------------------------------------------------------------------
_last_self_write: float = 0.0
_SELF_WRITE_COOLDOWN = 1.0  # seconds


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


def discover_pipelines() -> list[Path]:
    """Find pipeline .py files in the project root that contain ``haute.Pipeline``."""
    from haute.discovery import discover_pipelines as _discover

    return _discover()


def load_sidecar_positions(py_path: Path) -> dict[str, dict[str, float]]:
    """Load node positions from the sidecar .haute.json file."""
    sidecar = py_path.with_suffix(".haute.json")
    if sidecar.exists():
        try:
            data = _json.loads(sidecar.read_text())
            return data.get("positions", {})
        except Exception as e:
            logger.warning("corrupt_sidecar", file=sidecar.name, error=str(e))
    return {}


def save_sidecar(py_path: Path, graph: PipelineGraph) -> None:
    """Write node positions to the sidecar .haute.json file."""
    positions = {node.id: node.position for node in graph.nodes}
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
