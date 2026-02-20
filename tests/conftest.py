"""Shared test fixtures and helpers for the haute test suite."""

from __future__ import annotations

from pathlib import Path

import pytest

from haute._sandbox import _get_project_root, set_project_root
from haute._types import GraphEdge, GraphNode, NodeData


@pytest.fixture(autouse=True)
def _widen_sandbox_root():
    """Allow tests to load files from temp directories.

    Sets the sandbox project root to ``/`` for the duration of each test
    so that ``validate_project_path`` accepts paths in ``/tmp``.
    Restores the original root afterwards.
    """
    original = _get_project_root()
    set_project_root(Path("/"))
    yield
    set_project_root(original)


# ---------------------------------------------------------------------------
# Graph builder helpers — used across test_executor, test_trace, etc.
# ---------------------------------------------------------------------------


def make_source_node(nid: str, path: str) -> GraphNode:
    """Build a minimal dataSource node."""
    return GraphNode(
        id=nid,
        data=NodeData(label=nid, nodeType="dataSource", config={"path": path}),
    )


def make_transform_node(nid: str, code: str = "") -> GraphNode:
    """Build a minimal transform node."""
    return GraphNode(
        id=nid,
        data=NodeData(label=nid, nodeType="transform", config={"code": code}),
    )


def make_output_node(nid: str, fields: list[str] | None = None) -> GraphNode:
    """Build a minimal output node."""
    return GraphNode(
        id=nid,
        data=NodeData(label=nid, nodeType="output", config={"fields": fields or []}),
    )


def make_edge(src: str, tgt: str) -> GraphEdge:
    """Build a minimal edge."""
    return GraphEdge(id=f"e_{src}_{tgt}", source=src, target=tgt)
