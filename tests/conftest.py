"""Shared test fixtures and helpers for the haute test suite."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Graph builder helpers — used across test_executor, test_trace, etc.
# ---------------------------------------------------------------------------


def make_source_node(nid: str, path: str) -> dict:
    """Build a minimal dataSource node dict."""
    return {
        "id": nid,
        "data": {
            "label": nid,
            "nodeType": "dataSource",
            "config": {"path": path},
        },
    }


def make_transform_node(nid: str, code: str = "") -> dict:
    """Build a minimal transform node dict."""
    return {
        "id": nid,
        "data": {
            "label": nid,
            "nodeType": "transform",
            "config": {"code": code},
        },
    }


def make_output_node(nid: str, fields: list[str] | None = None) -> dict:
    """Build a minimal output node dict."""
    return {
        "id": nid,
        "data": {
            "label": nid,
            "nodeType": "output",
            "config": {"fields": fields or []},
        },
    }


def make_edge(src: str, tgt: str) -> dict:
    """Build a minimal edge dict."""
    return {"id": f"e_{src}_{tgt}", "source": src, "target": tgt}
