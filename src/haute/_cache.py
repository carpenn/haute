"""Graph fingerprinting for cache invalidation."""

from __future__ import annotations

import hashlib
import json as _json

from haute._types import PipelineGraph


def graph_fingerprint(graph: PipelineGraph, *extra_keys: str) -> str:
    """Deterministic hash of graph structure for cache invalidation.

    *extra_keys* are prepended (e.g. target_node_id, row_limit) so the
    same graph with different execution parameters gets a different hash.
    Used by both the trace cache (trace.py) and preview cache (executor.py).
    """
    parts: list[str] = list(extra_keys)
    for n in sorted(graph.nodes, key=lambda n: n.id):
        parts.append(
            f"{n.id}|{n.data.nodeType}|{_json.dumps(n.data.config, sort_keys=True, default=str)}",
        )
    for e in sorted(graph.edges, key=lambda e: (e.source, e.target)):
        parts.append(f"{e.source}->{e.target}")
    return hashlib.md5("\n".join(parts).encode()).hexdigest()
