"""Graph fingerprinting for cache invalidation."""

from __future__ import annotations

import hashlib
import json as _json

from haute._logging import get_logger
from haute._types import PipelineGraph

logger = get_logger(component="cache")

_FINGERPRINT_ATTR = "_haute_base_fingerprint"


def _graph_base_fingerprint(graph: PipelineGraph) -> str:
    """Compute (and cache) the base fingerprint of a graph's structure.

    The expensive part — JSON-serializing every node config and sorting
    nodes/edges — is done once per ``PipelineGraph`` instance.  Subsequent
    calls return the cached value in O(1).
    """
    cached = getattr(graph, _FINGERPRINT_ATTR, None)
    if cached is not None:
        return cached  # type: ignore[return-value]

    parts: list[str] = []
    for n in sorted(graph.nodes, key=lambda n: n.id):
        parts.append(
            f"{n.id}|{n.data.nodeType}|{_json.dumps(n.data.config, sort_keys=True, default=str)}",
        )
    for e in sorted(graph.edges, key=lambda e: (e.source, e.target)):
        parts.append(f"{e.source}->{e.target}")
    fp = hashlib.md5("\n".join(parts).encode()).hexdigest()

    # Cache on the instance — bypasses Pydantic validation via object.__setattr__
    try:
        object.__setattr__(graph, _FINGERPRINT_ATTR, fp)
    except (TypeError, AttributeError):
        pass  # frozen/slots model — just don't cache

    return fp


def graph_fingerprint(graph: PipelineGraph, *extra_keys: str) -> str:
    """Deterministic hash of graph structure for cache invalidation.

    *extra_keys* are prepended (e.g. target_node_id, row_limit) so the
    same graph with different execution parameters gets a different hash.
    Used by both the trace cache (trace.py) and preview cache (executor.py).

    The graph's base fingerprint (node configs + edge topology) is computed
    once per ``PipelineGraph`` instance and cached; only the extra-key
    combination adds overhead on subsequent calls.
    """
    base = _graph_base_fingerprint(graph)
    if not extra_keys:
        logger.debug("graph_fingerprint_computed", fingerprint=base[:8], extra_keys=())
        return base
    combined = "\n".join(extra_keys) + "\n" + base
    fp = hashlib.md5(combined.encode()).hexdigest()
    logger.debug("graph_fingerprint_computed", fingerprint=fp[:8], extra_keys=extra_keys)
    return fp
