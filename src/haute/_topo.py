"""Topological sorting and graph traversal algorithms."""

from __future__ import annotations

import heapq
from collections import deque

from haute._types import GraphEdge


def topo_sort_ids(node_ids: list[str], edges: list[GraphEdge]) -> list[str]:
    """Topological sort of node IDs based on edges (Kahn's algorithm)."""
    in_degree: dict[str, int] = {nid: 0 for nid in node_ids}
    children: dict[str, list[str]] = {nid: [] for nid in node_ids}

    for e in edges:
        src, tgt = e.source, e.target
        if tgt in in_degree:
            in_degree[tgt] += 1
        if src in children:
            children[src].append(tgt)

    heap = sorted(nid for nid, deg in in_degree.items() if deg == 0)
    heapq.heapify(heap)
    result: list[str] = []

    while heap:
        nid = heapq.heappop(heap)
        result.append(nid)
        for child in children.get(nid, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                heapq.heappush(heap, child)

    return result


def ancestors(target_id: str, edges: list[GraphEdge], all_ids: set[str]) -> set[str]:
    """Get all ancestor node IDs of target (inclusive)."""
    parents: dict[str, list[str]] = {nid: [] for nid in all_ids}
    for e in edges:
        if e.target in parents:
            parents[e.target].append(e.source)

    visited: set[str] = set()
    queue = deque([target_id])
    while queue:
        nid = queue.popleft()
        if nid in visited:
            continue
        visited.add(nid)
        queue.extend(parents.get(nid, []))
    return visited
