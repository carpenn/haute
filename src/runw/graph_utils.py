"""Shared graph utilities used by both codegen and executor."""

from __future__ import annotations


def _sanitize_func_name(label: str) -> str:
    """Convert a human label to a valid Python function name (preserves casing)."""
    name = label.strip()
    name = name.replace(" ", "_").replace("-", "_")
    name = "".join(c for c in name if c.isalnum() or c == "_")
    if name and name[0].isdigit():
        name = f"node_{name}"
    return name or "unnamed_node"


def topo_sort_ids(node_ids: list[str], edges: list[dict]) -> list[str]:
    """Topological sort of node IDs based on edges (Kahn's algorithm)."""
    in_degree: dict[str, int] = {nid: 0 for nid in node_ids}
    children: dict[str, list[str]] = {nid: [] for nid in node_ids}

    for e in edges:
        src, tgt = e["source"], e["target"]
        if tgt in in_degree:
            in_degree[tgt] += 1
        if src in children:
            children[src].append(tgt)

    queue = sorted([nid for nid, deg in in_degree.items() if deg == 0])
    result: list[str] = []

    while queue:
        nid = queue.pop(0)
        result.append(nid)
        for child in children.get(nid, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)
        queue.sort()

    return result


def ancestors(target_id: str, edges: list[dict], all_ids: set[str]) -> set[str]:
    """Get all ancestor node IDs of target (inclusive)."""
    parents: dict[str, list[str]] = {nid: [] for nid in all_ids}
    for e in edges:
        if e["target"] in parents:
            parents[e["target"]].append(e["source"])

    visited: set[str] = set()

    def walk(nid: str) -> None:
        if nid in visited:
            return
        visited.add(nid)
        for p in parents.get(nid, []):
            walk(p)

    walk(target_id)
    return visited
