"""Graph pruning for deployment — keep only ancestors of the output node."""

from __future__ import annotations

from haute.graph_utils import ancestors


def prune_for_deploy(
    graph: dict,
    output_node_id: str,
) -> tuple[dict, list[str], list[str]]:
    """Prune a graph to only the ancestors of the output node.

    Args:
        graph: Full React Flow graph with "nodes" and "edges".
        output_node_id: The node ID whose ancestors form the scoring path.

    Returns:
        (pruned_graph, kept_node_ids, removed_node_ids)

    Raises:
        ValueError: If output_node_id is not in the graph.
    """
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    all_ids = {n["id"] for n in nodes}
    if output_node_id not in all_ids:
        raise ValueError(
            f"Output node '{output_node_id}' not found in graph. "
            f"Available nodes: {sorted(all_ids)}"
        )

    needed = ancestors(output_node_id, edges, all_ids)

    kept_nodes = [n for n in nodes if n["id"] in needed]
    kept_edges = [e for e in edges if e["source"] in needed and e["target"] in needed]
    removed_ids = sorted(all_ids - needed)

    pruned_graph = {
        **graph,
        "nodes": kept_nodes,
        "edges": kept_edges,
    }

    return pruned_graph, sorted(needed), removed_ids


def find_output_node(graph: dict) -> str:
    """Find the single output node in a graph.

    Looks for nodes with ``nodeType="output"`` or ``config.output=True``.

    Raises:
        ValueError: If zero or multiple output nodes are found.
    """
    candidates: list[str] = []
    for n in graph.get("nodes", []):
        data = n.get("data", {})
        if data.get("nodeType") == "output":
            candidates.append(n["id"])
        elif data.get("config", {}).get("output"):
            candidates.append(n["id"])

    if len(candidates) == 0:
        raise ValueError(
            "No output node found. Mark a node with @pipeline.node(output=True)."
        )
    if len(candidates) > 1:
        raise ValueError(
            f"Multiple output nodes found: {candidates}. "
            "Only one node should be marked output=True."
        )
    return candidates[0]


def find_deploy_input_nodes(graph: dict) -> list[str]:
    """Find source nodes marked deploy_input=True in a graph.

    Returns:
        List of node IDs (may be empty if none are marked).
    """
    inputs: list[str] = []
    for n in graph.get("nodes", []):
        data = n.get("data", {})
        config = data.get("config", {})
        if data.get("nodeType") == "dataSource" and config.get("deploy_input"):
            inputs.append(n["id"])
    return inputs


def find_source_nodes(graph: dict) -> list[str]:
    """Find all dataSource nodes in a graph (regardless of deploy_input flag)."""
    sources: list[str] = []
    for n in graph.get("nodes", []):
        data = n.get("data", {})
        if data.get("nodeType") == "dataSource":
            sources.append(n["id"])
    return sources
