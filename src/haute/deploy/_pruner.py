"""Graph pruning for deployment - keep only ancestors of the output node."""

from __future__ import annotations

from haute.graph_utils import (
    GraphEdge,
    GraphNode,
    PipelineGraph,
    _sanitize_func_name,
    ancestors,
)


def _live_only_edges(
    nodes: list[GraphNode],
    edges: list[GraphEdge],
) -> list[GraphEdge]:
    """Filter edges so liveSwitch nodes only keep their first (live) input.

    For deployment we only want the live branch.  The first input edge
    (by source order matching the function's first parameter) is kept;
    all other input edges to the liveSwitch are dropped.
    """
    switch_ids: set[str] = set()
    for n in nodes:
        if n.data.nodeType == "liveSwitch":
            switch_ids.add(n.id)

    if not switch_ids:
        return edges

    # For each switch, identify the first input param name from config
    switch_live_source: dict[str, str | None] = {}
    node_map = {n.id: n for n in nodes}
    for sid in switch_ids:
        inputs = node_map[sid].data.config.get("inputs", [])
        first_input_name = inputs[0] if inputs else None
        if first_input_name:
            for e in edges:
                if e.target == sid:
                    src_label = node_map[e.source].data.label
                    if _sanitize_func_name(src_label) == first_input_name:
                        switch_live_source[sid] = e.source
                        break

    filtered: list[GraphEdge] = []
    for e in edges:
        if e.target in switch_ids:
            if switch_live_source.get(e.target) == e.source:
                filtered.append(e)
        else:
            filtered.append(e)

    return filtered


def prune_for_deploy(
    graph: PipelineGraph,
    output_node_id: str,
) -> tuple[PipelineGraph, list[str], list[str]]:
    """Prune a graph to only the ancestors of the output node.

    For liveSwitch nodes, only the live (first) input branch is kept.

    Args:
        graph: Full React Flow graph with "nodes" and "edges".
        output_node_id: The node ID whose ancestors form the scoring path.

    Returns:
        (pruned_graph, kept_node_ids, removed_node_ids)

    Raises:
        ValueError: If output_node_id is not in the graph.
    """
    nodes = graph.nodes
    edges = graph.edges

    all_ids = {n.id for n in nodes}
    if output_node_id not in all_ids:
        raise ValueError(
            f"Output node '{output_node_id}' not found in graph. Available nodes: {sorted(all_ids)}"
        )

    deploy_edges = _live_only_edges(nodes, edges)
    needed = ancestors(output_node_id, deploy_edges, all_ids)

    kept_nodes = [n for n in nodes if n.id in needed]
    kept_edges = [e for e in edges if e.source in needed and e.target in needed]
    removed_ids = sorted(all_ids - needed)

    pruned_graph = graph.model_copy(update={"nodes": kept_nodes, "edges": kept_edges})

    return pruned_graph, sorted(needed), removed_ids


def find_output_node(graph: PipelineGraph) -> str:
    """Find the single output node in a graph.

    Looks for nodes with ``nodeType="output"`` or ``config.output=True``.

    Raises:
        ValueError: If zero or multiple output nodes are found.
    """
    candidates: list[str] = []
    for n in graph.nodes:
        if n.data.nodeType == "output":
            candidates.append(n.id)
        elif n.data.config.get("output"):
            candidates.append(n.id)

    if len(candidates) == 0:
        raise ValueError("No output node found. Mark a node with @pipeline.node(output=True).")
    if len(candidates) > 1:
        raise ValueError(
            f"Multiple output nodes found: {candidates}. "
            "Only one node should be marked output=True."
        )
    return candidates[0]


def find_deploy_input_nodes(graph: PipelineGraph) -> list[str]:
    """Find apiInput nodes in a graph.

    Returns:
        List of node IDs (may be empty if none are marked).
    """
    return [n.id for n in graph.nodes if n.data.nodeType == "apiInput"]


def find_source_nodes(graph: PipelineGraph) -> list[str]:
    """Find all source nodes in a graph (dataSource and apiInput)."""
    return [n.id for n in graph.nodes if n.data.nodeType in ("dataSource", "apiInput")]
