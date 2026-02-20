"""Graph flattening — dissolve submodel nodes into a flat graph."""

from __future__ import annotations

from haute._types import PipelineGraph


def flatten_graph(graph: PipelineGraph) -> PipelineGraph:
    """Dissolve all submodel nodes into a flat graph for execution.

    Replaces each ``submodel`` node with its child nodes (stored in the
    ``submodels`` metadata) and rewires boundary edges so they point to
    the actual internal nodes.

    If the graph has no submodels, it is returned unchanged.
    """
    submodels = graph.get("submodels")
    if not submodels:
        return graph

    nodes = list(graph.get("nodes", []))
    edges = list(graph.get("edges", []))

    # Remove submodel placeholder nodes
    submodel_node_ids = {f"submodel__{name}" for name in submodels}
    nodes = [n for n in nodes if n["id"] not in submodel_node_ids]

    # Inline child nodes and internal edges from each submodel
    for sm_name, sm_meta in submodels.items():
        sm_graph = sm_meta.get("graph", {})
        nodes.extend(sm_graph.get("nodes", []))
        edges_to_add = sm_graph.get("edges", [])
        edges.extend(edges_to_add)

    # Rewire boundary edges: submodel handles → actual child nodes
    rewired_edges: list[dict] = []
    for edge in edges:
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        source_handle = edge.get("sourceHandle", "")
        target_handle = edge.get("targetHandle", "")

        new_edge = dict(edge)

        if src in submodel_node_ids and source_handle:
            # e.g. sourceHandle="out__frequency_model" → source="frequency_model"
            actual_src = source_handle.removeprefix("out__")
            new_edge["source"] = actual_src
            new_edge["id"] = f"e_{actual_src}_{tgt}"
            new_edge.pop("sourceHandle", None)

        if tgt in submodel_node_ids and target_handle:
            # e.g. targetHandle="in__frequency_model" → target="frequency_model"
            actual_tgt = target_handle.removeprefix("in__")
            new_edge["target"] = actual_tgt
            new_edge["id"] = f"e_{src}_{actual_tgt}"
            new_edge.pop("targetHandle", None)

        # Skip edges that still reference a submodel node (shouldn't happen)
        if new_edge["source"] in submodel_node_ids or new_edge["target"] in submodel_node_ids:
            continue

        rewired_edges.append(new_edge)

    # Deduplicate edges by (source, target)
    seen: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for e in rewired_edges:
        key = (e["source"], e["target"])
        if key not in seen:
            seen.add(key)
            deduped.append(e)

    result = {**graph, "nodes": nodes, "edges": deduped}
    result.pop("submodels", None)
    return result
