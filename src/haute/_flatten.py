"""Graph flattening — dissolve submodel nodes into a flat graph."""

from __future__ import annotations

from haute._logging import get_logger
from haute._types import GraphEdge, GraphNode, PipelineGraph

logger = get_logger(component="flatten")


def flatten_graph(graph: PipelineGraph) -> PipelineGraph:
    """Dissolve all submodel nodes into a flat graph for execution.

    Replaces each ``submodel`` node with its child nodes (stored in the
    ``submodels`` metadata) and rewires boundary edges so they point to
    the actual internal nodes.

    If the graph has no submodels, it is returned unchanged.
    """
    submodels = graph.submodels
    if not submodels:
        return graph

    nodes: list[GraphNode] = list(graph.nodes)
    edges: list[GraphEdge] = list(graph.edges)

    # Remove submodel placeholder nodes
    submodel_node_ids = {f"submodel__{name}" for name in submodels}
    nodes = [n for n in nodes if n.id not in submodel_node_ids]

    # Inline child nodes and internal edges from each submodel
    for _sm_name, sm_meta in submodels.items():
        sm_graph = sm_meta.get("graph", {})
        for nd in sm_graph.get("nodes", []):
            nodes.append(GraphNode.model_validate(nd) if isinstance(nd, dict) else nd)
        for ed in sm_graph.get("edges", []):
            edges.append(GraphEdge.model_validate(ed) if isinstance(ed, dict) else ed)

    # Rewire boundary edges: submodel handles → actual child nodes
    rewired: list[GraphEdge] = []
    for edge in edges:
        src = edge.source
        tgt = edge.target
        eid = edge.id
        sh = edge.sourceHandle or ""
        th = edge.targetHandle or ""
        new_sh = edge.sourceHandle
        new_th = edge.targetHandle

        if src in submodel_node_ids and sh:
            # e.g. sourceHandle="out__frequency_model" → source="frequency_model"
            src = sh.removeprefix("out__")
            eid = f"e_{src}_{tgt}"
            new_sh = None

        if tgt in submodel_node_ids and th:
            # e.g. targetHandle="in__frequency_model" → target="frequency_model"
            tgt = th.removeprefix("in__")
            eid = f"e_{src}_{tgt}"
            new_th = None

        # Skip edges that still reference a submodel node (shouldn't happen)
        if src in submodel_node_ids or tgt in submodel_node_ids:
            continue

        rewired.append(GraphEdge(
            id=eid, source=src, target=tgt,
            sourceHandle=new_sh, targetHandle=new_th,
        ))

    # Deduplicate edges by (source, target)
    seen: set[tuple[str, str]] = set()
    deduped: list[GraphEdge] = []
    for e in rewired:
        key = (e.source, e.target)
        if key not in seen:
            seen.add(key)
            deduped.append(e)

    logger.debug(
        "graph_flattened",
        submodel_count=len(submodels),
        node_count=len(nodes),
        edge_count=len(deduped),
    )
    return graph.model_copy(update={"nodes": nodes, "edges": deduped, "submodels": None})
