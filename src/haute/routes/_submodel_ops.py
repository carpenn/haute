"""Pure graph operations for submodel create/dissolve — no I/O or HTTP."""

from __future__ import annotations

from dataclasses import dataclass, field

from haute._types import PipelineGraph
from haute.graph_utils import (
    GraphEdge,
    GraphNode,
    NodeData,
    NodeType,
    _sanitize_func_name,
)


@dataclass
class SubmodelGraphResult:
    """Result of ``create_submodel_graph`` — everything the caller needs."""

    graph: PipelineGraph
    sm_file: str
    sm_name: str
    child_node_ids: list[str] = field(default_factory=list)


def create_submodel_graph(
    graph: PipelineGraph,
    node_ids: list[str],
    name: str,
) -> SubmodelGraphResult:
    """Split *node_ids* out of *graph* into a submodel named *name*.

    Returns a ``SubmodelGraphResult`` containing the new parent graph
    (with a placeholder submodel node and rewired edges) plus metadata.

    Raises ``ValueError`` if fewer than 2 nodes are selected.
    """
    sm_name = _sanitize_func_name(name)
    sm_file = f"modules/{sm_name}.py"
    selected_ids = set(node_ids)

    if len(selected_ids) < 2:
        raise ValueError("A submodel must contain at least 2 nodes.")

    nodes = graph.nodes
    edges = graph.edges

    # Separate child vs parent nodes
    child_nodes = [n for n in nodes if n.id in selected_ids]
    parent_nodes = [n for n in nodes if n.id not in selected_ids]
    child_node_ids = {n.id for n in child_nodes}

    # Classify edges
    internal_edges = [
        e for e in edges
        if e.source in child_node_ids and e.target in child_node_ids
    ]
    cross_edges = [
        e for e in edges
        if (e.source in child_node_ids) != (e.target in child_node_ids)
    ]
    external_edges = [
        e for e in edges
        if e.source not in child_node_ids
        and e.target not in child_node_ids
    ]

    # Determine input/output ports
    input_ports: list[str] = []
    output_ports: list[str] = []
    for e in cross_edges:
        if e.target in child_node_ids and e.source not in child_node_ids:
            if e.target not in input_ports:
                input_ports.append(e.target)
        if e.source in child_node_ids and e.target not in child_node_ids:
            if e.source not in output_ports:
                output_ports.append(e.source)

    # Build submodel internal graph dict
    sm_graph = {
        "nodes": [n.model_dump() for n in child_nodes],
        "edges": [e.model_dump() for e in internal_edges],
        "submodel_name": sm_name,
        "submodel_description": "",
        "source_file": sm_file,
    }

    # Build submodel placeholder node
    sm_node_id = f"submodel__{sm_name}"
    sm_node = GraphNode(
        id=sm_node_id,
        type=NodeType.SUBMODEL,
        position={"x": 0, "y": 0},
        data=NodeData(
            label=sm_name,
            description="",
            nodeType=NodeType.SUBMODEL,
            config={
                "file": sm_file,
                "childNodeIds": list(child_node_ids),
                "inputPorts": input_ports,
                "outputPorts": output_ports,
            },
        ),
    )

    # Rewire cross-boundary edges
    rewired_cross: list[GraphEdge] = []
    for e in cross_edges:
        if e.target in child_node_ids:
            rewired_cross.append(GraphEdge(
                id=f"e_{e.source}_{sm_node_id}__{e.target}",
                source=e.source,
                target=sm_node_id,
                targetHandle=f"in__{e.target}",
            ))
        elif e.source in child_node_ids:
            rewired_cross.append(GraphEdge(
                id=f"e_{sm_node_id}_{e.target}__{e.source}",
                source=sm_node_id,
                sourceHandle=f"out__{e.source}",
                target=e.target,
            ))

    # Assemble new parent graph
    existing_submodels = dict(graph.submodels or {})
    existing_submodels[sm_name] = {
        "file": sm_file,
        "childNodeIds": list(child_node_ids),
        "inputPorts": input_ports,
        "outputPorts": output_ports,
        "graph": sm_graph,
    }
    new_graph = graph.model_copy(update={
        "nodes": parent_nodes + [sm_node],
        "edges": external_edges + rewired_cross,
        "submodels": existing_submodels,
    })

    return SubmodelGraphResult(
        graph=new_graph,
        sm_file=sm_file,
        sm_name=sm_name,
        child_node_ids=list(child_node_ids),
    )
