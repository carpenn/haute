"""Submodel parsing and merging for the pipeline parser.

Handles:
- Extracting ``pipeline.submodel("path")`` calls from AST
- Parsing individual submodel .py files
- Merging submodel graphs into the parent pipeline graph
  (either flattened for execution or hierarchical for the GUI)
"""

from __future__ import annotations

import ast
from typing import Any

from haute._logging import get_logger
from haute._parser_helpers import (
    _build_edges,
    _build_node_config,
    _build_rf_nodes,
    _extract_connect_calls,
    _extract_function_bodies,
    _extract_submodel_meta,
    _get_decorator_kwargs,
    _get_docstring,
    _infer_node_type,
    _is_submodel_node_decorator,
)
from haute.graph_utils import GraphEdge, GraphNode, NodeData, PipelineGraph

logger = get_logger(component="parser.submodels")


def extract_submodel_calls(tree: ast.Module) -> list[str]:
    """Find pipeline.submodel("path") calls and return the file paths."""
    paths: list[str] = []
    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.Expr):
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue
        func = call.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "submodel"
            and isinstance(func.value, ast.Name)
        ):
            if call.args and isinstance(call.args[0], ast.Constant):
                paths.append(call.args[0].value)
    return paths


def parse_submodel_source(source: str, source_file: str = "") -> PipelineGraph:
    """Parse submodel source code and return a PipelineGraph."""

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return PipelineGraph(
            pipeline_name="unnamed",
            pipeline_description="",
            source_file=source_file,
            warning="Submodel file has syntax errors",
        )

    submodel_name, submodel_desc = _extract_submodel_meta(tree)

    func_bodies = _extract_function_bodies(source)
    raw_nodes: list[dict] = []

    for stmt in ast.iter_child_nodes(tree):
        if not isinstance(stmt, ast.FunctionDef):
            continue

        matched_decorator = None
        for dec in stmt.decorator_list:
            if _is_submodel_node_decorator(dec):
                matched_decorator = dec
                break

        if matched_decorator is None:
            continue

        func_name = stmt.name
        decorator_kwargs = _get_decorator_kwargs(matched_decorator)
        param_names = [arg.arg for arg in stmt.args.args]
        n_params = len(param_names)
        node_type = _infer_node_type(decorator_kwargs, n_params)
        description = _get_docstring(stmt)

        body = func_bodies.get(func_name, "")
        config = _build_node_config(node_type, decorator_kwargs, body, param_names)

        raw_nodes.append(
            {
                "func_name": func_name,
                "node_type": node_type,
                "description": description,
                "config": config,
                "param_names": param_names,
            }
        )

    edges = _build_edges(raw_nodes, _extract_connect_calls(tree, receiver="submodel"))
    rf_nodes = _build_rf_nodes(raw_nodes)

    return PipelineGraph(
        nodes=rf_nodes,
        edges=edges,
        pipeline_name=submodel_name,
        pipeline_description=submodel_desc,
        source_file=source_file,
    )


def merge_submodels(
    parent_graph: PipelineGraph,
    submodel_graphs: dict[str, PipelineGraph],
    submodel_files: dict[str, str],
    parent_edges: list[tuple[str, str]],
    *,
    flatten: bool = False,
) -> PipelineGraph:
    """Merge parsed submodels into the parent graph.

    When *flatten* is True, child nodes are inlined directly into the
    parent graph (for execution).  When False, a single ``submodel``
    node replaces the group (for the GUI).
    """
    if not submodel_graphs:
        return parent_graph

    parent_nodes: list[GraphNode] = list(parent_graph.nodes)
    parent_edge_list: list[GraphEdge] = list(parent_graph.edges)

    # Collect all child node IDs across all submodels
    all_child_ids: set[str] = set()
    for sm_graph in submodel_graphs.values():
        all_child_ids.update(n.id for n in sm_graph.nodes)

    # _build_edges drops edges where one endpoint is a submodel child node
    # (because it only knows about main-file nodes).  Reconstruct those
    # cross-boundary edges from the raw parent_edges tuples.
    existing_pairs = {(e.source, e.target) for e in parent_edge_list}
    for src, tgt in parent_edges:
        if (src, tgt) in existing_pairs:
            continue
        if src in all_child_ids or tgt in all_child_ids:
            parent_edge_list.append(GraphEdge(id=f"e_{src}_{tgt}", source=src, target=tgt))
            existing_pairs.add((src, tgt))

    if flatten:
        # Inline all child nodes + edges into the parent graph
        for _sm_name, sm_graph in submodel_graphs.items():
            parent_nodes.extend(sm_graph.nodes)
            parent_edge_list.extend(sm_graph.edges)

        return parent_graph.model_copy(update={"nodes": parent_nodes, "edges": parent_edge_list})

    # Hierarchical mode: create submodel placeholder nodes
    submodels_meta: dict[str, dict] = {}

    for sm_name, sm_graph in submodel_graphs.items():
        child_node_ids = [n.id for n in sm_graph.nodes]
        child_node_names = set(child_node_ids)

        # Determine input and output ports from cross-boundary edges
        input_ports: list[str] = []
        output_ports: list[str] = []

        for src, tgt in parent_edges:
            if tgt in child_node_names and src not in child_node_names:
                if tgt not in input_ports:
                    input_ports.append(tgt)
            if src in child_node_names and tgt not in child_node_names:
                if src not in output_ports:
                    output_ports.append(src)

        sm_node_id = f"submodel__{sm_name}"
        sm_file = submodel_files.get(sm_name, "")

        # Build the submodel placeholder node
        sm_node = GraphNode(
            id=sm_node_id,
            type="submodel",
            position={"x": 0, "y": 0},
            data=NodeData(
                label=sm_name,
                description=sm_graph.pipeline_description or "",
                nodeType="submodel",
                config={
                    "file": sm_file,
                    "childNodeIds": child_node_ids,
                    "inputPorts": input_ports,
                    "outputPorts": output_ports,
                },
            ),
        )
        parent_nodes.append(sm_node)

        # Rewire edges: replace references to internal child nodes
        # with references to the submodel node (using handles)
        new_edges: list[GraphEdge] = []
        for edge in parent_edge_list:
            src = edge.source
            tgt = edge.target
            if src in child_node_names and tgt not in child_node_names:
                # Internal → external: source becomes submodel node
                new_edges.append(GraphEdge(
                    id=f"e_{sm_node_id}_{tgt}__{src}",
                    source=sm_node_id,
                    sourceHandle=f"out__{src}",
                    target=tgt,
                ))
            elif tgt in child_node_names and src not in child_node_names:
                # External → internal: target becomes submodel node
                new_edges.append(GraphEdge(
                    id=f"e_{src}_{sm_node_id}__{tgt}",
                    source=src,
                    target=sm_node_id,
                    targetHandle=f"in__{tgt}",
                ))
            elif src in child_node_names and tgt in child_node_names:
                # Fully internal: skip (lives inside submodel)
                continue
            else:
                new_edges.append(edge)
        parent_edge_list = new_edges

        submodels_meta[sm_name] = {
            "file": sm_file,
            "childNodeIds": child_node_ids,
            "inputPorts": input_ports,
            "outputPorts": output_ports,
            "graph": sm_graph.model_dump(),
        }

    update: dict[str, Any] = {"nodes": parent_nodes, "edges": parent_edge_list}
    if submodels_meta:
        update["submodels"] = submodels_meta
    return parent_graph.model_copy(update=update)
