"""Parser: .py pipeline file → React Flow graph JSON.

Uses Python's ast module to extract @pipeline.node decorated functions
and pipeline.connect() calls, producing the same graph JSON format
that the frontend expects.

libcst is reserved for surgical write-back (codegen edits) where
preserving formatting matters.
"""

from __future__ import annotations

import ast
from pathlib import Path

from haute._logging import get_logger
from haute._parser_helpers import (
    _build_edges,
    _build_node_config,
    _build_rf_nodes,
    _extract_connect_calls,
    _extract_function_bodies,
    _extract_pipeline_meta,
    _extract_preamble,
    _get_decorator_kwargs,
    _get_docstring,
    _infer_node_type,
    _is_pipeline_node_decorator,
)
from haute._parser_regex import fallback_parse as _fallback_parse
from haute._parser_submodels import extract_submodel_calls as _extract_submodel_calls
from haute._parser_submodels import merge_submodels as _merge_submodels
from haute._parser_submodels import parse_submodel_source as _parse_submodel_source
from haute.graph_utils import PipelineGraph

logger = get_logger(component="parser")

__all__ = [
    "parse_pipeline_file",
    "parse_pipeline_source",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_pipeline_file(filepath: str | Path, *, flatten: bool = False) -> PipelineGraph:
    """Parse a pipeline .py file and return a PipelineGraph.

    Args:
        flatten: If *True*, dissolve submodel groupings into a flat graph
            (for executor / trace / deploy).  If *False* (default), keep
            submodel metadata so the GUI can render collapsed submodel nodes.

    On syntax errors the file is still parsed via regex fallback so
    that valid nodes are returned alongside broken ones.
    """
    filepath = Path(filepath)
    source = filepath.read_text()
    return parse_pipeline_source(
        source,
        source_file=str(filepath),
        flatten=flatten,
        _base_dir=filepath.parent,
    )


def parse_submodel_file(filepath: str | Path) -> PipelineGraph:
    """Parse a submodel .py file and return a PipelineGraph.

    The submodel name and description are stored in ``pipeline_name``
    and ``pipeline_description`` respectively.
    """
    filepath = Path(filepath)
    source = filepath.read_text()
    return _parse_submodel_source(source, source_file=str(filepath))


def parse_pipeline_source(
    source: str,
    source_file: str = "",
    *,
    flatten: bool = False,
    _base_dir: Path | None = None,
) -> PipelineGraph:
    """Parse pipeline source code and return a PipelineGraph.

    Args:
        flatten: If True, dissolve submodels into flat graph.
        _base_dir: Directory to resolve relative submodel paths against.
    """

    # Syntax check - fall back to regex if the file has errors
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        logger.warning("fallback_parse", file=source_file, line=e.lineno)
        return _fallback_parse(source, source_file, e)

    # Pipeline metadata
    pipeline_name, pipeline_desc = _extract_pipeline_meta(tree)

    # Find @pipeline.node decorated functions
    func_bodies = _extract_function_bodies(source)
    raw_nodes: list[dict] = []

    for stmt in ast.iter_child_nodes(tree):
        if not isinstance(stmt, ast.FunctionDef):
            continue

        # Check for @pipeline.node decorator
        matched_decorator = None
        for dec in stmt.decorator_list:
            if _is_pipeline_node_decorator(dec):
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

        # Build config
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

    if not raw_nodes:
        return PipelineGraph(
            pipeline_name=pipeline_name,
            pipeline_description=pipeline_desc,
            source_file=source_file,
        )

    # Build edges + nodes using shared helpers
    explicit_connects = _extract_connect_calls(tree)
    edges = _build_edges(raw_nodes, explicit_connects)
    rf_nodes = _build_rf_nodes(raw_nodes)
    preamble = _extract_preamble(source)

    graph = PipelineGraph(
        nodes=rf_nodes,
        edges=edges,
        pipeline_name=pipeline_name,
        pipeline_description=pipeline_desc,
        preamble=preamble,
        source_file=source_file,
    )

    # --- Submodel handling ---------------------------------------------------
    submodel_paths = _extract_submodel_calls(tree)
    if submodel_paths and _base_dir is not None:
        submodel_graphs: dict[str, PipelineGraph] = {}
        submodel_files: dict[str, str] = {}

        for rel_path in submodel_paths:
            sm_filepath = (_base_dir / rel_path).resolve()
            if not sm_filepath.is_file():
                continue
            sm_graph = parse_submodel_file(sm_filepath)
            sm_name = sm_graph.pipeline_name or sm_filepath.stem
            submodel_graphs[sm_name] = sm_graph
            submodel_files[sm_name] = rel_path

        if submodel_graphs:
            graph = _merge_submodels(
                graph,
                submodel_graphs,
                submodel_files,
                explicit_connects,
                flatten=flatten,
            )

    logger.info(
        "pipeline_parsed",
        file=source_file,
        node_count=len(graph.nodes),
        edge_count=len(graph.edges),
        pipeline_name=graph.pipeline_name,
    )
    return graph
