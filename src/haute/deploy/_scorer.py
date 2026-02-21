"""Runtime scoring engine for deployed pipelines.

Uses the same ``_execute_lazy`` / ``_build_node_fn`` infrastructure as
the development executor, with ``NodeBuildHooks`` to override specific
node types for live scoring:

- Injects live input DataFrames at apiInput source nodes
- Remaps artifact paths for externalFile and static dataSource nodes
- Returns a single collected DataFrame from the output node
"""

from __future__ import annotations

from pathlib import PurePosixPath

import polars as pl

from haute._execute_lazy import _execute_lazy
from haute._io import load_external_object, read_source
from haute._node_builder import NodeBuildHooks, NodeFnResult, node_fn_name, wrap_builder
from haute._types import (
    GraphNode,
    NodeType,
    PipelineGraph,
    _Frame,
)
from haute.executor import _build_node_fn, _exec_user_code


def score_graph(
    graph: PipelineGraph,
    input_df: pl.DataFrame,
    input_node_ids: list[str],
    output_node_id: str,
    artifact_paths: dict[str, str] | None = None,
    output_fields: list[str] | None = None,
) -> pl.DataFrame:
    """Execute a pruned pipeline graph with injected input data.

    Instead of loading from files, input source nodes receive the provided
    DataFrame.  Artifact paths are remapped to the MLflow artifact directory
    when ``artifact_paths`` is provided.

    Args:
        graph: Pruned React Flow graph JSON.
        input_df: The live input data (1 or N rows).
        input_node_ids: Source node IDs that receive the live input.
        output_node_id: The node whose output is the API response.
        artifact_paths: Optional remapped artifact paths
            (``artifact_name → local_path``).
        output_fields: Optional list of columns to select from output.

    Returns:
        Output DataFrame (1 or N rows).
    """
    input_set = set(input_node_ids)
    input_lf = input_df.lazy()
    remap = artifact_paths or {}

    def _intercept(node: GraphNode, source_names: list[str]) -> NodeFnResult | None:
        nid = node.id
        node_type = node.data.nodeType
        config = node.data.config
        func_name = node_fn_name(node)

        # Intercept: apiInput source → inject live DataFrame
        if node_type == NodeType.API_INPUT and nid in input_set:

            def inject_input() -> _Frame:
                return input_lf

            return func_name, inject_input, True

        # Intercept: externalFile with remapped artifact path
        if node_type == NodeType.EXTERNAL_FILE and remap:
            artifact_key = f"{nid}__{PurePosixPath(config.get('path', '')).name}"
            if artifact_key in remap:
                remapped_path = remap[artifact_key]
                code = config.get("code", "").strip()
                file_type = config.get("fileType", "pickle")
                model_class = config.get("modelClass", "classifier")
                _src_names = list(source_names)

                if code:

                    def external_fn(
                        *dfs: _Frame,
                        _p: str = remapped_path,
                        _ft: str = file_type,
                        _mc: str = model_class,
                        _code: str = code,
                        _sn: list[str] = _src_names,
                    ) -> _Frame:
                        obj = load_external_object(_p, _ft, _mc)
                        return _exec_user_code(_code, _sn, dfs, extra_ns={"obj": obj})

                    return func_name, external_fn, False
                else:

                    def external_passthrough(*dfs: _Frame) -> _Frame:
                        return dfs[0] if dfs else pl.LazyFrame()

                    return func_name, external_passthrough, False

        # Intercept: static dataSource with remapped artifact path
        if node_type == NodeType.DATA_SOURCE and nid not in input_set and remap:
            raw_path = config.get("path", "")
            artifact_key = f"{nid}__{PurePosixPath(raw_path).name}"
            if artifact_key in remap:
                remapped_path = remap[artifact_key]

                def static_source(_p: str = remapped_path) -> _Frame:
                    return read_source(_p)

                return func_name, static_source, True

        return None  # fall through to base builder

    builder = wrap_builder(
        _build_node_fn,
        NodeBuildHooks(before_build=_intercept),
    )

    lazy_outputs, order, _parents, _names = _execute_lazy(
        graph,
        builder,
        target_node_id=output_node_id,
    )

    output_lf = lazy_outputs.get(output_node_id)
    if output_lf is None:
        raise RuntimeError(
            f"Output node '{output_node_id}' produced no result. Executed nodes: {order}"
        )

    if output_fields:
        output_lf = output_lf.select(output_fields)

    return output_lf.collect()
