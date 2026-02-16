"""Input/output schema inference for deployed pipelines."""

from __future__ import annotations

import polars as pl


def infer_input_schema(graph: dict, input_node_id: str) -> dict[str, str]:
    """Infer the input schema by reading the input source node's data file.

    Reads the first 0 rows to get column names + types without loading data.

    Args:
        graph: Pruned graph with nodes and edges.
        input_node_id: The deploy_input source node.

    Returns:
        Dict of column_name → polars dtype string (e.g. ``{"Area": "String"}``).

    Raises:
        ValueError: If the input node has no path or the file can't be read.
    """
    node = _find_node(graph, input_node_id)
    config = node.get("data", {}).get("config", {})
    path = config.get("path", "")

    if not path:
        raise ValueError(
            f"Input node '{input_node_id}' has no path configured. Cannot infer schema."
        )

    try:
        if path.endswith(".csv"):
            df = pl.read_csv(path, n_rows=0)
        elif path.endswith(".json"):
            df = pl.read_json(path)
            df = df.head(0)
        else:
            df = pl.read_parquet(path, n_rows=0)
    except Exception as exc:
        raise ValueError(
            f"Failed to read schema from '{path}' for input node '{input_node_id}': {exc}"
        ) from exc

    return {col: str(df[col].dtype) for col in df.columns}


def infer_output_schema(
    graph: dict,
    output_node_id: str,
    input_node_ids: list[str],
) -> dict[str, str]:
    """Infer the output schema by dry-running one sample row.

    Executes the pruned graph with a single sample row injected at each
    input node, and reads the output node's columns + types.

    Args:
        graph: Pruned graph.
        output_node_id: The output node to read results from.
        input_node_ids: Source nodes that receive the sample input.

    Returns:
        Dict of column_name → polars dtype string.
    """
    from haute.deploy._scorer import score_graph

    # Build a 1-row sample from the first input node's data
    node = _find_node(graph, input_node_ids[0])
    config = node.get("data", {}).get("config", {})
    path = config.get("path", "")

    if not path:
        raise ValueError(
            f"Input node '{input_node_ids[0]}' has no path - cannot create sample row."
        )

    try:
        if path.endswith(".csv"):
            sample = pl.read_csv(path, n_rows=1)
        elif path.endswith(".json"):
            sample = pl.read_json(path).head(1)
        else:
            sample = pl.read_parquet(path, n_rows=1)
    except Exception as exc:
        raise ValueError(f"Failed to read sample from '{path}': {exc}") from exc

    result = score_graph(
        graph=graph,
        input_df=sample,
        input_node_ids=input_node_ids,
        output_node_id=output_node_id,
    )

    return {col: str(result[col].dtype) for col in result.columns}


def _find_node(graph: dict, node_id: str) -> dict:
    """Find a node by ID in a graph."""
    for n in graph.get("nodes", []):
        if n["id"] == node_id:
            return n
    raise ValueError(f"Node '{node_id}' not found in graph")
