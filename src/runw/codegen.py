"""Code generator: graph JSON → valid pipeline .py file."""

from __future__ import annotations

from typing import Any

# Template fragments for each node type
_SOURCE_FLAT_FILE = '''\
@pipeline.node(path="{path}")
def {func_name}() -> pl.DataFrame:
    """{description}"""
    return pl.read_parquet("{path}")
'''

_SOURCE_CSV = '''\
@pipeline.node(path="{path}")
def {func_name}() -> pl.DataFrame:
    """{description}"""
    return pl.read_csv("{path}")
'''

_SOURCE_DATABRICKS = '''\
@pipeline.node(table="{table}")
def {func_name}() -> pl.DataFrame:
    """{description}"""
    import databricks.sdk
    # TODO: implement Databricks table read
    raise NotImplementedError("Databricks source not yet implemented")
'''

_TRANSFORM_EMPTY = '''\
@pipeline.node
def {func_name}(df: pl.DataFrame) -> pl.DataFrame:
    """{description}"""
    return df
'''

_MODEL_SCORE = '''\
@pipeline.node(model_uri="{model_uri}")
def {func_name}(df: pl.DataFrame) -> pl.DataFrame:
    """{description}"""
    # TODO: implement model scoring
    return df
'''

_RATING_STEP = '''\
@pipeline.node(table="{table}", key="{key}")
def {func_name}(df: pl.DataFrame) -> pl.DataFrame:
    """{description}"""
    # TODO: implement rating step lookup
    return df
'''

_OUTPUT = '''\
@pipeline.node
def {func_name}(df: pl.DataFrame) -> pl.DataFrame:
    """{description}"""
    return df
'''


def _sanitize_func_name(label: str) -> str:
    """Convert a human label to a valid Python function name (preserves casing)."""
    name = label.strip()
    name = name.replace(" ", "_").replace("-", "_")
    name = "".join(c for c in name if c.isalnum() or c == "_")
    if name and name[0].isdigit():
        name = f"node_{name}"
    return name or "unnamed_node"


def _wrap_user_code(code: str, source_names: list[str]) -> str:
    """Wrap user code into indented function body lines.

    Rules:
    - If code starts with '.', it's a chain → wrap as `df = (<first_input>\\n<code>\\n)`
    - Otherwise it's a full expression → wrap as `df = <code>`
    """
    code = code.strip()
    if not code:
        first = source_names[0] if source_names else "df"
        return f"    return {first}"

    if code.startswith("."):
        # Chain syntax: .filter(...).select(...)
        first = source_names[0] if source_names else "df"
        chain_indented = "\n".join(f"        {line}" for line in code.splitlines())
        return (
            f"    df = (\n"
            f"        {first}\n"
            f"{chain_indented}\n"
            f"    )\n"
            f"    return df"
        )
    else:
        # Full expression: transform_2.join(transform_3, ...)
        indented = "\n".join(f"    {line}" for line in code.splitlines())
        return (
            f"    df = (\n"
            f"{indented}\n"
            f"    )\n"
            f"    return df"
        )


def _node_to_code(node: dict, source_names: list[str] | None = None) -> str:
    """Generate code for a single node.

    source_names: sanitized function names of upstream nodes (used as param names).
    """
    data = node.get("data", {})
    node_type = data.get("nodeType", "transform")
    config = data.get("config", {})
    label = data.get("label", "Unnamed")
    description = data.get("description", "") or f"{label} node"
    func_name = _sanitize_func_name(label)

    if source_names is None:
        source_names = []

    if node_type == "dataSource":
        path = config.get("path", "")
        source_type = config.get("sourceType", "flat_file")
        if source_type == "databricks":
            table = config.get("table", "catalog.schema.table")
            return _SOURCE_DATABRICKS.format(
                func_name=func_name, description=description, table=table
            )
        elif path.endswith(".csv"):
            return _SOURCE_CSV.format(
                func_name=func_name, description=description, path=path
            )
        else:
            return _SOURCE_FLAT_FILE.format(
                func_name=func_name, description=description, path=path
            )

    elif node_type == "modelScore":
        model_uri = config.get("model_uri", "models:/model/Production")
        return _MODEL_SCORE.format(
            func_name=func_name, description=description, model_uri=model_uri
        )

    elif node_type == "ratingStep":
        table = config.get("table", "")
        key = config.get("key", "")
        return _RATING_STEP.format(
            func_name=func_name, description=description, table=table, key=key
        )

    # transform / output — use source node names as params
    code = config.get("code", "").strip()
    params = ", ".join(f"{s}: pl.DataFrame" for s in source_names) if source_names else "df: pl.DataFrame"
    body = _wrap_user_code(code, source_names)

    return (
        f"@pipeline.node\n"
        f"def {func_name}({params}) -> pl.DataFrame:\n"
        f'    """{description}"""\n'
        f"{body}\n"
    )


def _topo_sort(nodes: list[dict], edges: list[dict]) -> list[dict]:
    """Sort nodes in topological order based on edges."""
    node_map = {n["id"]: n for n in nodes}
    in_edges: dict[str, list[str]] = {n["id"]: [] for n in nodes}
    for e in edges:
        if e["target"] in in_edges:
            in_edges[e["target"]].append(e["source"])

    visited: set[str] = set()
    result: list[str] = []

    def visit(nid: str) -> None:
        if nid in visited:
            return
        visited.add(nid)
        for dep in in_edges.get(nid, []):
            visit(dep)
        result.append(nid)

    for nid in in_edges:
        visit(nid)

    return [node_map[nid] for nid in result if nid in node_map]


def graph_to_code(
    graph: dict,
    pipeline_name: str = "my_pipeline",
    description: str = "",
) -> str:
    """Convert a React Flow graph to a valid runw pipeline .py file."""
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    sorted_nodes = _topo_sort(nodes, edges)

    # Build a map from node id → function name
    id_to_func: dict[str, str] = {}
    for node in sorted_nodes:
        data = node.get("data", {})
        label = data.get("label", "Unnamed")
        id_to_func[node["id"]] = _sanitize_func_name(label)

    lines = [
        f'"""Pipeline: {pipeline_name}"""',
        "",
        "import polars as pl",
        "import runw",
        "",
        f'pipeline = runw.Pipeline("{pipeline_name}", description="{description}")',
        "",
        "",
    ]

    # Build source names per node from edges (ordered list of upstream func names)
    node_sources: dict[str, list[str]] = {}
    for edge in edges:
        tgt = edge["target"]
        src_name = id_to_func.get(edge["source"], edge["source"])
        node_sources.setdefault(tgt, []).append(src_name)

    for node in sorted_nodes:
        source_names = node_sources.get(node["id"], [])
        lines.append(_node_to_code(node, source_names=source_names))
        lines.append("")

    # Emit edges as pipeline.connect() calls
    if edges:
        lines.append("")
        lines.append("# Wire nodes together — edges define data flow")
        for edge in edges:
            src_func = id_to_func.get(edge["source"], edge["source"])
            tgt_func = id_to_func.get(edge["target"], edge["target"])
            lines.append(f'pipeline.connect("{src_func}", "{tgt_func}")')
        lines.append("")

    return "\n".join(lines)
