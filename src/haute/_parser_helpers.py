"""Shared helper functions for the pipeline parser.

These are used by ``parser.py``, ``_parser_regex.py``, and
``_parser_submodels.py``.  Extracting them here breaks the circular
import that would otherwise exist between the parent module and its
extracted submodules.

Import direction:  _parser_helpers  ←  parser / _parser_regex / _parser_submodels
"""

from __future__ import annotations

import ast
from typing import Any

from haute.graph_utils import (
    OPTIMISER_APPLY_CONFIG_KEYS,
    OPTIMISER_CONFIG_KEYS,
    SCENARIO_EXPANDER_CONFIG_KEYS,
    GraphEdge,
    GraphNode,
    NodeData,
    NodeType,
)

# ---------------------------------------------------------------------------
# Node type inference
# ---------------------------------------------------------------------------


def _infer_node_type(decorator_kwargs: dict[str, Any], n_params: int) -> NodeType:
    """Infer the GUI node type from decorator config and param count."""
    if "instance_of" in decorator_kwargs:
        # Instance nodes borrow their type from the original at runtime;
        # default to transform here — the executor resolves the real type.
        return NodeType.TRANSFORM
    if "external" in decorator_kwargs:
        return NodeType.EXTERNAL_FILE
    if "sink" in decorator_kwargs:
        return NodeType.DATA_SINK
    if decorator_kwargs.get("api_input"):
        return NodeType.API_INPUT
    if decorator_kwargs.get("live_switch"):
        return NodeType.LIVE_SWITCH
    if decorator_kwargs.get("output"):
        return NodeType.OUTPUT
    if decorator_kwargs.get("scenario_expander"):
        return NodeType.SCENARIO_EXPANDER
    if decorator_kwargs.get("optimiser_apply"):
        return NodeType.OPTIMISER_APPLY
    if decorator_kwargs.get("optimiser"):
        return NodeType.OPTIMISER
    if decorator_kwargs.get("constant"):
        return NodeType.CONSTANT
    if decorator_kwargs.get("modelling"):
        return NodeType.MODELLING
    if decorator_kwargs.get("model_score"):
        return NodeType.MODEL_SCORE
    if "registered_model" in decorator_kwargs or (
        "source_type" in decorator_kwargs and "run_id" in decorator_kwargs
    ):
        return NodeType.MODEL_SCORE
    if "banding" in decorator_kwargs or "factors" in decorator_kwargs:
        return NodeType.BANDING
    if "tables" in decorator_kwargs:
        return NodeType.RATING_STEP
    if "table" in decorator_kwargs and "key" in decorator_kwargs:
        return NodeType.RATING_STEP
    if "path" in decorator_kwargs or n_params == 0:
        return NodeType.DATA_SOURCE
    return NodeType.TRANSFORM


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------


def _eval_ast_literal(node: ast.expr) -> Any:
    """Safely evaluate an AST literal node."""
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError):
        return ast.dump(node)


def _get_decorator_kwargs(decorator: ast.expr) -> dict[str, Any]:
    """Extract keyword arguments from a decorator.

    Handles both @pipeline.node and @pipeline.node(key=val, ...).
    """
    if isinstance(decorator, ast.Call):
        kwargs: dict[str, Any] = {}
        for kw in decorator.keywords:
            if kw.arg is not None:
                kwargs[kw.arg] = _eval_ast_literal(kw.value)
        return kwargs
    return {}


def _is_pipeline_node_decorator(decorator: ast.expr) -> bool:
    """Check if a decorator is @pipeline.node or @pipeline.node(...)."""
    # @pipeline.node
    if isinstance(decorator, ast.Attribute):
        if isinstance(decorator.value, ast.Name) and decorator.attr == "node":
            return True

    # @pipeline.node(...)
    if isinstance(decorator, ast.Call):
        return _is_pipeline_node_decorator(decorator.func)

    return False


def _is_submodel_node_decorator(decorator: ast.expr) -> bool:
    """Check if a decorator is @submodel.node or @submodel.node(...)."""
    if isinstance(decorator, ast.Attribute):
        if isinstance(decorator.value, ast.Name) and decorator.attr == "node":
            return decorator.value.id == "submodel"
    if isinstance(decorator, ast.Call):
        return _is_submodel_node_decorator(decorator.func)
    return False


def _get_docstring(func: ast.FunctionDef) -> str:
    """Extract the docstring from a function def."""
    return ast.get_docstring(func) or ""


def _strip_docstring(lines: list[str]) -> list[str]:
    """Remove the leading docstring from function body lines."""
    cleaned: list[str] = []
    in_docstring = False
    docstring_done = False

    for line in lines:
        stripped = line.strip()

        if not docstring_done:
            if in_docstring:
                if '"""' in stripped or "'''" in stripped:
                    in_docstring = False
                    docstring_done = True
                continue
            if not cleaned and (stripped.startswith('"""') or stripped.startswith("'''")):
                quote = stripped[:3]
                if stripped.count(quote) >= 2 and stripped.endswith(quote):
                    docstring_done = True
                    continue
                else:
                    in_docstring = True
                    continue
            docstring_done = True

        cleaned.append(line)

    return cleaned


def _dedent(code: str) -> str:
    """Remove common leading whitespace."""
    code_lines = code.splitlines()
    if not code_lines:
        return code
    indents = [len(line) - len(line.lstrip()) for line in code_lines if line.strip()]
    if not indents:
        return code
    m = min(indents)
    return "\n".join(line[m:] if len(line) >= m else line for line in code_lines)


def _extract_user_code(body_source: str, param_names: list[str]) -> str:
    """Extract the meaningful user code from a function body.

    Strips the docstring. For codegen-style bodies (df = (...)\nreturn df)
    it unwraps the assignment. For hand-written code (return expr) it
    preserves the expression.
    """
    lines = body_source.strip().splitlines()
    cleaned = _strip_docstring(lines)

    if not cleaned:
        return ""

    code = _dedent("\n".join(cleaned)).strip()

    # Pattern 1: codegen style  "df = (\n...\n)\nreturn df"
    if code.startswith("df = (") or code.startswith("df=("):
        inner = code.split("(", 1)[1]
        # Find the matching close of the assignment (before 'return df')
        # Remove trailing 'return df' first
        for suffix in ("\nreturn df", "\n    return df"):
            if inner.endswith(suffix):
                inner = inner[: -len(suffix)]
                break
        if inner.rstrip().endswith(")"):
            inner = inner.rstrip()[:-1]
        return _dedent(inner).strip()

    # Pattern 2: single "return <expr>" (hand-written)
    # Rejoin as a single return expression
    stripped_lines = []
    in_return = False
    for line in cleaned:
        s = line.strip()
        if s.startswith("return ") and not in_return:
            # Strip 'return ' prefix, keep the rest
            stripped_lines.append(line.replace("return ", "", 1))
            in_return = True
        elif in_return:
            # continuation of the return expression
            stripped_lines.append(line)
        elif not s.startswith("return"):
            stripped_lines.append(line)

    return _dedent("\n".join(stripped_lines)).strip()


def _extract_model_score_user_code(body_source: str) -> str:
    """Extract user post-processing code from a MODEL_SCORE function body.

    The codegen template wraps user code after a ``# -- user code --``
    sentinel comment.  Everything between that sentinel and the trailing
    ``return result`` is genuine user code.  If no sentinel is found the
    body is entirely auto-generated and we return an empty string.
    """
    sentinel = "# -- user code --"
    if sentinel not in body_source:
        return ""

    # Take everything after the sentinel
    _, _, after = body_source.partition(sentinel)
    lines = after.strip().splitlines()
    if not lines:
        return ""

    # Strip trailing "return result" (auto-generated)
    while lines and lines[-1].strip() in ("return result", ""):
        lines.pop()

    if not lines:
        return ""

    return _dedent("\n".join(lines)).strip()


def _extract_external_user_code(body_source: str, param_names: list[str]) -> str:
    """Extract user code from an externalFile function body.

    Strips the docstring, then scans forward to skip the file-loading
    boilerplate (import statements, with-open blocks, obj assignments /
    method calls).  Everything between the boilerplate and a trailing
    ``return df`` is the user code.
    """
    lines = body_source.strip().splitlines()
    cleaned = _strip_docstring(lines)

    if not cleaned:
        return ""

    # Determine the base indentation from the first non-blank line
    base_indent = 0
    for line in cleaned:
        if line.strip():
            base_indent = len(line) - len(line.lstrip())
            break

    # Scan from the start to skip loading boilerplate
    i = 0
    in_with = False
    while i < len(cleaned):
        s = cleaned[i].strip()
        line_indent = len(cleaned[i]) - len(cleaned[i].lstrip()) if s else 0

        if not s:
            i += 1
            continue

        # Inside a with-block: skip indented body lines
        if in_with:
            if line_indent > base_indent:
                i += 1
                continue
            in_with = False
            # fall through to check this line normally

        if s.startswith("import ") or s.startswith("from "):
            i += 1
            continue
        if s.startswith("with open("):
            in_with = True
            i += 1
            continue
        if s.startswith("obj = ") or s.startswith("obj."):
            i += 1
            continue

        break  # first user-code line

    user_lines = cleaned[i:]
    if not user_lines:
        return ""

    code = _dedent("\n".join(user_lines)).strip()

    # Strip trailing "return df"
    if code.endswith("\nreturn df"):
        code = code[: -len("\nreturn df")].rstrip()
    elif code == "return df":
        return ""

    return code


# ---------------------------------------------------------------------------
# Source extraction
# ---------------------------------------------------------------------------


def _extract_function_bodies(
    source: str,
    tree: ast.Module | None = None,
) -> dict[str, str]:
    """Extract raw source of each function body, keyed by function name.

    Args:
        source: The raw source code (needed for line extraction).
        tree: Pre-parsed AST tree.  If *None*, the source is parsed
            internally.  Passing a tree avoids a redundant ``ast.parse()``.
    """
    if tree is None:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return {}

    source_lines = source.splitlines()
    bodies: dict[str, str] = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            if node.body:
                start = node.body[0].lineno - 1
                end = node.body[-1].end_lineno or (start + 1)
                bodies[node.name] = "\n".join(source_lines[start:end])

    return bodies


def _extract_connect_calls(
    tree: ast.Module,
    receiver: str = "pipeline",
) -> list[tuple[str, str]]:
    """Find all <receiver>.connect("src", "tgt") calls at module level."""
    connects: list[tuple[str, str]] = []

    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.Expr):
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue

        # Check for <receiver>.connect(...)
        func = call.func
        if not isinstance(func, ast.Attribute) or func.attr != "connect":
            continue
        if isinstance(func.value, ast.Name) and func.value.id != receiver:
            continue

        args = call.args
        if len(args) >= 2:
            src = _eval_ast_literal(args[0])
            tgt = _eval_ast_literal(args[1])
            if isinstance(src, str) and isinstance(tgt, str):
                connects.append((src, tgt))

    return connects


# ---------------------------------------------------------------------------
# Graph building helpers
# ---------------------------------------------------------------------------


def _build_node_config(
    node_type: str,
    decorator_kwargs: dict[str, Any],
    body: str,
    param_names: list[str],
) -> dict[str, Any]:
    """Build the config dict for a node given its type and decorator kwargs."""
    config: dict[str, Any] = {}
    if node_type == NodeType.API_INPUT:
        config["path"] = decorator_kwargs.get("path", "")
        if decorator_kwargs.get("row_id_column"):
            config["row_id_column"] = decorator_kwargs["row_id_column"]
    elif node_type == NodeType.DATA_SOURCE:
        config["path"] = decorator_kwargs.get("path", "")
        if "table" in decorator_kwargs:
            config["sourceType"] = "databricks"
            config["table"] = decorator_kwargs["table"]
            if "http_path" in decorator_kwargs:
                config["http_path"] = decorator_kwargs["http_path"]
            if "query" in decorator_kwargs:
                config["query"] = decorator_kwargs["query"]
        else:
            config["sourceType"] = "flat_file"
    elif node_type == NodeType.LIVE_SWITCH:
        config["mode"] = decorator_kwargs.get("mode", "live")
        config["inputs"] = param_names
    elif node_type == NodeType.MODEL_SCORE:
        model_score_keys = (
            "source_type", "run_id", "artifact_path", "run_name",
            "registered_model", "version", "task", "output_column",
            "experiment_name", "experiment_id",
        )
        for key in model_score_keys:
            if key in decorator_kwargs:
                # Map snake_case decorator key to camelCase config key where needed
                config_key = "sourceType" if key == "source_type" else key
                config[config_key] = decorator_kwargs[key]
        # Only extract user post-processing code (after sentinel), not the
        # auto-generated scoring scaffolding that codegen produces.
        config["code"] = _extract_model_score_user_code(body) if body else ""
    elif node_type == NodeType.BANDING:
        if "factors" in decorator_kwargs:
            # Multi-factor format: factors=[{...}, {...}]
            raw_factors = decorator_kwargs["factors"]
            config["factors"] = [
                {
                    "banding": f.get("banding", "continuous"),
                    "column": f.get("column", ""),
                    "outputColumn": f.get("output_column", f.get("outputColumn", "")),
                    "rules": f.get("rules", []),
                    "default": f.get("default"),
                }
                for f in (raw_factors if isinstance(raw_factors, list) else [])
            ]
        else:
            # Single-factor format → wrap into factors array
            config["factors"] = [{
                "banding": decorator_kwargs.get("banding", "continuous"),
                "column": decorator_kwargs.get("column", ""),
                "outputColumn": decorator_kwargs.get("output_column", ""),
                "rules": decorator_kwargs.get("rules", []),
                "default": decorator_kwargs.get("default"),
            }]
    elif node_type == NodeType.RATING_STEP:
        if "tables" in decorator_kwargs:
            raw_tables = decorator_kwargs["tables"]
            config["tables"] = [
                {
                    "name": t.get("name", ""),
                    "factors": t.get("factors", []),
                    "outputColumn": t.get("output_column", t.get("outputColumn", "")),
                    "defaultValue": t.get("default_value", t.get("defaultValue")),
                    "entries": t.get("entries", []),
                }
                for t in (raw_tables if isinstance(raw_tables, list) else [])
            ]
        else:
            config["tables"] = []
        for t in config["tables"]:
            if not isinstance(t.get("entries"), list):
                t["entries"] = []
            if not isinstance(t.get("factors"), list):
                t["factors"] = []
        op = decorator_kwargs.get("operation", decorator_kwargs.get("op"))
        if op:
            config["operation"] = str(op)
        combined = decorator_kwargs.get(
            "combined_column", decorator_kwargs.get("combinedColumn"),
        )
        if combined:
            config["combinedColumn"] = str(combined)
    elif node_type == NodeType.SCENARIO_EXPANDER:
        for key in SCENARIO_EXPANDER_CONFIG_KEYS:
            if key in decorator_kwargs:
                config[key] = decorator_kwargs[key]
    elif node_type == NodeType.OPTIMISER_APPLY:
        for key in OPTIMISER_APPLY_CONFIG_KEYS:
            if key in decorator_kwargs:
                config[key] = decorator_kwargs[key]
    elif node_type == NodeType.OPTIMISER:
        for key in OPTIMISER_CONFIG_KEYS:
            if key in decorator_kwargs:
                config[key] = decorator_kwargs[key]
    elif node_type == NodeType.MODELLING:
        modelling_keys = (
            "name", "target", "weight", "exclude", "algorithm", "task",
            "params", "split", "metrics", "mlflow_experiment", "model_name",
            "output_dir",
        )
        for key in modelling_keys:
            if key in decorator_kwargs:
                config[key] = decorator_kwargs[key]
    elif node_type == NodeType.CONSTANT:
        raw_values = decorator_kwargs.get("values", [])
        config["values"] = [
            {"name": v.get("name", ""), "value": str(v.get("value", ""))}
            for v in (raw_values if isinstance(raw_values, list) else [])
        ]
    elif node_type == NodeType.DATA_SINK:
        config["path"] = decorator_kwargs.get("sink", "")
        config["format"] = decorator_kwargs.get("format", "parquet")
    elif node_type == NodeType.EXTERNAL_FILE:
        config["path"] = decorator_kwargs.get("external", "")
        config["fileType"] = decorator_kwargs.get("file_type", "pickle")
        if config["fileType"] == "catboost":
            config["modelClass"] = decorator_kwargs.get("model_class", "classifier")
        config["code"] = _extract_external_user_code(body, param_names) if body else ""
    elif node_type == NodeType.OUTPUT:
        config["fields"] = decorator_kwargs.get("fields", [])
    else:
        # transform
        config["code"] = _extract_user_code(body, param_names) if body else ""
    # Instance reference (works for any node type)
    if "instance_of" in decorator_kwargs:
        config["instanceOf"] = decorator_kwargs["instance_of"]
    return config


def _build_edges(
    raw_nodes: list[dict],
    explicit_connect_pairs: list[tuple[str, str]],
) -> list[GraphEdge]:
    """Build GraphEdge models from explicit connect() calls and implicit param-name matching."""
    node_names = {n["func_name"] for n in raw_nodes}
    edges: list[GraphEdge] = []
    explicit_edges: set[tuple[str, str]] = set()

    for src, tgt in explicit_connect_pairs:
        if src in node_names and tgt in node_names:
            explicit_edges.add((src, tgt))
            edges.append(GraphEdge(id=f"e_{src}_{tgt}", source=src, target=tgt))

    # Implicit edges from parameter names matching node names
    targets_with_explicit = {tgt for _, tgt in explicit_edges}
    for node_info in raw_nodes:
        if node_info["func_name"] in targets_with_explicit:
            continue
        for param in node_info["param_names"]:
            if param in node_names and param != node_info["func_name"]:
                pair = (param, node_info["func_name"])
                if pair not in explicit_edges:
                    edges.append(GraphEdge(
                        id=f"e_{pair[0]}_{pair[1]}",
                        source=pair[0],
                        target=pair[1],
                    ))

    # Fallback: if still no edges, infer linear chain from definition order
    if not edges and len(raw_nodes) > 1:
        for i in range(1, len(raw_nodes)):
            src = raw_nodes[i - 1]["func_name"]
            tgt = raw_nodes[i]["func_name"]
            edges.append(GraphEdge(id=f"e_{src}_{tgt}", source=src, target=tgt))

    return edges


def _build_rf_nodes(raw_nodes: list[dict], x_spacing: int = 300) -> list[GraphNode]:
    """Convert raw parsed nodes into GraphNode Pydantic models."""
    return [
        GraphNode(
            id=n["func_name"],
            type=n["node_type"],
            position={"x": i * x_spacing, "y": 0},
            data=NodeData(
                label=n["func_name"],
                description=n["description"],
                nodeType=n["node_type"],
                config=n["config"],
            ),
        )
        for i, n in enumerate(raw_nodes)
    ]


# ---------------------------------------------------------------------------
# Meta extraction
# ---------------------------------------------------------------------------


def _extract_meta(
    tree: ast.Module, var_name: str, default_name: str = "main",
) -> tuple[str, str]:
    """Find ``<var_name> = haute.<Class>("name", description="...")`` at module level."""
    name = default_name
    description = ""

    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or target.id != var_name:
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue

        if call.args:
            val = _eval_ast_literal(call.args[0])
            if isinstance(val, str):
                name = val

        for kw in call.keywords:
            if kw.arg == "description":
                val = _eval_ast_literal(kw.value)
                if isinstance(val, str):
                    description = val

        break

    return name, description


def _extract_pipeline_meta(tree: ast.Module) -> tuple[str, str]:
    """Find pipeline = haute.Pipeline("name", description="...") at module level."""
    return _extract_meta(tree, "pipeline", "main")


def _extract_submodel_meta(tree: ast.Module) -> tuple[str, str]:
    """Find submodel = haute.Submodel("name", description="...") at module level."""
    return _extract_meta(tree, "submodel", "unnamed")


# ---------------------------------------------------------------------------
# Preamble extraction
# ---------------------------------------------------------------------------

_STANDARD_IMPORTS = {"import polars as pl", "import haute"}


def _extract_preamble(source: str) -> str:
    """Extract user-defined preamble between standard imports and pipeline code.

    The preamble is any code that appears after the standard imports
    (``import polars as pl``, ``import haute``) but before the first
    ``@pipeline.node`` decorator or ``pipeline = haute.Pipeline(...)`` line.
    """
    lines = source.splitlines()
    # Find the end of standard imports region
    last_standard_idx = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped in _STANDARD_IMPORTS:
            last_standard_idx = i

    if last_standard_idx == -1:
        return ""

    # Find the start of pipeline code (pipeline = ... or @pipeline.node)
    pipeline_start_idx = len(lines)
    for i in range(last_standard_idx + 1, len(lines)):
        stripped = lines[i].strip()
        is_pipeline_def = stripped.startswith("pipeline") and (
            "haute.Pipeline" in stripped or "= haute.Pipeline" in stripped
        )
        if is_pipeline_def:
            pipeline_start_idx = i
            break
        if stripped.startswith("@pipeline.node"):
            pipeline_start_idx = i
            break

    # Extract lines between standard imports and pipeline code
    preamble_lines = lines[last_standard_idx + 1 : pipeline_start_idx]

    # Strip leading/trailing blank lines
    while preamble_lines and not preamble_lines[0].strip():
        preamble_lines.pop(0)
    while preamble_lines and not preamble_lines[-1].strip():
        preamble_lines.pop()

    return "\n".join(preamble_lines)


# ---------------------------------------------------------------------------
# Preserved block extraction
# ---------------------------------------------------------------------------

_PRESERVE_START = "# haute:preserve-start"
_PRESERVE_END = "# haute:preserve-end"


def _extract_preserved_blocks(source: str) -> list[str]:
    """Extract code between ``# haute:preserve-start`` / ``# haute:preserve-end`` markers.

    Returns a list of strings, one per matched block, with the marker
    lines themselves stripped.  Blocks are returned in source order.
    Unmatched start markers (no corresponding end) are silently ignored.
    """
    blocks: list[str] = []
    lines = source.splitlines()
    i = 0
    while i < len(lines):
        if lines[i].strip() == _PRESERVE_START:
            # Collect lines until the matching end marker
            block_lines: list[str] = []
            i += 1
            while i < len(lines) and lines[i].strip() != _PRESERVE_END:
                block_lines.append(lines[i])
                i += 1
            if i < len(lines):
                # Found the end marker — store the block
                # Strip leading/trailing blank lines but keep internal structure
                while block_lines and not block_lines[0].strip():
                    block_lines.pop(0)
                while block_lines and not block_lines[-1].strip():
                    block_lines.pop()
                blocks.append("\n".join(block_lines))
            # else: unmatched start marker — skip
        i += 1
    return blocks
