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
from typing import Any


# ---------------------------------------------------------------------------
# Node type inference
# ---------------------------------------------------------------------------

def _infer_node_type(decorator_kwargs: dict[str, Any], n_params: int) -> str:
    """Infer the GUI node type from decorator config and param count."""
    if "model_uri" in decorator_kwargs:
        return "modelScore"
    if "table" in decorator_kwargs and "key" in decorator_kwargs:
        return "ratingStep"
    if "path" in decorator_kwargs or n_params == 0:
        return "dataSource"
    return "transform"


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
        if (isinstance(decorator.value, ast.Name)
                and decorator.attr == "node"):
            return True

    # @pipeline.node(...)
    if isinstance(decorator, ast.Call):
        return _is_pipeline_node_decorator(decorator.func)

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
            if not cleaned and (stripped.startswith('"""') or stripped.startswith("'''")):
                quote = stripped[:3]
                if stripped.count(quote) >= 2 and stripped.endswith(quote):
                    docstring_done = True
                    continue
                else:
                    in_docstring = True
                    continue
            if in_docstring:
                if '"""' in stripped or "'''" in stripped:
                    in_docstring = False
                    docstring_done = True
                continue
            docstring_done = True

        cleaned.append(line)

    return cleaned


def _dedent(code: str) -> str:
    """Remove common leading whitespace."""
    code_lines = code.splitlines()
    if not code_lines:
        return code
    indents = [len(l) - len(l.lstrip()) for l in code_lines if l.strip()]
    if not indents:
        return code
    m = min(indents)
    return "\n".join(l[m:] if len(l) >= m else l for l in code_lines)


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


def _extract_function_bodies(source: str) -> dict[str, str]:
    """Extract raw source of each function body, keyed by function name."""
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


def _extract_connect_calls(tree: ast.Module) -> list[tuple[str, str]]:
    """Find all pipeline.connect("src", "tgt") calls at module level."""
    connects: list[tuple[str, str]] = []

    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.Expr):
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue

        # Check for *.connect(...)
        func = call.func
        if not isinstance(func, ast.Attribute) or func.attr != "connect":
            continue

        args = call.args
        if len(args) >= 2:
            src = _eval_ast_literal(args[0])
            tgt = _eval_ast_literal(args[1])
            if isinstance(src, str) and isinstance(tgt, str):
                connects.append((src, tgt))

    return connects


def _extract_pipeline_meta(tree: ast.Module) -> tuple[str, str]:
    """Find pipeline = runw.Pipeline("name", description="...") at module level."""
    name = "my_pipeline"
    description = ""

    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or target.id != "pipeline":
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue

        # Positional first arg = name
        if call.args:
            val = _eval_ast_literal(call.args[0])
            if isinstance(val, str):
                name = val

        # keyword description=
        for kw in call.keywords:
            if kw.arg == "description":
                val = _eval_ast_literal(kw.value)
                if isinstance(val, str):
                    description = val

        break

    return name, description


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_pipeline_file(filepath: str | Path) -> dict:
    """Parse a pipeline .py file and return a React Flow graph JSON.

    Returns:
        {
            "nodes": [...],
            "edges": [...],
            "pipeline_name": str,
            "pipeline_description": str,
            "source_file": str,
        }

    On syntax errors returns a dict with an "error" key.
    """
    filepath = Path(filepath)
    source = filepath.read_text()
    return parse_pipeline_source(source, source_file=str(filepath))


def parse_pipeline_source(source: str, source_file: str = "") -> dict:
    """Parse pipeline source code and return graph JSON."""

    # Syntax check
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return {
            "error": f"Syntax error on line {e.lineno}: {e.msg}",
            "nodes": [],
            "edges": [],
        }

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
        config: dict[str, Any] = {}
        if node_type == "dataSource":
            config["path"] = decorator_kwargs.get("path", "")
            if "table" in decorator_kwargs:
                config["sourceType"] = "databricks"
                config["table"] = decorator_kwargs["table"]
            else:
                config["sourceType"] = "flat_file"
        elif node_type == "modelScore":
            config["model_uri"] = decorator_kwargs.get("model_uri", "")
        elif node_type == "ratingStep":
            config["table"] = decorator_kwargs.get("table", "")
            config["key"] = decorator_kwargs.get("key", "")
        else:
            # transform — extract user code from body
            body = func_bodies.get(func_name, "")
            config["code"] = _extract_user_code(body, param_names) if body else ""

        raw_nodes.append({
            "func_name": func_name,
            "node_type": node_type,
            "description": description,
            "config": config,
            "param_names": param_names,
        })

    if not raw_nodes:
        return {
            "nodes": [],
            "edges": [],
            "pipeline_name": pipeline_name,
            "pipeline_description": pipeline_desc,
            "warning": "No @pipeline.node decorated functions found",
            "source_file": source_file,
        }

    # Build edges
    node_names = {n["func_name"] for n in raw_nodes}
    edges: list[dict] = []

    # Explicit connect() calls
    explicit_edges: set[tuple[str, str]] = set()
    for src, tgt in _extract_connect_calls(tree):
        if src in node_names and tgt in node_names:
            explicit_edges.add((src, tgt))
            edges.append({
                "id": f"e_{src}_{tgt}",
                "source": src,
                "target": tgt,
            })

    # Implicit edges from parameter names matching node names
    targets_with_explicit = {tgt for _, tgt in explicit_edges}
    for node_info in raw_nodes:
        if node_info["func_name"] in targets_with_explicit:
            continue
        for param in node_info["param_names"]:
            if param in node_names and param != node_info["func_name"]:
                pair = (param, node_info["func_name"])
                if pair not in explicit_edges:
                    edges.append({
                        "id": f"e_{pair[0]}_{pair[1]}",
                        "source": pair[0],
                        "target": pair[1],
                    })

    # Fallback: if still no edges, infer linear chain from definition order
    # (same behaviour as Pipeline._topo_order when no connect() calls)
    if not edges and len(raw_nodes) > 1:
        for i in range(1, len(raw_nodes)):
            src = raw_nodes[i - 1]["func_name"]
            tgt = raw_nodes[i]["func_name"]
            edges.append({
                "id": f"e_{src}_{tgt}",
                "source": src,
                "target": tgt,
            })

    # Build React Flow nodes (initial positions — will be replaced by ELK layout)
    x_spacing = 300
    rf_nodes = []
    for i, n in enumerate(raw_nodes):
        rf_nodes.append({
            "id": n["func_name"],
            "type": n["node_type"],
            "position": {"x": i * x_spacing, "y": 0},
            "data": {
                "label": n["func_name"],
                "description": n["description"],
                "nodeType": n["node_type"],
                "config": n["config"],
            },
        })

    return {
        "nodes": rf_nodes,
        "edges": edges,
        "pipeline_name": pipeline_name,
        "pipeline_description": pipeline_desc,
        "source_file": source_file,
    }
