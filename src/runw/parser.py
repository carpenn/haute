"""Parser: .py pipeline file → React Flow graph JSON.

Uses Python's ast module to extract @pipeline.node decorated functions
and pipeline.connect() calls, producing the same graph JSON format
that the frontend expects.

libcst is reserved for surgical write-back (codegen edits) where
preserving formatting matters.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Node type inference
# ---------------------------------------------------------------------------

def _infer_node_type(decorator_kwargs: dict[str, Any], n_params: int) -> str:
    """Infer the GUI node type from decorator config and param count."""
    if "external" in decorator_kwargs:
        return "externalFile"
    if "sink" in decorator_kwargs:
        return "dataSink"
    if decorator_kwargs.get("output"):
        return "output"
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
# Shared helpers for building graph output
# ---------------------------------------------------------------------------

def _build_node_config(
    node_type: str,
    decorator_kwargs: dict[str, Any],
    body: str,
    param_names: list[str],
) -> dict[str, Any]:
    """Build the config dict for a node given its type and decorator kwargs."""
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
    elif node_type == "dataSink":
        config["path"] = decorator_kwargs.get("sink", "")
        config["format"] = decorator_kwargs.get("format", "parquet")
    elif node_type == "externalFile":
        config["path"] = decorator_kwargs.get("external", "")
        config["fileType"] = decorator_kwargs.get("file_type", "pickle")
        if config["fileType"] == "catboost":
            config["modelClass"] = decorator_kwargs.get("model_class", "classifier")
        config["code"] = _extract_external_user_code(body, param_names) if body else ""
    elif node_type == "output":
        config["fields"] = decorator_kwargs.get("fields", [])
    else:
        # transform
        config["code"] = _extract_user_code(body, param_names) if body else ""
    return config


def _build_edges(
    raw_nodes: list[dict],
    explicit_connect_pairs: list[tuple[str, str]],
) -> list[dict]:
    """Build edge dicts from explicit connect() calls and implicit param-name matching."""
    node_names = {n["func_name"] for n in raw_nodes}
    edges: list[dict] = []
    explicit_edges: set[tuple[str, str]] = set()

    for src, tgt in explicit_connect_pairs:
        if src in node_names and tgt in node_names:
            explicit_edges.add((src, tgt))
            edges.append({"id": f"e_{src}_{tgt}", "source": src, "target": tgt})

    # Implicit edges from parameter names matching node names
    targets_with_explicit = {tgt for _, tgt in explicit_edges}
    for node_info in raw_nodes:
        if node_info["func_name"] in targets_with_explicit:
            continue
        for param in node_info["param_names"]:
            if param in node_names and param != node_info["func_name"]:
                pair = (param, node_info["func_name"])
                if pair not in explicit_edges:
                    edges.append({"id": f"e_{pair[0]}_{pair[1]}", "source": pair[0], "target": pair[1]})

    # Fallback: if still no edges, infer linear chain from definition order
    if not edges and len(raw_nodes) > 1:
        for i in range(1, len(raw_nodes)):
            src = raw_nodes[i - 1]["func_name"]
            tgt = raw_nodes[i]["func_name"]
            edges.append({"id": f"e_{src}_{tgt}", "source": src, "target": tgt})

    return edges


def _build_rf_nodes(raw_nodes: list[dict], x_spacing: int = 300) -> list[dict]:
    """Convert raw parsed nodes into React Flow node dicts."""
    return [
        {
            "id": n["func_name"],
            "type": n["node_type"],
            "position": {"x": i * x_spacing, "y": 0},
            "data": {
                "label": n["func_name"],
                "description": n["description"],
                "nodeType": n["node_type"],
                "config": n["config"],
            },
        }
        for i, n in enumerate(raw_nodes)
    ]


# ---------------------------------------------------------------------------
# Regex-based fallback for files with syntax errors
# ---------------------------------------------------------------------------

_RE_DECORATOR = re.compile(
    r"^(@pipeline\.node(?:\([^)]*\))?)\s*\n"
    r"def\s+(\w+)\s*\(([^)]*)\)",
    re.MULTILINE,
)

_RE_PIPELINE_META = re.compile(
    r'pipeline\s*=\s*runw\.Pipeline\(\s*["\']([^"\']*)["\']'
    r'(?:.*?description\s*=\s*["\']([^"\']*)["\'])?',
)

_RE_CONNECT = re.compile(
    r'pipeline\.connect\(\s*["\']([^"\']+)["\']\s*,\s*["\']([^"\']+)["\']\s*\)',
)

_RE_DECORATOR_KWARG = re.compile(
    r'(\w+)\s*=\s*["\']([^"\']*)["\']',
)


def _find_function_blocks(source: str) -> list[dict]:
    """Find @pipeline.node function blocks using regex.

    Returns a list of dicts with keys: func_name, decorator_text, params,
    body_text, start_line.
    """
    lines = source.splitlines()
    blocks: list[dict] = []

    for m in _RE_DECORATOR.finditer(source):
        decorator_text = m.group(1)
        func_name = m.group(2)
        params_text = m.group(3)

        # Find the line number of the def
        def_pos = m.start()
        start_line = source[:def_pos].count("\n")

        # Extract parameter names (strip type annotations)
        param_names = []
        for p in params_text.split(","):
            p = p.strip()
            if not p:
                continue
            name = p.split(":")[0].strip()
            if name:
                param_names.append(name)

        # Find the body: everything indented after the def line
        # The def line is somewhere after the decorator
        def_line_idx = source[:m.end()].count("\n")
        body_lines = []
        for i in range(def_line_idx + 1, len(lines)):
            line = lines[i]
            if line.strip() == "":
                body_lines.append(line)
                continue
            if line[0] == " " or line[0] == "\t":
                body_lines.append(line)
            else:
                break

        # Strip trailing empty lines
        while body_lines and not body_lines[-1].strip():
            body_lines.pop()

        blocks.append({
            "func_name": func_name,
            "decorator_text": decorator_text,
            "param_names": param_names,
            "body_text": "\n".join(body_lines),
            "start_line": start_line,
        })

    return blocks


_RE_DECORATOR_BOOL_KWARG = re.compile(
    r'(\w+)\s*=\s*(True|False)',
)


def _parse_decorator_kwargs_regex(decorator_text: str) -> dict[str, Any]:
    """Extract keyword arguments from a decorator using regex."""
    # Strip the @pipeline.node( ... ) wrapper
    if "(" in decorator_text:
        inner = decorator_text.split("(", 1)[1].rstrip(")")
        result: dict[str, Any] = dict(_RE_DECORATOR_KWARG.findall(inner))
        # Also capture boolean kwargs (e.g. output=True)
        for key, val in _RE_DECORATOR_BOOL_KWARG.findall(inner):
            if key not in result:
                result[key] = val == "True"
        return result
    return {}


def _fallback_parse(source: str, source_file: str, syntax_error: SyntaxError) -> dict:
    """Parse a pipeline file with syntax errors using regex fallback.

    Extracts all @pipeline.node functions, marks broken ones with an error
    in their config, and still returns the full graph.
    """
    # Pipeline metadata via regex
    meta_match = _RE_PIPELINE_META.search(source)
    pipeline_name = meta_match.group(1) if meta_match else "my_pipeline"
    pipeline_desc = (meta_match.group(2) or "") if meta_match else ""

    # Find function blocks
    blocks = _find_function_blocks(source)
    raw_nodes: list[dict] = []

    for block in blocks:
        func_name = block["func_name"]
        decorator_kwargs = _parse_decorator_kwargs_regex(block["decorator_text"])
        param_names = block["param_names"]
        n_params = len(param_names)
        node_type = _infer_node_type(decorator_kwargs, n_params)

        # Try to parse the function individually to get the docstring
        func_source = f"{block['decorator_text']}\ndef {func_name}({', '.join(param_names)}):\n{block['body_text']}"
        description = ""
        has_syntax_error = False

        try:
            func_tree = ast.parse(func_source)
            for stmt in ast.iter_child_nodes(func_tree):
                if isinstance(stmt, ast.FunctionDef):
                    description = _get_docstring(stmt)
                    break
        except SyntaxError:
            has_syntax_error = True

        body = block["body_text"] if not has_syntax_error else ""
        config = _build_node_config(node_type, decorator_kwargs, body, param_names)

        raw_nodes.append({
            "func_name": func_name,
            "node_type": node_type,
            "description": description or f"{func_name} node",
            "config": config,
            "param_names": param_names,
        })

    # Build edges + nodes using shared helpers
    connect_pairs = _RE_CONNECT.findall(source)
    edges = _build_edges(raw_nodes, connect_pairs)
    rf_nodes = _build_rf_nodes(raw_nodes)
    preamble = _extract_preamble(source)

    return {
        "nodes": rf_nodes,
        "edges": edges,
        "pipeline_name": pipeline_name,
        "pipeline_description": pipeline_desc,
        "preamble": preamble,
        "source_file": source_file,
        "warning": f"File has syntax errors — some nodes may show errors. {syntax_error}",
    }


# ---------------------------------------------------------------------------
# Preamble extraction
# ---------------------------------------------------------------------------

_STANDARD_IMPORTS = {"import polars as pl", "import runw"}


def _extract_preamble(source: str) -> str:
    """Extract user-defined preamble between standard imports and pipeline code.

    The preamble is any code that appears after the standard imports
    (``import polars as pl``, ``import runw``) but before the first
    ``@pipeline.node`` decorator or ``pipeline = runw.Pipeline(...)`` line.
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
        if stripped.startswith("pipeline") and ("runw.Pipeline" in stripped or "= runw.Pipeline" in stripped):
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
            "preamble": str,
            "source_file": str,
        }

    On syntax errors the file is still parsed via regex fallback so
    that valid nodes are returned alongside broken ones.
    """
    filepath = Path(filepath)
    source = filepath.read_text()
    return parse_pipeline_source(source, source_file=str(filepath))


def parse_pipeline_source(source: str, source_file: str = "") -> dict:
    """Parse pipeline source code and return graph JSON."""

    # Syntax check — fall back to regex if the file has errors
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
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

    # Build edges + nodes using shared helpers
    edges = _build_edges(raw_nodes, _extract_connect_calls(tree))
    rf_nodes = _build_rf_nodes(raw_nodes)
    preamble = _extract_preamble(source)

    return {
        "nodes": rf_nodes,
        "edges": edges,
        "pipeline_name": pipeline_name,
        "pipeline_description": pipeline_desc,
        "preamble": preamble,
        "source_file": source_file,
    }
