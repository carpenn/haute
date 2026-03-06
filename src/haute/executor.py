"""Graph executor: run a pipeline graph JSON dynamically.

Takes a React Flow graph (nodes + edges) and executes it as a real
Polars pipeline, without needing a saved .py file.

Node functions produce LazyFrames.  Preview and trace use eager
single-pass execution with per-graph caching so repeated clicks
don't re-execute the pipeline.  Source nodes are capped at
row_limit rows.  Sink and CLI paths use lazy execution so Polars
can optimise the full plan end-to-end.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import polars as pl

from haute._fingerprint_cache import FingerprintCache
from haute._logging import get_logger
from haute._rating import (
    _apply_banding,
    _apply_rating_table,
    _combine_rating_columns,
    _normalise_banding_factors,
)
from haute._sandbox import safe_globals, validate_user_code
from haute.graph_utils import (
    GraphNode,
    NodeType,
    PipelineGraph,
    _execute_eager_core,
    _execute_lazy,
    _Frame,
    _sanitize_func_name,
    build_instance_mapping,
    graph_fingerprint,
    load_external_object,
    read_source,
)
from haute.schemas import ColumnInfo, NodeResult, SchemaWarning, SinkResponse

logger = get_logger(component="executor")

# ── Default constants ─────────────────────────────────────────────
_DEFAULT_SCENARIO_MIN = 0.8  # scenario expander lower bound
_DEFAULT_SCENARIO_MAX = 1.2  # scenario expander upper bound
_DEFAULT_SCENARIO_STEPS = 21  # number of steps in scenario grid
_DEFAULT_CHUNK_SIZE = 500_000  # rows per chunk for optimiser apply
_MAX_PREVIEW_ROWS = 10_000  # safety cap for execute_graph JSON payload


class PreambleError(Exception):
    """Raised when the preamble (imports / utility code) fails to compile."""

    def __init__(self, message: str, source_line: int | None = None):
        super().__init__(message)
        self.source_line = source_line


def _compile_preamble(preamble: str) -> dict[str, Any]:
    """Compile user-defined preamble code into a namespace dict.

    The preamble (helper functions, constants, lambdas) is defined at the
    top of a pipeline file between imports and the first ``@pipeline.node``.
    This compiles it once and returns a dict of bindings that can be
    injected into ``_exec_user_code`` via ``extra_ns``.

    Uses a single dict for globals/locals so preamble functions can call
    each other (they share the same ``__globals__``).

    Raises ``PreambleError`` with a human-readable message and optional
    source line number when the preamble fails to execute (e.g. a utility
    module has a NameError).
    """
    if not preamble or not preamble.strip():
        return {}
    # Preamble may contain imports (e.g. from utility.features import …)
    # which are legitimate, but still validate against other dangerous
    # patterns (dunder access, eval, exec, etc.).
    validate_user_code(preamble, allow_imports=True)
    # Ensure project root is importable so `from utility.xxx import …` works
    # even when the server process was spawned by uvicorn reload.
    import os  # noqa: E401
    import sys
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    # Evict cached utility modules so edits in the GUI are picked up
    # on every run instead of serving stale bytecode from sys.modules.
    for mod_name in [k for k in sys.modules if k == "utility" or k.startswith("utility.")]:
        del sys.modules[mod_name]

    ns = safe_globals(pl=pl, allow_imports=True)
    base_keys = set(ns.keys())
    try:
        exec(preamble, ns)  # noqa: S102  — single dict = shared globals
    except Exception as exc:
        # Extract the most useful line number and source file from
        # the traceback or exception attributes.
        import traceback as _tb
        from pathlib import Path as _Path

        source_line: int | None = None
        source_file: str | None = None

        # SyntaxError carries .filename and .lineno directly
        if isinstance(exc, SyntaxError) and exc.filename:
            source_file = exc.filename
            source_line = exc.lineno

        # For runtime errors, walk the traceback to find the utility frame
        if source_file is None and exc.__traceback__:
            for frame in reversed(_tb.extract_tb(exc.__traceback__)):
                if "utility" in frame.filename:
                    source_line = frame.lineno
                    source_file = frame.filename
                    break
                if frame.filename == "<string>":
                    source_line = frame.lineno
                    break

        msg = f"Import/preamble error: {exc}"
        if source_file and source_file != "<string>":
            try:
                rel = _Path(source_file).relative_to(_Path.cwd())
            except ValueError:
                rel = source_file
            msg = f"Error in {rel} line {source_line}: {exc}"
        elif source_line:
            msg = f"Preamble line {source_line}: {exc}"

        raise PreambleError(msg, source_line=source_line) from exc

    return {k: v for k, v in ns.items() if k not in base_keys}


def _exec_user_code(
    code: str,
    src_names: list[str],
    dfs: tuple[_Frame, ...],
    extra_ns: dict[str, Any] | None = None,
    orig_source_names: list[str] | None = None,
    input_mapping: dict[str, str] | None = None,
) -> _Frame:
    """Wrap, execute, and return the result of user-provided code.

    Shared by transform and externalFile node types.
    - Injects ``pl``, input DataFrames by name, and ``df`` (first input).
    - Optionally merges *extra_ns* into the local namespace (e.g. ``obj``).
    - Handles the ``.chain`` / bare-expression wrapping and adjusts line
      numbers in error messages so they match the editor.
    """
    local_ns: dict[str, Any] = {"pl": pl}
    for i, d in enumerate(dfs):
        if i < len(src_names):
            local_ns[src_names[i]] = d
    # Instance alias injection: bind original source names so the original's
    # code can reference variables by their original upstream labels.
    if orig_source_names:
        mapping = build_instance_mapping(orig_source_names, src_names, input_mapping)
        for orig, inst in mapping.items():
            if orig not in local_ns and inst in local_ns:
                local_ns[orig] = local_ns[inst]
    if dfs:
        local_ns["df"] = dfs[0]
    if extra_ns:
        local_ns.update(extra_ns)

    exec_code = code
    line_offset = 0
    if code.startswith("."):
        first = src_names[0] if src_names else "df"
        exec_code = f"df = (\n    {first}\n    {code}\n)"
        line_offset = 2
    elif "df =" not in code and "df=" not in code:
        exec_code = f"df = (\n    {code}\n)"
        line_offset = 1

    # Validate the original user code at the AST level before exec().
    # This blocks dunder access, imports, getattr, class defs, etc.
    # at the structural level — a stronger layer than restricted builtins.
    validate_user_code(code)

    try:
        exec(exec_code, safe_globals(pl=pl, **(extra_ns or {})), local_ns)
    except SyntaxError as exc:
        if exc.lineno is not None:
            exc.lineno = max(1, exc.lineno - line_offset)
        raise
    except Exception as exc:
        msg = str(exc)
        if line_offset and re.search(r"line \d+", msg):
            msg = re.sub(
                r"line (\d+)",
                lambda m: f"line {max(1, int(m.group(1)) - line_offset)}",
                msg,
            )
            raise type(exc)(msg) from None
        # For errors without "line N" in the message (e.g. NameError),
        # extract the line from the traceback and adjust for preamble offset.
        if exc.__traceback__:
            import traceback as _tb
            for frame in reversed(_tb.extract_tb(exc.__traceback__)):
                if frame.filename == "<string>":
                    exc._user_code_line = max(1, frame.lineno - line_offset)
                    break
        raise

    result = local_ns.get("df", dfs[0] if dfs else pl.LazyFrame())
    if isinstance(result, pl.DataFrame):
        result = result.lazy()
    return result  # type: ignore[no-any-return]


def resolve_instance_node(node: GraphNode, node_map: dict[str, GraphNode]) -> GraphNode:
    """If *node* is an instance, return a merged node with the original's config.

    The returned node keeps the instance's own id, label, and position but
    uses the original node's ``nodeType`` and ``config`` (minus the
    ``instanceOf`` key itself).  If the original cannot be found the
    instance is returned unchanged.
    """
    config = node.data.config
    ref = config.get("instanceOf")
    if not ref or ref not in node_map:
        return node
    original = node_map[ref]
    orig_config = {k: v for k, v in original.data.config.items() if k != "instanceOf"}
    # Preserve instance-specific keys (inputMapping) that the UI sets
    instance_keys = {k: v for k, v in config.items() if k in ("inputMapping",)}
    merged_config = {**orig_config, "instanceOf": ref, **instance_keys}
    merged_data = node.data.model_copy(update={
        "nodeType": original.data.nodeType,
        "config": merged_config,
    })
    return node.model_copy(update={"data": merged_data})


# ---------------------------------------------------------------------------
# Node builder registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NodeBuildContext:
    """Parameters shared by all node builder functions."""

    node: GraphNode
    source_names: list[str]
    row_limit: int | None
    node_map: dict[str, GraphNode] | None
    orig_source_names: list[str] | None
    preamble_ns: dict[str, Any] | None
    scenario: str | None


# Type alias for builder functions.
NodeBuilder = Callable[[NodeBuildContext], tuple[str, Callable, bool]]

_NODE_BUILDERS: dict[NodeType, NodeBuilder] = {}


def _register(node_type: NodeType) -> Callable[[NodeBuilder], NodeBuilder]:
    """Decorator to register a node builder for a given NodeType."""
    def decorator(fn: NodeBuilder) -> NodeBuilder:
        _NODE_BUILDERS[node_type] = fn
        return fn
    return decorator


def _passthrough_fn(*dfs: _Frame) -> _Frame:
    """Shared passthrough: return the first input or an empty LazyFrame."""
    return dfs[0] if dfs else pl.LazyFrame()


@_register(NodeType.API_INPUT)
def _build_api_input(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    path = config.get("path", "")
    flat_schema = config.get("flattenSchema")

    if path.endswith((".json", ".jsonl")):
        def api_source_fn(_path: str = path, _schema: dict | None = flat_schema) -> _Frame:
            from haute._json_flatten import _json_cache_path

            cache_path = _json_cache_path(_path)
            if cache_path.exists():
                return pl.scan_parquet(cache_path)
            raise RuntimeError(
                "JSON data has not been cached yet. "
                "Click 'Cache as Parquet' on the API Input node to process it."
            )
    else:
        def api_source_fn() -> _Frame:
            return read_source(path)

    return func_name, api_source_fn, True


@_register(NodeType.DATA_SOURCE)
def _build_data_source(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    path = config.get("path", "")
    source_type = config.get("sourceType", "flat_file")

    if source_type == "databricks":
        table = config.get("table", "")

        def _databricks_source(_table: str = table) -> _Frame:
            from haute._databricks_io import read_cached_table

            return read_cached_table(_table)

        return func_name, _databricks_source, True

    def source_fn() -> _Frame:
        return read_source(path)

    return func_name, source_fn, True


@_register(NodeType.CONSTANT)
def _build_constant(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    raw_values = config.get("values", []) or []

    def constant_fn() -> _Frame:
        data: dict[str, list] = {}
        for v in raw_values:
            name = v.get("name", "")
            if not name:
                continue
            val = v.get("value", "")
            try:
                data[name] = [float(val)]
            except (ValueError, TypeError):
                data[name] = [val]
        if not data:
            data = {"constant": [0]}
        return pl.LazyFrame(data)

    return func_name, constant_fn, True


@_register(NodeType.LIVE_SWITCH)
def _build_live_switch(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    input_scenario_map: dict[str, str] = config.get("input_scenario_map", {})
    input_names = list(ctx.source_names)
    _scenario = ctx.scenario or "live"

    def switch_fn(*dfs: _Frame) -> _Frame:
        # Find the input mapped to the active scenario
        for inp, scn in input_scenario_map.items():
            if scn == _scenario:
                for i, name in enumerate(input_names):
                    if name == inp:
                        return dfs[i]
        # Fallback: first input + log warning
        if input_scenario_map:
            logger.warning(
                "live_switch_unmapped_scenario",
                scenario=_scenario,
                mapped_scenarios=list(input_scenario_map.values()),
                falling_back_to=input_names[0] if input_names else "<none>",
            )
        return dfs[0]

    return func_name, switch_fn, False


@_register(NodeType.DATA_SINK)
def _build_data_sink(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    func_name = _sanitize_func_name(ctx.node.data.label)
    # During normal run/preview, dataSink is a pass-through.
    # Actual writing happens via execute_sink() on explicit user action.
    return func_name, _passthrough_fn, False


@_register(NodeType.EXTERNAL_FILE)
def _build_external_file(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    code = config.get("code", "").strip()
    path = config.get("path", "")
    file_type = config.get("fileType", "pickle")
    model_class = config.get("modelClass", "classifier")
    _src_names = list(ctx.source_names)

    _orig_src = list(ctx.orig_source_names) if ctx.orig_source_names else None
    _in_map = dict(config.get("inputMapping", {})) or None
    _preamble_ext = dict(ctx.preamble_ns) if ctx.preamble_ns else {}
    if code:

        def external_fn(*dfs: _Frame) -> _Frame:
            ens = {"obj": load_external_object(path, file_type, model_class)}
            ens.update(_preamble_ext)
            return _exec_user_code(
                code, _src_names, dfs,
                extra_ns=ens,
                orig_source_names=_orig_src,
                input_mapping=_in_map,
            )

        return func_name, external_fn, False
    else:
        return func_name, _passthrough_fn, False


@_register(NodeType.OUTPUT)
def _build_output(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    fields = config.get("fields", []) or []

    def output_fn(*dfs: _Frame) -> _Frame:
        lf = dfs[0] if dfs else pl.LazyFrame()
        if fields:
            lf = lf.select(fields)
        return lf

    return func_name, output_fn, False


@_register(NodeType.BANDING)
def _build_banding(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    factors = _normalise_banding_factors(config)

    def banding_fn(*dfs: _Frame, _factors: list = list(factors)) -> _Frame:
        lf = dfs[0] if dfs else pl.LazyFrame()
        for f in _factors:
            col = f.get("column", "")
            out = f.get("outputColumn", "")
            rules = f.get("rules", []) or []
            if not col or not out or not rules:
                continue
            lf = _apply_banding(
                lf, col, out, f.get("banding", "continuous"),
                rules, f.get("default"),
            )
        return lf

    return func_name, banding_fn, False


@_register(NodeType.RATING_STEP)
def _build_rating_step(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    tables: list[dict[str, Any]] = config.get("tables", []) or []
    # GUI config may send None for these fields, so `or` ensures a usable default
    _rs_operation: str = config.get("operation", "multiply") or "multiply"
    _rs_combined: str = config.get("combinedColumn", "") or ""

    def rating_fn(
        *dfs: _Frame,
        _tables: list = list(tables),
        _op: str = _rs_operation,
        _combined: str = _rs_combined,
    ) -> _Frame:
        lf = dfs[0] if dfs else pl.LazyFrame()
        out_cols: list[str] = []
        for t in _tables:
            lf = _apply_rating_table(lf, t)
            oc = t.get("outputColumn", "")
            if oc:
                out_cols.append(oc)
        if _combined and len(out_cols) >= 2:
            logger.info(
                "combining_rating_columns",
                columns=out_cols,
                operation=_op,
                output=_combined,
            )
            lf = _combine_rating_columns(lf, out_cols, _op, _combined)
        return lf

    return func_name, rating_fn, False


@_register(NodeType.SCENARIO_EXPANDER)
def _build_scenario_expander(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    _col_name = config.get("column_name", "scenario_value")
    _min_val = float(config.get("min_value", _DEFAULT_SCENARIO_MIN))
    _max_val = float(config.get("max_value", _DEFAULT_SCENARIO_MAX))
    _steps = int(config.get("steps", _DEFAULT_SCENARIO_STEPS))
    _step_col = config.get("step_column", "scenario_index")

    def scenario_expand_fn(
        *dfs: _Frame,
        _cn: str = _col_name,
        _mn: float = _min_val,
        _mx: float = _max_val,
        _st: int = _steps,
        _sc: str = _step_col,
    ) -> _Frame:
        import numpy as np

        lf = dfs[0] if dfs else pl.LazyFrame()
        vals = np.linspace(_mn, _mx, _st)
        scenarios = pl.DataFrame({
            _sc: pl.Series(range(_st), dtype=pl.Int32),
            # Float32 to match Rust QuoteGrid schema (price-contour ingests f32)
            _cn: pl.Series(vals.tolist(), dtype=pl.Float32),
        }).lazy()
        return lf.join(scenarios, how="cross")

    return func_name, scenario_expand_fn, False


@_register(NodeType.OPTIMISER)
def _build_optimiser(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    func_name = _sanitize_func_name(ctx.node.data.label)
    # Pass-through in preview mode. Solving happens via /api/optimiser/solve.
    return func_name, _passthrough_fn, False


@_register(NodeType.OPTIMISER_APPLY)
def _build_optimiser_apply(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    _artifact_path = config.get("artifact_path", "")
    _version_col = config.get("version_column", "__optimiser_version__")
    _source_type = config.get("sourceType", "")
    _run_id = config.get("run_id", "")
    _registered_model = config.get("registered_model", "")
    _opt_version = config.get("version", "latest")

    # Determine if we have a valid source configured
    _has_file = bool(_artifact_path) and _source_type in ("", "file")
    _has_mlflow = _source_type in ("run", "registered") and (
        (_source_type == "run" and _run_id)
        or (_source_type == "registered" and _registered_model)
    )

    if not _has_file and not _has_mlflow:
        return func_name, _passthrough_fn, False

    def optimiser_apply_fn(
        *dfs: _Frame,
        _path: str = _artifact_path,
        _vcol: str = _version_col,
        _st: str = _source_type,
        _rid: str = _run_id,
        _rm: str = _registered_model,
        _ver: str = _opt_version,
    ) -> _Frame:
        if _st in ("run", "registered"):
            from haute._optimiser_io import load_mlflow_optimiser_artifact

            artifact = load_mlflow_optimiser_artifact(
                source_type=_st,
                run_id=_rid,
                registered_model=_rm,
                version=_ver,
            )
        else:
            from haute._optimiser_io import load_optimiser_artifact

            artifact = load_optimiser_artifact(_path)

        return _dispatch_apply(dfs[0] if dfs else pl.LazyFrame(), artifact, _vcol)

    return func_name, optimiser_apply_fn, False


@_register(NodeType.MODELLING)
def _build_modelling(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    func_name = _sanitize_func_name(ctx.node.data.label)
    # Pass-through in preview mode. Training happens via /api/modelling/train.
    return func_name, _passthrough_fn, False


@_register(NodeType.MODEL_SCORE)
def _build_model_score(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    source_type = config.get("sourceType", "")
    _run_id = config.get("run_id", "")
    _artifact_path = config.get("artifact_path", "")
    _registered_model = config.get("registered_model", "")
    _task = config.get("task", "regression")

    # If no model source configured, passthrough
    if not source_type or (source_type == "run" and not _run_id) or (
        source_type == "registered" and not _registered_model
    ):
        return func_name, _passthrough_fn, False

    from haute._model_scorer import ModelScorer

    scorer = ModelScorer(
        source_type=source_type,
        run_id=_run_id,
        artifact_path=_artifact_path,
        registered_model=config.get("registered_model", ""),
        version=config.get("version", "latest"),
        task=_task,
        output_col=config.get("output_column", "prediction"),
        code=config.get("code", "").strip(),
        source_names=list(ctx.source_names),
        scenario=ctx.scenario or "live",
        row_limit=ctx.row_limit,
    )

    return func_name, scorer.score, False


@_register(NodeType.TRANSFORM)
def _build_transform(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    config = ctx.node.data.config
    func_name = _sanitize_func_name(ctx.node.data.label)
    code = config.get("code", "").strip()
    _src_names = list(ctx.source_names)
    _orig_src = list(ctx.orig_source_names) if ctx.orig_source_names else None
    _in_map = dict(config.get("inputMapping", {})) or None
    _preamble = dict(ctx.preamble_ns) if ctx.preamble_ns else None

    if code:

        def transform_fn(*dfs: _Frame) -> _Frame:
            return _exec_user_code(
                code, _src_names, dfs,
                extra_ns=_preamble,
                orig_source_names=_orig_src,
                input_mapping=_in_map,
            )

        return func_name, transform_fn, False
    else:
        return func_name, _passthrough_fn, False


# SUBMODEL and SUBMODEL_PORT are pass-through types with no special logic.
# Register them explicitly so _build_node_fn doesn't raise on unknown types.
@_register(NodeType.SUBMODEL)
def _build_submodel(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    func_name = _sanitize_func_name(ctx.node.data.label)
    return func_name, _passthrough_fn, False


@_register(NodeType.SUBMODEL_PORT)
def _build_submodel_port(ctx: NodeBuildContext) -> tuple[str, Callable, bool]:
    func_name = _sanitize_func_name(ctx.node.data.label)
    return func_name, _passthrough_fn, False


# ---------------------------------------------------------------------------
# Main dispatcher
# ---------------------------------------------------------------------------


def _build_node_fn(
    node: GraphNode,
    source_names: list[str] | None = None,
    row_limit: int | None = None,
    node_map: dict[str, GraphNode] | None = None,
    orig_source_names: list[str] | None = None,
    preamble_ns: dict[str, Any] | None = None,
    scenario: str | None = None,
) -> tuple[str, Callable, bool]:
    """Build an executable function from a graph node dict.

    Returns (func_name, fn, is_source).
    source_names: sanitized names of upstream nodes (used as variable names).
    row_limit: if set, Databricks sources push this into SQL LIMIT so the
        full table is never fetched during preview/trace.
    node_map: full graph node_map — used to resolve ``instanceOf`` references.
    scenario: the active execution scenario (``"live"`` for eager scoring,
        anything else for batched parquet scoring).
    """
    # Resolve instance → use original's config/nodeType
    if node_map:
        node = resolve_instance_node(node, node_map)

    if source_names is None:
        source_names = []

    ctx = NodeBuildContext(
        node=node,
        source_names=source_names,
        row_limit=row_limit,
        node_map=node_map,
        orig_source_names=orig_source_names,
        preamble_ns=preamble_ns,
        scenario=scenario,
    )

    builder = _NODE_BUILDERS.get(node.data.nodeType)
    if builder is not None:
        return builder(ctx)

    # Fallback for any unrecognised node type: pass-through
    func_name = _sanitize_func_name(node.data.label)
    return func_name, _passthrough_fn, False


# ---------------------------------------------------------------------------
# OptimiserApply helpers
# ---------------------------------------------------------------------------


def _dispatch_apply(
    lf: _Frame,
    artifact: dict[str, Any],
    version_col: str,
) -> _Frame:
    """Route to the correct apply function based on artifact mode."""
    mode = artifact.get("mode", "online")
    version = artifact.get("version", "")
    if mode == "ratebook":
        return _apply_ratebook(lf, artifact, version, version_col)
    return _apply_online(lf, artifact, version, version_col)


def _apply_online(
    lf: _Frame,
    artifact: dict[str, Any],
    version: str,
    version_col: str,
) -> _Frame:
    """Apply online optimisation: Lagrangian argmax with stored lambdas."""
    from price_contour import ApplyOptimiser

    qid_col = artifact.get("quote_id", "quote_id")
    step_col = artifact.get("scenario_index", "scenario_index")
    mult_col = artifact.get("scenario_value", "scenario_value")
    objective = artifact.get("objective", "expected_income")
    constraints = artifact.get("constraints") or {}

    # Cast columns to the types price-contour expects (same as solve endpoint)
    cast_exprs = [
        pl.col(qid_col).cast(pl.Utf8),
        pl.col(step_col).cast(pl.Int32),
        pl.col(mult_col).cast(pl.Float32),
        pl.col(objective).cast(pl.Float32),
    ]
    for c in constraints:
        cast_exprs.append(pl.col(c).cast(pl.Float32))

    df_eager = lf.with_columns(cast_exprs).collect()

    applier = ApplyOptimiser(
        lambdas=artifact["lambdas"],
        objective=objective,
        constraints=constraints,
        quote_id=qid_col,
        scenario_index=step_col,
        scenario_value=mult_col,
        chunk_size=artifact.get("chunk_size", _DEFAULT_CHUNK_SIZE),
    )
    result = applier.apply(df_eager)
    result_df = result.dataframe

    if version:
        result_df = result_df.with_columns(pl.lit(version).alias(version_col))

    return result_df.lazy()


def _apply_ratebook(
    lf: _Frame,
    artifact: dict[str, Any],
    version: str,
    version_col: str,
) -> _Frame:
    """Apply ratebook optimisation: factor table lookups with stored tables.

    Each factor group produces a ``{name}_optimised_factor`` column, and
    they are multiplied together into ``optimised_factor`` so that
    downstream nodes have a single combined relativity.
    """
    factor_tables = artifact.get("factor_tables", {})
    if not factor_tables:
        logger.warning("ratebook_apply_no_factor_tables", artifact_keys=list(artifact.keys()))
        result_lf = lf
    else:
        result_lf = lf
        factor_cols: list[str] = []
        for _name, entries in factor_tables.items():
            if not entries:
                continue
            # factor_tables format from save: list of
            # {"__factor_group__": level, "optimal_scenario_value": value}
            # Convert to the rating table format expected by _apply_rating_table
            factor_col = "__factor_group__"
            out_col = f"{_name}_optimised_factor"
            table = {
                "factors": [_name],
                "outputColumn": out_col,
                "entries": [
                    {_name: e[factor_col], "value": e["optimal_scenario_value"]}
                    for e in entries
                    if factor_col in e
                ],
                "defaultValue": "1.0",
            }
            result_lf = _apply_rating_table(result_lf, table)
            factor_cols.append(out_col)

        # Combine individual factor columns into a single relativity
        if len(factor_cols) > 1:
            result_lf = _combine_rating_columns(
                result_lf, factor_cols, "multiply", "optimised_factor",
            )
        elif len(factor_cols) == 1:
            result_lf = result_lf.with_columns(
                pl.col(factor_cols[0]).alias("optimised_factor"),
            )

    if version:
        result_lf = result_lf.with_columns(pl.lit(version).alias(version_col))

    return result_lf


# ---------------------------------------------------------------------------
# Preview cache — same principle as the trace cache in trace.py.
# The pipeline doesn't change between node clicks — only the target node
# changes.  Cache the materialized DataFrames so clicking different nodes
# is instant instead of re-executing model scoring on 678K rows each time.
# ---------------------------------------------------------------------------


_preview_cache = FingerprintCache(
    slots=("eager_outputs", "errors", "order", "timings", "memory_bytes", "error_lines"),
)


def execute_graph(
    graph: PipelineGraph,
    target_node_id: str | None = None,
    row_limit: int | None = None,
    max_preview_rows: int = _MAX_PREVIEW_ROWS,
    scenario: str = "live",
) -> dict[str, NodeResult]:
    """Execute a graph and return per-node results.

    Uses eager single-pass execution with a single-entry cache so
    clicking different nodes doesn't re-execute the full pipeline.

    Args:
        graph: React Flow graph with "nodes" and "edges".
        target_node_id: If set, only execute nodes up to (and including) this node.
        row_limit: If set, apply .head(row_limit) to source nodes so only
                   that many rows flow through the pipeline.
        max_preview_rows: Max rows to include in the JSON preview payload.

    Returns:
        Dict mapping node_id → {
            "status": "ok" | "error",
            "row_count": int,
            "columns": [...],
            "preview": [...],
            "error": str | None,
        }
    """
    if not graph.nodes:
        return {}

    fp = graph_fingerprint(graph, f"{row_limit}:{scenario}")

    errors: dict[str, str] = {}
    error_lines: dict[str, int] = {}

    # Check if we can extend the cache (same graph, new target is a superset)
    cached = _preview_cache.try_get(fp)
    if cached is not None:
        prev_outputs = cached["eager_outputs"]
        if target_node_id is None or target_node_id in prev_outputs:
            # Full cache hit — all required nodes already materialised
            logger.debug(
                "preview_cache_hit",
                fingerprint=fp[:8],
                target=target_node_id,
                cached_nodes=len(prev_outputs),
            )
            eager_outputs = prev_outputs
            order = cached["order"]
            errors = cached["errors"]
            timings = cached["timings"]
            memory_bytes = cached["memory_bytes"]
            error_lines = cached["error_lines"]
        else:
            # Partial hit — extend with newly-needed nodes
            logger.debug(
                "preview_cache_extend",
                fingerprint=fp[:8],
                target=target_node_id,
                cached_nodes=len(prev_outputs),
            )
            raw_outputs, order, errors, timings, memory_bytes, error_lines = _eager_execute(
                graph, target_node_id, row_limit, scenario=scenario,
            )
            eager_outputs = {k: v for k, v in raw_outputs.items() if v is not None}
            merged = {**prev_outputs, **eager_outputs}
            merged_errors = {**cached["errors"], **errors}
            merged_timings = {**cached["timings"], **timings}
            merged_memory = {**cached["memory_bytes"], **memory_bytes}
            merged_error_lines = {**cached["error_lines"], **error_lines}
            merged_order = list(dict.fromkeys(cached["order"] + order))
            _preview_cache.store(
                fp,
                eager_outputs=merged,
                errors=merged_errors,
                order=merged_order,
                timings=merged_timings,
                memory_bytes=merged_memory,
                error_lines=merged_error_lines,
            )
            eager_outputs = merged
            errors = merged_errors
            timings = merged_timings
            memory_bytes = merged_memory
            error_lines = merged_error_lines
            order = merged_order
    else:
        # Complete cache miss — execute from scratch
        logger.debug(
            "preview_cache_miss",
            fingerprint=fp[:8],
            target=target_node_id,
            prev_fingerprint=(_preview_cache.fingerprint or "")[:8],
        )
        raw_outputs, order, errors, timings, memory_bytes, error_lines = _eager_execute(
            graph, target_node_id, row_limit, scenario=scenario,
        )
        eager_outputs = {k: v for k, v in raw_outputs.items() if v is not None}
        _preview_cache.store(
            fp,
            eager_outputs=eager_outputs,
            errors=errors,
            order=order,
            timings=timings,
            memory_bytes=memory_bytes,
            error_lines=error_lines,
        )

    # Pre-compute schema warnings for instance nodes by comparing the
    # columns available at the instance's inputs vs the original's inputs.
    node_map = graph.node_map
    parents_of = graph.parents_of

    schema_warnings: dict[str, list[SchemaWarning]] = {}
    for nid in order:
        ref = node_map[nid].data.config.get("instanceOf")
        if not ref or ref not in node_map:
            continue
        # Columns feeding into the original node
        orig_input_cols: set[str] = set()
        for pid in parents_of.get(ref, []):
            df = eager_outputs.get(pid)
            if df is not None:
                orig_input_cols.update(df.columns)
        # Columns feeding into the instance node
        inst_input_cols: set[str] = set()
        for pid in parents_of.get(nid, []):
            df = eager_outputs.get(pid)
            if df is not None:
                inst_input_cols.update(df.columns)
        missing = orig_input_cols - inst_input_cols
        if missing:
            schema_warnings[nid] = [
                SchemaWarning(column=c, status="missing") for c in sorted(missing)
            ]

    results: dict[str, NodeResult] = {}
    for nid in order:
        if nid in errors:
            results[nid] = NodeResult(
                status="error",
                error=errors[nid],
                error_line=error_lines.get(nid),
                timing_ms=timings.get(nid, 0),
                memory_bytes=memory_bytes.get(nid, 0),
                schema_warnings=schema_warnings.get(nid, []),
            )
            continue
        df = eager_outputs.get(nid)
        if df is None:
            results[nid] = NodeResult(
                status="error",
                error="No output",
                timing_ms=timings.get(nid, 0),
                memory_bytes=memory_bytes.get(nid, 0),
            )
            continue
        columns = [
            ColumnInfo(name=c, dtype=str(df[c].dtype)) for c in df.columns
        ]
        results[nid] = NodeResult(
            status="ok",
            row_count=len(df),
            column_count=len(df.columns),
            columns=columns,
            preview=df.head(max_preview_rows).to_dicts(),
            timing_ms=timings.get(nid, 0),
            memory_bytes=memory_bytes.get(nid, 0),
            schema_warnings=schema_warnings.get(nid, []),
        )

    error_count = sum(1 for r in results.values() if r.status == "error")
    logger.info(
        "graph_executed",
        node_count=len(results),
        error_count=error_count,
        target=target_node_id,
    )
    return results


def _eager_execute(
    graph: PipelineGraph,
    target_node_id: str | None,
    row_limit: int | None,
    scenario: str = "live",
) -> tuple[
    dict[str, pl.DataFrame | None], list[str],
    dict[str, str], dict[str, float], dict[str, int],
    dict[str, int],
]:
    """Execute the graph eagerly in topo order.

    Returns (outputs, order, errors, timings, memory_bytes, error_lines)
    where errors maps node_id → message for nodes that failed, timings maps
    node_id → execution milliseconds, memory_bytes maps
    node_id → output DataFrame size in bytes, and error_lines maps
    node_id → 1-based line number in user code for the error.
    """
    try:
        preamble_ns = _compile_preamble(graph.preamble or "")
    except PreambleError as exc:
        # Preamble failed — mark every node in the graph as errored so the
        # frontend shows the problem on every node rather than a raw 500.
        order = [n.id for n in graph.nodes]
        err_msg = str(exc)
        errors = {nid: err_msg for nid in order}
        empty: dict[str, pl.DataFrame | None] = {nid: None for nid in order}
        no_timing: dict[str, float] = {}
        no_mem: dict[str, int] = {}
        no_lines: dict[str, int] = {}
        return empty, order, errors, no_timing, no_mem, no_lines

    result = _execute_eager_core(
        graph,
        _build_node_fn,
        target_node_id=target_node_id,
        row_limit=row_limit,
        swallow_errors=True,
        preamble_ns=preamble_ns or None,
        scenario=scenario,
    )
    return (
        result.outputs, result.order, result.errors,
        result.timings, result.memory_bytes, result.error_lines,
    )


def execute_sink(graph: PipelineGraph, sink_node_id: str, scenario: str = "live") -> SinkResponse:
    """Execute the pipeline up to a sink node and write its input to disk.

    Uses Polars streaming sinks (``sink_parquet`` / ``sink_csv``) so the
    full dataset is never materialised in memory at once.  Falls back to
    ``collect(engine="streaming")`` + eager write if the streaming sink raises
    (e.g. when the plan contains an operation that doesn't support the
    streaming engine).

    This is called on-demand (not during normal run/preview).
    Returns a ``SinkResponse`` with row count and output path.
    """
    from pathlib import Path

    sink_node = graph.node_map.get(sink_node_id)
    if not sink_node:
        raise ValueError(f"Sink node '{sink_node_id}' not found")

    config = sink_node.data.config
    path = config.get("path", "")
    fmt = config.get("format", "parquet")

    if not path:
        raise ValueError("Sink node has no output path configured")

    preamble_ns = _compile_preamble(graph.preamble or "")
    lazy_outputs, _order, _parents, _names = _execute_lazy(
        graph,
        _build_node_fn,
        target_node_id=sink_node_id,
        preamble_ns=preamble_ns or None,
        scenario=scenario,
    )

    lf = lazy_outputs.get(sink_node_id)
    if lf is None:
        raise RuntimeError("Failed to compute sink input")

    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    try:
        if fmt == "csv":
            lf.sink_csv(out)
        else:
            lf.sink_parquet(out)
    except (pl.exceptions.ComputeError, pl.exceptions.InvalidOperationError):
        # Fallback: collect with streaming hint, then write eagerly.
        logger.info("sink_streaming_fallback", path=path, format=fmt)
        df = lf.collect(engine="streaming")
        if fmt == "csv":
            df.write_csv(out)
        else:
            df.write_parquet(out)

    # Read back row count cheaply from file metadata.
    if fmt == "csv":
        row_count = pl.scan_csv(out).select(pl.len()).collect().item()
    else:
        row_count = pl.scan_parquet(out).select(pl.len()).collect().item()

    return SinkResponse(
        status="ok",
        message=f"Wrote {row_count:,} rows to {path}",
        row_count=row_count,
        path=path,
        format=fmt,
    )
