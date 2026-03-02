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
import threading
from collections.abc import Callable
from typing import Any

import polars as pl

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


def _compile_preamble(preamble: str) -> dict[str, Any]:
    """Compile user-defined preamble code into a namespace dict.

    The preamble (helper functions, constants, lambdas) is defined at the
    top of a pipeline file between imports and the first ``@pipeline.node``.
    This compiles it once and returns a dict of bindings that can be
    injected into ``_exec_user_code`` via ``extra_ns``.

    Uses a single dict for globals/locals so preamble functions can call
    each other (they share the same ``__globals__``).
    """
    if not preamble or not preamble.strip():
        return {}
    # Preamble may contain imports (e.g. from helpers.features import …)
    # which are legitimate, but still validate against other dangerous
    # patterns (dunder access, eval, exec, etc.).
    validate_user_code(preamble, allow_imports=True)
    # Ensure project root is importable so `from helpers.xxx import …` works
    # even when the server process was spawned by uvicorn reload.
    import os  # noqa: E401
    import sys
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    ns = safe_globals(pl=pl, allow_imports=True)
    base_keys = set(ns.keys())
    exec(preamble, ns)  # noqa: S102  — single dict = shared globals
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

    data = node.data
    node_type = data.nodeType
    config = data.config
    label = data.label
    func_name = _sanitize_func_name(label)

    if source_names is None:
        source_names = []

    if node_type == NodeType.API_INPUT:
        path = config.get("path", "")
        flat_schema = config.get("flattenSchema")

        if path.endswith((".json", ".jsonl")):
            def api_source_fn(_path: str = path, _schema: dict | None = flat_schema) -> _Frame:
                from haute._json_flatten import (
                    _json_cache_path,
                    is_large_json,
                    read_json_flat,
                )

                cache_path = _json_cache_path(_path)
                if is_large_json(_path) and not cache_path.exists():
                    from pathlib import Path

                    size_mb = round(Path(_path).stat().st_size / (1024 * 1024), 1)
                    raise RuntimeError(
                        f"Data file ({size_mb} MB) has not been cached yet. "
                        "Click 'Cache as Parquet' on the API Input node to process it."
                    )
                if cache_path.exists():
                    return pl.scan_parquet(cache_path)
                return read_json_flat(_path, schema=_schema)
        else:
            def api_source_fn() -> _Frame:
                return read_source(path)

        return func_name, api_source_fn, True

    if node_type == NodeType.DATA_SOURCE:
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

    if node_type == NodeType.CONSTANT:
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

    elif node_type == NodeType.LIVE_SWITCH:
        input_scenario_map: dict[str, str] = config.get("input_scenario_map", {})
        input_names = list(source_names)
        _scenario = scenario or "live"

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

    elif node_type == NodeType.DATA_SINK:
        # During normal run/preview, dataSink is a pass-through.
        # Actual writing happens via execute_sink() on explicit user action.
        def sink_passthrough(*dfs: _Frame) -> _Frame:
            return dfs[0] if dfs else pl.LazyFrame()

        return func_name, sink_passthrough, False

    elif node_type == NodeType.EXTERNAL_FILE:
        code = config.get("code", "").strip()
        path = config.get("path", "")
        file_type = config.get("fileType", "pickle")
        model_class = config.get("modelClass", "classifier")
        _src_names = list(source_names)

        _orig_src = list(orig_source_names) if orig_source_names else None
        _in_map = dict(config.get("inputMapping", {})) or None
        _preamble_ext = dict(preamble_ns) if preamble_ns else {}
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

            def external_passthrough(*dfs: _Frame) -> _Frame:
                return dfs[0] if dfs else pl.LazyFrame()

            return func_name, external_passthrough, False

    elif node_type == NodeType.OUTPUT:
        fields = config.get("fields", []) or []

        def output_fn(*dfs: _Frame) -> _Frame:
            lf = dfs[0] if dfs else pl.LazyFrame()
            if fields:
                lf = lf.select(fields)
            return lf

        return func_name, output_fn, False

    elif node_type == NodeType.BANDING:
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

    elif node_type == NodeType.RATING_STEP:
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

    elif node_type == NodeType.SCENARIO_EXPANDER:
        _col_name = config.get("column_name", "scenario_value")
        _min_val = float(config.get("min_value", 0.8))
        _max_val = float(config.get("max_value", 1.2))
        _steps = int(config.get("steps", 21))
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

    elif node_type == NodeType.OPTIMISER:
        # Pass-through in preview mode. Solving happens via /api/optimiser/solve.
        def optimiser_passthrough(*dfs: _Frame) -> _Frame:
            return dfs[0] if dfs else pl.LazyFrame()

        return func_name, optimiser_passthrough, False

    elif node_type == NodeType.OPTIMISER_APPLY:
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

            def optimiser_apply_passthrough(*dfs: _Frame) -> _Frame:
                return dfs[0] if dfs else pl.LazyFrame()

            return func_name, optimiser_apply_passthrough, False

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

    elif node_type == NodeType.MODELLING:
        # Pass-through in preview mode. Training happens via /api/modelling/train.
        def modelling_passthrough(*dfs: _Frame) -> _Frame:
            return dfs[0] if dfs else pl.LazyFrame()

        return func_name, modelling_passthrough, False

    elif node_type == NodeType.MODEL_SCORE:
        source_type = config.get("sourceType", "")
        _run_id = config.get("run_id", "")
        _artifact_path = config.get("artifact_path", "")
        _registered_model = config.get("registered_model", "")
        _version = config.get("version", "latest")
        _task = config.get("task", "regression")
        _output_col = config.get("output_column", "prediction")
        code = config.get("code", "").strip()
        _src_names = list(source_names)

        # If no model source configured, passthrough
        if not source_type or (source_type == "run" and not _run_id) or (
            source_type == "registered" and not _registered_model
        ):

            def model_score_passthrough(*dfs: _Frame) -> _Frame:
                return dfs[0] if dfs else pl.LazyFrame()

            return func_name, model_score_passthrough, False

        def _prepare_predict_frame(
            model: Any, df_eager: pl.DataFrame, features: list[str],
        ) -> Any:
            """Prepare a DataFrame for CatBoost predict — match _build_pool null handling."""
            cat_idx = (
                set(model.get_cat_feature_indices())
                if hasattr(model, "get_cat_feature_indices") else set()
            )
            cat_names = {features[i] for i in cat_idx if i < len(features)}
            numeric_cols = [c for c in features if c not in cat_names]
            cat_cols = [c for c in features if c in cat_names]
            selected = df_eager.select(features)
            if numeric_cols:
                selected = selected.with_columns(
                    [pl.col(c).cast(pl.Float32) for c in numeric_cols]
                )
            if cat_cols:
                selected = selected.with_columns(
                    [pl.col(c).fill_null("_MISSING_").cast(pl.Categorical) for c in cat_cols]
                )
            return selected.to_pandas() if cat_cols else selected.to_numpy()

        def _sink_to_temp(lf: pl.LazyFrame) -> str:
            """Sink a LazyFrame to a temp parquet file via streaming."""
            import os
            import tempfile

            fd, path = tempfile.mkstemp(
                suffix=".parquet", prefix="haute_score_in_",
            )
            os.close(fd)
            try:
                lf.sink_parquet(path)
            except Exception:
                lf.collect(engine="streaming").write_parquet(path)
            return path

        _score_batch = 500_000

        def _batch_score_to_parquet(
            model: Any,
            input_path: str,
            features: list[str],
            output_col: str,
            task: str,
        ) -> str:
            """Score a parquet file in batches, return path to scored output."""
            import os
            import tempfile

            import pyarrow.parquet as pq

            fd, out_path = tempfile.mkstemp(
                suffix=".parquet", prefix="haute_score_out_",
            )
            os.close(fd)

            pf = pq.ParquetFile(input_path)
            writer = None
            want_proba = (
                task == "classification"
                and hasattr(model, "predict_proba")
            )

            try:
                for batch in pf.iter_batches(
                    batch_size=_score_batch,
                ):
                    chunk = pl.from_arrow(batch)
                    x_data = _prepare_predict_frame(
                        model, chunk, features,
                    )
                    preds = model.predict(x_data).flatten()
                    chunk = chunk.with_columns(
                        pl.Series(output_col, preds),
                    )
                    if want_proba:
                        probas = model.predict_proba(x_data)
                        if probas.ndim == 2:
                            probas = probas[:, 1]
                        chunk = chunk.with_columns(
                            pl.Series(
                                f"{output_col}_proba", probas,
                            ),
                        )
                    table = chunk.to_arrow()
                    if writer is None:
                        writer = pq.ParquetWriter(
                            out_path, table.schema,
                        )
                    writer.write_table(table)
                    del chunk, x_data, table
            finally:
                if writer is not None:
                    writer.close()
            return out_path

        def _score_eager(
            model: Any, lf: pl.LazyFrame, features: list[str],
        ) -> pl.LazyFrame:
            """Collect and score in-memory — fast path for preview/trace."""
            df_eager = lf.collect()
            x_data = _prepare_predict_frame(model, df_eager, features)
            preds = model.predict(x_data).flatten()
            df_eager = df_eager.with_columns(
                pl.Series(_output_col, preds),
            )
            if (
                _task == "classification"
                and hasattr(model, "predict_proba")
            ):
                probas = model.predict_proba(x_data)
                if probas.ndim == 2:
                    probas = probas[:, 1]
                df_eager = df_eager.with_columns(
                    pl.Series(f"{_output_col}_proba", probas),
                )
            return df_eager.lazy()

        def _score_batched(
            model: Any, lf: pl.LazyFrame, features: list[str],
        ) -> pl.LazyFrame:
            """Sink → batch score → lazy scan — low-memory path."""
            import atexit
            import os

            input_path = _sink_to_temp(lf)
            scored_path = _batch_score_to_parquet(
                model, input_path, features,
                _output_col, _task,
            )
            atexit.register(
                lambda p=scored_path: os.unlink(p)
                if os.path.exists(p) else None,
            )
            os.unlink(input_path)
            return pl.scan_parquet(scored_path)

        _scenario = scenario or "live"

        def model_score_fn(*dfs: _Frame) -> _Frame:
            from haute._mlflow_io import load_mlflow_model

            model = load_mlflow_model(
                source_type=source_type,
                run_id=_run_id,
                artifact_path=_artifact_path,
                registered_model=_registered_model,
                version=_version,
                task=_task,
            )
            lf = dfs[0] if dfs else pl.LazyFrame()
            available_cols = set(lf.collect_schema().names())
            features = [
                f for f in model.feature_names_
                if f in available_cols
            ]

            # "live" scenario → eager collect+predict (small data, fast)
            # Any other scenario → sink→batch score→scan parquet (large data, low memory)
            if _scenario == "live":
                result_lf = _score_eager(model, lf, features)
            else:
                result_lf = _score_batched(model, lf, features)

            if code:
                result_lf = _exec_user_code(
                    code, _src_names, (result_lf,),
                    extra_ns={"model": model},
                )
            return result_lf

        return func_name, model_score_fn, False

    elif node_type == NodeType.TRANSFORM:
        code = config.get("code", "").strip()
        _src_names = list(source_names)
        _orig_src = list(orig_source_names) if orig_source_names else None
        _in_map = dict(config.get("inputMapping", {})) or None
        _preamble = dict(preamble_ns) if preamble_ns else None

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

            def passthrough(*dfs: _Frame) -> _Frame:
                return dfs[0] if dfs else pl.LazyFrame()

            return func_name, passthrough, False

    else:

        def default_passthrough(*dfs: _Frame) -> _Frame:
            return dfs[0] if dfs else pl.LazyFrame()

        return func_name, default_passthrough, False


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
        chunk_size=artifact.get("chunk_size", 500_000),
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


class _PreviewCache:
    """Thread-safe single-entry cache for the most recent pipeline execution."""

    __slots__ = ("fingerprint", "eager_outputs", "errors", "order", "timings", "_lock")

    def __init__(self) -> None:
        self.fingerprint: str | None = None
        self.eager_outputs: dict[str, pl.DataFrame] = {}
        self.errors: dict[str, str] = {}
        self.order: list[str] = []
        self.timings: dict[str, float] = {}
        self._lock = threading.Lock()

    def invalidate(self) -> None:
        with self._lock:
            self.fingerprint = None
            self.eager_outputs.clear()
            self.errors.clear()
            self.timings.clear()


_preview_cache = _PreviewCache()


def execute_graph(
    graph: PipelineGraph,
    target_node_id: str | None = None,
    row_limit: int | None = None,
    max_preview_rows: int = 100,
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

    # Check if we can extend the cache (same graph, new target is a superset)
    with _preview_cache._lock:
        if fp == _preview_cache.fingerprint and _preview_cache.eager_outputs:
            cached = _preview_cache.eager_outputs
            if target_node_id is None or target_node_id in cached:
                eager_outputs = cached
                order = _preview_cache.order
                errors = _preview_cache.errors
                timings = _preview_cache.timings
            else:
                raw_outputs, order, errors, timings = _eager_execute(
                    graph, target_node_id, row_limit, scenario=scenario,
                )
                eager_outputs = {k: v for k, v in raw_outputs.items() if v is not None}
                merged = {**cached, **eager_outputs}
                _preview_cache.eager_outputs = merged
                _preview_cache.errors = {**_preview_cache.errors, **errors}
                _preview_cache.timings = {**_preview_cache.timings, **timings}
                _preview_cache.order = list(
                    dict.fromkeys(_preview_cache.order + order),
                )
                eager_outputs = merged
                errors = _preview_cache.errors
                timings = _preview_cache.timings
                order = _preview_cache.order
        else:
            raw_outputs, order, errors, timings = _eager_execute(
                graph, target_node_id, row_limit, scenario=scenario,
            )
            eager_outputs = {k: v for k, v in raw_outputs.items() if v is not None}
            _preview_cache.fingerprint = fp
            _preview_cache.eager_outputs = eager_outputs
            _preview_cache.errors = errors
            _preview_cache.timings = timings
            _preview_cache.order = order

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
                timing_ms=timings.get(nid, 0),
                schema_warnings=schema_warnings.get(nid, []),
            )
            continue
        df = eager_outputs.get(nid)
        if df is None:
            results[nid] = NodeResult(
                status="error",
                error="No output",
                timing_ms=timings.get(nid, 0),
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
) -> tuple[dict[str, pl.DataFrame | None], list[str], dict[str, str], dict[str, float]]:
    """Execute the graph eagerly in topo order.

    Returns (outputs, order, errors, timings) where errors maps
    node_id → message for nodes that failed, and timings maps
    node_id → execution milliseconds.
    """
    preamble_ns = _compile_preamble(graph.preamble or "")
    result = _execute_eager_core(
        graph,
        _build_node_fn,
        target_node_id=target_node_id,
        row_limit=row_limit,
        swallow_errors=True,
        preamble_ns=preamble_ns or None,
        scenario=scenario,
    )
    return result.outputs, result.order, result.errors, result.timings


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
    except Exception:
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
