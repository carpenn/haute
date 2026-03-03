"""Code generator: graph JSON → valid pipeline .py file."""

from __future__ import annotations

from collections.abc import Callable

from haute._config_io import config_path_for_node, has_config_folder
from haute._logging import get_logger
from haute._types import (
    MODELLING_CONFIG_KEYS,
    OPTIMISER_APPLY_CONFIG_KEYS,
    OPTIMISER_CONFIG_KEYS,
    SCENARIO_EXPANDER_CONFIG_KEYS,
)
from haute.graph_utils import (
    GraphEdge,
    GraphNode,
    NodeType,
    PipelineGraph,
    _sanitize_func_name,
    build_instance_mapping,
    topo_sort_ids,
)

logger = get_logger(component="codegen")

__all__ = [
    "graph_to_code",
    "graph_to_code_multi",
]


def _build_extra_kwargs(config: dict, keys: tuple[str, ...]) -> list[str]:
    """Build ``"key={value!r}"`` decorator kwarg strings for present config keys.

    Skips keys whose value is ``None``, ``""``, or ``[]``.
    """
    parts: list[str] = []
    for key in keys:
        val = config.get(key)
        if val is not None and val != "" and val != []:
            parts.append(f"{key}={val!r}")
    return parts


def _build_params(source_names: list[str]) -> str:
    """Build the function parameter string from upstream node names."""
    if source_names:
        return ", ".join(f"{s}: pl.LazyFrame" for s in source_names)
    return "df: pl.LazyFrame"


# Template fragments for each node type


def _api_input_template(path: str) -> str:
    """Return the API input template string for the given file path.

    JSON/JSONL files use ``read_json_flat``, CSV uses ``scan_csv``,
    everything else (parquet / flat) uses ``scan_parquet``.
    """
    if path.endswith((".json", ".jsonl")):
        body = (
            '    from haute._json_flatten import read_json_flat\n'
            '    return read_json_flat("{path}", config_path="{config_path}")'
        )
    elif path.endswith(".csv"):
        body = '    return pl.scan_csv("{path}")'
    else:
        body = '    return pl.scan_parquet("{path}")'

    return (
        '@pipeline.node(api_input=True, path="{path}"{row_id_kw})\n'
        'def {func_name}() -> pl.LazyFrame:\n'
        '    """{description}"""\n'
        + body + '\n'
    )

_LIVE_SWITCH = '''\
@pipeline.node(live_switch=True, input_scenario_map={input_scenario_map_repr})
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    return {active_param}
'''

_SOURCE_FLAT_FILE = '''\
@pipeline.node(path="{path}")
def {func_name}() -> pl.LazyFrame:
    """{description}"""
    return pl.scan_parquet("{path}")
'''

_SOURCE_CSV = '''\
@pipeline.node(path="{path}")
def {func_name}() -> pl.LazyFrame:
    """{description}"""
    return pl.scan_csv("{path}")
'''

_SOURCE_DATABRICKS = '''\
@pipeline.node(table="{table}"{http_path_kw}{query_kw})
def {func_name}() -> pl.LazyFrame:
    """{description}"""
    from haute._databricks_io import read_cached_table
    return read_cached_table("{table}")
'''

_MODEL_SCORE = '''\
@pipeline.node({decorator_kwargs})
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    import atexit
    import os
    import tempfile

    import pyarrow.parquet as pq

    from haute.graph_utils import load_mlflow_model

    lf = {first_param}
    model = load_mlflow_model({load_kwargs})
    features = [
        f for f in model.feature_names_
        if f in set(lf.collect_schema().names())
    ]

    # Null handling: float32 for numerics (null→NaN), sentinel for cats
    cat_idx = (
        set(model.get_cat_feature_indices())
        if hasattr(model, "get_cat_feature_indices") else set()
    )
    cat_names = {{features[i] for i in cat_idx if i < len(features)}}
    _num = [c for c in features if c not in cat_names]
    _cat = [c for c in features if c in cat_names]

    def _prepare(chunk: pl.DataFrame):
        sel = chunk.select(features)
        if _num:
            sel = sel.with_columns(
                [pl.col(c).cast(pl.Float32) for c in _num]
            )
        if _cat:
            sel = sel.with_columns(
                [pl.col(c).fill_null("_MISSING_").cast(pl.Categorical)
                 for c in _cat]
            )
        return sel.to_pandas() if _cat else sel.to_numpy()

    # 1. Sink upstream to temp parquet (streaming — low memory)
    fd1, in_path = tempfile.mkstemp(suffix=".parquet", prefix="haute_score_in_")
    os.close(fd1)
    try:
        lf.sink_parquet(in_path)
    except Exception:
        lf.collect(engine="streaming").write_parquet(in_path)

    # 2. Score in batches → scored parquet
    fd2, out_path = tempfile.mkstemp(suffix=".parquet", prefix="haute_score_out_")
    os.close(fd2)
    atexit.register(lambda p=out_path: os.unlink(p) if os.path.exists(p) else None)
    pf = pq.ParquetFile(in_path)
    writer = None
    try:
        for batch in pf.iter_batches(batch_size=500_000):
            chunk = pl.from_arrow(batch)
            X = _prepare(chunk)
            preds = model.predict(X).flatten()
            chunk = chunk.with_columns(pl.Series("{output_column}", preds))
{proba_block_batched}            table = chunk.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema)
            writer.write_table(table)
            del chunk, X, table
    finally:
        if writer is not None:
            writer.close()
    os.unlink(in_path)

    result = pl.scan_parquet(out_path)
{user_code_block}    return result
'''

_MODEL_SCORE_USER_CODE_SENTINEL = "# -- user code --"

_BANDING_SINGLE = '''\
@pipeline.node(banding="{banding}", column="{column}",
               output_column="{output_column}"{rules_kw}{default_kw})
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    return df
'''

_BANDING_MULTI = '''\
@pipeline.node(factors={factors_repr})
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    return df
'''

_RATING_STEP = '''\
@pipeline.node(tables={tables_repr}{extra_kwargs})
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    return df
'''

_SINK_PARQUET = '''\
@pipeline.node(sink="{path}", format="parquet")
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    {first}.collect().write_parquet("{path}")
    return {first}
'''

_SINK_CSV = '''\
@pipeline.node(sink="{path}", format="csv")
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    {first}.collect().write_csv("{path}")
    return {first}
'''

_SCENARIO_EXPANDER = '''\
@pipeline.node(scenario_expander=True{extra_kwargs})
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    return {first}
'''

_OPTIMISER = '''\
@pipeline.node(optimiser=True{extra_kwargs})
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    return {first}
'''

_OPTIMISER_APPLY = '''\
@pipeline.node(optimiser_apply=True{extra_kwargs})
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    return {first}
'''

_MODELLING = '''\
@pipeline.node(modelling=True{extra_kwargs})
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    return df
'''

_CONSTANT = '''\
@pipeline.node(constant=True, values={values_repr})
def {func_name}() -> pl.LazyFrame:
    """{description}"""
    return pl.LazyFrame({data_dict})
'''

_EXTERNAL_PICKLE = '''\
@pipeline.node(external="{path}", file_type="pickle")
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    from haute.graph_utils import load_external_object
    obj = load_external_object("{path}", "pickle")
{body}
'''

_EXTERNAL_JSON = '''\
@pipeline.node(external="{path}", file_type="json")
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    from haute.graph_utils import load_external_object
    obj = load_external_object("{path}", "json")
{body}
'''

_EXTERNAL_JOBLIB = '''\
@pipeline.node(external="{path}", file_type="joblib")
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    from haute.graph_utils import load_external_object
    obj = load_external_object("{path}", "joblib")
{body}
'''

_EXTERNAL_CATBOOST = '''\
@pipeline.node(external="{path}", file_type="catboost", model_class="{model_class}")
def {func_name}({params}) -> pl.LazyFrame:
    """{description}"""
    from haute.graph_utils import load_external_object
    obj = load_external_object("{path}", "catboost", "{model_class}")
{body}
'''


def _wrap_external_code(code: str) -> str:
    """Wrap external file user code: indent each line and append ``return df``.

    Unlike transforms, external file code is multi-statement - the user
    is responsible for assigning a Polars DataFrame to ``df``.
    """
    code = code.strip()
    if not code:
        return "    return df"
    indented = "\n".join(f"    {line}" for line in code.splitlines())
    return f"{indented}\n    return df"


def _wrap_user_code(code: str, source_names: list[str]) -> str:
    """Wrap user code into indented function body lines.

    Rules:
    - If code starts with '.', it's a chain → wrap as `df = (<first_input>\\n<code>\\n)`
    - If code already contains a statement (``df =``, ``return``, etc.), indent it as-is.
    - Otherwise it's a bare expression → wrap as `df = <code>`
    """
    code = code.strip()
    if not code:
        first = source_names[0] if source_names else "df"
        return f"    return {first}"

    if code.startswith("."):
        # Chain syntax: .filter(...).select(...)
        first = source_names[0] if source_names else "df"
        chain_indented = "\n".join(f"        {line}" for line in code.splitlines())
        return f"    df = (\n        {first}\n{chain_indented}\n    )\n    return df"

    # Code that already contains an assignment or return — indent as-is
    first_line = code.split("\n", 1)[0]
    if "=" in first_line.split("(", 1)[0] or first_line.startswith("return "):
        indented = "\n".join(f"    {line}" for line in code.splitlines())
        return f"{indented}\n    return df"

    # Bare expression: wrap as `df = (<code>)`
    indented = "\n".join(f"    {line}" for line in code.splitlines())
    return f"    df = (\n{indented}\n    )\n    return df"


def _node_to_code(node: GraphNode, source_names: list[str] | None = None) -> str:
    """Generate code for a single node.

    Delegates to ``_generate_node_code`` for the type-specific body, then
    replaces the decorator line with a ``config=`` file reference for node
    types that use external JSON config files.
    """
    code = _generate_node_code(node, source_names)

    node_type = node.data.nodeType
    if has_config_folder(node_type):
        func_name = _sanitize_func_name(node.data.label)
        cfg_path = str(config_path_for_node(node_type, func_name))
        try:
            def_idx = code.index("\ndef ")
            code = f'@pipeline.node(config="{cfg_path}")' + code[def_idx:]
        except ValueError:
            logger.warning("no_def_in_generated_code", node=node.data.label)
    return code


# ---------------------------------------------------------------------------
# Codegen dispatch table — mirrors _NODE_BUILDERS in executor.py
# ---------------------------------------------------------------------------

#: Builder signature: (node, source_names) -> generated Python code string.
CodegenBuilder = Callable[[GraphNode, list[str]], str]

_CODEGEN_BUILDERS: dict[NodeType, CodegenBuilder] = {}


def _register_codegen(node_type: NodeType) -> Callable[[CodegenBuilder], CodegenBuilder]:
    """Decorator to register a codegen builder for a given NodeType."""
    def decorator(fn: CodegenBuilder) -> CodegenBuilder:
        _CODEGEN_BUILDERS[node_type] = fn
        return fn
    return decorator


# ---------------------------------------------------------------------------
# Per-type builders
# ---------------------------------------------------------------------------


@_register_codegen(NodeType.API_INPUT)
def _gen_api_input(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    path = config.get("path", "")
    row_id_kw = ""
    if config.get("row_id_column"):
        row_id_kw = f', row_id_column="{config["row_id_column"]}"'
    cfg_path = str(config_path_for_node(data.nodeType, func_name))
    template = _api_input_template(path)
    return template.format(
        func_name=func_name,
        description=description,
        path=path,
        row_id_kw=row_id_kw,
        config_path=cfg_path,
    )


@_register_codegen(NodeType.LIVE_SWITCH)
def _gen_live_switch(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    params = ", ".join(f"{s}: pl.LazyFrame" for s in source_names)
    input_scenario_map: dict[str, str] = config.get("input_scenario_map", {})
    first_param = source_names[0] if source_names else "df"
    # Generated code always routes to the "live" input
    active_param = first_param
    for inp, scn in input_scenario_map.items():
        if scn == "live" and inp in source_names:
            active_param = inp
            break
    return _LIVE_SWITCH.format(
        func_name=func_name,
        description=description,
        params=params,
        input_scenario_map_repr=repr(input_scenario_map),
        active_param=active_param,
    )


@_register_codegen(NodeType.DATA_SOURCE)
def _gen_data_source(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    path = config.get("path", "")
    source_type = config.get("sourceType", "flat_file")
    if source_type == "databricks":
        table = config.get("table", "catalog.schema.table")
        http_path = config.get("http_path", "")
        http_path_kw = f', http_path="{http_path}"' if http_path else ""
        query = config.get("query", "")
        query_kw = f', query="{query}"' if query else ""
        return _SOURCE_DATABRICKS.format(
            func_name=func_name,
            description=description,
            table=table,
            http_path_kw=http_path_kw,
            query_kw=query_kw,
        )
    elif path.endswith(".csv"):
        return _SOURCE_CSV.format(
            func_name=func_name,
            description=description,
            path=path,
        )
    else:
        return _SOURCE_FLAT_FILE.format(
            func_name=func_name,
            description=description,
            path=path,
        )


@_register_codegen(NodeType.CONSTANT)
def _gen_constant(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    raw_values = config.get("values", []) or []
    # Build the repr for the decorator kwarg
    values_repr = repr([
        {"name": v.get("name", ""), "value": v.get("value", "")}
        for v in raw_values
    ])
    # Build a dict literal for the LazyFrame constructor
    data_pairs: list[str] = []
    for v in raw_values:
        name = v.get("name", "col")
        val = v.get("value", "")
        # Try numeric coercion for the code literal
        try:
            num = float(val)
            data_pairs.append(f'"{name}": [{num!r}]')
        except (ValueError, TypeError):
            data_pairs.append(f'"{name}": ["{val}"]')
    data_dict = "{" + ", ".join(data_pairs) + "}" if data_pairs else '{"constant": [0]}'
    return _CONSTANT.format(
        func_name=func_name,
        description=description,
        values_repr=values_repr,
        data_dict=data_dict,
    )


@_register_codegen(NodeType.MODEL_SCORE)
def _gen_model_score(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    source_type = config.get("sourceType", "run")
    task_val = config.get("task", "regression")
    output_column = config.get("output_column", "prediction")
    user_code = (config.get("code") or "").strip()
    params = _build_params(source_names)
    first_param = source_names[0] if source_names else "df"

    proba_block_batched = ""
    if task_val == "classification":
        proba_block_batched = (
            f'            if hasattr(model, "predict_proba"):\n'
            f"                probas = model.predict_proba(X)\n"
            f"                if probas.ndim == 2:\n"
            f"                    probas = probas[:, 1]\n"
            f"                chunk = chunk.with_columns(\n"
            f'                    pl.Series("{output_column}_proba",'
            f" probas))\n"
        )

    user_code_block = ""
    if user_code:
        wrapped = _wrap_external_code(user_code).rstrip("\n") + "\n"
        # Replace the trailing "    return df" that _wrap_external_code appends
        if wrapped.rstrip().endswith("return df"):
            wrapped = wrapped.rstrip()
            wrapped = wrapped[: wrapped.rfind("return df")].rstrip() + "\n"
        user_code_block = f"    {_MODEL_SCORE_USER_CODE_SENTINEL}\n{wrapped}"

    if source_type == "registered":
        reg_model = config.get("registered_model", "")
        ver = config.get("version", "latest")
        decorator_kwargs = (
            f'model_score=True, source_type="registered", '
            f'registered_model="{reg_model}", version="{ver}", '
            f'task="{task_val}", output_column="{output_column}"'
        )
        load_kwargs = (
            f'source_type="registered", registered_model="{reg_model}", '
            f'version="{ver}", task="{task_val}"'
        )
    else:
        rid = config.get("run_id", "")
        apath = config.get("artifact_path", "")
        rname = config.get("run_name", "")
        exp_name = config.get("experiment_name", "")
        exp_id = config.get("experiment_id", "")
        decorator_kwargs = (
            f'model_score=True, source_type="run", '
            f'run_id="{rid}", artifact_path="{apath}", '
            f'task="{task_val}", output_column="{output_column}"'
        )
        if rname:
            decorator_kwargs += f', run_name="{rname}"'
        if exp_name:
            decorator_kwargs += f', experiment_name="{exp_name}"'
        if exp_id:
            decorator_kwargs += f', experiment_id="{exp_id}"'
        load_kwargs = (
            f'source_type="run", run_id="{rid}", '
            f'artifact_path="{apath}", task="{task_val}"'
        )

    return _MODEL_SCORE.format(
        func_name=func_name,
        description=description,
        params=params,
        first_param=first_param,
        decorator_kwargs=decorator_kwargs,
        load_kwargs=load_kwargs,
        output_column=output_column,
        proba_block_batched=proba_block_batched,
        user_code_block=user_code_block,
    )


@_register_codegen(NodeType.BANDING)
def _gen_banding(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    factors = config.get("factors", []) or []
    params = _build_params(source_names)
    if len(factors) == 1:
        f = factors[0]
        banding = f.get("banding", "continuous")
        column = f.get("column", "")
        output_column = f.get("outputColumn", "")
        rules = f.get("rules", []) or []
        default = f.get("default")
        rules_kw = f", rules={rules!r}" if rules else ""
        default_kw = f', default="{default}"' if default else ""
        return _BANDING_SINGLE.format(
            func_name=func_name,
            description=description,
            banding=banding,
            column=column,
            output_column=output_column,
            rules_kw=rules_kw,
            default_kw=default_kw,
            params=params,
        )
    else:
        # Multi-factor: emit factors list with output_column key for decorator
        emit_factors = []
        for f in factors:
            ef: dict = {
                "banding": f.get("banding", "continuous"),
                "column": f.get("column", ""),
                "output_column": f.get("outputColumn", ""),
                "rules": f.get("rules", []),
            }
            if f.get("default"):
                ef["default"] = f["default"]
            emit_factors.append(ef)
        return _BANDING_MULTI.format(
            func_name=func_name,
            description=description,
            factors_repr=repr(emit_factors),
            params=params,
        )


@_register_codegen(NodeType.RATING_STEP)
def _gen_rating_step(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    tables = config.get("tables", []) or []
    params = _build_params(source_names)
    emit_tables = []
    for t in tables:
        et: dict = {
            "name": t.get("name", ""),
            "factors": t.get("factors", []),
            "output_column": t.get("outputColumn", ""),
            "entries": t.get("entries", []),
        }
        if t.get("defaultValue") is not None:
            et["default_value"] = t["defaultValue"]
        emit_tables.append(et)
    extra_parts: list[str] = []
    op = config.get("operation")
    if op and op != "multiply":
        extra_parts.append(f"operation={op!r}")
    combined = config.get("combinedColumn")
    if combined:
        extra_parts.append(f"combined_column={combined!r}")
    extra_kwargs = (", " + ", ".join(extra_parts)) if extra_parts else ""
    return _RATING_STEP.format(
        func_name=func_name,
        description=description,
        tables_repr=repr(emit_tables),
        params=params,
        extra_kwargs=extra_kwargs,
    )


@_register_codegen(NodeType.SCENARIO_EXPANDER)
def _gen_scenario_expander(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    params = _build_params(source_names)
    first = source_names[0] if source_names else "df"
    extra_parts = _build_extra_kwargs(config, SCENARIO_EXPANDER_CONFIG_KEYS)
    extra_kwargs = (", " + ", ".join(extra_parts)) if extra_parts else ""
    return _SCENARIO_EXPANDER.format(
        func_name=func_name,
        description=description,
        params=params,
        first=first,
        extra_kwargs=extra_kwargs,
    )


@_register_codegen(NodeType.OPTIMISER)
def _gen_optimiser(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    params = _build_params(source_names)
    first = source_names[0] if source_names else "df"
    extra_parts = _build_extra_kwargs(config, OPTIMISER_CONFIG_KEYS)
    extra_kwargs = (", " + ", ".join(extra_parts)) if extra_parts else ""
    return _OPTIMISER.format(
        func_name=func_name,
        description=description,
        params=params,
        first=first,
        extra_kwargs=extra_kwargs,
    )


@_register_codegen(NodeType.OPTIMISER_APPLY)
def _gen_optimiser_apply(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    params = _build_params(source_names)
    first = source_names[0] if source_names else "df"
    extra_parts = _build_extra_kwargs(config, OPTIMISER_APPLY_CONFIG_KEYS)
    extra_kwargs = (", " + ", ".join(extra_parts)) if extra_parts else ""
    return _OPTIMISER_APPLY.format(
        func_name=func_name,
        description=description,
        params=params,
        first=first,
        extra_kwargs=extra_kwargs,
    )


@_register_codegen(NodeType.MODELLING)
def _gen_modelling(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    params = _build_params(source_names)
    extra_parts = _build_extra_kwargs(config, MODELLING_CONFIG_KEYS)
    extra_kwargs = (", " + ", ".join(extra_parts)) if extra_parts else ""
    return _MODELLING.format(
        func_name=func_name,
        description=description,
        params=params,
        extra_kwargs=extra_kwargs,
    )


@_register_codegen(NodeType.EXTERNAL_FILE)
def _gen_external_file(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    path = config.get("path", "model.pkl")
    file_type = config.get("fileType", "pickle")
    code = (config.get("code") or "").strip()
    params = _build_params(source_names)
    body = _wrap_external_code(code)
    if file_type == "catboost":
        model_class = config.get("modelClass", "classifier")
        cb_class = "CatBoostRegressor" if model_class == "regressor" else "CatBoostClassifier"
        return _EXTERNAL_CATBOOST.format(
            func_name=func_name,
            description=description,
            path=path,
            params=params,
            body=body,
            model_class=model_class,
            cb_class=cb_class,
        )
    templates = {
        "pickle": _EXTERNAL_PICKLE,
        "json": _EXTERNAL_JSON,
        "joblib": _EXTERNAL_JOBLIB,
    }
    template = templates.get(file_type, _EXTERNAL_PICKLE)
    return template.format(
        func_name=func_name,
        description=description,
        path=path,
        params=params,
        body=body,
    )


@_register_codegen(NodeType.DATA_SINK)
def _gen_data_sink(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    path = config.get("path", "output.parquet")
    fmt = config.get("format", "parquet")
    params = _build_params(source_names)
    first = source_names[0] if source_names else "df"
    template = _SINK_CSV if fmt == "csv" else _SINK_PARQUET
    return template.format(
        func_name=func_name,
        description=description,
        path=path,
        params=params,
        first=first,
    )


@_register_codegen(NodeType.OUTPUT)
def _gen_output(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    fields = config.get("fields", []) or []
    params = _build_params(source_names)
    first = source_names[0] if source_names else "df"
    dec_parts = ["output=True"]
    if fields:
        dec_parts.append(f"fields={fields!r}")
        select_args = ", ".join(f'"{f}"' for f in fields)
        body = f"    return {first}.select({select_args})"
    else:
        body = f"    return {first}"
    dec = ", ".join(dec_parts)
    return (
        f"@pipeline.node({dec})\n"
        f"def {func_name}({params}) -> pl.LazyFrame:\n"
        f'    """{description}"""\n'
        f"{body}\n"
    )


@_register_codegen(NodeType.TRANSFORM)
def _gen_transform(node: GraphNode, source_names: list[str]) -> str:
    data = node.data
    config = data.config
    func_name = _sanitize_func_name(data.label)
    description = data.description or f"{data.label} node"

    code = (config.get("code") or "").strip()
    params = _build_params(source_names)
    body = _wrap_user_code(code, source_names)

    return (
        f"@pipeline.node\n"
        f"def {func_name}({params}) -> pl.LazyFrame:\n"
        f'    """{description}"""\n'
        f"{body}\n"
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def _generate_node_code(node: GraphNode, source_names: list[str] | None = None) -> str:
    """Generate code for a single node (with legacy inline decorator).

    source_names: sanitized function names of upstream nodes (used as param names).
    """
    if source_names is None:
        source_names = []

    builder = _CODEGEN_BUILDERS.get(node.data.nodeType)
    if builder is not None:
        return builder(node, source_names)

    # Fallback: treat unknown types as transforms
    return _gen_transform(node, source_names)


def _instance_to_code(
    node: GraphNode,
    original_func_name: str,
    source_names: list[str] | None = None,
    orig_source_names: list[str] | None = None,
) -> str:
    """Generate code for an instance node that delegates to the original function.

    When *orig_source_names* is provided the wrapper emits keyword arguments so
    that each original parameter receives the correct instance input regardless
    of edge ordering.
    """
    data = node.data
    label = data.label
    description = data.description or f"Instance of {original_func_name}"
    func_name = _sanitize_func_name(label)

    if source_names is None:
        source_names = []

    params = _build_params(source_names)

    # Prefer explicit inputMapping from config (set via the UI)
    explicit_map = data.config.get("inputMapping")

    if orig_source_names and source_names:
        explicit = dict(explicit_map) if explicit_map and isinstance(explicit_map, dict) else None
        mapping = build_instance_mapping(orig_source_names, source_names, explicit)
        args = ", ".join(f"{orig}={mapping[orig]}" for orig in orig_source_names if orig in mapping)
    else:
        args = ", ".join(source_names) if source_names else "df"

    return (
        f'@pipeline.node(instance_of="{original_func_name}")\n'
        f"def {func_name}({params}) -> pl.LazyFrame:\n"
        f'    """{description}"""\n'
        f"    return {original_func_name}({args})\n"
    )


def _topo_sort(nodes: list[GraphNode], edges: list[GraphEdge]) -> list[GraphNode]:
    """Sort nodes in topological order based on edges."""
    node_map = {n.id: n for n in nodes}
    order = topo_sort_ids(list(node_map.keys()), edges)
    return [node_map[nid] for nid in order if nid in node_map]


def _emit_preserved_blocks(preserved_blocks: list[str]) -> list[str]:
    """Wrap each preserved block in start/end markers and return as lines."""
    lines: list[str] = []
    for block in preserved_blocks:
        lines.append("# haute:preserve-start")
        lines.append(block)
        lines.append("# haute:preserve-end")
        lines.append("")
    return lines


def graph_to_code(
    graph: PipelineGraph,
    pipeline_name: str = "main",
    description: str = "",
    preamble: str = "",
    preserved_blocks: list[str] | None = None,
) -> str:
    """Convert a React Flow graph to a valid haute pipeline .py file."""
    nodes = graph.nodes
    edges = graph.edges

    sorted_nodes = _topo_sort(nodes, edges)

    # Build a map from node id → function name
    id_to_func: dict[str, str] = {}
    for node in sorted_nodes:
        id_to_func[node.id] = _sanitize_func_name(node.data.label)

    lines = [
        f'"""Pipeline: {pipeline_name}"""',
        "",
        "import polars as pl",
        "import haute",
    ]

    # User-defined preamble (extra imports, helpers, constants)
    if preamble.strip():
        lines.append("")
        lines.append(preamble.rstrip())

    lines += [
        "",
        f'pipeline = haute.Pipeline("{pipeline_name}", description={description!r})',
        "",
        "",
    ]

    # Preserved blocks (user code that survives GUI regeneration)
    all_preserved = preserved_blocks if preserved_blocks is not None else graph.preserved_blocks
    if all_preserved:
        lines.extend(_emit_preserved_blocks(all_preserved))
        lines.append("")

    # Build source names per node from edges (ordered list of upstream func names)
    node_sources: dict[str, list[str]] = {}
    for edge in edges:
        src_name = id_to_func.get(edge.source, edge.source)
        node_sources.setdefault(edge.target, []).append(src_name)

    # Partition: emit originals before instances to ensure the function exists
    # when the instance wrapper calls it.
    instance_of_map: dict[str, str] = {}
    for node in sorted_nodes:
        ref = node.data.config.get("instanceOf")
        if ref:
            instance_of_map[node.id] = ref

    originals = [n for n in sorted_nodes if n.id not in instance_of_map]
    instances = [n for n in sorted_nodes if n.id in instance_of_map]

    for node in originals:
        source_names = node_sources.get(node.id, [])
        lines.append(_node_to_code(node, source_names=source_names))
        lines.append("")

    for node in instances:
        source_names = node_sources.get(node.id, [])
        orig_id = instance_of_map[node.id]
        orig_func = id_to_func.get(orig_id, orig_id)
        orig_src = node_sources.get(orig_id, [])
        lines.append(_instance_to_code(
            node, orig_func,
            source_names=source_names,
            orig_source_names=orig_src,
        ))
        lines.append("")

    # Emit edges as pipeline.connect() calls
    if edges:
        lines.append("")
        lines.append("# Wire nodes together - edges define data flow")
        for edge in edges:
            src_func = id_to_func.get(edge.source, edge.source)
            tgt_func = id_to_func.get(edge.target, edge.target)
            lines.append(f'pipeline.connect("{src_func}", "{tgt_func}")')
        lines.append("")

    logger.info("code_generated", pipeline_name=pipeline_name, node_count=len(sorted_nodes))
    return "\n".join(lines)


def _submodel_node_to_code(node: GraphNode, source_names: list[str] | None = None) -> str:
    """Generate code for a single node inside a submodel file.

    Identical to ``_node_to_code`` but uses ``@submodel.node`` instead of
    ``@pipeline.node``.
    """
    code = _node_to_code(node, source_names=source_names)
    return code.replace("@pipeline.node", "@submodel.node", 1)


def graph_to_code_multi(
    graph: PipelineGraph,
    pipeline_name: str = "main",
    description: str = "",
    preamble: str = "",
    source_file: str = "",
    preserved_blocks: list[str] | None = None,
) -> dict[str, str]:
    """Generate code for a pipeline with submodels.

    Returns a dict mapping relative file path → generated Python code.
    E.g. ``{"main.py": "...", "modules/model_scoring.py": "..."}``.

    If the graph has no submodels, the result contains only the main file.
    """
    submodels = graph.submodels or {}

    if not submodels:
        # No submodels — single-file output
        main_key = source_file or f"{pipeline_name}.py"
        code = graph_to_code(graph, pipeline_name, description, preamble, preserved_blocks)
        return {main_key: code}

    # ── Separate nodes into root-level vs submodel children ──────────
    all_child_ids: set[str] = set()
    submodel_node_ids: set[str] = set()
    for sm_name, sm_meta in submodels.items():
        all_child_ids.update(sm_meta.get("childNodeIds", []))
        submodel_node_ids.add(f"submodel__{sm_name}")

    nodes = graph.nodes
    edges = graph.edges

    # Root-level nodes: not children and not the submodel placeholder itself
    root_nodes = [
        n for n in nodes
        if n.id not in all_child_ids and n.id not in submodel_node_ids
    ]

    # Root-level edges: only between root-level nodes OR crossing submodel boundary
    root_node_ids = {n.id for n in root_nodes}

    # Build id → func_name for root nodes (needed by submodel cross-boundary resolution)
    root_id_to_func: dict[str, str] = {}
    for node in root_nodes:
        root_id_to_func[node.id] = _sanitize_func_name(node.data.label)

    # ── Generate submodel files ──────────────────────────────────────
    files: dict[str, str] = {}

    for sm_name, sm_meta in submodels.items():
        sm_graph = sm_meta.get("graph", {})
        sm_file = sm_meta.get("file", f"modules/{sm_name}.py")
        raw_nodes = sm_graph.get("nodes", [])
        raw_edges = sm_graph.get("edges", [])
        sm_nodes = [
            GraphNode.model_validate(n) if isinstance(n, dict) else n
            for n in raw_nodes
        ]
        sm_edges = [
            GraphEdge.model_validate(e) if isinstance(e, dict) else e
            for e in raw_edges
        ]

        sorted_sm_nodes = _topo_sort(sm_nodes, sm_edges)

        # Build id → func_name map for submodel nodes
        sm_id_to_func: dict[str, str] = {}
        for node in sorted_sm_nodes:
            sm_id_to_func[node.id] = _sanitize_func_name(node.data.label)

        # Build source names per node (internal edges)
        sm_node_sources: dict[str, list[str]] = {}
        for edge in sm_edges:
            src_name = sm_id_to_func.get(edge.source, edge.source)
            sm_node_sources.setdefault(edge.target, []).append(src_name)

        # Also include cross-boundary inputs from parent graph edges
        sm_node_id = f"submodel__{sm_name}"
        sm_child_ids = {n.id for n in sm_nodes}
        for edge in edges:
            if edge.target == sm_node_id and edge.targetHandle:
                child_id = edge.targetHandle.removeprefix("in__")
                if child_id in sm_child_ids:
                    src_name = root_id_to_func.get(edge.source, _sanitize_func_name(edge.source))
                    sm_node_sources.setdefault(child_id, []).append(src_name)

        sm_lines = [
            f'"""Submodel: {sm_name}"""',
            "",
            "import polars as pl",
            "import haute",
            "",
            "",
            f'submodel = haute.Submodel("{sm_name}")',
            "",
            "",
        ]

        for node in sorted_sm_nodes:
            source_names = sm_node_sources.get(node.id, [])
            sm_lines.append(_submodel_node_to_code(node, source_names=source_names))
            sm_lines.append("")

        # Emit submodel.connect() calls for internal edges
        if sm_edges:
            sm_lines.append("")
            for edge in sm_edges:
                src_func = sm_id_to_func.get(edge.source, edge.source)
                tgt_func = sm_id_to_func.get(edge.target, edge.target)
                sm_lines.append(f'submodel.connect("{src_func}", "{tgt_func}")')
            sm_lines.append("")

        files[sm_file] = "\n".join(sm_lines)

    # ── Generate main pipeline file ──────────────────────────────────

    sorted_root = _topo_sort(root_nodes, [
        e for e in edges
        if e.source in root_node_ids and e.target in root_node_ids
    ]) if root_nodes else []

    # Also map submodel child node IDs to func names (for edge generation)
    for sm_name, sm_meta in submodels.items():
        sm_graph = sm_meta.get("graph", {})
        for n in sm_graph.get("nodes", []):
            nd = GraphNode.model_validate(n) if isinstance(n, dict) else n
            root_id_to_func[nd.id] = _sanitize_func_name(nd.data.label)

    main_lines = [
        f'"""Pipeline: {pipeline_name}"""',
        "",
        "import polars as pl",
        "import haute",
    ]

    if preamble.strip():
        main_lines.append("")
        main_lines.append(preamble.rstrip())

    main_lines += [
        "",
        f'pipeline = haute.Pipeline("{pipeline_name}", description={description!r})',
        "",
        "",
    ]

    # Preserved blocks (user code that survives GUI regeneration)
    all_preserved = preserved_blocks if preserved_blocks is not None else graph.preserved_blocks
    if all_preserved:
        main_lines.extend(_emit_preserved_blocks(all_preserved))
        main_lines.append("")

    # Build source names per root node from root-level edges AND
    # cross-boundary edges (resolving submodel handles to child node names).
    root_node_sources: dict[str, list[str]] = {}
    for edge in edges:
        src = edge.source
        tgt = edge.target
        sh = edge.sourceHandle or ""
        th = edge.targetHandle or ""

        # Resolve submodel handles to actual child node names
        actual_src = src
        if src in submodel_node_ids and sh:
            actual_src = sh.removeprefix("out__")
        actual_tgt = tgt
        if tgt in submodel_node_ids and th:
            actual_tgt = th.removeprefix("in__")

        # Only care about edges feeding into root nodes
        if actual_tgt not in root_node_ids:
            continue
        src_name = root_id_to_func.get(actual_src, _sanitize_func_name(actual_src))
        root_node_sources.setdefault(actual_tgt, []).append(src_name)

    # Partition: emit originals before instances
    root_instance_of: dict[str, str] = {}
    for node in sorted_root:
        ref = node.data.config.get("instanceOf")
        if ref:
            root_instance_of[node.id] = ref

    root_originals = [n for n in sorted_root if n.id not in root_instance_of]
    root_instances = [n for n in sorted_root if n.id in root_instance_of]

    for node in root_originals:
        source_names = root_node_sources.get(node.id, [])
        main_lines.append(_node_to_code(node, source_names=source_names))
        main_lines.append("")

    for node in root_instances:
        source_names = root_node_sources.get(node.id, [])
        orig_id = root_instance_of[node.id]
        orig_func = root_id_to_func.get(orig_id, orig_id)
        orig_src = root_node_sources.get(orig_id, [])
        main_lines.append(_instance_to_code(
            node, orig_func,
            source_names=source_names,
            orig_source_names=orig_src,
        ))
        main_lines.append("")

    # Emit pipeline.submodel() imports
    for sm_name, sm_meta in submodels.items():
        sm_file = sm_meta.get("file", f"modules/{sm_name}.py")
        main_lines.append(f'pipeline.submodel("{sm_file}")')
    main_lines.append("")

    # Emit pipeline.connect() calls for ALL edges (cross-boundary use real node names)
    all_id_to_func = dict(root_id_to_func)
    all_edges = edges
    # Also include cross-boundary edges that reference submodel handles
    connect_pairs: list[tuple[str, str]] = []
    for edge in all_edges:
        src = edge.source
        tgt = edge.target
        sh = edge.sourceHandle or ""
        th = edge.targetHandle or ""

        # Resolve submodel handles to actual node names
        actual_src = src
        if src in submodel_node_ids and sh:
            actual_src = sh.removeprefix("out__")
        actual_tgt = tgt
        if tgt in submodel_node_ids and th:
            actual_tgt = th.removeprefix("in__")

        src_func = all_id_to_func.get(actual_src, _sanitize_func_name(actual_src))
        tgt_func = all_id_to_func.get(actual_tgt, _sanitize_func_name(actual_tgt))
        connect_pairs.append((src_func, tgt_func))

    if connect_pairs:
        main_lines.append("")
        main_lines.append("# Wire nodes together - edges define data flow")
        seen: set[tuple[str, str]] = set()
        for src_func, tgt_func in connect_pairs:
            if (src_func, tgt_func) not in seen:
                seen.add((src_func, tgt_func))
                main_lines.append(f'pipeline.connect("{src_func}", "{tgt_func}")')
        main_lines.append("")

    main_key = source_file or f"{pipeline_name}.py"
    files[main_key] = "\n".join(main_lines)
    return files
