"""Optimiser endpoints: solve, status, apply, save, mlflow log."""

from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute._sandbox import _get_project_root
from haute.graph_utils import NodeType
from haute.schemas import (
    OptimiserApplyRequest,
    OptimiserApplyResponse,
    OptimiserMlflowLogRequest,
    OptimiserMlflowLogResponse,
    OptimiserSaveRequest,
    OptimiserSaveResponse,
    OptimiserSolveRequest,
    OptimiserSolveResponse,
    OptimiserStatusResponse,
)

logger = get_logger(component="server.optimiser")

router = APIRouter(prefix="/api/optimiser", tags=["optimiser"])

# In-memory job store — same pattern as modelling.
_jobs: dict[str, dict[str, Any]] = {}
_JOB_TTL_SECONDS = 24 * 60 * 60  # 24 hours


def _evict_stale_jobs() -> None:
    """Remove jobs older than TTL to bound memory usage."""
    cutoff = time.time() - _JOB_TTL_SECONDS
    stale = [jid for jid, j in _jobs.items() if j.get("created_at", 0) < cutoff]
    for jid in stale:
        del _jobs[jid]


def _find_optimiser_node(graph: Any, node_id: str) -> Any:
    """Find and validate an optimiser node in the graph."""
    node = graph.node_map.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    if node.data.nodeType != NodeType.OPTIMISER:
        raise HTTPException(
            status_code=400,
            detail=f"Node '{node_id}' is not an optimiser node (got {node.data.nodeType})",
        )
    return node


def _solve_online(
    scored_df: Any,
    config: dict[str, Any],
    job: dict[str, Any],
    start_time: float,
) -> None:
    """Run the online optimiser solver."""
    import polars as pl
    from price_contour import OnlineOptimiser

    objective = config["objective"]
    constraints = config["constraints"]
    qid_col = config.get("quote_id", "quote_id")
    mult_col = config.get("multiplier", "multiplier")
    step_col = config.get("scenario_step", "scenario_step")
    constraint_cols = list(constraints.keys()) if isinstance(constraints, dict) else []

    # Cast columns to types the Rust solver expects
    cast_map: dict[str, pl.DataType] = {}
    cast_map[qid_col] = pl.Utf8
    cast_map[step_col] = pl.Int32
    cast_map[mult_col] = pl.Float32
    cast_map[objective] = pl.Float32
    for c in constraint_cols:
        cast_map[c] = pl.Float32
    cast_exprs = [
        pl.col(c).cast(t)
        for c, t in cast_map.items()
        if c in scored_df.columns and scored_df[c].dtype != t
    ]
    if cast_exprs:
        scored_df = scored_df.with_columns(cast_exprs)

    # Drop null quote_ids
    scored_df = scored_df.filter(pl.col(qid_col).is_not_null())

    solver = OnlineOptimiser(
        objective=objective,
        constraints=constraints,
        quote_id=qid_col,
        scenario_step=step_col,
        multiplier=mult_col,
        max_iter=config.get("max_iter", 50),
        chunk_size=config.get("chunk_size", 500_000),
        tolerance=config.get("tolerance", 1e-6),
        record_history=config.get("record_history", False),
    )
    solve_result = solver.solve(scored_df)
    elapsed = time.monotonic() - start_time
    converged = solve_result.converged
    logger.info("solve_completed", mode="online", elapsed=f"{elapsed:.2f}s", converged=converged)

    job.update({
        "status": "completed",
        "progress": 1.0,
        "message": "Completed",
        "elapsed_seconds": elapsed,
        "solver": solver,
        "solve_result": solve_result,
        "result": {
            "mode": "online",
            "total_objective": solve_result.total_objective,
            "baseline_objective": solve_result.baseline_objective,
            "constraints": solve_result.total_constraints,
            "baseline_constraints": solve_result.baseline_constraints,
            "lambdas": solve_result.lambdas,
            "converged": solve_result.converged,
            "iterations": solve_result.iterations,
            "n_quotes": solve_result.n_quotes,
            "n_steps": solve_result.n_steps,
        },
    })


def _solve_ratebook(
    scored_df: Any,
    config: dict[str, Any],
    pipeline_result: Any,
    job: dict[str, Any],
    start_time: float,
) -> None:
    """Run the ratebook optimiser solver."""
    import polars as pl
    from price_contour import RatebookOptimiser

    objective = config["objective"]
    constraints = config["constraints"]
    qid_col = config.get("quote_id", "quote_id")
    mult_col = config.get("multiplier", "multiplier")
    step_col = config.get("scenario_step", "scenario_step")

    # QuoteGridBuilder expects hardcoded columns: quote_id, scenario_step, multiplier
    # Rename user-specified columns, then cast to required types.
    # Skip rename if the target column already exists (e.g. scenario_step
    # from the expander) to avoid duplicate column errors.
    rename_map: dict[str, str] = {}
    if qid_col != "quote_id" and "quote_id" not in scored_df.columns:
        rename_map[qid_col] = "quote_id"
    if step_col != "scenario_step" and "scenario_step" not in scored_df.columns:
        rename_map[step_col] = "scenario_step"
    if mult_col != "multiplier" and "multiplier" not in scored_df.columns:
        rename_map[mult_col] = "multiplier"
    if rename_map:
        scored_df = scored_df.rename(rename_map)

    eff_objective = rename_map.get(objective, objective)
    eff_constraints = {rename_map.get(k, k): v for k, v in constraints.items()}

    # Cast to required types using a dict to prevent duplicates
    cast_map: dict[str, pl.DataType] = {}
    cast_map["quote_id"] = pl.Utf8
    cast_map["scenario_step"] = pl.Int32
    cast_map["multiplier"] = pl.Float32
    cast_map[eff_objective] = pl.Float32
    for c in eff_constraints:
        cast_map[c] = pl.Float32
    cast_exprs = [
        pl.col(c).cast(t)
        for c, t in cast_map.items()
        if c in scored_df.columns and scored_df[c].dtype != t
    ]
    if cast_exprs:
        scored_df = scored_df.with_columns(cast_exprs)

    # Drop null quote_ids
    scored_df = scored_df.filter(pl.col("quote_id").is_not_null())

    # Get per-quote factors from the banding source node
    banding_source_id = config.get("banding_source")
    if banding_source_id and banding_source_id in pipeline_result.outputs:
        factors_df = pipeline_result.outputs[banding_source_id]
    else:
        raise RuntimeError(
            "Ratebook mode requires a banding source. "
            "Select a banding node in the Rating Factor Source dropdown."
        )

    # Filter factor_columns to only groups whose columns exist in factors_df
    raw_factor_columns = config.get("factor_columns", [])
    available_cols = set(factors_df.columns)
    factor_columns_valid = [
        group for group in raw_factor_columns
        if all(c in available_cols for c in group)
    ]
    if not factor_columns_valid:
        missing = [c for group in raw_factor_columns for c in group if c not in available_cols]
        raise RuntimeError(
            f"No valid factor groups found. Missing columns in banding source: {missing}. "
            f"Available columns: {sorted(available_cols)}"
        )

    solver = RatebookOptimiser(
        objective=eff_objective,
        constraints=eff_constraints,
        factor_columns=factor_columns_valid,
        max_iter=config.get("max_iter", 50),
        max_cd_iterations=config.get("max_cd_iterations", 10),
        cd_tolerance=config.get("cd_tolerance", 1e-3),
        tolerance=config.get("tolerance", 1e-6),
        chunk_size=config.get("chunk_size", 500_000),
    )

    # Select only the factor columns + quote ID for alignment
    factor_cols_flat = [c for group in factor_columns_valid for c in group]
    avail = factors_df.columns
    if qid_col in avail:
        keep = [qid_col] + [c for c in factor_cols_flat if c in avail]
        factors_df = factors_df.select(keep)
        if qid_col != "quote_id":
            factors_df = factors_df.rename({qid_col: "quote_id"})
    elif "quote_id" in avail:
        keep = ["quote_id"] + [c for c in factor_cols_flat if c in avail]
        factors_df = factors_df.select(keep)

    # Cast quote_id to Utf8 to match scored_df
    factors_df = factors_df.with_columns(pl.col("quote_id").cast(pl.Utf8))

    # Deduplicate to one row per quote
    factors_df = factors_df.unique(subset=["quote_id"])

    # Align factor rows to match scored_df quote order
    quote_order = scored_df.select("quote_id").unique(maintain_order=True)
    factors_df = quote_order.join(factors_df, on="quote_id", how="left")
    factors_df = factors_df.drop("quote_id")

    solve_result = solver.solve(scored_df, factors_df)
    elapsed = time.monotonic() - start_time
    converged = solve_result.converged
    logger.info("solve_completed", mode="ratebook", elapsed=f"{elapsed:.2f}s", converged=converged)

    # factor_tables is dict[str, dict[str, float]] in installed version
    factor_tables_serialised = {}
    for name, table in solve_result.factor_tables.items():
        factor_tables_serialised[name] = [
            {"__factor_group__": level, "optimal_multiplier": mult}
            for level, mult in table.items()
        ]

    job.update({
        "status": "completed",
        "progress": 1.0,
        "message": "Completed",
        "elapsed_seconds": elapsed,
        "solver": solver,
        "solve_result": solve_result,
        "result": {
            "mode": "ratebook",
            "total_objective": solve_result.total_objective,
            "baseline_objective": solve_result.baseline_objective,
            "constraints": solve_result.total_constraints,
            "baseline_constraints": solve_result.baseline_constraints,
            "lambdas": solve_result.lambdas,
            "converged": solve_result.converged,
            "cd_iterations": solve_result.cd_iterations,
            "factor_tables": factor_tables_serialised,
        },
    })


@router.post("/solve", response_model=OptimiserSolveResponse)
def solve(body: OptimiserSolveRequest) -> OptimiserSolveResponse:
    """Start optimisation for an optimiser node.

    Executes the pipeline up to the optimiser node to materialise the
    scored DataFrame, then runs the solver in a background thread.
    """
    node = _find_optimiser_node(body.graph, body.node_id)
    config = node.data.config

    # --- Upfront validation ---
    objective = config.get("objective")
    if not objective:
        raise HTTPException(
            status_code=400,
            detail="No objective column configured. Open the config panel and set an objective.",
        )

    constraints = config.get("constraints")
    if not constraints:
        raise HTTPException(
            status_code=400,
            detail="No constraints configured. Add at least one constraint.",
        )

    mode = config.get("mode", "online")
    if mode not in ("online", "ratebook"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported optimiser mode '{mode}'. Currently supported: online, ratebook.",
        )

    if mode == "ratebook":
        factor_columns = config.get("factor_columns")
        if not factor_columns:
            raise HTTPException(
                status_code=400,
                detail="Ratebook mode requires factor_columns. Add at least one factor group.",
            )

    _evict_stale_jobs()
    job_id = uuid.uuid4().hex[:12]
    logger.info("solve_started", node_id=body.node_id, mode=mode, job_id=job_id)
    _jobs[job_id] = {
        "status": "running",
        "progress": 0.0,
        "message": "Starting",
        "config": dict(config),
        "node_label": node.data.label,
        "created_at": time.time(),
    }

    # --- Execute pipeline to get scored DataFrame ---
    try:
        from haute.executor import _build_node_fn
        from haute.graph_utils import _execute_eager_core

        result = _execute_eager_core(
            body.graph, _build_node_fn,
            target_node_id=body.node_id,
            row_limit=None,
            swallow_errors=True,
        )
    except Exception as exc:
        error_msg = f"Pipeline execution failed: {exc}"
        logger.error("pipeline_exec_failed", error=str(exc), node_id=body.node_id)
        _jobs[job_id] = {"status": "error", "message": error_msg}
        raise HTTPException(status_code=500, detail=error_msg)

    # If the user selected a specific data input, use that node's output
    # instead of the optimiser's own pass-through output.
    data_input_id = config.get("data_input")
    if data_input_id and data_input_id in result.outputs:
        scored_df = result.outputs.get(data_input_id)
    else:
        scored_df = result.outputs.get(body.node_id)
    if scored_df is not None and scored_df.shape[0] == 0:
        upstream_errs = {
            result.id_to_name.get(nid, nid): err
            for nid, err in result.errors.items()
        }
        detail = (
            f"DataFrame arrived at optimiser with 0 rows. "
            f"Upstream errors: {upstream_errs}" if upstream_errs
            else "DataFrame arrived at optimiser with 0 rows — check upstream data."
        )
        _jobs[job_id] = {"status": "error", "message": detail}
        raise HTTPException(status_code=400, detail=detail)
    if scored_df is None:
        upstream_errors = {
            nid: err for nid, err in result.errors.items() if nid != body.node_id
        }
        if upstream_errors:
            failed_node = next(iter(upstream_errors))
            failed_name = result.id_to_name.get(failed_node, failed_node)
            error_msg = (
                f"Upstream node '{failed_name}' failed: {upstream_errors[failed_node]}. "
                f"Fix the error in that node before optimising."
            )
        elif body.node_id in result.errors:
            error_msg = f"Optimiser node error: {result.errors[body.node_id]}"
        else:
            error_msg = (
                "No data arrived at the optimiser node. "
                "Make sure an upstream data source is connected and producing data."
            )
        _jobs[job_id] = {"status": "error", "message": error_msg}
        raise HTTPException(status_code=400, detail=error_msg)

    # --- Build solver and run in background ---
    start_time = time.monotonic()
    _jobs[job_id]["start_time"] = start_time
    node_id = body.node_id  # capture for closure

    def _solve_background() -> None:
        try:
            job = _jobs[job_id]
            job.update({
                "message": "Solving",
                "progress": 0.1,
                "elapsed_seconds": time.monotonic() - start_time,
            })
            if mode == "ratebook":
                _solve_ratebook(scored_df, config, result, job, start_time)
            else:
                _solve_online(scored_df, config, job, start_time)
        except Exception as exc:
            error_msg = f"Optimisation failed: {exc}"
            logger.error("solve_failed", error=str(exc), node_id=node_id)
            _jobs[job_id].update({
                "status": "error",
                "message": error_msg,
                "elapsed_seconds": time.monotonic() - start_time,
            })

    thread = threading.Thread(target=_solve_background, daemon=True)
    thread.start()
    return OptimiserSolveResponse(status="started", job_id=job_id)


@router.get("/solve/status/{job_id}", response_model=OptimiserStatusResponse)
async def solve_status(job_id: str) -> OptimiserStatusResponse:
    """Poll optimisation job progress."""
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    return OptimiserStatusResponse(
        status=job.get("status", "unknown"),
        progress=job.get("progress", 0.0),
        message=job.get("message", ""),
        elapsed_seconds=job.get("elapsed_seconds", 0.0),
        result=job.get("result"),
    )


@router.post("/apply", response_model=OptimiserApplyResponse)
def apply_lambdas(body: OptimiserApplyRequest) -> OptimiserApplyResponse:
    """Apply solved lambdas to the scored data."""
    logger.info("apply_requested", job_id=body.job_id)
    job = _jobs.get(body.job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{body.job_id}' not found")
    if job.get("status") != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job '{body.job_id}' is not completed (status: {job.get('status')})",
        )

    solve_result = job.get("solve_result")
    if solve_result is None:
        raise HTTPException(status_code=400, detail="Job has no solve result")

    try:
        df = solve_result.dataframe
        return OptimiserApplyResponse(
            status="ok",
            total_objective=solve_result.total_objective,
            constraints=solve_result.total_constraints,
            preview=df.head(100).to_dicts(),
            row_count=len(df),
        )
    except Exception as exc:
        logger.error("apply_failed", error=str(exc), job_id=body.job_id)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/save", response_model=OptimiserSaveResponse)
def save_result(body: OptimiserSaveRequest) -> OptimiserSaveResponse:
    """Save the optimisation result to disk."""
    job = _jobs.get(body.job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{body.job_id}' not found")
    if job.get("status") != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job '{body.job_id}' is not completed (status: {job.get('status')})",
        )

    solve_result = job.get("solve_result")
    solver = job.get("solver")
    if solve_result is None or solver is None:
        raise HTTPException(status_code=400, detail="Job has no solve result")

    base = _get_project_root()
    out = (base / body.output_path).resolve()
    if not str(out).startswith(str(base)):
        raise HTTPException(status_code=403, detail="Cannot save outside project root")

    try:
        out.parent.mkdir(parents=True, exist_ok=True)

        # Save config + lambdas as JSON
        payload = {
            "lambdas": solve_result.lambdas,
            "total_objective": solve_result.total_objective,
            "total_constraints": solve_result.total_constraints,
            "converged": solve_result.converged,
            "iterations": getattr(solve_result, "iterations", None),
            "cd_iterations": getattr(solve_result, "cd_iterations", None),
        }
        out.write_text(json.dumps(payload, indent=2, default=str))
        logger.info("result_saved", path=str(out), job_id=body.job_id)

        return OptimiserSaveResponse(
            status="ok",
            path=str(out),
            message=f"Saved optimisation result to {out}",
        )
    except Exception as exc:
        logger.error("save_failed", error=str(exc), job_id=body.job_id)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/mlflow/log", response_model=OptimiserMlflowLogResponse)
def mlflow_log(body: OptimiserMlflowLogRequest) -> OptimiserMlflowLogResponse:
    """Log optimisation results to MLflow."""
    job = _jobs.get(body.job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{body.job_id}' not found")
    if job.get("status") != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job '{body.job_id}' is not completed (status: {job.get('status')})",
        )

    solver = job.get("solver")
    solve_result = job.get("solve_result")
    if solver is None or solve_result is None:
        raise HTTPException(status_code=400, detail="Job has no solve result")

    try:
        import mlflow
    except ImportError:
        raise HTTPException(
            status_code=400,
            detail="MLflow is not installed. Install with: pip install mlflow",
        )

    try:
        from haute.modelling._mlflow_log import resolve_tracking_backend

        tracking_uri, backend = resolve_tracking_backend()
        mlflow.set_tracking_uri(tracking_uri)
        if backend == "databricks":
            mlflow.set_registry_uri("databricks-uc")

        summary = solver.summary(solve_result)

        node_label = job.get("node_label", "optimiser")
        job_config = job.get("config", {})
        experiment_name = (
            body.experiment_name
            or job_config.get("mlflow_experiment")
            or f"/Shared/haute/{node_label}"
        )
        mlflow.set_experiment(experiment_name)

        with mlflow.start_run(run_name=node_label) as run:
            mlflow.log_params(summary["params"])
            mlflow.log_metrics(summary["metrics"])

            # Log artifacts as JSON files
            import tempfile
            with tempfile.TemporaryDirectory() as tmpdir:
                artifacts = summary.get("artifacts", {})
                for name, data in artifacts.items():
                    if data is None:
                        continue
                    artifact_path = Path(tmpdir) / f"{name}.json"
                    artifact_path.write_text(json.dumps(data, indent=2, default=str))
                    mlflow.log_artifact(str(artifact_path))

            run_id = run.info.run_id
            run_url = None
            if backend == "databricks":
                import os
                host = os.getenv("DATABRICKS_HOST", "")
                try:
                    exp = mlflow.get_experiment_by_name(experiment_name)
                    if exp and host:
                        run_url = f"{host}/#mlflow/experiments/{exp.experiment_id}/runs/{run_id}"
                except Exception:
                    logger.debug("run_url_build_failed", exc_info=True)

        return OptimiserMlflowLogResponse(
            status="ok",
            backend=backend,
            experiment_name=experiment_name,
            run_id=run_id,
            run_url=run_url,
            tracking_uri=tracking_uri,
        )
    except Exception as exc:
        logger.error("mlflow_log_failed", error=str(exc), job_id=body.job_id)
        raise HTTPException(status_code=500, detail=str(exc))
