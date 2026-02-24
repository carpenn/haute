"""Optimiser endpoints: solve, status, apply, save, frontier, mlflow log."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
from fastapi import APIRouter, HTTPException

if TYPE_CHECKING:
    import polars as pl
    from price_contour import QuoteGrid

from haute._logging import get_logger
from haute._sandbox import _get_project_root
from haute.graph_utils import NodeType
from haute.schemas import (
    OptimiserApplyRequest,
    OptimiserApplyResponse,
    OptimiserFrontierRequest,
    OptimiserFrontierResponse,
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


_DEFAULT_TIMEOUT = 300  # seconds


def _compute_scenario_value_stats(
    solve_result: Any,
) -> tuple[dict[str, float], dict[str, list[float]]]:
    """Compute scenario value distribution statistics and histogram from solve result.

    Returns (stats_dict, histogram_dict).  Falls back to empty dicts if the
    result doesn't expose a per-quote ``dataframe`` (e.g. RatebookResult).
    """
    if not hasattr(solve_result, "dataframe"):
        return {}, {}
    df = solve_result.dataframe
    if "optimal_scenario_value" not in df.columns:
        return {}, {}

    col = df["optimal_scenario_value"]
    n = len(col)
    stats = {
        "mean": float(col.mean()),
        "std": float(col.std()),
        "min": float(col.min()),
        "max": float(col.max()),
        "p5": float(col.quantile(0.05)),
        "p25": float(col.quantile(0.25)),
        "p50": float(col.quantile(0.50)),
        "p75": float(col.quantile(0.75)),
        "p95": float(col.quantile(0.95)),
        "pct_increase": float((col > 1.0).sum() / n) if n else 0.0,
        "pct_decrease": float((col < 1.0).sum() / n) if n else 0.0,
    }

    # Histogram via numpy (Polars has no native histogram)
    vals = col.to_numpy()
    counts, edges = np.histogram(vals, bins=20)
    histogram = {
        "counts": [int(c) for c in counts],
        "edges": [float(e) for e in edges],
    }
    return stats, histogram



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
    quote_grid: QuoteGrid,
    config: dict[str, Any],
    job: dict[str, Any],
    start_time: float,
) -> None:
    """Run the online optimiser solver on a pre-built QuoteGrid."""
    from price_contour import OnlineOptimiser

    solver = OnlineOptimiser(
        objective=config["objective"],
        constraints=config["constraints"],
        max_iter=config.get("max_iter", 50),
        chunk_size=config.get("chunk_size", 500_000),
        tolerance=config.get("tolerance", 1e-6),
        record_history=config.get("record_history", False),
    )
    solve_result = solver.solve(quote_grid)
    elapsed = time.monotonic() - start_time
    converged = solve_result.converged
    logger.info("solve_completed", mode="online", elapsed=f"{elapsed:.2f}s", converged=converged)

    # Scenario value stats & histogram
    scenario_value_stats, scenario_value_histogram = _compute_scenario_value_stats(solve_result)

    result_dict: dict[str, Any] = {
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
        "history": solve_result.history if config.get("record_history") else None,
        "scenario_value_stats": scenario_value_stats,
        "scenario_value_histogram": scenario_value_histogram,
    }
    if not solve_result.converged:
        result_dict["warning"] = (
            "Solver did not converge. Consider increasing max_iter or relaxing tolerance."
        )

    # Store QuoteGrid (Arc-shared, compact) instead of DataFrame for frontier reuse
    job.update({
        "status": "completed",
        "progress": 1.0,
        "message": "Completed",
        "elapsed_seconds": elapsed,
        "solver": solver,
        "solve_result": solve_result,
        "quote_grid": solve_result.grid,
        "result": result_dict,
    })


def _solve_ratebook(
    quote_grid: QuoteGrid,
    config: dict[str, Any],
    factors_df: pl.DataFrame,
    job: dict[str, Any],
    start_time: float,
) -> None:
    """Run the ratebook optimiser solver on a pre-built QuoteGrid."""
    import polars as pl
    from price_contour import RatebookOptimiser

    if factors_df is None:
        raise RuntimeError(
            "Ratebook mode requires a banding source. "
            "Select a banding node in the Rating Factor Source dropdown."
        )

    constraints = config["constraints"]
    qid_col = config.get("quote_id", "quote_id")

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
        objective=config["objective"],
        constraints=constraints,
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

    # Align factor rows to match the grid's quote order
    quote_order = pl.DataFrame({"quote_id": quote_grid.quote_ids})
    quote_order = quote_order.unique(maintain_order=True)
    factors_df = quote_order.join(factors_df, on="quote_id", how="left")
    factors_df = factors_df.drop("quote_id")

    solve_result = solver.solve(quote_grid, factors_df)
    elapsed = time.monotonic() - start_time
    converged = solve_result.converged
    logger.info("solve_completed", mode="ratebook", elapsed=f"{elapsed:.2f}s", converged=converged)

    # factor_tables is dict[str, dict[str, float]] in installed version
    factor_tables_serialised = {}
    for name, table in solve_result.factor_tables.items():
        factor_tables_serialised[name] = [
            {"__factor_group__": level, "optimal_scenario_value": sv}
            for level, sv in table.items()
        ]

    # Scenario value stats & histogram
    scenario_value_stats, scenario_value_histogram = _compute_scenario_value_stats(solve_result)

    result_dict: dict[str, Any] = {
        "mode": "ratebook",
        "total_objective": solve_result.total_objective,
        "baseline_objective": solve_result.baseline_objective,
        "constraints": solve_result.total_constraints,
        "baseline_constraints": solve_result.baseline_constraints,
        "lambdas": solve_result.lambdas,
        "converged": solve_result.converged,
        "cd_iterations": solve_result.cd_iterations,
        "factor_tables": factor_tables_serialised,
        "clamp_rate": getattr(solve_result, "clamp_rate", None),
        "history": None,
        "scenario_value_stats": scenario_value_stats,
        "scenario_value_histogram": scenario_value_histogram,
    }
    if not solve_result.converged:
        result_dict["warning"] = (
            "Solver did not converge. Consider increasing max_iter or relaxing tolerance."
        )

    # Store QuoteGrid (Arc-shared, compact) instead of DataFrame for frontier reuse
    job.update({
        "status": "completed",
        "progress": 1.0,
        "message": "Completed",
        "elapsed_seconds": elapsed,
        "solver": solver,
        "solve_result": solve_result,
        "quote_grid": quote_grid,
        "result": result_dict,
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

    # --- Execute pipeline lazily (no intermediate DataFrames in memory) ---
    import polars as pl

    try:
        from haute.executor import _build_node_fn
        from haute.graph_utils import _execute_lazy

        lazy_outputs, *_ = _execute_lazy(
            body.graph, _build_node_fn,
            target_node_id=body.node_id,
        )
    except Exception as exc:
        error_msg = f"Pipeline execution failed: {exc}"
        logger.error("pipeline_exec_failed", error=str(exc), node_id=body.node_id)
        _jobs[job_id] = {"status": "error", "message": error_msg}
        raise HTTPException(status_code=500, detail=error_msg)

    # If the user selected a specific data input, use that node's lazy output
    data_input_id = config.get("data_input")
    if data_input_id and data_input_id in lazy_outputs:
        source_lf = lazy_outputs[data_input_id]
    else:
        source_lf = lazy_outputs.get(body.node_id)

    if source_lf is None:
        error_msg = (
            "No data arrived at the optimiser node. "
            "Make sure an upstream data source is connected and producing data."
        )
        _jobs[job_id] = {"status": "error", "message": error_msg}
        raise HTTPException(status_code=400, detail=error_msg)

    # --- Column validation via schema (no collect) ---
    qid_col = config.get("quote_id", "quote_id")
    mult_col = config.get("scenario_value", "scenario_value")
    step_col = config.get("scenario_index", "scenario_index")
    available_cols = set(source_lf.collect_schema().names())
    required_cols = {objective, qid_col, mult_col, step_col}
    for cname in constraints:
        required_cols.add(cname)
    missing_cols = sorted(required_cols - available_cols)
    if missing_cols:
        avail = sorted(available_cols)
        detail = f"Missing columns in scored data: {missing_cols}. Available: {avail}"
        _jobs[job_id] = {"status": "error", "message": detail}
        raise HTTPException(status_code=400, detail=detail)

    constraint_cols = list(constraints.keys()) if isinstance(constraints, dict) else []

    # --- Build cast + select as lazy expressions (projection pushdown) ---
    solver_cols = [qid_col, step_col, mult_col, objective] + [
        c for c in constraint_cols if c in available_cols
    ]
    cast_map: dict[str, pl.DataType] = {
        qid_col: pl.Utf8,
        step_col: pl.Int32,
        mult_col: pl.Float32,
        objective: pl.Float32,
    }
    for c in constraint_cols:
        cast_map[c] = pl.Float32
    cast_exprs = [pl.col(c).cast(t) for c, t in cast_map.items()]

    scored_lf = (
        source_lf
        .select(solver_cols)
        .with_columns(cast_exprs)
        .filter(pl.col(qid_col).is_not_null())
    )

    # --- Extract ratebook factors ---
    # Eager collect is justified: factors_df is small (one row per factor level)
    # and RatebookOptimiser.solve() requires an eager DataFrame for alignment.
    factors_df = None
    if mode == "ratebook":
        banding_source_id = config.get("banding_source")
        if banding_source_id and banding_source_id in lazy_outputs:
            factors_df = lazy_outputs[banding_source_id].collect(engine="streaming")

    del lazy_outputs  # free all LazyFrame references

    # --- Sink to parquet and build QuoteGrid in Rust ---
    # The lazy plan streams to a temp parquet file (Python never holds
    # the full DataFrame).  Rust reads the file and builds the grid
    # directly, keeping peak Python memory near zero.
    from price_contour import build_grid_from_parquet

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
    os.close(tmp_fd)
    try:
        scored_lf.sink_parquet(tmp_path)
        del scored_lf

        quote_grid = build_grid_from_parquet(
            tmp_path,
            constraint_cols,
            quote_id=qid_col,
            scenario_index=step_col,
            scenario_value_col=mult_col,
            objective=objective,
        )
    except HTTPException:
        raise
    except Exception as exc:
        detail = f"Grid construction failed: {exc}"
        logger.error("grid_build_failed", error=str(exc), node_id=body.node_id)
        _jobs[job_id].update({"status": "error", "message": detail})
        raise HTTPException(status_code=400, detail=detail) from exc
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    start_time = time.monotonic()
    _jobs[job_id]["start_time"] = start_time
    _jobs[job_id]["timeout"] = config.get("timeout", _DEFAULT_TIMEOUT)
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
                _solve_ratebook(
                    quote_grid, config, factors_df, job, start_time,
                )
            else:
                _solve_online(quote_grid, config, job, start_time)
        except ValueError as exc:
            error_msg = f"Data error: {exc}"
            logger.error("solve_failed", error=str(exc), node_id=node_id, category="data")
            _jobs[job_id].update({
                "status": "error",
                "message": error_msg,
                "elapsed_seconds": time.monotonic() - start_time,
            })
        except RuntimeError as exc:
            error_msg = f"Algorithm error: {exc}"
            logger.error("solve_failed", error=str(exc), node_id=node_id, category="algorithm")
            _jobs[job_id].update({
                "status": "error",
                "message": error_msg,
                "elapsed_seconds": time.monotonic() - start_time,
            })
        except Exception as exc:
            error_msg = f"Unexpected error: {exc}"
            logger.error("solve_failed", error=str(exc), node_id=node_id, category="unexpected")
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

    # Check for timeout on running jobs
    if job.get("status") == "running":
        start = job.get("start_time")
        timeout = job.get("timeout", _DEFAULT_TIMEOUT)
        if start and (time.monotonic() - start) > timeout:
            job.update({
                "status": "error",
                "message": f"Solve timed out after {timeout}s. "
                "Increase timeout or simplify the problem.",
                "elapsed_seconds": time.monotonic() - start,
            })

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


@router.post("/frontier", response_model=OptimiserFrontierResponse)
def run_frontier(body: OptimiserFrontierRequest) -> OptimiserFrontierResponse:
    """Compute efficient frontier for a completed optimisation job."""
    job = _jobs.get(body.job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{body.job_id}' not found")
    if job.get("status") != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job '{body.job_id}' is not completed (status: {job.get('status')})",
        )

    solver = job.get("solver")
    quote_grid = job.get("quote_grid")
    if solver is None or quote_grid is None:
        raise HTTPException(status_code=400, detail="Job has no solver or quote grid")

    try:
        # Convert threshold ranges from lists to tuples for Rust binding
        ranges = {
            k: tuple(v) for k, v in body.threshold_ranges.items()
        }
        frontier_result = solver.frontier(
            quote_grid,
            threshold_ranges=ranges,
            n_points_per_dim=body.n_points_per_dim,
        )
        points_df = frontier_result.points
        return OptimiserFrontierResponse(
            status="ok",
            points=points_df.to_dicts(),
            n_points=len(points_df),
            constraint_names=list(body.threshold_ranges.keys()),
        )
    except Exception as exc:
        logger.error("frontier_failed", error=str(exc), job_id=body.job_id)
        raise HTTPException(status_code=500, detail=str(exc))


def _build_artifact_payload(
    job: dict[str, Any],
    solve_result: Any,
    version_override: str = "",
) -> dict[str, Any]:
    """Build the JSON payload for an optimiser artifact.

    Shared by both file-save and MLflow-log paths to avoid duplication.
    """
    from datetime import datetime, timezone

    node_label = job.get("node_label", "optimiser")
    label_slug = node_label.lower().replace(" ", "_")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")  # noqa: UP017
    auto_version = f"{label_slug}_{ts}"
    job_config = job.get("config", {})

    payload: dict[str, Any] = {
        "version": version_override or auto_version,
        "created_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        "mode": job_config.get("mode", "online"),
        "lambdas": solve_result.lambdas,
        "total_objective": solve_result.total_objective,
        "baseline_objective": getattr(solve_result, "baseline_objective", None),
        "total_constraints": solve_result.total_constraints,
        "baseline_constraints": getattr(solve_result, "baseline_constraints", None),
        "constraints": job_config.get("constraints"),
        "objective": job_config.get("objective"),
        "quote_id": job_config.get("quote_id", "quote_id"),
        "scenario_index": job_config.get("scenario_index", "scenario_index"),
        "scenario_value": job_config.get("scenario_value", "scenario_value"),
        "chunk_size": job_config.get("chunk_size", 500_000),
        "converged": solve_result.converged,
        "iterations": getattr(solve_result, "iterations", None),
        "cd_iterations": getattr(solve_result, "cd_iterations", None),
    }
    if job_config.get("mode") == "ratebook":
        payload["factor_tables"] = job.get("result", {}).get("factor_tables")
        payload["clamp_rate"] = getattr(solve_result, "clamp_rate", None)
    return payload


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

        payload = _build_artifact_payload(job, solve_result, version_override=body.version)
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

                # Also log the complete artifact used by OPTIMISER_APPLY
                complete_payload = _build_artifact_payload(job, solve_result)
                complete_path = Path(tmpdir) / "optimiser_result.json"
                complete_path.write_text(json.dumps(complete_payload, indent=2, default=str))
                mlflow.log_artifact(str(complete_path))

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
