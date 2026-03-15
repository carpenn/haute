"""Optimiser endpoints: solve, status, apply, save, frontier, mlflow log."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute._sandbox import _get_project_root
from haute._types import SolveResultLike
from haute.routes._helpers import _INTERNAL_ERROR_DETAIL, validate_safe_path
from haute.routes._job_store import JobStore
from haute.routes._optimiser_service import (
    _DEFAULT_CHUNK_SIZE,
    _DEFAULT_TIMEOUT,
    OptimiserSolveService,
)
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
_store = JobStore()
_solve_service = OptimiserSolveService(_store)

_APPLY_PREVIEW_ROWS = 100  # max rows returned in the apply preview payload


@router.post("/solve", response_model=OptimiserSolveResponse)
def solve(body: OptimiserSolveRequest) -> OptimiserSolveResponse:
    """Start optimisation for an optimiser node.

    Executes the pipeline up to the optimiser node to materialise the
    scored DataFrame, then runs the solver in a background thread.
    """
    return _solve_service.start(body)


@router.get("/solve/status/{job_id}", response_model=OptimiserStatusResponse)
async def solve_status(job_id: str) -> OptimiserStatusResponse:
    """Poll optimisation job progress."""
    job = _store.require_job(job_id)

    # Check for timeout on running jobs
    if job.get("status") == "running":
        start = job.get("start_time")
        timeout = job.get("timeout", _DEFAULT_TIMEOUT)
        if start and (time.monotonic() - start) > timeout:
            # P7: Atomic update to avoid races with background solver thread
            _store.atomic_update(job_id, {
                "status": "error",
                "message": f"Solve timed out after {timeout}s. "
                "Increase timeout or simplify the problem.",
                "elapsed_seconds": time.monotonic() - start,
            })
            job = _store.require_job(job_id)

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
    job = _store.require_completed_job(body.job_id)

    solve_result = job.get("solve_result")
    if solve_result is None:
        raise HTTPException(status_code=400, detail="Job has no solve result")

    try:
        df = solve_result.dataframe
        return OptimiserApplyResponse(
            status="ok",
            total_objective=solve_result.total_objective,
            constraints=solve_result.total_constraints,
            preview=df.head(_APPLY_PREVIEW_ROWS).to_dicts(),
            row_count=len(df),
        )
    except Exception as exc:
        logger.error("apply_failed", error=str(exc), job_id=body.job_id)
        raise HTTPException(status_code=500, detail=_INTERNAL_ERROR_DETAIL)


@router.post("/frontier", response_model=OptimiserFrontierResponse)
def run_frontier(body: OptimiserFrontierRequest) -> OptimiserFrontierResponse:
    """Compute efficient frontier for a completed optimisation job."""
    job = _store.require_completed_job(body.job_id)

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
        raise HTTPException(status_code=500, detail=_INTERNAL_ERROR_DETAIL)


def _build_artifact_payload(
    job: dict[str, Any],
    solve_result: SolveResultLike,
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
        "chunk_size": job_config.get("chunk_size", _DEFAULT_CHUNK_SIZE),
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
    job = _store.require_completed_job(body.job_id)

    solve_result = job.get("solve_result")
    solver = job.get("solver")
    if solve_result is None or solver is None:
        raise HTTPException(status_code=400, detail="Job has no solve result")

    base = _get_project_root()
    out = validate_safe_path(base, body.output_path)

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
    except HTTPException:
        raise
    except OSError as exc:
        logger.error("save_failed", error=str(exc), job_id=body.job_id)
        raise HTTPException(
            status_code=500,
            detail="Filesystem error saving optimiser result. Check the server logs for details.",
        )
    except Exception as exc:
        logger.error("save_failed", error=str(exc), job_id=body.job_id)
        raise HTTPException(status_code=500, detail=_INTERNAL_ERROR_DETAIL)


@router.post("/mlflow/log", response_model=OptimiserMlflowLogResponse)
def mlflow_log(body: OptimiserMlflowLogRequest) -> OptimiserMlflowLogResponse:
    """Log optimisation results to MLflow."""
    job = _store.require_completed_job(body.job_id)

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
        raise HTTPException(status_code=500, detail=_INTERNAL_ERROR_DETAIL)
