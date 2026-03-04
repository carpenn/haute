"""Modelling endpoints: train, status, export."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute.routes._job_store import JobStore
from haute.routes._train_service import (
    TrainService,
    _VramCheck,
    _check_gpu_vram,
    _clamp_row_limit,
    _find_modelling_node,
)
from haute.schemas import (
    ExportScriptRequest,
    ExportScriptResponse,
    LogExperimentRequest,
    LogExperimentResponse,
    MlflowCheckResponse,
    TrainEstimateRequest,
    TrainEstimateResponse,
    TrainRequest,
    TrainResponse,
    TrainStatusResponse,
)

logger = get_logger(component="server.modelling")

router = APIRouter(prefix="/api/modelling", tags=["modelling"])

# In-memory job store — fine for single-server dev tool.
_store = JobStore()
_train_service = TrainService(_store)


@router.post("/train", response_model=TrainResponse)
def train_model(body: TrainRequest) -> TrainResponse:
    """Start model training for a modelling node.

    Executes the pipeline up to the modelling node to materialise the
    training DataFrame, then runs TrainingJob in a background thread.
    """
    return _train_service.start(body)


@router.get("/train/status/{job_id}", response_model=TrainStatusResponse)
async def train_status(job_id: str) -> TrainStatusResponse:
    """Poll training job progress."""
    job = _store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    return TrainStatusResponse(
        status=job.get("status", "unknown"),
        progress=job.get("progress", 0.0),
        message=job.get("message", ""),
        iteration=job.get("iteration", 0),
        total_iterations=job.get("total_iterations", 0),
        train_loss=job.get("train_loss", {}),
        elapsed_seconds=job.get("elapsed_seconds", 0.0),
        result=job.get("result"),
        warning=job.get("warning"),
    )


@router.post("/estimate", response_model=TrainEstimateResponse)
def estimate_training(body: TrainEstimateRequest) -> TrainEstimateResponse:
    """Estimate RAM and row requirements for training a modelling node.

    Runs a small probe (1 000 rows) through the pipeline to measure
    per-row memory, then extrapolates to the full dataset.  Returns
    immediately — typically <1 s.  Also estimates GPU VRAM if the
    node's params specify ``task_type: GPU``.
    """
    node = _find_modelling_node(body.graph, body.node_id)

    from haute._ram_estimate import estimate_safe_training_rows
    from haute.executor import _build_node_fn, _compile_preamble

    preamble_ns = _compile_preamble(body.graph.preamble or "") or None

    try:
        ram_est = estimate_safe_training_rows(
            body.graph, body.node_id, _build_node_fn,
            preamble_ns=preamble_ns,
        )
    except Exception as exc:
        logger.warning("estimate_failed", error=str(exc), node_id=body.node_id)
        return TrainEstimateResponse()

    from haute._ram_estimate import _CATBOOST_OVERHEAD_MULTIPLIER

    data_mb = ram_est.estimated_bytes / 1024**2
    training_mb = data_mb * _CATBOOST_OVERHEAD_MULTIPLIER

    # Apply user row limit to the estimate
    safe_limit = _clamp_row_limit(ram_est.safe_row_limit, node.data.config.get("row_limit"))

    # GPU VRAM estimation
    vram_check = _VramCheck()
    node_params = node.data.config.get("params", {})
    if str(node_params.get("task_type", "")).upper() == "GPU":
        effective_rows = safe_limit or ram_est.total_rows or 0
        vram_check = _check_gpu_vram(effective_rows, ram_est.probe_columns, node_params)
        if vram_check.warning:
            vram_check.warning += " Training will fall back to CPU automatically."

    return TrainEstimateResponse(
        total_rows=ram_est.total_rows,
        safe_row_limit=safe_limit,
        estimated_mb=round(data_mb, 1),
        training_mb=round(training_mb, 1),
        available_mb=round(ram_est.available_bytes / 1024**2, 1),
        bytes_per_row=round(ram_est.bytes_per_row, 1),
        was_downsampled=ram_est.was_downsampled,
        warning=ram_est.warning,
        gpu_vram_estimated_mb=vram_check.estimated_mb,
        gpu_vram_available_mb=vram_check.available_mb,
        gpu_warning=vram_check.warning,
    )


@router.get("/mlflow/check", response_model=MlflowCheckResponse)
async def mlflow_check() -> MlflowCheckResponse:
    """Check whether MLflow is installed and detect the tracking backend."""
    try:
        import mlflow as _mlflow  # noqa: F401
        mlflow_installed = True
    except ImportError:
        return MlflowCheckResponse(mlflow_installed=False)

    import os

    from haute.modelling._mlflow_log import resolve_tracking_backend

    _uri, backend = resolve_tracking_backend()
    databricks_host = os.getenv("DATABRICKS_HOST", "") if backend == "databricks" else ""

    return MlflowCheckResponse(
        mlflow_installed=mlflow_installed,
        backend=backend,
        databricks_host=databricks_host,
    )


@router.post("/mlflow/log", response_model=LogExperimentResponse)
async def mlflow_log(body: LogExperimentRequest) -> LogExperimentResponse:
    """Log a completed training job's results to MLflow."""
    job = _store.get_job(body.job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{body.job_id}' not found")

    if job.get("status") != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job '{body.job_id}' is not completed (status: {job.get('status')})",
        )

    result: TrainResponse | None = job.get("result")
    if result is None:
        raise HTTPException(status_code=400, detail="Job has no result data")

    config = job.get("config", {})
    node_label = job.get("node_label", "model")

    # Build experiment name: user override > config > default
    experiment_name = (
        body.experiment_name
        or config.get("mlflow_experiment")
        or f"/Shared/haute/{node_label}"
    )
    model_name = body.model_name or config.get("model_name") or None

    try:
        from haute.modelling._mlflow_log import log_experiment

        log_result = log_experiment(
            experiment_name=experiment_name,
            run_name=node_label,
            metrics=result.metrics,
            params={
                "algorithm": config.get("algorithm", "catboost"),
                "task": config.get("task", "regression"),
                "target": config.get("target", ""),
                "weight": config.get("weight", ""),
            },
            model_path=result.model_path or None,
            model_name=model_name,
            shap_summary=result.shap_summary or None,
            feature_importance_loss=result.feature_importance_loss or None,
            cv_results=result.cv_results,
            double_lift=result.double_lift or None,
            loss_history=result.loss_history or None,
            feature_importance=result.feature_importance or None,
            ave_per_feature=result.ave_per_feature or None,
            algorithm=config.get("algorithm", "catboost"),
            task=config.get("task", "regression"),
            train_rows=result.train_rows,
            test_rows=result.test_rows,
            best_iteration=result.best_iteration,
            features=result.features or None,
            split_config=config.get("split", {}),
        )

        return LogExperimentResponse(
            status="ok",
            backend=log_result.backend,
            experiment_name=log_result.experiment_name,
            run_id=log_result.run_id,
            run_url=log_result.run_url,
            tracking_uri=log_result.tracking_uri,
        )
    except Exception as exc:
        logger.error("mlflow_log_failed", error=str(exc), job_id=body.job_id)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/export", response_model=ExportScriptResponse)
async def export_script(body: ExportScriptRequest) -> ExportScriptResponse:
    """Generate a standalone training script from a modelling node's config."""
    node = _find_modelling_node(body.graph, body.node_id)
    config = dict(node.data.config)

    # Use the node label as the default name
    if "name" not in config:
        config["name"] = node.data.label

    from haute.modelling import generate_training_script

    data_path = body.data_path or f"output/{config.get('name', 'model')}.parquet"
    script = generate_training_script(config, data_path)
    filename = f"train_{config.get('name', 'model')}.py"

    return ExportScriptResponse(script=script, filename=filename)
