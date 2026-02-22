"""Modelling endpoints: train, status, export."""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute._types import NodeType
from haute.modelling._algorithms import ALGORITHM_REGISTRY
from haute.schemas import (
    ExportScriptRequest,
    ExportScriptResponse,
    LogExperimentRequest,
    LogExperimentResponse,
    MlflowCheckResponse,
    TrainRequest,
    TrainResponse,
    TrainStatusResponse,
)

logger = get_logger(component="server.modelling")

router = APIRouter(prefix="/api/modelling", tags=["modelling"])

# In-memory job store — fine for single-server dev tool
_jobs: dict[str, dict[str, Any]] = {}


def _find_modelling_node(graph: Any, node_id: str) -> Any:
    """Find and validate a modelling node in the graph."""
    node_map = {n.id: n for n in graph.nodes}
    node = node_map.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    if node.data.nodeType != NodeType.MODELLING:
        raise HTTPException(
            status_code=400,
            detail=f"Node '{node_id}' is not a modelling node (got {node.data.nodeType})",
        )
    return node


def _friendly_error(exc: Exception) -> str:
    """Translate common training exceptions into actionable messages."""
    msg = str(exc)

    # ValueError from TrainingJob validation — already clear
    if isinstance(exc, ValueError):
        return msg

    # FileNotFoundError — data path issues
    if isinstance(exc, FileNotFoundError):
        return f"File not found: {msg}"

    # CatBoost errors
    exc_type = type(exc).__name__
    if "CatBoost" in exc_type or "catboost" in msg.lower():
        # NaN in features
        if "nan" in msg.lower() or "inf" in msg.lower():
            return (
                "Training failed: the data contains NaN or infinite values. "
                "Add a transform node upstream to handle missing values "
                "(e.g. .fill_null() or .drop_nulls()) before training."
            )
        # Column count mismatch
        if "feature" in msg.lower() and "number" in msg.lower():
            return f"Training failed: feature mismatch. {msg}"
        # Generic CatBoost error — keep the original but add context
        return f"CatBoost error: {msg}"

    # Permission / OS errors saving the model
    if isinstance(exc, OSError):
        return f"Could not save model file: {msg}"

    # Fallback — include the exception type for debuggability
    return f"Training failed ({exc_type}): {msg}"


@router.post("/train", response_model=TrainResponse)
def train_model(body: TrainRequest) -> TrainResponse:
    """Start model training for a modelling node.

    Executes the pipeline up to the modelling node to materialise the
    training DataFrame, then runs TrainingJob in a background thread.
    """
    node = _find_modelling_node(body.graph, body.nodeId)
    config = node.data.config

    # --- Upfront config validation (fast, before any pipeline execution) ---

    target = config.get("target")
    if not target:
        return TrainResponse(
            status="error",
            error="No target column selected. Open the config panel and choose a target column.",
        )

    algorithm = config.get("algorithm", "catboost")
    if algorithm not in ALGORITHM_REGISTRY:
        return TrainResponse(
            status="error",
            error=(
                f"Unknown algorithm '{algorithm}'. "
                f"Available algorithms: {', '.join(ALGORITHM_REGISTRY.keys())}."
            ),
        )

    job_id = uuid.uuid4().hex[:12]
    _jobs[job_id] = {
        "status": "running",
        "progress": 0.0,
        "message": "Starting",
        "config": dict(config),
        "node_label": node.data.label,
    }

    # --- Execute pipeline to get training DataFrame ---

    try:
        from haute._execute_lazy import _execute_eager_core
        from haute.executor import _build_node_fn

        result = _execute_eager_core(
            body.graph, _build_node_fn,
            target_node_id=body.nodeId,
            row_limit=None,
            swallow_errors=True,
        )
    except Exception as exc:
        error_msg = f"Pipeline execution failed: {exc}"
        logger.error("pipeline_exec_failed", error=str(exc), node_id=body.nodeId)
        _jobs[job_id] = {"status": "error", "message": error_msg}
        return TrainResponse(status="error", job_id=job_id, error=error_msg)

    # Check for upstream errors that prevented data from reaching this node
    train_df = result.outputs.get(body.nodeId)
    if train_df is None:
        # Find the actual failing node for a better message
        upstream_errors = {
            nid: err for nid, err in result.errors.items() if nid != body.nodeId
        }
        if upstream_errors:
            failed_node = next(iter(upstream_errors))
            failed_name = result.id_to_name.get(failed_node, failed_node)
            error_msg = (
                f"Upstream node '{failed_name}' failed: {upstream_errors[failed_node]}. "
                f"Fix the error in that node before training."
            )
        elif body.nodeId in result.errors:
            error_msg = f"Modelling node error: {result.errors[body.nodeId]}"
        else:
            error_msg = (
                "No training data arrived at the modelling node. "
                "Make sure an upstream data source is connected and producing data."
            )
        _jobs[job_id] = {"status": "error", "message": error_msg}
        return TrainResponse(status="error", job_id=job_id, error=error_msg)

    # --- Build TrainingJob and start in background ---

    from haute.modelling import TrainingJob

    name = config.get("name", node.data.label)
    split_raw = config.get("split", {"strategy": "random", "test_size": 0.2, "seed": 42})

    start_time = time.monotonic()
    _jobs[job_id]["start_time"] = start_time

    def _progress(msg: str, frac: float) -> None:
        _jobs[job_id].update({
            "progress": frac,
            "message": msg,
            "elapsed_seconds": time.monotonic() - start_time,
        })

    def _on_iteration(iteration: int, total: int, metrics: dict[str, float]) -> None:
        _jobs[job_id].update({
            "iteration": iteration,
            "total_iterations": total,
            "train_loss": metrics,
            "elapsed_seconds": time.monotonic() - start_time,
        })

    job = TrainingJob(
        name=name,
        data=train_df,
        target=target,
        weight=config.get("weight") or None,
        exclude=config.get("exclude", []),
        algorithm=algorithm,
        task=config.get("task", "regression"),
        params=config.get("params", {}),
        split=split_raw,
        metrics=config.get("metrics", ["gini", "rmse"]),
        mlflow_experiment=config.get("mlflow_experiment") or None,
        model_name=config.get("model_name") or None,
        output_dir=config.get("output_dir", "models"),
        loss_function=config.get("loss_function") or None,
        variance_power=config.get("variance_power"),
        offset=config.get("offset") or None,
        monotone_constraints=config.get("monotone_constraints") or None,
        feature_weights=config.get("feature_weights") or None,
        cv_folds=config.get("cv_folds"),
    )

    node_id = body.nodeId  # capture for the closure

    def _train_background() -> None:
        try:
            train_result = job.run(_progress, _on_iteration)
            response = TrainResponse(
                status="completed",
                job_id=job_id,
                metrics=train_result.metrics,
                feature_importance=train_result.feature_importance,
                model_path=train_result.model_path,
                train_rows=train_result.train_rows,
                test_rows=train_result.test_rows,
                best_iteration=train_result.best_iteration,
                loss_history=train_result.loss_history,
                double_lift=train_result.double_lift,
                shap_summary=train_result.shap_summary,
                feature_importance_loss=train_result.feature_importance_loss,
                cv_results=train_result.cv_results,
                ave_per_feature=train_result.ave_per_feature,
            )
            _jobs[job_id].update({
                "status": "completed",
                "result": response,
                "elapsed_seconds": time.monotonic() - start_time,
            })
        except ValueError as exc:
            error_msg = str(exc)
            logger.warning("training_validation_error", error=error_msg, node_id=node_id)
            _jobs[job_id].update({"status": "error", "message": error_msg})
        except Exception as exc:
            error_msg = _friendly_error(exc)
            logger.error("training_failed", error=str(exc), node_id=node_id)
            _jobs[job_id].update({"status": "error", "message": error_msg})

    thread = threading.Thread(target=_train_background, daemon=True)
    thread.start()
    return TrainResponse(status="started", job_id=job_id)


@router.get("/train/status/{job_id}", response_model=TrainStatusResponse)
async def train_status(job_id: str) -> TrainStatusResponse:
    """Poll training job progress."""
    job = _jobs.get(job_id)
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
    job = _jobs.get(body.job_id)
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
            features=[],  # not stored in TrainResponse
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
        return LogExperimentResponse(status="error", error=str(exc))


@router.post("/export", response_model=ExportScriptResponse)
async def export_script(body: ExportScriptRequest) -> ExportScriptResponse:
    """Generate a standalone training script from a modelling node's config."""
    node = _find_modelling_node(body.graph, body.nodeId)
    config = dict(node.data.config)

    # Use the node label as the default name
    if "name" not in config:
        config["name"] = node.data.label

    from haute.modelling import generate_training_script

    data_path = body.data_path or f"output/{config.get('name', 'model')}.parquet"
    script = generate_training_script(config, data_path)
    filename = f"train_{config.get('name', 'model')}.py"

    return ExportScriptResponse(script=script, filename=filename)
