"""Modelling endpoints: train, status, export."""

from __future__ import annotations

import gc
import os
import tempfile
import threading
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute.graph_utils import NodeType
from haute.modelling._algorithms import ALGORITHM_REGISTRY
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
# Jobs older than _JOB_TTL_SECONDS are evicted on each new write.
_jobs: dict[str, dict[str, Any]] = {}
_JOB_TTL_SECONDS = 24 * 60 * 60  # 24 hours


def _evict_stale_jobs() -> None:
    """Remove jobs older than TTL to bound memory usage."""
    cutoff = time.time() - _JOB_TTL_SECONDS
    stale = [jid for jid, j in _jobs.items() if j.get("created_at", 0) < cutoff]
    for jid in stale:
        del _jobs[jid]


def _find_modelling_node(graph: Any, node_id: str) -> Any:
    """Find and validate a modelling node in the graph."""
    node = graph.node_map.get(node_id)
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


class _VramCheck:
    """Result of a GPU VRAM feasibility check."""

    __slots__ = ("estimated_mb", "available_mb", "warning")

    def __init__(
        self,
        estimated_mb: float | None = None,
        available_mb: float | None = None,
        warning: str | None = None,
    ) -> None:
        self.estimated_mb = estimated_mb
        self.available_mb = available_mb
        self.warning = warning


def _check_gpu_vram(
    effective_rows: int,
    probe_columns: int,
    params: dict[str, Any],
) -> _VramCheck:
    """Estimate GPU VRAM requirements and return a check result.

    Returns a ``_VramCheck`` with estimated/available VRAM in MB and an
    optional warning string if VRAM is insufficient.
    """
    if effective_rows <= 0 or probe_columns <= 0:
        return _VramCheck()

    from haute._ram_estimate import available_vram_bytes, estimate_gpu_vram_bytes

    vram_needed = estimate_gpu_vram_bytes(
        effective_rows, probe_columns,
        border_count=params.get("border_count", 128),
        depth=params.get("depth", 6),
    )
    estimated_mb = round(vram_needed / 1024**2, 1)

    vram = available_vram_bytes()
    available_mb = round(vram / 1024**2, 1) if vram is not None else None

    warning: str | None = None
    if vram is not None and vram_needed > vram:
        warning = (
            f"GPU training needs ~{vram_needed / 1024**3:.1f} GB VRAM "
            f"but GPU has {vram / 1024**3:.1f} GB."
        )

    return _VramCheck(
        estimated_mb=estimated_mb,
        available_mb=available_mb,
        warning=warning,
    )


@router.post("/train", response_model=TrainResponse)
def train_model(body: TrainRequest) -> TrainResponse:
    """Start model training for a modelling node.

    Executes the pipeline up to the modelling node to materialise the
    training DataFrame, then runs TrainingJob in a background thread.
    """
    node = _find_modelling_node(body.graph, body.node_id)
    config = node.data.config

    # --- Upfront config validation (fast, before any pipeline execution) ---

    target = config.get("target")
    if not target:
        raise HTTPException(
            status_code=400,
            detail="No target column selected. Open the config panel and choose a target column.",
        )

    algorithm = config.get("algorithm", "catboost")
    if algorithm not in ALGORITHM_REGISTRY:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown algorithm '{algorithm}'. "
                f"Available algorithms: {', '.join(ALGORITHM_REGISTRY.keys())}."
            ),
        )

    _evict_stale_jobs()

    # Reject if a training job is already running — concurrent runs double
    # memory and can easily OOM on 32 GB machines with 10M-row datasets.
    running = [jid for jid, j in _jobs.items() if j.get("status") == "running"]
    if running:
        raise HTTPException(
            status_code=409,
            detail="A training job is already running. Please wait for it to finish.",
        )

    job_id = uuid.uuid4().hex[:12]
    _jobs[job_id] = {
        "status": "running",
        "progress": 0.0,
        "message": "Starting",
        "config": dict(config),
        "node_label": node.data.label,
        "created_at": time.time(),
    }

    # --- Estimate RAM and decide on row limit ---

    from haute._execute_lazy import _execute_lazy
    from haute.executor import _build_node_fn, _compile_preamble

    preamble_ns = _compile_preamble(body.graph.preamble or "") or None
    ram_warning: str | None = None
    total_source_rows: int | None = None
    probe_columns: int = 0

    try:
        from haute._ram_estimate import estimate_safe_training_rows

        _jobs[job_id].update({"message": "Estimating memory requirements"})
        ram_est = estimate_safe_training_rows(
            body.graph, body.node_id, _build_node_fn,
            preamble_ns=preamble_ns,
        )
        row_limit = ram_est.safe_row_limit
        ram_warning = ram_est.warning
        total_source_rows = ram_est.total_rows
        probe_columns = ram_est.probe_columns
        if ram_warning:
            _jobs[job_id]["warning"] = ram_warning
    except Exception as exc:
        logger.warning("ram_estimate_failed", error=str(exc))
        row_limit = None

    # --- Check GPU VRAM if task_type is GPU ---
    train_params = {**config.get("params", {})}

    if str(train_params.get("task_type", "")).upper() == "GPU":
        try:
            effective_rows = row_limit or (total_source_rows or 0)
            vram_check = _check_gpu_vram(effective_rows, probe_columns, train_params)
            if vram_check.warning:
                train_params["task_type"] = "CPU"
                gpu_warning = f"{vram_check.warning} Falling back to CPU."
                logger.warning(
                    "gpu_vram_fallback",
                    estimated_mb=vram_check.estimated_mb,
                    available_mb=vram_check.available_mb,
                )
                _jobs[job_id]["gpu_warning"] = gpu_warning
                ram_warning = (
                    f"{ram_warning}\n{gpu_warning}" if ram_warning
                    else gpu_warning
                )
                _jobs[job_id]["warning"] = ram_warning
        except Exception as exc:
            logger.warning("vram_estimate_failed", error=str(exc))

    # --- Execute pipeline lazily, collect only the target node ---
    #
    # Uses the lazy path so Polars builds a single optimised query plan
    # across all nodes and only materialises once at .collect().
    # The eager path materialises every intermediate node, which for
    # 10M+ rows can exceed available RAM.

    from haute.modelling._algorithms import _MEM_LOG, _mem_checkpoint
    _MEM_LOG.write_text("")
    _mem_checkpoint("train_model endpoint START")

    # Free the preview cache — it holds eagerly-materialised DataFrames
    # from node clicks, which can consume tens of GB with large datasets.
    from haute.executor import _preview_cache
    _preview_cache.invalidate()
    gc.collect()
    _mem_checkpoint("cleared preview cache")

    # --- Sink pipeline to temp parquet (optimiser pattern) ---
    #
    # Instead of .collect() (which holds the full DataFrame in Python),
    # sink the LazyFrame directly to a temp parquet file.  Python never
    # holds the full dataset; TrainingJob reads train/test partitions
    # from disk with predicate pushdown.
    tmp_fd, tmp_parquet = tempfile.mkstemp(suffix=".parquet", prefix="haute_train_")
    os.close(tmp_fd)

    try:
        _jobs[job_id].update({"message": "Executing pipeline"})
        _mem_checkpoint("before _execute_lazy")
        lazy_outputs, _order, _parents, _id_to_name = _execute_lazy(
            body.graph, _build_node_fn,
            target_node_id=body.node_id,
            preamble_ns=preamble_ns,
        )

        target_lf = lazy_outputs.get(body.node_id)
        if target_lf is None:
            raise ValueError(
                "No training data arrived at the modelling node. "
                "Make sure an upstream data source is connected and producing data."
            )

        if row_limit:
            target_lf = target_lf.head(row_limit)

        _mem_checkpoint("before sink_parquet")
        try:
            target_lf.sink_parquet(tmp_parquet)
        except Exception as sink_exc:
            # Fallback: some lazy plans don't support streaming sink
            # (e.g. Python UDFs in transform nodes).
            logger.info("sink_streaming_fallback", node_id=body.node_id, reason=str(sink_exc))
            _mem_checkpoint("sink_parquet failed, collecting")
            df = target_lf.collect()
            df.write_parquet(tmp_parquet)
            del df

        # Free the lazy plan and return malloc pages to the OS.
        # The streaming fallback collects the full DataFrame — del + gc
        # frees it in Python but glibc holds the pages.  malloc_trim
        # forces them back to the OS (can recover 10+ GB).
        del lazy_outputs, target_lf
        gc.collect()
        from haute.modelling._algorithms import _malloc_trim
        _malloc_trim()
        _mem_checkpoint("sunk to temp parquet")
    except Exception as exc:
        # Clean up temp file on pipeline failure
        if os.path.exists(tmp_parquet):
            os.unlink(tmp_parquet)
        error_msg = f"Pipeline execution failed: {exc}"
        logger.error("pipeline_exec_failed", error=str(exc), node_id=body.node_id)
        _jobs[job_id] = {"status": "error", "message": error_msg}
        raise HTTPException(status_code=500, detail=error_msg)

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
        data=tmp_parquet,
        target=target,
        weight=config.get("weight") or None,
        exclude=config.get("exclude", []),
        algorithm=algorithm,
        task=config.get("task", "regression"),
        params=train_params,
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

    node_id = body.node_id  # capture for the closure

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
                warning=ram_warning,
                total_source_rows=total_source_rows,
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
        finally:
            # Clean up route-level temp parquet (optimiser pattern)
            if os.path.exists(tmp_parquet):
                os.unlink(tmp_parquet)

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

    # GPU VRAM estimation
    vram_check = _VramCheck()
    node_params = node.data.config.get("params", {})
    if str(node_params.get("task_type", "")).upper() == "GPU":
        effective_rows = ram_est.safe_row_limit or ram_est.total_rows or 0
        vram_check = _check_gpu_vram(effective_rows, ram_est.probe_columns, node_params)
        if vram_check.warning:
            vram_check.warning += " Training will fall back to CPU automatically."

    return TrainEstimateResponse(
        total_rows=ram_est.total_rows,
        safe_row_limit=ram_est.safe_row_limit,
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
