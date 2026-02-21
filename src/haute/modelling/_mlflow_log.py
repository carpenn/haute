"""Standalone MLflow experiment logging for training results.

Used by:
- The "Log to MLflow" button in the UI (via routes/modelling.py)
- TrainingJob._log_to_mlflow (when mlflow_experiment is set during training)
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from haute._logging import get_logger

logger = get_logger(component="mlflow_log")


@dataclass
class MLflowLogResult:
    """Result of logging an experiment to MLflow."""

    backend: str  # "databricks" or "local"
    experiment_name: str
    run_id: str
    tracking_uri: str
    run_url: str | None  # Databricks URL to the run, or None for local


def resolve_tracking_backend() -> tuple[str, str]:
    """Detect whether to use Databricks MLflow or local file-based MLflow.

    Returns:
        (tracking_uri, backend_label) — e.g. ("databricks", "databricks")
        or ("file:///path/to/mlruns", "local").
    """
    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")

    if host and token:
        return "databricks", "databricks"

    mlruns_dir = Path.cwd() / "mlruns"
    return f"file://{mlruns_dir}", "local"


def _log_json_artifact(mlflow: Any, data: Any, prefix: str, artifact_dir: str) -> None:
    """Write *data* to a temp JSON file and log it as an MLflow artifact."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix=f"{prefix}_", delete=False,
    ) as f:
        json.dump(data, f, indent=2)
    try:
        mlflow.log_artifact(f.name, artifact_dir)
    finally:
        os.unlink(f.name)


def log_experiment(
    *,
    experiment_name: str,
    run_name: str,
    metrics: dict[str, float],
    params: dict[str, Any],
    model_path: str | None = None,
    model_name: str | None = None,
    shap_summary: list[dict[str, Any]] | None = None,
    feature_importance_loss: list[dict[str, Any]] | None = None,
    cv_results: dict[str, Any] | None = None,
    # --- additional artifacts ---
    feature_importance: list[dict[str, Any]] | None = None,
    double_lift: list[dict[str, Any]] | None = None,
    loss_history: list[dict[str, float]] | None = None,
    ave_per_feature: list[dict[str, Any]] | None = None,
    # --- metadata for model card ---
    algorithm: str = "",
    task: str = "",
    train_rows: int = 0,
    test_rows: int = 0,
    best_iteration: int | None = None,
    features: list[str] | None = None,
    split_config: dict[str, Any] | None = None,
) -> MLflowLogResult:
    """Log a training experiment to MLflow.

    Auto-detects Databricks (when DATABRICKS_HOST/TOKEN present)
    vs local file-based MLflow.

    Args:
        experiment_name: MLflow experiment path (e.g. "/Shared/haute/my-model").
        run_name: Name for this run.
        metrics: Metric dict to log.
        params: Parameter dict to log.
        model_path: Optional path to model file to log as artifact.
        model_name: Optional registered model name (Databricks UC only).
        shap_summary: Optional SHAP summary to log as artifact.
        feature_importance_loss: Optional LossFunctionChange importance to log.
        cv_results: Optional cross-validation results to log.
        feature_importance: Optional PredictionValuesChange importance.
        double_lift: Optional double-lift data.
        loss_history: Optional training loss history.
        ave_per_feature: Optional AvE per-feature data.
        algorithm: Algorithm name for model card.
        task: Task type for model card.
        train_rows: Training set size.
        test_rows: Test set size.
        best_iteration: Best iteration from early stopping.
        features: List of feature names.
        split_config: Split configuration dict.

    Returns:
        MLflowLogResult with backend, experiment name, run ID, and URLs.
    """
    import mlflow

    tracking_uri, backend = resolve_tracking_backend()
    logger.info("mlflow_logging_started", experiment=experiment_name, backend=backend)

    mlflow.set_tracking_uri(tracking_uri)
    if backend == "databricks":
        mlflow.set_registry_uri("databricks-uc")

    mlflow.set_experiment(experiment_name)

    # Enhanced params: add training metadata
    enhanced_params = dict(params)
    if train_rows:
        enhanced_params["train_rows"] = train_rows
    if test_rows:
        enhanced_params["test_rows"] = test_rows
    if features:
        enhanced_params["n_features"] = len(features)
    if best_iteration is not None:
        enhanced_params["best_iteration"] = best_iteration

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_params(enhanced_params)
        mlflow.log_metrics(metrics)

        # Log model file as artifact
        if model_path and Path(model_path).exists():
            mlflow.log_artifact(model_path)

        # Log SHAP summary
        if shap_summary:
            _log_json_artifact(mlflow, shap_summary, "shap_summary", "shap")

        # Log LossFunctionChange importance
        if feature_importance_loss:
            _log_json_artifact(mlflow, feature_importance_loss, "importance_loss", "importance")

        # Log CV results
        if cv_results:
            _log_json_artifact(mlflow, cv_results, "cv_results", "cv")
            for k, v in cv_results.get("mean_metrics", {}).items():
                mlflow.log_metric(f"cv_mean_{k}", v)

        # Log double lift
        if double_lift:
            _log_json_artifact(mlflow, double_lift, "double_lift", "diagnostics")

        # Log loss history
        if loss_history:
            _log_json_artifact(mlflow, loss_history, "loss_history", "diagnostics")

        # Log PredictionValuesChange importance
        if feature_importance:
            _log_json_artifact(mlflow, feature_importance, "importance_prediction", "importance")

        # Log AvE per feature
        if ave_per_feature:
            _log_json_artifact(mlflow, ave_per_feature, "ave_per_feature", "diagnostics")

        # Generate and log model card (best-effort — never fails the run)
        try:
            _log_model_card(
                mlflow,
                name=run_name,
                algorithm=algorithm,
                task=task,
                metrics=metrics,
                params=params,
                train_rows=train_rows,
                test_rows=test_rows,
                features=features or [],
                split_config=split_config or {},
                best_iteration=best_iteration,
                loss_history=loss_history,
                double_lift=double_lift,
                feature_importance=feature_importance,
                shap_summary=shap_summary,
                feature_importance_loss=feature_importance_loss,
                cv_results=cv_results,
                ave_per_feature=ave_per_feature,
            )
        except Exception:
            logger.warning("model_card_generation_failed", exc_info=True)

        # Register model (Databricks UC only)
        if model_name and model_path and backend == "databricks":
            mlflow.register_model(
                f"runs:/{run.info.run_id}/{Path(model_path).name}",
                model_name,
            )

        run_id = run.info.run_id

    # Build run URL for Databricks
    run_url: str | None = None
    if backend == "databricks":
        host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        run_url = f"{host}#mlflow/experiments/{experiment_name}/runs/{run_id}"

    logger.info("mlflow_logging_completed", run_id=run_id, backend=backend)
    return MLflowLogResult(
        backend=backend,
        experiment_name=experiment_name,
        run_id=run_id,
        tracking_uri=tracking_uri,
        run_url=run_url,
    )


def _log_model_card(
    mlflow: Any,
    **kwargs: Any,
) -> None:
    """Generate HTML model card and log as MLflow artifact."""
    from haute.modelling._model_card import generate_model_card

    html_content = generate_model_card(**kwargs)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".html", prefix="model_card_", delete=False,
    ) as f:
        f.write(html_content)
    try:
        mlflow.log_artifact(f.name, "model_card")
    finally:
        os.unlink(f.name)
