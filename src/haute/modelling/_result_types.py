"""Shared data types for model diagnostics and metadata.

Used by ``_model_card``, ``_mlflow_log``, and ``_training_job``
to bundle diagnostic data without 25+ positional parameters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ModelDiagnostics:
    """Bundled diagnostic chart data produced during model evaluation."""

    feature_importance: list[dict[str, Any]] = field(default_factory=list)
    shap_summary: list[dict[str, float]] = field(default_factory=list)
    feature_importance_loss: list[dict[str, Any]] = field(default_factory=list)
    double_lift: list[dict[str, Any]] = field(default_factory=list)
    loss_history: list[dict[str, float]] = field(default_factory=list)
    cv_results: dict[str, Any] | None = None
    ave_per_feature: list[dict[str, Any]] = field(default_factory=list)
    residuals_histogram: list[dict[str, Any]] = field(default_factory=list)
    residuals_stats: dict[str, float] = field(default_factory=dict)
    actual_vs_predicted: list[dict[str, float]] = field(default_factory=list)
    lorenz_curve: list[dict[str, float]] = field(default_factory=list)
    lorenz_curve_perfect: list[dict[str, float]] = field(default_factory=list)
    pdp_data: list[dict[str, Any]] = field(default_factory=list)
    holdout_metrics: dict[str, float] = field(default_factory=dict)
    diagnostics_set: str = "validation"


@dataclass
class ModelCardMetadata:
    """Training context metadata for model cards and MLflow logging."""

    algorithm: str = ""
    task: str = ""
    train_rows: int = 0
    test_rows: int = 0
    holdout_rows: int = 0
    features: list[str] = field(default_factory=list)
    split_config: dict[str, Any] = field(default_factory=dict)
    best_iteration: int | None = None
