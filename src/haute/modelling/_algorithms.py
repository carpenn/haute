"""Algorithm abstraction for model training."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import numpy as np
import polars as pl

# Callback type: (iteration, total_iterations, metrics_dict) -> None
IterationCallback = Callable[[int, int, dict[str, float]], None]


@dataclass
class FitResult:
    """Result of algorithm.fit() — model plus training artifacts."""

    model: Any
    best_iteration: int | None = None
    loss_history: list[dict[str, float]] = field(default_factory=list)


class BaseAlgorithm(ABC):
    """Abstract base class for training algorithms."""

    @abstractmethod
    def fit(
        self,
        train_df: pl.DataFrame,
        features: list[str],
        cat_features: list[str],
        target: str,
        weight: str | None,
        params: dict[str, Any],
        task: str,
        on_iteration: IterationCallback | None = None,
        eval_df: pl.DataFrame | None = None,
    ) -> FitResult:
        """Train a model and return a FitResult."""

    @abstractmethod
    def predict(
        self, model: Any, df: pl.DataFrame, features: list[str],
    ) -> np.ndarray:
        """Generate predictions from a fitted model."""

    @abstractmethod
    def feature_importance(self, model: Any) -> list[dict[str, Any]]:
        """Return feature importances as [{feature, importance}, ...]."""

    @abstractmethod
    def save(self, model: Any, path: Path) -> None:
        """Save the model to disk."""


class _CatBoostProgressCallback:
    """CatBoost after_iteration callback that reports to an IterationCallback and collects loss history."""

    def __init__(
        self,
        on_iteration: IterationCallback | None,
        total_iterations: int,
        loss_history: list[dict[str, float]],
    ) -> None:
        self._on_iteration = on_iteration
        self._total = total_iterations
        self._loss_history = loss_history

    def after_iteration(self, info: Any) -> bool:
        metrics: dict[str, float] = {}
        history_entry: dict[str, float] = {"iteration": float(info.iteration + 1)}
        if info.metrics:
            for dataset_name, metric_dict in info.metrics.items():
                for metric_name, values in metric_dict.items():
                    label = metric_name if dataset_name == "learn" else f"{dataset_name}_{metric_name}"
                    if values:
                        metrics[label] = values[-1]
                    # Store in loss history with explicit train_/eval_ prefixes
                    prefix = "train" if dataset_name == "learn" else "eval"
                    if values:
                        history_entry[f"{prefix}_{metric_name}"] = values[-1]
        self._loss_history.append(history_entry)
        if self._on_iteration:
            self._on_iteration(info.iteration + 1, self._total, metrics)
        return True  # True = continue training


# User-friendly loss name → CatBoost loss_function string.
# For Tweedie, the caller appends `:variance_power=X` via resolve_loss_function().
REGRESSION_LOSSES = {"RMSE", "MAE", "Poisson", "Tweedie"}
CLASSIFICATION_LOSSES = {"Logloss", "CrossEntropy"}


def resolve_loss_function(
    loss_name: str | None, task: str, variance_power: float | None = None,
) -> str | None:
    """Map a user-facing loss name to a CatBoost ``loss_function`` param value.

    Returns ``None`` if no explicit loss was requested (CatBoost uses its own default).
    """
    if not loss_name:
        return None

    valid = REGRESSION_LOSSES if task == "regression" else CLASSIFICATION_LOSSES
    if loss_name not in valid:
        raise ValueError(
            f"Loss '{loss_name}' is not valid for task '{task}'. "
            f"Choose from: {sorted(valid)}"
        )

    if loss_name == "Tweedie":
        vp = variance_power if variance_power is not None else 1.5
        return f"Tweedie:variance_power={vp}"

    return loss_name


class CatBoostAlgorithm(BaseAlgorithm):
    """CatBoost gradient boosting implementation."""

    def fit(
        self,
        train_df: pl.DataFrame,
        features: list[str],
        cat_features: list[str],
        target: str,
        weight: str | None,
        params: dict[str, Any],
        task: str,
        on_iteration: IterationCallback | None = None,
        eval_df: pl.DataFrame | None = None,
        offset: str | None = None,
        monotone_constraints: dict[str, int] | None = None,
        feature_weights: dict[str, float] | None = None,
    ) -> FitResult:
        from catboost import CatBoostClassifier, CatBoostRegressor, Pool

        X = train_df.select(features).to_pandas()
        y = train_df[target].to_numpy()
        w = train_df[weight].to_numpy() if weight else None
        baseline = train_df[offset].to_numpy() if offset else None

        # Build cat_feature indices for CatBoost
        cat_indices = [features.index(f) for f in cat_features if f in features]

        pool = Pool(data=X, label=y, weight=w, cat_features=cat_indices, baseline=baseline)

        # Build eval pool if eval_df provided
        eval_pool = None
        if eval_df is not None:
            eval_X = eval_df.select(features).to_pandas()
            eval_y = eval_df[target].to_numpy()
            eval_w = eval_df[weight].to_numpy() if weight else None
            eval_baseline = eval_df[offset].to_numpy() if offset else None
            eval_pool = Pool(
                data=eval_X, label=eval_y, weight=eval_w,
                cat_features=cat_indices, baseline=eval_baseline,
            )

        model_params = {**params}
        # Suppress verbose output and training log files by default
        if "verbose" not in model_params:
            model_params["verbose"] = 0
        if "allow_writing_files" not in model_params:
            model_params["allow_writing_files"] = False

        # Enable early stopping when eval set is present and user hasn't explicitly set it
        if eval_pool is not None and "early_stopping_rounds" not in model_params:
            early_stop = model_params.pop("early_stopping_rounds", 50)
            if early_stop and early_stop > 0:
                model_params["early_stopping_rounds"] = early_stop

        # Map feature-name-based monotone_constraints to index-based list
        if monotone_constraints:
            mc_list = [monotone_constraints.get(f, 0) for f in features]
            model_params["monotone_constraints"] = mc_list

        # Map feature-name-based feature_weights to index-based list
        if feature_weights:
            fw_list = [feature_weights.get(f, 1.0) for f in features]
            model_params["feature_weights"] = fw_list

        total_iterations = model_params.get("iterations", 1000)

        if task == "classification":
            model = CatBoostClassifier(**model_params)
        else:
            model = CatBoostRegressor(**model_params)

        # Always collect loss history via callback
        loss_history: list[dict[str, float]] = []
        callbacks = [_CatBoostProgressCallback(on_iteration, total_iterations, loss_history)]

        fit_kwargs: dict[str, Any] = {"callbacks": callbacks}
        if eval_pool is not None:
            fit_kwargs["eval_set"] = eval_pool

        model.fit(pool, **fit_kwargs)

        # Capture best iteration if early stopping was active
        best_iteration: int | None = None
        if eval_pool is not None and hasattr(model, "best_iteration_"):
            best_iteration = model.best_iteration_

        return FitResult(
            model=model,
            best_iteration=best_iteration,
            loss_history=loss_history,
        )

    def predict(
        self, model: Any, df: pl.DataFrame, features: list[str],
    ) -> np.ndarray:
        X = df.select(features).to_pandas()
        return model.predict(X).flatten()

    def feature_importance(self, model: Any) -> list[dict[str, Any]]:
        names = model.feature_names_
        importances = model.get_feature_importance()
        pairs = sorted(
            zip(names, importances), key=lambda x: x[1], reverse=True,
        )
        return [
            {"feature": name, "importance": float(imp)}
            for name, imp in pairs
        ]

    def feature_importance_typed(
        self, model: Any, pool: Any, type_name: str,
    ) -> list[dict[str, Any]]:
        """Get feature importance using a specific CatBoost importance type.

        Supported types: PredictionValuesChange, LossFunctionChange, ShapValues.
        """
        names = model.feature_names_
        importances = model.get_feature_importance(data=pool, type=type_name)
        pairs = sorted(
            zip(names, importances), key=lambda x: x[1], reverse=True,
        )
        return [
            {"feature": name, "importance": float(imp)}
            for name, imp in pairs
        ]

    def shap_summary(
        self, model: Any, df: pl.DataFrame, features: list[str],
        max_rows: int = 1000,
    ) -> list[dict[str, float]]:
        """Compute mean |SHAP| per feature using CatBoost's native SHAP.

        Subsamples to max_rows for performance. Returns
        [{feature, mean_abs_shap}, ...] sorted by importance desc.
        """
        from catboost import Pool

        sample = df.sample(min(len(df), max_rows), seed=42) if len(df) > max_rows else df
        X = sample.select(features).to_pandas()
        pool = Pool(data=X, cat_features=[])

        # CatBoost ShapValues returns shape (n_samples, n_features + 1), last col is base value
        shap_values = model.get_feature_importance(data=pool, type="ShapValues")
        # Drop the base value column
        shap_values = shap_values[:, :-1]

        mean_abs = np.abs(shap_values).mean(axis=0)
        pairs = sorted(zip(features, mean_abs), key=lambda x: x[1], reverse=True)
        return [
            {"feature": name, "mean_abs_shap": float(val)}
            for name, val in pairs
        ]

    def cross_validate(
        self,
        train_df: pl.DataFrame,
        features: list[str],
        cat_features: list[str],
        target: str,
        weight: str | None,
        params: dict[str, Any],
        task: str,
        n_folds: int = 5,
    ) -> dict[str, Any]:
        """Run CatBoost's built-in cross-validation.

        Returns a dict with:
          fold_metrics: list of per-fold metric dicts
          mean_metrics: mean across folds
          std_metrics: std across folds
        """
        from catboost import Pool, cv

        X = train_df.select(features).to_pandas()
        y = train_df[target].to_numpy()
        w = train_df[weight].to_numpy() if weight else None
        cat_indices = [features.index(f) for f in cat_features if f in features]

        pool = Pool(data=X, label=y, weight=w, cat_features=cat_indices)

        cv_params = {**params}
        if "verbose" not in cv_params:
            cv_params["verbose"] = 0
        if "allow_writing_files" not in cv_params:
            cv_params["allow_writing_files"] = False
        # CatBoost cv() requires loss_function to be set explicitly
        if "loss_function" not in cv_params:
            cv_params["loss_function"] = "Logloss" if task == "classification" else "RMSE"

        # CatBoost cv returns a DataFrame-like dict of metric columns
        cv_result = cv(pool, cv_params, fold_count=n_folds, as_pandas=True)

        # Extract final-iteration metrics per fold from the CV result
        # cv_result columns look like "test-RMSE-mean", "test-RMSE-std", etc.
        mean_metrics: dict[str, float] = {}
        std_metrics: dict[str, float] = {}
        for col in cv_result.columns:
            if col.startswith("test-") and col.endswith("-mean"):
                metric_name = col.replace("test-", "").replace("-mean", "")
                mean_metrics[metric_name] = float(cv_result[col].iloc[-1])
            if col.startswith("test-") and col.endswith("-std"):
                metric_name = col.replace("test-", "").replace("-std", "")
                std_metrics[metric_name] = float(cv_result[col].iloc[-1])

        return {
            "fold_metrics": [],  # CatBoost cv doesn't expose per-fold, only mean/std
            "mean_metrics": mean_metrics,
            "std_metrics": std_metrics,
            "n_folds": n_folds,
        }

    def save(self, model: Any, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        model.save_model(str(path))


ALGORITHM_REGISTRY: dict[str, type[BaseAlgorithm]] = {
    "catboost": CatBoostAlgorithm,
}
