"""Metric computation for model evaluation."""

from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl


def compute_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    weight: np.ndarray | None = None,
    metric_names: list[str] | None = None,
) -> dict[str, float]:
    """Compute requested metrics between actuals and predictions.

    All metrics support optional sample weights (exposure weighting
    is standard in insurance).
    """
    if metric_names is None:
        metric_names = ["gini", "rmse"]

    results: dict[str, float] = {}
    for name in metric_names:
        fn = _METRIC_REGISTRY.get(name.lower())
        if fn is None:
            raise ValueError(f"Unknown metric: {name}. Available: {list(_METRIC_REGISTRY.keys())}")
        results[name] = fn(y_true, y_pred, weight)
    return results


def _gini(
    y_true: np.ndarray, y_pred: np.ndarray, weight: np.ndarray | None,
) -> float:
    """Normalised Gini coefficient — measures rank ordering of predictions.

    Gini = 2 * AUC - 1 for binary targets, or the ratio of the area
    between the Lorenz curve and the line of equality.  Critical for
    insurance pricing — a model with Gini=0.5 means predictions
    meaningfully separate high- from low-risk.
    """
    n = len(y_true)
    if n == 0:
        return 0.0

    w = weight if weight is not None else np.ones(n)

    # Sort by predicted values (descending)
    order = np.argsort(-y_pred)
    y_sorted = y_true[order]
    w_sorted = w[order]

    # Weighted cumulative sums
    cum_weight = np.cumsum(w_sorted)
    cum_loss = np.cumsum(y_sorted * w_sorted)

    total_weight = cum_weight[-1]
    total_loss = cum_loss[-1]

    if total_weight == 0 or total_loss == 0:
        return 0.0

    # Normalised cumulative fractions
    cum_weight_frac = cum_weight / total_weight
    cum_loss_frac = cum_loss / total_loss

    # Gini = 1 - 2 * area under Lorenz curve
    # Area under Lorenz curve via trapezoidal rule
    area = np.trapezoid(cum_loss_frac, cum_weight_frac)
    raw_gini = 1.0 - 2.0 * area

    # Normalise by perfect model's Gini
    perfect_order = np.argsort(-y_true)
    y_perfect = y_true[perfect_order]
    w_perfect = w[perfect_order]
    cum_loss_perfect = np.cumsum(y_perfect * w_perfect) / total_loss
    cum_weight_perfect = np.cumsum(w_perfect) / total_weight
    area_perfect = np.trapezoid(cum_loss_perfect, cum_weight_perfect)
    perfect_gini = 1.0 - 2.0 * area_perfect

    if perfect_gini == 0:
        return 0.0

    return raw_gini / perfect_gini


def _rmse(
    y_true: np.ndarray, y_pred: np.ndarray, weight: np.ndarray | None,
) -> float:
    """Root Mean Squared Error (optionally weighted)."""
    residuals = y_true - y_pred
    if weight is not None:
        return float(np.sqrt(np.average(residuals**2, weights=weight)))
    return float(np.sqrt(np.mean(residuals**2)))


def _mae(
    y_true: np.ndarray, y_pred: np.ndarray, weight: np.ndarray | None,
) -> float:
    """Mean Absolute Error (optionally weighted)."""
    residuals = np.abs(y_true - y_pred)
    if weight is not None:
        return float(np.average(residuals, weights=weight))
    return float(np.mean(residuals))


def _mse(
    y_true: np.ndarray, y_pred: np.ndarray, weight: np.ndarray | None,
) -> float:
    """Mean Squared Error (optionally weighted)."""
    residuals = y_true - y_pred
    if weight is not None:
        return float(np.average(residuals**2, weights=weight))
    return float(np.mean(residuals**2))


def _r2(
    y_true: np.ndarray, y_pred: np.ndarray, weight: np.ndarray | None,
) -> float:
    """R-squared / coefficient of determination (optionally weighted)."""
    if weight is not None:
        ss_res = np.average((y_true - y_pred) ** 2, weights=weight)
        ss_tot = np.average((y_true - np.average(y_true, weights=weight)) ** 2, weights=weight)
    else:
        ss_res = np.mean((y_true - y_pred) ** 2)
        ss_tot = np.mean((y_true - np.mean(y_true)) ** 2)
    if ss_tot == 0:
        return 0.0
    return float(1 - ss_res / ss_tot)


def _auc(
    y_true: np.ndarray, y_pred: np.ndarray, weight: np.ndarray | None,
) -> float:
    """Area Under the ROC Curve (binary classification)."""
    from sklearn.metrics import roc_auc_score

    if weight is not None:
        return float(roc_auc_score(y_true, y_pred, sample_weight=weight))
    return float(roc_auc_score(y_true, y_pred))


def _logloss(
    y_true: np.ndarray, y_pred: np.ndarray, weight: np.ndarray | None,
) -> float:
    """Log Loss / binary cross-entropy."""
    from sklearn.metrics import log_loss

    if weight is not None:
        return float(log_loss(y_true, y_pred, sample_weight=weight))
    return float(log_loss(y_true, y_pred))


def _poisson_deviance(
    y_true: np.ndarray, y_pred: np.ndarray, weight: np.ndarray | None,
) -> float:
    """Poisson deviance — appropriate for count/frequency models."""
    y_pred_safe = np.maximum(y_pred, 1e-10)
    dev = 2.0 * (y_true * np.log(np.maximum(y_true, 1e-10) / y_pred_safe) - (y_true - y_pred_safe))
    if weight is not None:
        return float(np.average(dev, weights=weight))
    return float(np.mean(dev))


def _tweedie_deviance(
    y_true: np.ndarray, y_pred: np.ndarray, weight: np.ndarray | None,
    variance_power: float = 1.5,
) -> float:
    """Tweedie deviance — generalises Poisson (p=1) and Gamma (p=2)."""
    p = variance_power
    y_pred_safe = np.maximum(y_pred, 1e-10)
    y_true_safe = np.maximum(y_true, 0.0)

    if abs(p - 1.0) < 1e-8:
        # Poisson case
        return _poisson_deviance(y_true, y_pred, weight)
    elif abs(p - 2.0) < 1e-8:
        # Gamma case
        dev = 2.0 * (-np.log(np.maximum(y_true_safe, 1e-10) / y_pred_safe) + (y_true_safe - y_pred_safe) / y_pred_safe)
    else:
        term1 = np.power(y_true_safe, 2 - p) / ((1 - p) * (2 - p)) if np.any(y_true_safe > 0) else 0.0
        term2 = y_true_safe * np.power(y_pred_safe, 1 - p) / (1 - p)
        term3 = np.power(y_pred_safe, 2 - p) / (2 - p)
        dev = 2.0 * (term1 - term2 + term3)

    if weight is not None:
        return float(np.average(dev, weights=weight))
    return float(np.mean(dev))


def compute_double_lift(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    weight: np.ndarray | None = None,
    n_bins: int = 10,
) -> list[dict[str, Any]]:
    """Compute actual-vs-predicted by decile for double-lift analysis.

    Returns a list of dicts: [{decile, actual, predicted, count}, ...]
    """
    n = len(y_true)
    if n == 0:
        return []

    w = weight if weight is not None else np.ones(n)

    # Sort by predicted value
    order = np.argsort(y_pred)
    y_true_sorted = y_true[order]
    y_pred_sorted = y_pred[order]
    w_sorted = w[order]

    # Split into bins
    bins = np.array_split(np.arange(n), n_bins)
    result = []
    for i, idx in enumerate(bins):
        if len(idx) == 0:
            continue
        bw = w_sorted[idx]
        total_w = bw.sum()
        if total_w == 0:
            continue
        actual = float(np.average(y_true_sorted[idx], weights=bw))
        predicted = float(np.average(y_pred_sorted[idx], weights=bw))
        result.append({
            "decile": i + 1,
            "actual": round(actual, 6),
            "predicted": round(predicted, 6),
            "count": len(idx),
        })
    return result


def compute_ave_per_feature(
    df: pl.DataFrame,
    features: list[str],
    cat_features: list[str],
    y_true: np.ndarray,
    y_pred: np.ndarray,
    weight: np.ndarray | None = None,
    *,
    n_bins: int = 10,
    max_features: int = 15,
    max_categories: int = 15,
) -> list[dict[str, Any]]:
    """Compute Actual vs Expected (AvE) one-way analysis per feature.

    For each feature (up to *max_features*, in the order given — the caller
    should pre-sort by importance):
    - Numeric features are quantile-binned into *n_bins*.
    - Categorical features are grouped by category (top *max_categories*
      by weight, remainder lumped into ``"Other"``).

    Returns a list of dicts::

        {feature, type ("numeric"|"categorical"),
         bins: [{label, exposure, avg_actual, avg_predicted}]}
    """
    if len(features) == 0 or len(y_true) == 0:
        return []

    w = weight if weight is not None else np.ones(len(y_true))
    cat_set = set(cat_features)
    results: list[dict[str, Any]] = []

    for feat in features[:max_features]:
        if feat not in df.columns:
            continue
        col = df[feat]
        is_cat = feat in cat_set

        if is_cat:
            bins = _ave_categorical_bins(col, y_true, y_pred, w, max_categories)
            feat_type = "categorical"
        else:
            bins = _ave_numeric_bins(col, y_true, y_pred, w, n_bins)
            feat_type = "numeric"

        results.append({"feature": feat, "type": feat_type, "bins": bins})

    return results


def _ave_numeric_bins(
    col: pl.Series,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    weight: np.ndarray,
    n_bins: int,
) -> list[dict[str, Any]]:
    """Bin a numeric feature by quantile and compute weighted AvE per bin."""
    vals = col.to_numpy().astype(float)
    n = len(vals)

    # Separate NaN mask
    nan_mask = np.isnan(vals)
    valid_mask = ~nan_mask

    valid_count = int(valid_mask.sum())
    bins: list[dict[str, Any]] = []

    if valid_count > 0:
        effective_bins = min(n_bins, max(2, valid_count // 3))

        # Compute quantile edges on valid values
        valid_vals = vals[valid_mask]
        quantiles = np.linspace(0, 100, effective_bins + 1)
        edges = np.unique(np.percentile(valid_vals, quantiles))

        # Digitize: assign each valid value to a bin (1-indexed)
        bin_indices = np.digitize(valid_vals, edges[1:-1])  # 0 to len(edges)-2

        # Build a full-length bin assignment (-1 for NaN)
        full_bins = np.full(n, -1, dtype=int)
        full_bins[valid_mask] = bin_indices

        actual_n_bins = int(bin_indices.max()) + 1 if len(bin_indices) > 0 else 0
        for b in range(actual_n_bins):
            mask = full_bins == b
            if not mask.any():
                continue
            bw = weight[mask]
            total_w = bw.sum()
            if total_w == 0:
                continue

            bin_vals = vals[mask]
            lo, hi = float(np.nanmin(bin_vals)), float(np.nanmax(bin_vals))
            label = f"{lo:.4g}–{hi:.4g}" if lo != hi else f"{lo:.4g}"

            bins.append({
                "label": label,
                "exposure": round(float(total_w), 4),
                "avg_actual": round(float(np.average(y_true[mask], weights=bw)), 6),
                "avg_predicted": round(float(np.average(y_pred[mask], weights=bw)), 6),
            })

    # NaN bin
    if nan_mask.any():
        bw = weight[nan_mask]
        total_w = bw.sum()
        if total_w > 0:
            bins.append({
                "label": "Missing",
                "exposure": round(float(total_w), 4),
                "avg_actual": round(float(np.average(y_true[nan_mask], weights=bw)), 6),
                "avg_predicted": round(float(np.average(y_pred[nan_mask], weights=bw)), 6),
            })

    return bins


def _ave_categorical_bins(
    col: pl.Series,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    weight: np.ndarray,
    max_categories: int,
) -> list[dict[str, Any]]:
    """Group a categorical feature by category and compute weighted AvE."""
    vals = col.cast(pl.Utf8).to_list()
    n = len(vals)

    # Count exposure per category
    cat_exposure: dict[str, float] = {}
    for i in range(n):
        key = vals[i] if vals[i] is not None else "__MISSING__"
        cat_exposure[key] = cat_exposure.get(key, 0.0) + weight[i]

    # Top categories by exposure
    sorted_cats = sorted(cat_exposure.items(), key=lambda x: -x[1])
    top_cats = {c for c, _ in sorted_cats[:max_categories]}
    has_other = len(sorted_cats) > max_categories

    # Build category → index mapping
    cat_labels = [c for c, _ in sorted_cats[:max_categories]]
    if has_other:
        cat_labels.append("Other")

    # Assign each row to a category label
    bins_data: dict[str, dict[str, float]] = {
        label: {"exposure": 0.0, "sum_actual": 0.0, "sum_predicted": 0.0}
        for label in cat_labels
    }

    for i in range(n):
        key = vals[i] if vals[i] is not None else "__MISSING__"
        if key in top_cats:
            label = "Missing" if key == "__MISSING__" else key
            if label not in bins_data:
                label = key  # use raw key if "Missing" wasn't in top_cats
        else:
            label = "Other"

        if label not in bins_data:
            bins_data[label] = {"exposure": 0.0, "sum_actual": 0.0, "sum_predicted": 0.0}

        bins_data[label]["exposure"] += weight[i]
        bins_data[label]["sum_actual"] += y_true[i] * weight[i]
        bins_data[label]["sum_predicted"] += y_pred[i] * weight[i]

    bins: list[dict[str, Any]] = []
    for label in cat_labels:
        # Rename __MISSING__ to Missing for display
        display_label = "Missing" if label == "__MISSING__" else label
        d = bins_data.get(label, bins_data.get(display_label, None))
        if d is None or d["exposure"] == 0:
            continue
        bins.append({
            "label": display_label,
            "exposure": round(d["exposure"], 4),
            "avg_actual": round(d["sum_actual"] / d["exposure"], 6),
            "avg_predicted": round(d["sum_predicted"] / d["exposure"], 6),
        })

    return bins


_METRIC_REGISTRY: dict[str, Any] = {
    "gini": _gini,
    "rmse": _rmse,
    "mae": _mae,
    "mse": _mse,
    "r2": _r2,
    "auc": _auc,
    "logloss": _logloss,
    "poisson_deviance": _poisson_deviance,
    "tweedie_deviance": _tweedie_deviance,
}
