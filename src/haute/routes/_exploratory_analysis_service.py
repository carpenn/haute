"""Helpers for exploratory-analysis profiling and one-way chart aggregation."""

from __future__ import annotations

from datetime import date, datetime
from math import isnan
from typing import Any

import pandas as pd
import polars as pl
from ydata_profiling import ProfileReport

ROLE_VALUES: tuple[str, ...] = (
    "policy_key",
    "claim_key",
    "underwriting_date",
    "accident_date",
    "reporting_date",
    "transaction_date",
    "exposure",
    "target",
    "covariate",
    "fold",
)
ONE_WAY_ROLE_VALUES: frozenset[str] = frozenset(
    {
        "underwriting_date",
        "accident_date",
        "reporting_date",
        "transaction_date",
        "covariate",
        "fold",
    }
)
NUMERIC_DTYPES: frozenset[str] = frozenset(
    {
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "Decimal",
    }
)
TEMPORAL_DTYPES: frozenset[str] = frozenset({"Date", "Datetime", "Duration", "Time"})
DISGUISED_MISSING_MARKERS: frozenset[str] = frozenset(
    {
        "",
        "na",
        "n/a",
        "n.a.",
        "nan",
        "none",
        "null",
        "missing",
        "unknown",
        "unk",
        "?",
        "-",
        "--",
        "(blank)",
        "not available",
    }
)
MAX_LIST_VALUES = 25
MAX_CHART_BUCKETS = 24
MULTI_CORRELATIONS: dict[str, dict[str, bool]] = {
    "auto": {"calculate": True},
    "pearson": {"calculate": True},
    "spearman": {"calculate": True},
    "kendall": {"calculate": True},
    "cramers": {"calculate": True},
    "phi_k": {"calculate": True},
}


def normalize_field_roles(field_roles: dict[str, Any] | None) -> dict[str, str]:
    """Return a de-duplicated field→role mapping with only recognised roles."""
    if not isinstance(field_roles, dict):
        return {}
    normalized: dict[str, str] = {}
    used_roles: set[str] = set()
    for field, role in field_roles.items():
        field_name = str(field).strip()
        role_name = str(role).strip()
        if not field_name or role_name not in ROLE_VALUES or role_name in used_roles:
            continue
        normalized[field_name] = role_name
        used_roles.add(role_name)
    return normalized


def build_exploratory_analysis_payload(
    rows: list[dict[str, Any]],
    columns: list[dict[str, str]],
    field_roles: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build all exploratory-analysis tab payloads from the full dataset."""
    normalized_roles = normalize_field_roles(field_roles)
    dtype_map = {col["name"]: col["dtype"] for col in columns}
    selected_fields = [col["name"] for col in columns if col["name"] in normalized_roles]

    df = pl.from_dicts(rows) if rows else pl.DataFrame()
    pdf = df.to_pandas() if df.height > 0 else pd.DataFrame(rows)
    if pdf.empty:
        return {
            "status": "ok",
            "row_count": 0,
            "field_roles": normalized_roles,
            "descriptive_statistics": [
                {
                    "field": field,
                    "role": normalized_roles[field],
                    "dtype": dtype_map.get(field, ""),
                    "profile_type": None,
                    "non_missing_count": 0,
                    "missing_count": 0,
                    "missing_proportion": 0.0,
                    "distinct_count": 0,
                    "distinct_proportion": 0.0,
                    "mean": None,
                    "std": None,
                    "min": None,
                    "p5": None,
                    "p25": None,
                    "median": None,
                    "p75": None,
                    "p95": None,
                    "max": None,
                    "top_values": [],
                    "distribution": None,
                }
                for field in selected_fields
            ],
            "outliers_inliers": [
                {
                    "field": field,
                    "role": normalized_roles[field],
                    "dtype": dtype_map.get(field, ""),
                    "outlier": [],
                    "outlier_proportion": 0.0,
                    "inlier": [],
                    "inlier_proportion": 0.0,
                }
                for field in selected_fields
            ],
            "disguised_missings": [
                {
                    "field": field,
                    "role": normalized_roles[field],
                    "dtype": dtype_map.get(field, ""),
                    "missing_values": [],
                    "missing_proportion": 0.0,
                }
                for field in selected_fields
            ],
            "correlations": {"fields": [], "types": [], "cells": []},
            "one_way_options": [
                {
                    "field": field,
                    "role": normalized_roles[field],
                    "dtype": dtype_map.get(field, ""),
                }
                for field in selected_fields
                if normalized_roles[field] in ONE_WAY_ROLE_VALUES
            ],
            "default_x_field": None,
            "chart": None,
        }

    profile = ProfileReport(
        pdf,
        title="Exploratory Analysis",
        explorative=True,
        minimal=False,
        progress_bar=False,
        correlations=MULTI_CORRELATIONS,
    )
    description = profile.get_description()

    chart_options = [
        {
            "field": field,
            "role": normalized_roles[field],
            "dtype": dtype_map.get(field, ""),
        }
        for field in selected_fields
        if normalized_roles[field] in ONE_WAY_ROLE_VALUES
    ]
    default_x_field = chart_options[0]["field"] if chart_options else None

    return {
        "status": "ok",
        "row_count": len(rows),
        "field_roles": normalized_roles,
        "descriptive_statistics": [
            _build_descriptive_row(
                field,
                normalized_roles[field],
                dtype_map.get(field, ""),
                description.variables.get(field, {}),
            )
            for field in selected_fields
        ],
        "outliers_inliers": [
            _build_outlier_row(
                pdf[field] if field in pdf.columns else pd.Series(dtype="object"),
                field,
                normalized_roles[field],
                dtype_map.get(field, ""),
                len(rows),
            )
            for field in selected_fields
        ],
        "disguised_missings": [
            _build_missing_row(
                pdf[field] if field in pdf.columns else pd.Series(dtype="object"),
                field,
                normalized_roles[field],
                dtype_map.get(field, ""),
                len(rows),
            )
            for field in selected_fields
        ],
        "correlations": _build_correlation_matrix(description.correlations),
        "one_way_options": chart_options,
        "default_x_field": default_x_field,
        "chart": build_one_way_chart_payload(rows, columns, normalized_roles, default_x_field),
    }


def build_one_way_chart_payload(
    rows: list[dict[str, Any]],
    columns: list[dict[str, str]],
    field_roles: dict[str, Any] | None,
    x_field: str | None,
) -> dict[str, Any] | None:
    """Build the one-way bar/line chart payload for a selected x-axis field."""
    normalized_roles = normalize_field_roles(field_roles)
    dtype_map = {col["name"]: col["dtype"] for col in columns}
    if not x_field:
        return None
    if x_field not in normalized_roles:
        return {"x_field": x_field, "error": "Select an assigned field for the x-axis."}
    if normalized_roles[x_field] not in ONE_WAY_ROLE_VALUES:
        return {"x_field": x_field, "error": "Selected field cannot be used on the x-axis."}

    target_field = next((field for field, role in normalized_roles.items() if role == "target"), None)
    if not target_field:
        return {"x_field": x_field, "error": "Assign a target field to render the line series."}

    pdf = pd.DataFrame(rows)
    if x_field not in pdf.columns:
        return {"x_field": x_field, "error": f"Field '{x_field}' is not available in the dataset."}
    if target_field not in pdf.columns:
        return {"x_field": x_field, "error": f"Target field '{target_field}' is not available in the dataset."}

    buckets, bucket_order, was_binned = _bucket_series(pdf[x_field], dtype_map.get(x_field, ""))
    chart_df = pd.DataFrame({"bucket": buckets})
    chart_df["line"] = pd.to_numeric(pdf[target_field], errors="coerce").fillna(0.0)

    claim_key_field = next(
        (field for field, role in normalized_roles.items() if role == "claim_key" and field in pdf.columns),
        None,
    )
    if claim_key_field:
        chart_df["bar_source"] = pdf[claim_key_field]
        grouped = (
            chart_df.groupby("bucket", sort=False, dropna=False)
            .agg(
                bar_value=("bar_source", lambda s: int(s.dropna().nunique())),
                line_value=("line", "sum"),
            )
            .reset_index()
        )
        bar_label = f"Unique {claim_key_field}"
    else:
        grouped = (
            chart_df.groupby("bucket", sort=False, dropna=False)
            .agg(bar_value=("line", "size"), line_value=("line", "sum"))
            .reset_index()
        )
        bar_label = "Rows"

    order_lookup = {label: idx for idx, label in enumerate(bucket_order)}
    grouped["bucket_order"] = grouped["bucket"].map(lambda value: order_lookup.get(str(value), len(order_lookup)))
    grouped = grouped.sort_values("bucket_order")

    points = [
        {
            "x": str(row["bucket"]),
            "bar_value": int(row["bar_value"]),
            "line_value": float(row["line_value"]),
        }
        for _, row in grouped.iterrows()
    ]

    return {
        "x_field": x_field,
        "x_label": x_field,
        "bar_label": bar_label,
        "line_label": f"Sum of {target_field}",
        "binned": was_binned,
        "points": points,
    }


def _build_descriptive_row(
    field: str,
    role: str,
    dtype: str,
    meta: dict[str, Any],
) -> dict[str, Any]:
    value_counts = meta.get("value_counts_without_nan")
    return {
        "field": field,
        "role": role,
        "dtype": dtype,
        "profile_type": _to_json_value(meta.get("type")),
        "non_missing_count": int(meta.get("count", 0) or 0),
        "missing_count": int(meta.get("n_missing", 0) or 0),
        "missing_proportion": float(meta.get("p_missing", 0.0) or 0.0),
        "distinct_count": int(meta.get("n_distinct", 0) or 0),
        "distinct_proportion": float(meta.get("p_distinct", 0.0) or 0.0),
        "mean": _to_json_value(meta.get("mean")),
        "std": _to_json_value(meta.get("std")),
        "min": _to_json_value(meta.get("min")),
        "p5": _to_json_value(meta.get("5%")),
        "p25": _to_json_value(meta.get("25%")),
        "median": _to_json_value(meta.get("50%")),
        "p75": _to_json_value(meta.get("75%")),
        "p95": _to_json_value(meta.get("95%")),
        "max": _to_json_value(meta.get("max")),
        "top_values": _summarize_value_counts(value_counts),
        "distribution": _build_distribution(meta, value_counts),
    }


def _build_outlier_row(
    series: pd.Series,
    field: str,
    role: str,
    dtype: str,
    total_rows: int,
) -> dict[str, Any]:
    outlier_mask = _outlier_mask(series, dtype)
    inlier_mask = _inlier_mask(series, outlier_mask)
    return {
        "field": field,
        "role": role,
        "dtype": dtype,
        "outlier": _summarize_distinct_values(series[outlier_mask].tolist()),
        "outlier_proportion": (int(outlier_mask.sum()) / total_rows) if total_rows else 0.0,
        "inlier": _summarize_distinct_values(series[inlier_mask].tolist()),
        "inlier_proportion": (int(inlier_mask.sum()) / total_rows) if total_rows else 0.0,
    }


def _build_missing_row(
    series: pd.Series,
    field: str,
    role: str,
    dtype: str,
    total_rows: int,
) -> dict[str, Any]:
    explicit_mask = series.isna()
    disguised_values = _disguised_missing_values(series, dtype)
    stringified = series.astype("string").str.strip().str.lower()
    disguised_mask = stringified.isin(disguised_values) if len(series) else pd.Series(dtype=bool)
    missing_values: list[str] = []
    if bool(explicit_mask.any()):
        missing_values.append("<null>")
    missing_values.extend(_summarize_distinct_values(series[disguised_mask].tolist()))
    missing_count = int(explicit_mask.sum()) + int(disguised_mask.sum())
    return {
        "field": field,
        "role": role,
        "dtype": dtype,
        "missing_values": missing_values,
        "missing_proportion": (missing_count / total_rows) if total_rows else 0.0,
    }


def _build_distribution(meta: dict[str, Any], value_counts: Any) -> dict[str, Any] | None:
    histogram = meta.get("histogram")
    if isinstance(histogram, tuple) and len(histogram) >= 1:
        counts = histogram[0]
        return {
            "kind": "histogram",
            "values": [float(v) for v in list(counts)],
        }
    if value_counts is not None:
        return {
            "kind": "frequency",
            "values": [float(v) for v in list(value_counts.head(12).tolist())],
        }
    return None


def _build_correlation_matrix(correlations: dict[str, Any]) -> dict[str, Any]:
    if not correlations:
        return {"fields": [], "types": [], "cells": []}
    fields = sorted({str(field) for matrix in correlations.values() for field in matrix.index.tolist()})
    correlation_types = [name for name in MULTI_CORRELATIONS if name in correlations]
    cells: list[list[dict[str, Any]]] = []
    for row_field in fields:
        row_cells: list[dict[str, Any]] = []
        for col_field in fields:
            cell: dict[str, Any] = {}
            for corr_name in correlation_types:
                matrix = correlations[corr_name]
                if row_field in matrix.index and col_field in matrix.columns:
                    raw_value = matrix.loc[row_field, col_field]
                    cell[corr_name] = _to_json_value(raw_value)
            row_cells.append(cell)
        cells.append(row_cells)
    return {"fields": fields, "types": correlation_types, "cells": cells}


def _bucket_series(series: pd.Series, dtype: str) -> tuple[pd.Series, list[str], bool]:
    if dtype in TEMPORAL_DTYPES:
        dt = pd.to_datetime(series, errors="coerce")
        if dt.dropna().empty:
            labels = series.astype("string").fillna("<missing>")
            order = list(dict.fromkeys(labels.astype(str).tolist()))
            return labels.astype(str), order, False
        candidates = [
            dt.dt.to_period("D").astype(str),
            dt.dt.to_period("W").astype(str),
            dt.dt.to_period("M").astype(str),
            dt.dt.to_period("Q").astype(str),
            dt.dt.to_period("Y").astype(str),
        ]
        labels = candidates[-1]
        was_binned = False
        for candidate in candidates:
            label_strings = candidate.where(dt.notna(), "<missing>")
            if label_strings.nunique(dropna=False) <= MAX_CHART_BUCKETS:
                labels = label_strings
                was_binned = candidate is not candidates[0]
                break
        order = list(dict.fromkeys(labels.astype(str).tolist()))
        return labels.astype(str), order, was_binned

    if dtype in NUMERIC_DTYPES:
        numeric = pd.to_numeric(series, errors="coerce")
        distinct = numeric.nunique(dropna=True)
        if distinct <= MAX_CHART_BUCKETS:
            labels = numeric.round(6).astype("string").fillna("<missing>")
            order = list(dict.fromkeys(labels.astype(str).tolist()))
            return labels.astype(str), order, False
        bins = min(20, max(2, int(distinct)))
        bucketed = pd.cut(numeric, bins=bins, duplicates="drop")
        labels = bucketed.astype("string").fillna("<missing>")
        order = [str(cat) for cat in bucketed.cat.categories] if hasattr(bucketed, "cat") else []
        if "<missing>" in labels.astype(str).tolist():
            order.append("<missing>")
        return labels.astype(str), order, True

    labels = series.astype("string").fillna("<missing>")
    distinct = labels.nunique(dropna=False)
    if distinct <= MAX_CHART_BUCKETS:
        order = list(dict.fromkeys(labels.astype(str).tolist()))
        return labels.astype(str), order, False
    top_labels = labels.value_counts(dropna=False).head(20).index.astype(str).tolist()
    collapsed = labels.astype(str).map(lambda value: value if value in top_labels else "Other")
    order = top_labels + (["Other"] if "Other" in collapsed.tolist() else [])
    return collapsed.astype(str), order, True


def _outlier_mask(series: pd.Series, dtype: str) -> pd.Series:
    if len(series) == 0:
        return pd.Series(dtype=bool)
    if dtype in NUMERIC_DTYPES:
        numeric = pd.to_numeric(series, errors="coerce")
        clean = numeric.dropna()
        if clean.empty:
            return pd.Series([False] * len(series))
        q1 = clean.quantile(0.25)
        q3 = clean.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - (1.5 * iqr)
        upper = q3 + (1.5 * iqr)
        return numeric.lt(lower) | numeric.gt(upper)
    if dtype in TEMPORAL_DTYPES:
        dt = pd.to_datetime(series, errors="coerce")
        numeric = dt.view("int64").astype("float64")
        clean = pd.Series(numeric).replace({pd.NA: None}).dropna()
        if clean.empty:
            return pd.Series([False] * len(series))
        q1 = clean.quantile(0.25)
        q3 = clean.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - (1.5 * iqr)
        upper = q3 + (1.5 * iqr)
        return pd.Series(numeric).lt(lower) | pd.Series(numeric).gt(upper)
    value_counts = series.astype("string").value_counts(dropna=True)
    rare_values = set(
        value_counts[
            (value_counts <= max(1, int(len(series) * 0.01))) | (value_counts <= 2)
        ].index.astype(str).tolist()
    )
    return series.astype("string").isin(rare_values)


def _inlier_mask(series: pd.Series, outlier_mask: pd.Series) -> pd.Series:
    non_missing = ~series.isna()
    if len(outlier_mask) != len(series):
        return non_missing
    return non_missing & ~outlier_mask


def _disguised_missing_values(series: pd.Series, dtype: str) -> set[str]:
    if len(series) == 0:
        return set()
    if dtype not in TEMPORAL_DTYPES and dtype not in NUMERIC_DTYPES:
        normalized = series.astype("string").str.strip().str.lower()
        return {
            marker
            for marker in normalized.dropna().unique().tolist()
            if marker in DISGUISED_MISSING_MARKERS
        }
    return set()


def _summarize_value_counts(value_counts: Any) -> list[str]:
    if value_counts is None:
        return []
    items = []
    for value, count in value_counts.head(5).items():
        items.append(f"{_to_json_value(value)} ({int(count)})")
    return items


def _summarize_distinct_values(values: list[Any]) -> list[str]:
    seen: list[str] = []
    seen_set: set[str] = set()
    for value in values:
        rendered = str(_to_json_value(value))
        if rendered not in seen_set:
            seen.append(rendered)
            seen_set.add(rendered)
        if len(seen) >= MAX_LIST_VALUES:
            break
    return seen


def _to_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (pd.isna(value) or not pd.notna(value) or isnan(value)):
            return None
        return value
    if hasattr(value, "item"):
        return _to_json_value(value.item())
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return str(value)
