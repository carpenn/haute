"""Exploratory Data Analysis service.

Computes descriptive statistics, outlier/inlier detection, disguised
missing-value detection, correlation matrices, and one-way chart data
from a collected Polars DataFrame.

Correlations are computed via ``ydata-profiling`` when available, with a
pure-Polars/pandas fallback when the package is not installed.
"""

from __future__ import annotations

import base64
import math
from typing import Any

import polars as pl

from haute._logging import get_logger

logger = get_logger(component="eda_service")

# ---------------------------------------------------------------------------
# Role definitions
# ---------------------------------------------------------------------------

ROLE_OPTIONS = [
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
]

# Roles whose fields are eligible for statistical analysis
_ANALYSABLE_ROLES = {
    "exposure", "target", "covariate", "fold",
    "underwriting_date", "accident_date", "reporting_date", "transaction_date",
}

# Date-like roles (used for one-way x-axis)
DATE_ROLES = {"underwriting_date", "accident_date", "reporting_date", "transaction_date"}
# One-way eligible x-axis roles
ONE_WAY_X_ROLES = DATE_ROLES | {"covariate", "fold"}

# Common disguised-missing sentinel values
_NUMERIC_SENTINELS: set[float] = {-999.0, -9999.0, -1.0, 999.0, 9999.0, 99.0, 999999.0}
_STRING_SENTINELS: set[str] = {
    "n/a", "na", "n.a.", "#n/a", "none", "null", "nil", "unknown",
    "missing", "not available", "not applicable", "?", "", "nan",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_numeric_dtype(dtype: str) -> bool:
    return any(
        dtype.startswith(p)
        for p in ("Int", "UInt", "Float", "Decimal", "Duration")
    )


def _is_date_dtype(dtype: str) -> bool:
    return dtype.startswith(("Date", "Datetime"))


def _is_string_dtype(dtype: str) -> bool:
    return dtype in ("Utf8", "String", "Categorical", "Enum")


def _safe_float(val: Any) -> float | None:
    try:
        v = float(val)
        return None if math.isnan(v) or math.isinf(v) else v
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Sparkline generation
# ---------------------------------------------------------------------------

_SPARK_W = 60
_SPARK_H = 20
_SPARK_BINS = 12


def _sparkline_svg(values: list[float]) -> str:
    """Return a tiny inline SVG histogram string for a list of numeric values."""
    if not values:
        return ""
    mn, mx = min(values), max(values)
    if mn == mx:
        # Constant column – flat bar
        bar = f'<rect x="0" y="0" width="{_SPARK_W}" height="{_SPARK_H}" fill="currentColor" opacity="0.5"/>'
        return f'<svg xmlns="http://www.w3.org/2000/svg" width="{_SPARK_W}" height="{_SPARK_H}" viewBox="0 0 {_SPARK_W} {_SPARK_H}">{bar}</svg>'

    bins: list[int] = [0] * _SPARK_BINS
    rng = mx - mn
    for v in values:
        idx = int((v - mn) / rng * (_SPARK_BINS - 1))
        bins[min(idx, _SPARK_BINS - 1)] += 1

    max_count = max(bins) or 1
    bar_w = _SPARK_W / _SPARK_BINS
    bars = []
    for i, cnt in enumerate(bins):
        bh = max(1, int(cnt / max_count * _SPARK_H))
        x = i * bar_w
        y = _SPARK_H - bh
        bars.append(
            f'<rect x="{x:.1f}" y="{y}" width="{bar_w - 1:.1f}" height="{bh}"'
            f' fill="currentColor" opacity="0.7"/>'
        )
    inner = "".join(bars)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{_SPARK_W}" height="{_SPARK_H}"'
        f' viewBox="0 0 {_SPARK_W} {_SPARK_H}">{inner}</svg>'
    )


def _sparkline_data_uri(values: list[float]) -> str:
    svg = _sparkline_svg(values)
    if not svg:
        return ""
    b64 = base64.b64encode(svg.encode()).decode()
    return f"data:image/svg+xml;base64,{b64}"


# ---------------------------------------------------------------------------
# Descriptive statistics
# ---------------------------------------------------------------------------


def compute_descriptive(
    df: pl.DataFrame,
    field_roles: dict[str, str],
) -> list[dict[str, Any]]:
    """Compute per-field descriptive statistics."""
    rows: list[dict[str, Any]] = []
    total = len(df)
    schema = {col: str(dtype) for col, dtype in df.schema.items()}

    for field, role in field_roles.items():
        if field not in schema:
            continue
        dtype = schema[field]
        series = df[field]
        null_count = series.null_count()

        row: dict[str, Any] = {
            "field": field,
            "role": role,
            "dtype": dtype,
            "count": total,
            "missing_count": null_count,
            "missing_prop": null_count / total if total else 0.0,
            "mean": None,
            "std": None,
            "min": None,
            "q25": None,
            "median": None,
            "q75": None,
            "max": None,
            "skewness": None,
            "n_unique": None,
            "top_value": None,
            "sparkline": None,
        }

        non_null = series.drop_nulls()
        n_unique = non_null.n_unique() if len(non_null) > 0 else 0
        row["n_unique"] = n_unique

        if _is_numeric_dtype(dtype):
            cast = non_null.cast(pl.Float64, strict=False).drop_nulls()
            vals = cast.to_list()
            if vals:
                row["mean"] = _safe_float(cast.mean())
                row["std"] = _safe_float(cast.std())
                row["min"] = _safe_float(cast.min())
                row["max"] = _safe_float(cast.max())
                row["q25"] = _safe_float(cast.quantile(0.25, interpolation="linear"))
                row["median"] = _safe_float(cast.quantile(0.50, interpolation="linear"))
                row["q75"] = _safe_float(cast.quantile(0.75, interpolation="linear"))
                # Skewness via pandas (polars doesn't expose it directly)
                try:
                    import pandas as pd  # pandas is already a project dependency

                    row["skewness"] = _safe_float(pd.Series(vals).skew())
                except Exception:
                    pass
                row["sparkline"] = _sparkline_data_uri(vals)

        elif _is_date_dtype(dtype):
            if len(non_null) > 0:
                row["min"] = str(non_null.min())
                row["max"] = str(non_null.max())

        elif _is_string_dtype(dtype):
            if len(non_null) > 0:
                top = (
                    df.select(pl.col(field).drop_nulls())
                    .group_by(field)
                    .agg(pl.len().alias("cnt"))
                    .sort("cnt", descending=True)
                    .head(1)[field]
                    .to_list()
                )
                row["top_value"] = str(top[0]) if top else None

        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Outlier / inlier detection
# ---------------------------------------------------------------------------


def compute_outliers(
    df: pl.DataFrame,
    field_roles: dict[str, str],
) -> list[dict[str, Any]]:
    """Detect outliers/inliers using the IQR fence method for numeric fields.

    For categorical/string fields a frequency-based approach is used:
    values appearing in fewer than 1 % of rows are flagged as outliers.
    """
    rows: list[dict[str, Any]] = []
    total = len(df)
    schema = {col: str(dtype) for col, dtype in df.schema.items()}

    for field, role in field_roles.items():
        if field not in schema:
            continue
        dtype = schema[field]
        series = df[field]
        non_null = series.drop_nulls()

        row: dict[str, Any] = {
            "field": field,
            "role": role,
            "dtype": dtype,
            "outlier_values": [],
            "outlier_count": 0,
            "outlier_prop": 0.0,
            "inlier_count": 0,
            "inlier_prop": 0.0,
        }

        if _is_numeric_dtype(dtype) and len(non_null) > 0:
            cast = non_null.cast(pl.Float64, strict=False).drop_nulls()
            q1 = cast.quantile(0.25, interpolation="linear")
            q3 = cast.quantile(0.75, interpolation="linear")
            if q1 is not None and q3 is not None:
                iqr = q3 - q1
                lo = q1 - 1.5 * iqr
                hi = q3 + 1.5 * iqr
                is_outlier = (
                    df.select(
                        pl.col(field)
                        .cast(pl.Float64, strict=False)
                        .is_between(lo, hi)
                        .not_()
                        .fill_null(False)
                        .alias("_out")
                    )["_out"]
                )
                out_mask = is_outlier
                outlier_count = out_mask.sum()
                inlier_count = total - outlier_count

                # Collect up to 20 distinct outlier values
                out_vals = (
                    df.filter(out_mask)
                    .select(pl.col(field).cast(pl.Float64, strict=False).drop_nulls())
                    .unique()
                    .sort(field)
                    .head(20)[field]
                    .to_list()
                )

                row["outlier_values"] = [_safe_float(v) for v in out_vals]
                row["outlier_count"] = int(outlier_count)
                row["outlier_prop"] = int(outlier_count) / total if total else 0.0
                row["inlier_count"] = int(inlier_count)
                row["inlier_prop"] = int(inlier_count) / total if total else 0.0

        elif _is_string_dtype(dtype) and len(non_null) > 0:
            freq = (
                df.select(pl.col(field).drop_nulls())
                .group_by(field)
                .agg(pl.len().alias("cnt"))
            )
            threshold = max(1, int(total * 0.01))
            rare = freq.filter(pl.col("cnt") < threshold)
            common = freq.filter(pl.col("cnt") >= threshold)

            rare_vals = rare[field].to_list()
            outlier_count = (
                df.select(pl.col(field).is_in(rare_vals).fill_null(False).alias("_o"))
            )["_o"].sum()
            inlier_count = (
                df.select(pl.col(field).is_in(common[field].to_list()).fill_null(False).alias("_i"))
            )["_i"].sum()

            row["outlier_values"] = [str(v) for v in rare_vals[:20]]
            row["outlier_count"] = int(outlier_count)
            row["outlier_prop"] = int(outlier_count) / total if total else 0.0
            row["inlier_count"] = int(inlier_count)
            row["inlier_prop"] = int(inlier_count) / total if total else 0.0

        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Disguised missing values
# ---------------------------------------------------------------------------


def compute_disguised_missings(
    df: pl.DataFrame,
    field_roles: dict[str, str],
) -> list[dict[str, Any]]:
    """Detect explicit nulls and common disguised-missing sentinel values."""
    rows: list[dict[str, Any]] = []
    total = len(df)
    schema = {col: str(dtype) for col, dtype in df.schema.items()}

    for field, role in field_roles.items():
        if field not in schema:
            continue
        dtype = schema[field]
        series = df[field]

        found_missing: list[Any] = []
        total_missing_count = 0

        # Explicit nulls
        null_count = series.null_count()
        if null_count > 0:
            found_missing.append(None)
            total_missing_count += null_count

        if _is_numeric_dtype(dtype):
            cast = series.drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
            vals_set = set(cast.to_list())
            for sentinel in _NUMERIC_SENTINELS:
                if sentinel in vals_set:
                    cnt = int(
                        df.select(
                            (pl.col(field).cast(pl.Float64, strict=False) == sentinel)
                            .fill_null(False)
                            .alias("_m")
                        )["_m"].sum()
                    )
                    if cnt > 0:
                        found_missing.append(sentinel)
                        total_missing_count += cnt

        elif _is_string_dtype(dtype):
            # Check lower-cased versions
            lower_series = series.drop_nulls().cast(pl.Utf8).str.to_lowercase()
            present = set(lower_series.unique().to_list())
            for sentinel in _STRING_SENTINELS:
                if sentinel in present:
                    cnt = int(
                        df.select(
                            pl.col(field)
                            .cast(pl.Utf8, strict=False)
                            .str.to_lowercase()
                            .is_in([sentinel])
                            .fill_null(False)
                            .alias("_m")
                        )["_m"].sum()
                    )
                    if cnt > 0:
                        found_missing.append("(empty string)" if sentinel == "" else sentinel)
                        total_missing_count += cnt

        rows.append({
            "field": field,
            "role": role,
            "dtype": dtype,
            "missing_values": found_missing,
            "missing_count": total_missing_count,
            "missing_prop": total_missing_count / total if total else 0.0,
        })

    return rows


# ---------------------------------------------------------------------------
# Correlations
# ---------------------------------------------------------------------------


def compute_correlations(
    df: pl.DataFrame,
    field_roles: dict[str, str],
) -> dict[str, Any]:
    """Compute Pearson, Spearman and Cramér-V correlation matrices.

    Uses ``ydata-profiling`` when available (imports lazily), otherwise
    falls back to a pandas-based implementation.
    """
    analysable = [f for f, r in field_roles.items() if f in df.columns and r in _ANALYSABLE_ROLES]
    if not analysable:
        return {"fields": [], "pearson": [], "spearman": [], "cramer": []}

    schema = {col: str(dtype) for col, dtype in df.schema.items()}
    numeric_fields = [f for f in analysable if _is_numeric_dtype(schema.get(f, ""))]

    try:
        result = _correlations_via_ydata(df, analysable, numeric_fields)
    except Exception as exc:
        logger.info("ydata_profiling_unavailable", reason=str(exc), fallback="pandas")
        result = _correlations_via_pandas(df, analysable, numeric_fields)

    return result


def _correlations_via_ydata(
    df: pl.DataFrame,
    fields: list[str],
    numeric_fields: list[str],
) -> dict[str, Any]:
    """Use ydata-profiling to compute correlation matrices."""
    from ydata_profiling import ProfileReport  # noqa: PLC0415  (optional dep)

    sub = df.select(fields).to_pandas()
    profile = ProfileReport(sub, minimal=True, progress_bar=False)
    desc = profile.description_set

    pearson = _extract_ydata_corr(desc, "pearson", fields)
    spearman = _extract_ydata_corr(desc, "spearman", fields)
    cramer = _extract_ydata_corr(desc, "cramers", fields)

    return {"fields": fields, "pearson": pearson, "spearman": spearman, "cramer": cramer}


def _extract_ydata_corr(
    desc: Any,
    key: str,
    fields: list[str],
) -> list[list[float | None]]:
    """Extract a named correlation matrix from a ydata-profiling description set."""
    try:
        corr_df = desc["correlations"][key]
        # corr_df is a pandas DataFrame indexed and columned by field name
        n = len(fields)
        matrix: list[list[float | None]] = []
        for row_field in fields:
            row: list[float | None] = []
            for col_field in fields:
                try:
                    val = float(corr_df.loc[row_field, col_field])
                    row.append(None if math.isnan(val) else val)
                except (KeyError, TypeError, ValueError):
                    row.append(None)
            matrix.append(row)
        return matrix
    except (KeyError, TypeError):
        n = len(fields)
        return [[None] * n for _ in range(n)]


def _correlations_via_pandas(
    df: pl.DataFrame,
    fields: list[str],
    numeric_fields: list[str],
) -> dict[str, Any]:
    """Fallback correlation calculation using pandas."""
    import pandas as pd  # noqa: PLC0415

    sub = df.select(fields).to_pandas()
    n = len(fields)

    # Pearson
    try:
        pearson_pd = sub[numeric_fields].corr(method="pearson")
        pearson = _pd_corr_to_matrix(pearson_pd, fields, numeric_fields)
    except Exception:
        pearson = [[None] * n for _ in range(n)]

    # Spearman
    try:
        spearman_pd = sub[numeric_fields].corr(method="spearman")
        spearman = _pd_corr_to_matrix(spearman_pd, fields, numeric_fields)
    except Exception:
        spearman = [[None] * n for _ in range(n)]

    # Cramér's V for categoricals (simple implementation)
    try:
        cramer = _cramers_v_matrix(sub, fields)
    except Exception:
        cramer = [[None] * n for _ in range(n)]

    return {"fields": fields, "pearson": pearson, "spearman": spearman, "cramer": cramer}


def _pd_corr_to_matrix(
    corr_pd: Any,
    all_fields: list[str],
    numeric_fields: list[str],
) -> list[list[float | None]]:
    """Convert a pandas correlation DataFrame to a full matrix over all_fields."""
    n = len(all_fields)
    matrix: list[list[float | None]] = []
    for r in all_fields:
        row: list[float | None] = []
        for c in all_fields:
            if r in numeric_fields and c in numeric_fields:
                try:
                    val = float(corr_pd.loc[r, c])
                    row.append(None if math.isnan(val) else val)
                except (KeyError, TypeError, ValueError):
                    row.append(None)
            else:
                row.append(None)
        matrix.append(row)
    return matrix


def _cramers_v_matrix(sub: Any, fields: list[str]) -> list[list[float | None]]:
    """Compute Cramér's V for all pairs of fields."""
    import numpy as np  # noqa: PLC0415
    import pandas as pd  # noqa: PLC0415

    n = len(fields)
    matrix: list[list[float | None]] = [[None] * n for _ in range(n)]
    for i, fi in enumerate(fields):
        for j, fj in enumerate(fields):
            if i == j:
                matrix[i][j] = 1.0
                continue
            try:
                ct = pd.crosstab(sub[fi].astype(str), sub[fj].astype(str))
                chi2 = float(
                    ((ct - ct.sum(axis=0) * ct.sum(axis=1).values.reshape(-1, 1) / ct.values.sum()) ** 2
                     / (ct.sum(axis=0) * ct.sum(axis=1).values.reshape(-1, 1) / ct.values.sum())).sum().sum()
                )
                n_obs = ct.values.sum()
                phi2 = chi2 / n_obs if n_obs else 0.0
                k, r = ct.shape
                v = float(np.sqrt(phi2 / max(min(k - 1, r - 1), 1)))
                matrix[i][j] = None if math.isnan(v) else v
            except Exception:
                matrix[i][j] = None
    return matrix


# ---------------------------------------------------------------------------
# One-way chart data
# ---------------------------------------------------------------------------

_MAX_X_BINS = 30  # maximum distinct x values before binning


def compute_one_way(
    df: pl.DataFrame,
    field_roles: dict[str, str],
    x_field: str,
) -> dict[str, Any]:
    """Compute bar-line chart data for the one-way view.

    Returns x_labels (str), claim_counts (int), and target_sums (float).
    """
    schema = {col: str(dtype) for col, dtype in df.schema.items()}

    if x_field not in schema:
        return {"x_field": x_field, "x_labels": [], "claim_counts": [], "target_sums": []}

    claim_key_fields = [f for f, r in field_roles.items() if r == "claim_key" and f in schema]
    target_fields = [f for f, r in field_roles.items() if r == "target" and f in schema]

    # ------------------------------------------------------------------
    # Build the x-axis expression (binned if needed)
    # ------------------------------------------------------------------
    x_dtype = schema[x_field]
    x_col_expr: pl.Expr

    if _is_date_dtype(x_dtype):
        # Truncate to year for now; may produce at most N years worth of values
        x_col_expr = pl.col(x_field).dt.year().cast(pl.Utf8).alias("__x__")
    elif _is_numeric_dtype(x_dtype):
        n_unique = df[x_field].drop_nulls().n_unique()
        if n_unique > _MAX_X_BINS:
            # Bin into equal-width buckets
            cast_col = pl.col(x_field).cast(pl.Float64, strict=False)
            mn = df[x_field].drop_nulls().cast(pl.Float64, strict=False).min()
            mx = df[x_field].drop_nulls().cast(pl.Float64, strict=False).max()
            if mn is not None and mx is not None and mn != mx:
                bin_w = (mx - mn) / _MAX_X_BINS
                x_col_expr = (
                    ((cast_col - mn) / bin_w).floor().clip(0, _MAX_X_BINS - 1).cast(pl.Int64).alias("__x__")
                )
                # We'll label bins as "mn + i*bin_w" later
            else:
                x_col_expr = cast_col.cast(pl.Utf8).alias("__x__")
        else:
            x_col_expr = pl.col(x_field).cast(pl.Utf8).alias("__x__")
    else:
        # String / categorical — use as-is but cap at MAX_X_BINS top values
        top_vals = (
            df.select(pl.col(x_field).drop_nulls())
            .group_by(x_field)
            .agg(pl.len().alias("cnt"))
            .sort("cnt", descending=True)
            .head(_MAX_X_BINS)[x_field]
            .to_list()
        )
        df = df.filter(pl.col(x_field).is_in(top_vals).fill_null(False))
        x_col_expr = pl.col(x_field).cast(pl.Utf8).alias("__x__")

    # ------------------------------------------------------------------
    # Build aggregation expressions
    # ------------------------------------------------------------------
    agg_exprs: list[pl.Expr] = []

    if claim_key_fields:
        ck = claim_key_fields[0]
        agg_exprs.append(pl.col(ck).drop_nulls().n_unique().alias("__claim_count__"))
    else:
        agg_exprs.append(pl.len().alias("__claim_count__"))

    if target_fields:
        tf = target_fields[0]
        agg_exprs.append(
            pl.col(tf).cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("__target_sum__")
        )
    else:
        agg_exprs.append(pl.lit(0.0).alias("__target_sum__"))

    try:
        result = (
            df.with_columns(x_col_expr)
            .drop_nulls("__x__")
            .group_by("__x__")
            .agg(*agg_exprs)
            .sort("__x__")
        )

        x_labels = [str(v) for v in result["__x__"].to_list()]
        claim_counts = [int(v) for v in result["__claim_count__"].to_list()]
        target_sums = [float(v) if v is not None else 0.0 for v in result["__target_sum__"].to_list()]

        # If we used numeric bins, replace bin indices with range labels
        if _is_numeric_dtype(x_dtype) and df[x_field].drop_nulls().n_unique() > _MAX_X_BINS:
            mn2 = df[x_field].drop_nulls().cast(pl.Float64, strict=False).min()
            mx2 = df[x_field].drop_nulls().cast(pl.Float64, strict=False).max()
            if mn2 is not None and mx2 is not None and mn2 != mx2:
                bin_w2 = (mx2 - mn2) / _MAX_X_BINS
                x_labels = [
                    f"{mn2 + int(lbl) * bin_w2:.2g}–{mn2 + (int(lbl) + 1) * bin_w2:.2g}"
                    for lbl in x_labels
                ]

        return {
            "x_field": x_field,
            "x_labels": x_labels,
            "claim_counts": claim_counts,
            "target_sums": target_sums,
        }
    except Exception as exc:
        logger.warning("one_way_chart_failed", field=x_field, error=str(exc))
        return {"x_field": x_field, "x_labels": [], "claim_counts": [], "target_sums": []}
