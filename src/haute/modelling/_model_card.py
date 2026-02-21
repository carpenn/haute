"""Self-contained HTML model card generation.

Produces a ``<!DOCTYPE html>`` document with embedded SVG charts,
summary tables, and diagnostics. Designed so a head of pricing can
open the file in a browser and decide whether a model goes live.
"""

from __future__ import annotations

import html
from datetime import UTC, datetime
from typing import Any

from haute.modelling._charts import (
    COLOR_IMPORTANCE,
    COLOR_SHAP,
    render_ave_feature_svg,
    render_double_lift_svg,
    render_horizontal_bars_svg,
    render_loss_curve_svg,
)

# ---------------------------------------------------------------------------
# Shared HTML helpers
# ---------------------------------------------------------------------------


def _html_table(
    headers: list[str],
    rows: list[list[str]],
    align: list[str] | None = None,
) -> str:
    """Render a simple HTML table. *align* defaults to left for all columns."""
    if align is None:
        align = ["left"] * len(headers)
    parts = ['<table>']
    parts.append("<thead><tr>")
    for h, a in zip(headers, align):
        parts.append(f'<th style="text-align:{a}">{html.escape(str(h))}</th>')
    parts.append("</tr></thead>")
    parts.append("<tbody>")
    for row in rows:
        parts.append("<tr>")
        for cell, a in zip(row, align):
            parts.append(f'<td style="text-align:{a}">{html.escape(str(cell))}</td>')
        parts.append("</tr>")
    parts.append("</tbody></table>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_model_card(
    *,
    name: str,
    algorithm: str,
    task: str,
    metrics: dict[str, float],
    params: dict[str, Any],
    train_rows: int,
    test_rows: int,
    features: list[str],
    split_config: dict[str, Any],
    best_iteration: int | None = None,
    loss_history: list[dict[str, float]] | None = None,
    double_lift: list[dict[str, Any]] | None = None,
    feature_importance: list[dict[str, Any]] | None = None,
    shap_summary: list[dict[str, Any]] | None = None,
    feature_importance_loss: list[dict[str, Any]] | None = None,
    cv_results: dict[str, Any] | None = None,
    ave_per_feature: list[dict[str, Any]] | None = None,
) -> str:
    """Generate a self-contained HTML model card document."""
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    sections: list[str] = []

    # --- Header ---
    sections.append(
        f"<h1>{html.escape(name)}</h1>"
        f'<p class="subtitle">{html.escape(algorithm)} &middot; {html.escape(task)} '
        f"&middot; generated {now}</p>"
    )

    # --- Training summary ---
    split_desc = split_config.get("strategy", "random") if split_config else "random"
    summary_rows = [
        ["Train rows", f"{train_rows:,}"],
        ["Test rows", f"{test_rows:,}"],
        ["Features", f"{len(features):,}" if features else "—"],
        ["Split strategy", split_desc],
    ]
    if best_iteration is not None:
        summary_rows.append(["Best iteration", str(best_iteration)])
    sections.append("<h2>Training Summary</h2>")
    sections.append(_html_table(["Property", "Value"], summary_rows, ["left", "right"]))

    # --- Metrics ---
    if metrics:
        metric_rows = [[k, f"{v:.4f}"] for k, v in metrics.items()]
        sections.append("<h2>Metrics</h2>")
        sections.append(_html_table(["Metric", "Value"], metric_rows, ["left", "right"]))

    # --- CV results ---
    if cv_results and cv_results.get("mean_metrics"):
        means = cv_results["mean_metrics"]
        stds = cv_results.get("std_metrics", {})
        n_folds = cv_results.get("n_folds", "—")
        cv_rows = [
            [k, f"{means[k]:.4f}", f"{stds.get(k, 0):.4f}"]
            for k in means
        ]
        sections.append(f"<h2>Cross-Validation ({n_folds} folds)</h2>")
        sections.append(
            _html_table(
                ["Metric", "Mean", "Std"],
                cv_rows,
                ["left", "right", "right"],
            )
        )

    # --- Double Lift ---
    if double_lift:
        sections.append("<h2>Double Lift</h2>")
        sections.append(f'<div class="chart">{render_double_lift_svg(double_lift)}</div>')
        dl_rows = [
            [str(d["decile"]), f'{d["actual"]:.4f}', f'{d["predicted"]:.4f}', str(d["count"])]
            for d in double_lift
        ]
        sections.append(
            _html_table(
                ["Decile", "Actual", "Predicted", "Count"],
                dl_rows,
                ["right", "right", "right", "right"],
            )
        )

    # --- Loss curve ---
    if loss_history:
        sections.append("<h2>Loss Curve</h2>")
        sections.append(
            f'<div class="chart">{render_loss_curve_svg(loss_history, best_iteration)}</div>'
        )

    # --- Feature Importance (PredictionValuesChange) ---
    if feature_importance:
        sections.append("<h2>Feature Importance (PredictionValuesChange)</h2>")
        svg = render_horizontal_bars_svg(
            feature_importance, "feature", "importance",
            title="PredictionValuesChange", color=COLOR_IMPORTANCE,
        )
        sections.append(f'<div class="chart">{svg}</div>')

    # --- SHAP Summary ---
    if shap_summary:
        sections.append("<h2>SHAP Summary</h2>")
        svg = render_horizontal_bars_svg(
            shap_summary, "feature", "mean_abs_shap",
            title="SHAP (mean |SHAP|)", color=COLOR_SHAP,
        )
        sections.append(f'<div class="chart">{svg}</div>')

    # --- Feature Importance (LossFunctionChange) ---
    if feature_importance_loss:
        sections.append("<h2>Feature Importance (LossFunctionChange)</h2>")
        svg = render_horizontal_bars_svg(
            feature_importance_loss, "feature", "importance",
            title="LossFunctionChange", color=COLOR_IMPORTANCE,
        )
        sections.append(f'<div class="chart">{svg}</div>')

    # --- AvE per Feature ---
    if ave_per_feature:
        sections.append("<h2>Actual vs Expected — Per Feature</h2>")
        for feat_data in ave_per_feature:
            is_cat = feat_data.get("type") == "categorical"
            svg = render_ave_feature_svg(feat_data["feature"], feat_data["bins"], is_cat)
            sections.append(f'<div class="chart">{svg}</div>')

    # --- Parameters ---
    if params:
        param_rows = [[str(k), str(v)] for k, v in params.items()]
        sections.append("<h2>Parameters</h2>")
        sections.append(_html_table(["Parameter", "Value"], param_rows))

    body = "\n".join(sections)
    return _wrap_html(name, body)


def _wrap_html(title: str, body: str) -> str:
    """Wrap body content in a full HTML document with embedded styles."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)} — Model Card</title>
<style>
  body {{
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    background: #f9fafb; color: #1f2937; margin: 0; padding: 24px;
    line-height: 1.5;
  }}
  .container {{ max-width: 900px; margin: 0 auto; }}
  h1 {{ font-size: 1.6rem; margin: 0 0 4px; }}
  h2 {{
    font-size: 1.15rem; margin: 32px 0 12px;
    border-bottom: 1px solid #e5e7eb; padding-bottom: 6px;
  }}
  .subtitle {{ color: #6b7280; margin: 0 0 20px; font-size: 0.9rem; }}
  table {{
    border-collapse: collapse; width: 100%; margin-bottom: 16px;
    font-size: 0.85rem;
  }}
  th, td {{ border: 1px solid #e5e7eb; padding: 6px 10px; }}
  th {{ background: #f3f4f6; font-weight: 600; }}
  tr:nth-child(even) {{ background: #f9fafb; }}
  .chart {{ margin: 12px 0; overflow-x: auto; }}
  .chart svg {{ max-width: 100%; height: auto; }}
</style>
</head>
<body>
<div class="container">
{body}
</div>
</body>
</html>"""
