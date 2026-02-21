"""Pure-SVG chart generation for model diagnostics.

All functions return SVG strings — zero dependencies beyond stdlib.
"""

from __future__ import annotations

import html
import math
from typing import Any

# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------

COLOR_BARS = "#D1D5DB"       # grey — exposure / count bars
COLOR_ACTUAL = "#2563EB"     # blue — actual line
COLOR_PREDICTED = "#F97316"  # orange — predicted line
COLOR_TRAIN = "#8B5CF6"      # purple — train loss
COLOR_EVAL = "#22C55E"       # green — eval loss
COLOR_BEST_ITER = "#EF4444"  # red — best iteration marker
COLOR_IMPORTANCE = "#2563EB" # blue — importance bars
COLOR_SHAP = "#F97316"       # orange — SHAP bars
COLOR_AXIS = "#374151"
COLOR_GRID = "#E5E7EB"

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _escape(text: str) -> str:
    """XML/HTML-safe text."""
    return html.escape(str(text))


def _truncate_label(label: str, max_len: int = 25) -> str:
    if len(label) <= max_len:
        return label
    return label[: max_len - 1] + "…"


def _nice_ticks(min_val: float, max_val: float, n_ticks: int = 5) -> list[float]:
    """Generate clean tick values for an axis range."""
    if min_val == max_val:
        return [min_val]
    span = max_val - min_val
    raw_step = span / max(n_ticks - 1, 1)

    # Round step to a "nice" number
    magnitude = 10 ** math.floor(math.log10(max(abs(raw_step), 1e-15)))
    residual = raw_step / magnitude
    if residual <= 1.5:
        nice_step = 1 * magnitude
    elif residual <= 3.5:
        nice_step = 2 * magnitude
    elif residual <= 7.5:
        nice_step = 5 * magnitude
    else:
        nice_step = 10 * magnitude

    start = math.floor(min_val / nice_step) * nice_step
    ticks = []
    v = start
    while v <= max_val + nice_step * 0.01:
        ticks.append(round(v, 10))
        v += nice_step
    return ticks


def _format_tick(val: float) -> str:
    """Format a tick value: 1.23, 1.2k, 1.2M."""
    abs_val = abs(val)
    if abs_val == 0:
        return "0"
    if abs_val >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    if abs_val >= 1_000:
        return f"{val / 1_000:.1f}k"
    if abs_val >= 1:
        return f"{val:.2f}"
    return f"{val:.4g}"


def _placeholder_svg(width: int, height: int, message: str) -> str:
    """Fallback SVG for empty or invalid data."""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">'
        f'<rect width="{width}" height="{height}" fill="#F9FAFB" rx="4"/>'
        f'<text x="{width // 2}" y="{height // 2}" text-anchor="middle" '
        f'dominant-baseline="middle" fill="{COLOR_AXIS}" font-size="13" '
        f'font-family="system-ui, sans-serif">{_escape(message)}</text>'
        f"</svg>"
    )


# ---------------------------------------------------------------------------
# 1. Double lift chart
# ---------------------------------------------------------------------------


def render_double_lift_svg(double_lift: list[dict[str, Any]]) -> str:
    """Dual-axis double-lift chart: bars for count, lines for actual/predicted."""
    width, height = 600, 360
    if not double_lift:
        return _placeholder_svg(width, height, "No double-lift data")

    margin = {"top": 30, "right": 55, "bottom": 50, "left": 55}
    plot_w = width - margin["left"] - margin["right"]
    plot_h = height - margin["top"] - margin["bottom"]

    deciles = [d["decile"] for d in double_lift]
    actuals = [d["actual"] for d in double_lift]
    predicted = [d["predicted"] for d in double_lift]
    counts = [d["count"] for d in double_lift]

    n = len(double_lift)
    bar_w = max(plot_w / n * 0.6, 4)
    gap = plot_w / n

    # Y-axis scales
    max_count = max(counts) if counts else 1
    all_vals = actuals + predicted
    y_min = min(all_vals) if all_vals else 0
    y_max = max(all_vals) if all_vals else 1
    if y_min == y_max:
        y_min, y_max = y_min - 0.5, y_max + 0.5
    y_pad = (y_max - y_min) * 0.1
    y_min -= y_pad
    y_max += y_pad

    def x_pos(i: int) -> float:
        return margin["left"] + gap * i + gap / 2

    def y_bar(v: float) -> float:
        return margin["top"] + plot_h * (1 - v / max_count)

    def y_line(v: float) -> float:
        return margin["top"] + plot_h * (1 - (v - y_min) / (y_max - y_min))

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="system-ui, sans-serif">',
        f'<rect width="{width}" height="{height}" fill="white" rx="4"/>',
        f'<text x="{width // 2}" y="18" text-anchor="middle" font-size="13" '
        f'font-weight="600" fill="{COLOR_AXIS}">Double Lift (Actual vs Predicted by Decile)</text>',
    ]

    # Grid lines
    for tick in _nice_ticks(y_min, y_max, 5):
        yp = y_line(tick)
        if margin["top"] <= yp <= margin["top"] + plot_h:
            parts.append(
                f'<line x1="{margin["left"]}" y1="{yp:.1f}" '
                f'x2="{margin["left"] + plot_w}" y2="{yp:.1f}" '
                f'stroke="{COLOR_GRID}" stroke-width="1"/>'
            )
            parts.append(
                f'<text x="{margin["left"] - 5}" y="{yp:.1f}" '
                f'text-anchor="end" dominant-baseline="middle" '
                f'font-size="10" fill="{COLOR_AXIS}">{_format_tick(tick)}</text>'
            )

    # Bars (count)
    for i, c in enumerate(counts):
        bx = x_pos(i) - bar_w / 2
        by = y_bar(c)
        bh = margin["top"] + plot_h - by
        parts.append(
            f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" '
            f'height="{bh:.1f}" fill="{COLOR_BARS}" rx="1"/>'
        )

    # Actual line
    points_a = " ".join(f"{x_pos(i):.1f},{y_line(actuals[i]):.1f}" for i in range(n))
    parts.append(
        f'<polyline points="{points_a}" fill="none" '
        f'stroke="{COLOR_ACTUAL}" stroke-width="2" stroke-linejoin="round"/>'
    )
    for i in range(n):
        parts.append(
            f'<circle cx="{x_pos(i):.1f}" cy="{y_line(actuals[i]):.1f}" '
            f'r="3" fill="{COLOR_ACTUAL}"/>'
        )

    # Predicted line
    points_p = " ".join(f"{x_pos(i):.1f},{y_line(predicted[i]):.1f}" for i in range(n))
    parts.append(
        f'<polyline points="{points_p}" fill="none" '
        f'stroke="{COLOR_PREDICTED}" stroke-width="2" stroke-linejoin="round"/>'
    )
    for i in range(n):
        parts.append(
            f'<circle cx="{x_pos(i):.1f}" cy="{y_line(predicted[i]):.1f}" '
            f'r="3" fill="{COLOR_PREDICTED}"/>'
        )

    # X-axis labels
    for i, d in enumerate(deciles):
        parts.append(
            f'<text x="{x_pos(i):.1f}" y="{margin["top"] + plot_h + 16}" '
            f'text-anchor="middle" font-size="10" fill="{COLOR_AXIS}">{d}</text>'
        )

    # Right-axis label for count
    parts.append(
        f'<text x="{width - 5}" y="{margin["top"] + plot_h // 2}" '
        f'text-anchor="end" font-size="10" fill="{COLOR_BARS}" '
        f'transform="rotate(-90, {width - 5}, {margin["top"] + plot_h // 2})">Count</text>'
    )

    # Legend
    lx = margin["left"] + 10
    ly = margin["top"] + 12
    parts.append(
        f'<rect x="{lx}" y="{ly - 4}" width="10" height="3" fill="{COLOR_ACTUAL}"/>'
        f'<text x="{lx + 14}" y="{ly}" font-size="10" fill="{COLOR_AXIS}">Actual</text>'
        f'<rect x="{lx + 60}" y="{ly - 4}" width="10" height="3" fill="{COLOR_PREDICTED}"/>'
        f'<text x="{lx + 74}" y="{ly}" font-size="10" fill="{COLOR_AXIS}">Predicted</text>'
        f'<rect x="{lx + 140}" y="{ly - 6}" width="10" height="10" fill="{COLOR_BARS}" rx="1"/>'
        f'<text x="{lx + 154}" y="{ly}" font-size="10" fill="{COLOR_AXIS}">Count</text>'
    )

    parts.append("</svg>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# 2. Loss curve chart
# ---------------------------------------------------------------------------


def render_loss_curve_svg(
    loss_history: list[dict[str, float]],
    best_iteration: int | None = None,
) -> str:
    """Loss curve: train (purple), eval (green), best iteration (red dashed)."""
    width, height = 600, 320
    if not loss_history:
        return _placeholder_svg(width, height, "No loss history data")

    margin = {"top": 30, "right": 20, "bottom": 40, "left": 60}
    plot_w = width - margin["left"] - margin["right"]
    plot_h = height - margin["top"] - margin["bottom"]

    # Subsample if >300 points
    data = loss_history
    if len(data) > 300:
        step = len(data) / 300
        indices = set()
        for i in range(300):
            indices.add(int(i * step))
        indices.add(0)
        indices.add(len(data) - 1)
        if best_iteration is not None and 0 <= best_iteration < len(data):
            indices.add(best_iteration)
        sorted_idx = sorted(indices)
        data = [loss_history[i] for i in sorted_idx]

    # Extract series
    iterations = [d.get("iteration", i) for i, d in enumerate(data)]
    train_keys = [k for k in data[0] if k.startswith("train_")]
    eval_keys = [k for k in data[0] if k.startswith("eval_")]
    train_key = train_keys[0] if train_keys else None
    eval_key = eval_keys[0] if eval_keys else None

    train_vals = [d[train_key] for d in data] if train_key else []
    eval_vals = [d[eval_key] for d in data] if eval_key else []

    all_vals = train_vals + eval_vals
    if not all_vals:
        return _placeholder_svg(width, height, "No loss values found")

    y_min_val = min(all_vals)
    y_max_val = max(all_vals)
    if y_min_val == y_max_val:
        y_min_val -= 0.5
        y_max_val += 0.5
    y_pad = (y_max_val - y_min_val) * 0.05
    y_min_val -= y_pad
    y_max_val += y_pad

    x_min = min(iterations)
    x_max = max(iterations)
    if x_min == x_max:
        x_max = x_min + 1

    def x_pos(v: float) -> float:
        return margin["left"] + plot_w * (v - x_min) / (x_max - x_min)

    def y_pos(v: float) -> float:
        return margin["top"] + plot_h * (1 - (v - y_min_val) / (y_max_val - y_min_val))

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="system-ui, sans-serif">',
        f'<rect width="{width}" height="{height}" fill="white" rx="4"/>',
        f'<text x="{width // 2}" y="18" text-anchor="middle" font-size="13" '
        f'font-weight="600" fill="{COLOR_AXIS}">Training Loss Curve</text>',
    ]

    # Grid
    for tick in _nice_ticks(y_min_val, y_max_val, 5):
        yp = y_pos(tick)
        if margin["top"] <= yp <= margin["top"] + plot_h:
            parts.append(
                f'<line x1="{margin["left"]}" y1="{yp:.1f}" '
                f'x2="{margin["left"] + plot_w}" y2="{yp:.1f}" '
                f'stroke="{COLOR_GRID}" stroke-width="1"/>'
            )
            parts.append(
                f'<text x="{margin["left"] - 5}" y="{yp:.1f}" '
                f'text-anchor="end" dominant-baseline="middle" '
                f'font-size="10" fill="{COLOR_AXIS}">{_format_tick(tick)}</text>'
            )

    # Train line
    if train_vals:
        points = " ".join(
            f"{x_pos(iterations[i]):.1f},{y_pos(train_vals[i]):.1f}"
            for i in range(len(data))
        )
        parts.append(
            f'<polyline points="{points}" fill="none" '
            f'stroke="{COLOR_TRAIN}" stroke-width="2"/>'
        )

    # Eval line
    if eval_vals:
        points = " ".join(
            f"{x_pos(iterations[i]):.1f},{y_pos(eval_vals[i]):.1f}"
            for i in range(len(data))
        )
        parts.append(
            f'<polyline points="{points}" fill="none" '
            f'stroke="{COLOR_EVAL}" stroke-width="2"/>'
        )

    # Best iteration line
    if best_iteration is not None:
        clamped = min(best_iteration, x_max)
        bx = x_pos(clamped)
        parts.append(
            f'<line x1="{bx:.1f}" y1="{margin["top"]}" '
            f'x2="{bx:.1f}" y2="{margin["top"] + plot_h}" '
            f'stroke="{COLOR_BEST_ITER}" stroke-width="1.5" '
            f'stroke-dasharray="6,3"/>'
        )
        parts.append(
            f'<text x="{bx:.1f}" y="{margin["top"] - 4}" '
            f'text-anchor="middle" font-size="9" fill="{COLOR_BEST_ITER}">'
            f"best={best_iteration}</text>"
        )

    # X-axis labels
    for tick in _nice_ticks(x_min, x_max, 6):
        xp = x_pos(tick)
        if margin["left"] <= xp <= margin["left"] + plot_w:
            parts.append(
                f'<text x="{xp:.1f}" y="{margin["top"] + plot_h + 16}" '
                f'text-anchor="middle" font-size="10" fill="{COLOR_AXIS}">'
                f"{int(tick)}</text>"
            )

    # Legend
    lx = margin["left"] + 10
    ly = margin["top"] + 12
    if train_key:
        parts.append(
            f'<rect x="{lx}" y="{ly - 4}" width="10" height="3" fill="{COLOR_TRAIN}"/>'
            f'<text x="{lx + 14}" y="{ly}" font-size="10" fill="{COLOR_AXIS}">Train</text>'
        )
    if eval_key:
        parts.append(
            f'<rect x="{lx + 55}" y="{ly - 4}" width="10" height="3" fill="{COLOR_EVAL}"/>'
            f'<text x="{lx + 69}" y="{ly}" font-size="10" fill="{COLOR_AXIS}">Eval</text>'
        )

    parts.append("</svg>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# 3. Horizontal bar chart (feature importance / SHAP)
# ---------------------------------------------------------------------------


def render_horizontal_bars_svg(
    data: list[dict[str, Any]],
    name_key: str,
    value_key: str,
    *,
    title: str = "",
    color: str = COLOR_IMPORTANCE,
    max_items: int = 15,
) -> str:
    """Horizontal bar chart — shared by feature importance and SHAP."""
    if not data:
        return _placeholder_svg(400, 200, f"No {title.lower() or 'data'}")

    items = data[:max_items]
    n = len(items)
    bar_height = 20
    gap = 6
    margin = {"top": 30, "right": 20, "bottom": 10, "left": 150}
    total_height = margin["top"] + n * (bar_height + gap) + margin["bottom"]
    width = 500
    plot_w = width - margin["left"] - margin["right"]

    max_val = max(abs(d[value_key]) for d in items) if items else 1
    if max_val == 0:
        max_val = 1

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{total_height}" '
        f'viewBox="0 0 {width} {total_height}" font-family="system-ui, sans-serif">',
        f'<rect width="{width}" height="{total_height}" fill="white" rx="4"/>',
    ]

    if title:
        parts.append(
            f'<text x="{width // 2}" y="18" text-anchor="middle" font-size="13" '
            f'font-weight="600" fill="{COLOR_AXIS}">{_escape(title)}</text>'
        )

    for i, item in enumerate(items):
        y = margin["top"] + i * (bar_height + gap)
        val = abs(item[value_key])
        bar_w = plot_w * val / max_val

        label = _truncate_label(str(item[name_key]))
        parts.append(
            f'<text x="{margin["left"] - 5}" y="{y + bar_height / 2 + 1}" '
            f'text-anchor="end" dominant-baseline="middle" '
            f'font-size="11" fill="{COLOR_AXIS}">{_escape(label)}</text>'
        )
        parts.append(
            f'<rect x="{margin["left"]}" y="{y}" width="{bar_w:.1f}" '
            f'height="{bar_height}" fill="{color}" rx="2"/>'
        )
        parts.append(
            f'<text x="{margin["left"] + bar_w + 4}" y="{y + bar_height / 2 + 1}" '
            f'dominant-baseline="middle" font-size="10" fill="{COLOR_AXIS}">'
            f"{item[value_key]:.4f}</text>"
        )

    parts.append("</svg>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# 4. AvE per-feature chart (dual axis: bars + lines)
# ---------------------------------------------------------------------------


def render_ave_feature_svg(
    feature_name: str,
    bins: list[dict[str, Any]],
    is_categorical: bool,
) -> str:
    """Dual-axis AvE chart for a single feature."""
    width, height = 600, 320
    if not bins:
        return _placeholder_svg(width, height, f"No data for {_escape(feature_name)}")

    margin = {"top": 30, "right": 55, "bottom": 70, "left": 55}
    plot_w = width - margin["left"] - margin["right"]
    plot_h = height - margin["top"] - margin["bottom"]

    labels = [b["label"] for b in bins]
    exposures = [b["exposure"] for b in bins]
    actuals = [b["avg_actual"] for b in bins]
    predicted = [b["avg_predicted"] for b in bins]

    n = len(bins)
    bar_w = max(plot_w / n * 0.6, 4)
    gap = plot_w / n

    max_exp = max(exposures) if exposures else 1
    if max_exp == 0:
        max_exp = 1

    all_line_vals = actuals + predicted
    y_min = min(all_line_vals) if all_line_vals else 0
    y_max = max(all_line_vals) if all_line_vals else 1
    if y_min == y_max:
        y_min -= 0.5
        y_max += 0.5
    y_pad = (y_max - y_min) * 0.1
    y_min -= y_pad
    y_max += y_pad

    def x_pos(i: int) -> float:
        return margin["left"] + gap * i + gap / 2

    def y_bar(v: float) -> float:
        return margin["top"] + plot_h * (1 - v / max_exp)

    def y_line(v: float) -> float:
        return margin["top"] + plot_h * (1 - (v - y_min) / (y_max - y_min))

    # Check if we need rotated labels
    any_long = any(len(str(lab)) > 5 for lab in labels)

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="system-ui, sans-serif">',
        f'<rect width="{width}" height="{height}" fill="white" rx="4"/>',
        f'<text x="{width // 2}" y="18" text-anchor="middle" font-size="13" '
        f'font-weight="600" fill="{COLOR_AXIS}">{_escape(feature_name)}</text>',
    ]

    # Grid
    for tick in _nice_ticks(y_min, y_max, 5):
        yp = y_line(tick)
        if margin["top"] <= yp <= margin["top"] + plot_h:
            parts.append(
                f'<line x1="{margin["left"]}" y1="{yp:.1f}" '
                f'x2="{margin["left"] + plot_w}" y2="{yp:.1f}" '
                f'stroke="{COLOR_GRID}" stroke-width="1"/>'
            )
            parts.append(
                f'<text x="{margin["left"] - 5}" y="{yp:.1f}" '
                f'text-anchor="end" dominant-baseline="middle" '
                f'font-size="10" fill="{COLOR_AXIS}">{_format_tick(tick)}</text>'
            )

    # Bars
    for i, exp in enumerate(exposures):
        bx = x_pos(i) - bar_w / 2
        by = y_bar(exp)
        bh = margin["top"] + plot_h - by
        parts.append(
            f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" '
            f'height="{max(bh, 0):.1f}" fill="{COLOR_BARS}" rx="1"/>'
        )

    # Actual line
    if n > 1:
        points_a = " ".join(
            f"{x_pos(i):.1f},{y_line(actuals[i]):.1f}" for i in range(n)
        )
        parts.append(
            f'<polyline points="{points_a}" fill="none" '
            f'stroke="{COLOR_ACTUAL}" stroke-width="2" stroke-linejoin="round"/>'
        )
    for i in range(n):
        parts.append(
            f'<circle cx="{x_pos(i):.1f}" cy="{y_line(actuals[i]):.1f}" '
            f'r="3" fill="{COLOR_ACTUAL}"/>'
        )

    # Predicted line
    if n > 1:
        points_p = " ".join(
            f"{x_pos(i):.1f},{y_line(predicted[i]):.1f}" for i in range(n)
        )
        parts.append(
            f'<polyline points="{points_p}" fill="none" '
            f'stroke="{COLOR_PREDICTED}" stroke-width="2" stroke-linejoin="round"/>'
        )
    for i in range(n):
        parts.append(
            f'<circle cx="{x_pos(i):.1f}" cy="{y_line(predicted[i]):.1f}" '
            f'r="3" fill="{COLOR_PREDICTED}"/>'
        )

    # X-axis labels
    for i, label in enumerate(labels):
        display = _truncate_label(str(label), 15)
        xp = x_pos(i)
        label_y = margin["top"] + plot_h + 14
        if any_long:
            parts.append(
                f'<text x="{xp:.1f}" y="{label_y}" '
                f'text-anchor="end" font-size="9" fill="{COLOR_AXIS}" '
                f'transform="rotate(-45, {xp:.1f}, {label_y})">'
                f"{_escape(display)}</text>"
            )
        else:
            parts.append(
                f'<text x="{xp:.1f}" y="{label_y}" '
                f'text-anchor="middle" font-size="10" fill="{COLOR_AXIS}">'
                f"{_escape(display)}</text>"
            )

    parts.append("</svg>")
    return "\n".join(parts)
