"""Chainladder-based triangle processing service.

Converts a flat (origin, development, value) DataFrame into a structured
actuarial loss-development triangle using the ``chainladder`` package.
"""

from __future__ import annotations

import math
from typing import Any

from haute._logging import get_logger

logger = get_logger(component="triangle_service")

# Mapping from UI grain codes to chainladder grain-string fragments
_GRAIN_MAP: dict[str, str] = {"Y": "Y", "Q": "Q", "M": "M"}


def process_triangle(
    preview_rows: list[dict[str, Any]],
    origin_field: str,
    dev_field: str,
    value_field: str,
    origin_grain: str = "Y",
    dev_grain: str = "Y",
    triangle_type: str = "incremental",
) -> dict[str, Any]:
    """Build a chainladder triangle from flat preview rows.

    Parameters
    ----------
    preview_rows:
        List of row dicts as returned by the Polars executor (date columns
        are Python ``datetime.date`` objects or ISO-format strings).
    origin_field:
        Name of the origin period column (date type).
    dev_field:
        Name of the development period column (date type).
    value_field:
        Name of the incremental value column (numeric).
    origin_grain:
        Target grain for origin axis: ``"Y"`` (annual), ``"Q"`` (quarterly),
        or ``"M"`` (monthly).  Defaults to ``"Y"``.
    dev_grain:
        Target grain for development axis.  Same options as *origin_grain*.
        Defaults to ``"Y"``.
    triangle_type:
        ``"incremental"`` (default) or ``"cumulative"`` — controls whether
        ``incr_to_cum`` is applied after grain adjustment.

    Returns
    -------
    dict with keys:
        ``origins``       – list of origin label strings
        ``developments``  – list of development label strings
        ``values``        – 2-D list (origins × developments), ``None`` for
                           upper-right triangle cells with no data
    """
    try:
        import chainladder as cl  # type: ignore[import-untyped]
        import pandas as pd
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "The 'chainladder' and 'pandas' packages are required for the Triangle "
            "Viewer.  Install them with: pip install chainladder pandas"
        ) from exc

    if not preview_rows:
        return {"origins": [], "developments": [], "values": []}

    # Build pandas DataFrame from preview rows
    df_pd = pd.DataFrame(preview_rows)

    for col in (origin_field, dev_field):
        if col not in df_pd.columns:
            raise ValueError(f"Column '{col}' not found in triangle data")
    if value_field not in df_pd.columns:
        raise ValueError(f"Column '{value_field}' not found in triangle data")

    # Ensure value column is numeric
    df_pd[value_field] = pd.to_numeric(df_pd[value_field], errors="coerce").fillna(0.0)

    # Build chainladder Triangle (input is always treated as incremental)
    tri = cl.Triangle(
        df_pd,
        origin=origin_field,
        development=dev_field,
        columns=[value_field],
        cumulative=False,
    )

    # Apply grain transformation
    origin_g = _GRAIN_MAP.get(origin_grain.upper(), "Y")
    dev_g = _GRAIN_MAP.get(dev_grain.upper(), "Y")
    grain_str = f"O{origin_g}D{dev_g}"
    try:
        tri = tri.grain(grain_str)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "triangle_grain_change_failed",
            grain=grain_str,
            error=str(exc),
        )
        # Fall back: apply each axis independently where possible
        try:
            tri = tri.grain(f"O{origin_g}D{origin_g}")
        except Exception:  # noqa: BLE001
            pass  # Keep original grain

    # Apply cumulative conversion
    if triangle_type == "cumulative":
        tri = tri.incr_to_cum()

    # Convert to wide pandas DataFrame
    df_frame = tri[value_field].to_frame(origin_as_datetime=False)

    origins = [str(idx) for idx in df_frame.index]
    developments = [str(col) for col in df_frame.columns]

    values: list[list[float | None]] = []
    for origin in df_frame.index:
        row: list[float | None] = []
        for dev in df_frame.columns:
            v = df_frame.loc[origin, dev]
            if v is None or (isinstance(v, float) and math.isnan(v)):
                row.append(None)
            else:
                try:
                    row.append(float(v))
                except (TypeError, ValueError):
                    row.append(None)
        values.append(row)

    return {
        "origins": origins,
        "developments": developments,
        "values": values,
    }
