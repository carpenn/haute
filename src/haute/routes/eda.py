"""FastAPI endpoints for the Exploratory Data Analysis (EDA) node."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute.graph_utils import flatten_graph
from haute.routes._helpers import _INTERNAL_ERROR_DETAIL
from haute.routes.pipeline import _ensure_source_file
from haute.schemas import (
    EdaOneWayRequest,
    EdaOneWayResponse,
    EdaRequest,
    EdaResponse,
)

logger = get_logger(component="server.eda")

router = APIRouter(prefix="/api", tags=["eda"])

_EDA_TIMEOUT = 180.0  # seconds — EDA on large datasets can be slow


def _run_pipeline_and_collect(graph, node_id: str, source: str):  # type: ignore[no-untyped-def]
    """Execute the graph up to *node_id* and return a collected Polars DataFrame."""
    import polars as pl

    from haute.executor import execute_graph

    results = execute_graph(graph, target_node_id=node_id, row_limit=0, source=source)
    node_result = results.get(node_id)
    if not node_result:
        raise ValueError(f"Node '{node_id}' produced no result")
    if node_result.status == "error":
        raise ValueError(node_result.error or "Node execution failed")

    # Re-construct a Polars DataFrame from the preview rows
    if not node_result.preview:
        return pl.DataFrame()
    return pl.DataFrame(node_result.preview)


@router.post("/pipeline/eda", response_model=EdaResponse)
async def eda_analysis(body: EdaRequest) -> EdaResponse:
    """Run the full pipeline and compute EDA analytics on the result.

    Returns descriptive statistics, outliers/inliers, disguised missing
    values, and a multi-type correlation matrix for all fields that have
    been assigned a role in the config panel.
    """
    from haute.routes._eda_service import (
        compute_correlations,
        compute_descriptive,
        compute_disguised_missings,
        compute_outliers,
    )

    graph = flatten_graph(body.graph)
    _ensure_source_file(graph)

    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    if not graph.node_map.get(body.node_id):
        raise HTTPException(
            status_code=404,
            detail=f"Node '{body.node_id}' not found",
        )

    if not body.field_roles:
        return EdaResponse(
            status="error",
            error="No field roles configured — assign roles to fields in the config panel.",
        )

    try:
        df = await asyncio.wait_for(
            asyncio.to_thread(_run_pipeline_and_collect, graph, body.node_id, body.source),
            timeout=_EDA_TIMEOUT,
        )
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"EDA execution timed out ({_EDA_TIMEOUT:.0f}s limit)",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("eda_execution_failed", error=str(exc))
        return EdaResponse(status="error", error=str(exc))

    try:
        field_roles = body.field_roles
        descriptive_rows = compute_descriptive(df, field_roles)
        outlier_rows = compute_outliers(df, field_roles)
        disguised_rows = compute_disguised_missings(df, field_roles)
        corr_data = compute_correlations(df, field_roles)

        from haute.schemas import (
            EdaCorrelations,
            EdaDescriptiveRow,
            EdaDisguisedMissingRow,
            EdaOutlierRow,
        )

        return EdaResponse(
            status="ok",
            descriptive=[EdaDescriptiveRow(**r) for r in descriptive_rows],
            outliers=[EdaOutlierRow(**r) for r in outlier_rows],
            disguised_missings=[EdaDisguisedMissingRow(**r) for r in disguised_rows],
            correlations=EdaCorrelations(**corr_data),
        )
    except Exception as exc:
        logger.error("eda_analysis_failed", error=str(exc))
        return EdaResponse(status="error", error=_INTERNAL_ERROR_DETAIL)


@router.post("/pipeline/eda/one_way", response_model=EdaOneWayResponse)
async def eda_one_way(body: EdaOneWayRequest) -> EdaOneWayResponse:
    """Compute one-way chart data for a specific x-axis field.

    The frontend calls this endpoint whenever the user changes the x-axis
    dropdown in the One-way Charts tab.
    """
    from haute.routes._eda_service import compute_one_way

    graph = flatten_graph(body.graph)
    _ensure_source_file(graph)

    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    if not graph.node_map.get(body.node_id):
        raise HTTPException(
            status_code=404,
            detail=f"Node '{body.node_id}' not found",
        )

    if not body.x_field:
        return EdaOneWayResponse(
            status="error",
            error="No x-axis field selected.",
        )

    try:
        df = await asyncio.wait_for(
            asyncio.to_thread(_run_pipeline_and_collect, graph, body.node_id, body.source),
            timeout=_EDA_TIMEOUT,
        )
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"EDA one-way execution timed out ({_EDA_TIMEOUT:.0f}s limit)",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("eda_one_way_execution_failed", error=str(exc))
        return EdaOneWayResponse(status="error", error=str(exc), x_field=body.x_field)

    try:
        chart = compute_one_way(df, body.field_roles, body.x_field)
        return EdaOneWayResponse(
            status="ok",
            x_field=chart["x_field"],
            x_labels=chart["x_labels"],
            claim_counts=chart["claim_counts"],
            target_sums=chart["target_sums"],
        )
    except Exception as exc:
        logger.error("eda_one_way_failed", error=str(exc))
        return EdaOneWayResponse(
            status="error",
            error=_INTERNAL_ERROR_DETAIL,
            x_field=body.x_field,
        )
