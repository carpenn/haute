"""Pipeline CRUD, preview, trace, and sink endpoints."""

from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute.graph_utils import PipelineGraph
from haute.routes._helpers import (
    discover_pipelines,
    lookup_pipeline_by_name,
    parse_pipeline_to_graph,
    raise_pipeline_not_found,
)
from haute.schemas import (
    PipelineSummary,
    PreviewNodeRequest,
    PreviewNodeResponse,
    SavePipelineRequest,
    SavePipelineResponse,
    SinkRequest,
    SinkResponse,
    TraceRequest,
    TraceResponse,
)

logger = get_logger(component="server.pipeline")

router = APIRouter(prefix="/api", tags=["pipeline"])

# ── Timeout constants (seconds) ──────────────────────────────────
_TRACE_TIMEOUT = 120.0  # single-row trace
_PREVIEW_TIMEOUT = 120.0  # node preview execution
_SINK_TIMEOUT = 300.0  # sink (write-to-disk) execution


@router.get("/pipelines", response_model=list[PipelineSummary])
async def list_pipelines() -> list[PipelineSummary]:
    """List all discovered pipelines."""
    from haute.parser import parse_pipeline_file

    files = discover_pipelines()
    cwd = Path.cwd()

    async def _parse_one(f: Path) -> PipelineSummary:
        try:
            graph = await asyncio.to_thread(parse_pipeline_file, f)
            return PipelineSummary(
                name=graph.pipeline_name or f.stem,
                description=graph.pipeline_description or "",
                file=str(f.relative_to(cwd)),
                node_count=len(graph.nodes),
            )
        except Exception as e:
            return PipelineSummary(
                name=f.stem,
                file=str(f),
                error=str(e),
            )

    return list(await asyncio.gather(*[_parse_one(f) for f in files]))


@router.get("/pipeline/{name}", response_model=PipelineGraph)
async def get_pipeline(name: str) -> PipelineGraph:
    """Return the graph for a specific pipeline."""

    def _find() -> PipelineGraph | None:
        # O(1) lookup via cached index
        f = lookup_pipeline_by_name(name)
        if f is not None:
            try:
                return parse_pipeline_to_graph(f)
            except Exception as e:
                logger.warning("parse_failed", file=f.name, error=str(e))

        # Fallback: linear scan (index may be stale)
        for f in discover_pipelines():
            try:
                graph = parse_pipeline_to_graph(f)
                if graph.pipeline_name == name:
                    return graph
            except Exception as e:
                logger.warning("parse_failed", file=f.name, error=str(e))
                continue
        return None

    graph = await asyncio.to_thread(_find)
    if graph is None:
        raise_pipeline_not_found(name)
    return graph


@router.get("/pipeline", response_model=PipelineGraph)
async def get_first_pipeline() -> PipelineGraph:
    """Return the graph for the active pipeline, or an empty canvas.

    Python file is the source of truth. Sidecar .haute.json provides positions.
    """
    cwd = Path.cwd()

    def _find_first() -> PipelineGraph:
        for f in discover_pipelines():
            try:
                graph = parse_pipeline_to_graph(f)
                if graph.nodes:
                    graph.source_file = str(f.relative_to(cwd))
                    return graph
            except Exception as e:
                logger.warning("parse_failed", file=f.name, error=str(e))
                continue
        return PipelineGraph()

    return await asyncio.to_thread(_find_first)


@router.post("/pipeline/save", response_model=SavePipelineResponse)
async def save_pipeline(body: SavePipelineRequest) -> SavePipelineResponse:
    """Save a graph: .py (source of truth) + config JSON + .haute.json (positions).

    When the graph contains submodels, multiple files are written via
    ``graph_to_code_multi``.
    """
    from haute.routes._save_pipeline import SavePipelineService

    svc = SavePipelineService(project_root=Path.cwd())
    return svc.save(body)


@router.post("/pipeline/trace", response_model=TraceResponse)
async def trace_row(body: TraceRequest) -> TraceResponse:
    """Trace a single row through the pipeline, returning per-node snapshots."""
    from haute.graph_utils import flatten_graph
    from haute.trace import execute_trace, trace_result_to_dict

    graph = flatten_graph(body.graph)
    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(
                execute_trace,
                graph,
                row_index=body.row_index,
                target_node_id=body.target_node_id,
                column=body.column,
                row_limit=body.row_limit,
                scenario=body.scenario,
            ),
            timeout=_TRACE_TIMEOUT,
        )
        return TraceResponse(
            status="ok",
            trace=trace_result_to_dict(result),  # type: ignore[arg-type]
        )
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"Trace execution timed out ({_TRACE_TIMEOUT:.0f}s limit)",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pipeline/preview", response_model=PreviewNodeResponse)
async def preview_node(body: PreviewNodeRequest) -> PreviewNodeResponse:
    """Run pipeline up to a specific node and return its output.

    Accepts an optional ``row_limit`` (default 1000) that is pushed into
    the Polars lazy query plan so only that many rows are scanned.
    """
    from haute._topo import ancestors
    from haute.executor import execute_graph
    from haute.graph_utils import (
        _prune_live_switch_edges,
        flatten_graph,
    )

    graph = flatten_graph(body.graph)
    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        results = await asyncio.wait_for(
            asyncio.to_thread(
                execute_graph,
                graph,
                target_node_id=body.node_id,
                row_limit=body.row_limit,
                scenario=body.scenario,
            ),
            timeout=_PREVIEW_TIMEOUT,
        )
        node_result = results.get(body.node_id)
        if not node_result:
            raise HTTPException(
                status_code=404,
                detail=f"Node '{body.node_id}' not found in results",
            )

        node_map = graph.node_map

        # Only include timings/memory for ancestors of the target node
        # (+ itself), pruned by the active scenario so the unused
        # live_switch branch is excluded.
        if body.node_id:
            pruned = _prune_live_switch_edges(
                graph.edges, node_map, body.scenario,
            )
            relevant = ancestors(
                body.node_id, pruned, set(node_map.keys()),
            )
        else:
            relevant = set(results.keys())

        timings = [
            {
                "node_id": nid,
                "label": node_map[nid].data.label,
                "timing_ms": r.timing_ms,
            }
            for nid, r in results.items()
            if nid in node_map and nid in relevant
        ]

        memory = [
            {
                "node_id": nid,
                "label": node_map[nid].data.label,
                "memory_bytes": r.memory_bytes,
            }
            for nid, r in results.items()
            if nid in node_map and nid in relevant
        ]

        node_statuses = {
            nid: r.status for nid, r in results.items() if nid in relevant
        }

        return PreviewNodeResponse(
            node_id=body.node_id,
            status=node_result.status,
            row_count=node_result.row_count,
            column_count=node_result.column_count,
            columns=node_result.columns,
            preview=node_result.preview,
            error=node_result.error,
            error_line=node_result.error_line,
            timing_ms=node_result.timing_ms,
            memory_bytes=node_result.memory_bytes,
            timings=timings,
            memory=memory,
            schema_warnings=node_result.schema_warnings,
            node_statuses=node_statuses,
        )
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"Preview execution timed out ({_PREVIEW_TIMEOUT:.0f}s limit)",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pipeline/sink", response_model=SinkResponse)
async def execute_sink_node(body: SinkRequest) -> SinkResponse:
    """Execute the pipeline up to a sink node and write output to disk.

    Only called on explicit user action (Write button), not during normal run/preview.
    """
    from haute.executor import execute_sink
    from haute.graph_utils import flatten_graph

    graph = flatten_graph(body.graph)
    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(
                execute_sink, graph, sink_node_id=body.node_id, scenario=body.scenario,
            ),
            timeout=_SINK_TIMEOUT,
        )
        return result
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"Sink execution timed out ({_SINK_TIMEOUT:.0f}s limit)",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
