"""Pipeline CRUD, run, preview, trace, and sink endpoints."""

from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute._types import NodeType
from haute.graph_utils import PipelineGraph
from haute.routes._helpers import (
    discover_pipelines,
    mark_self_write,
    parse_pipeline_to_graph,
    save_sidecar,
)
from haute.schemas import (
    PipelineSummary,
    PreviewNodeRequest,
    PreviewNodeResponse,
    RunPipelineRequest,
    RunPipelineResponse,
    SavePipelineRequest,
    SavePipelineResponse,
    SinkRequest,
    SinkResponse,
    TraceRequest,
    TraceResponse,
)

logger = get_logger(component="server.pipeline")

router = APIRouter(prefix="/api", tags=["pipeline"])


@router.get("/pipelines", response_model=list[PipelineSummary])
async def list_pipelines() -> list[PipelineSummary]:
    """List all discovered pipelines."""
    from haute.parser import parse_pipeline_file

    files = discover_pipelines()
    result: list[PipelineSummary] = []
    for f in files:
        try:
            graph = parse_pipeline_file(f)
            result.append(
                PipelineSummary(
                    name=graph.pipeline_name or f.stem,
                    description=graph.pipeline_description or "",
                    file=str(f.relative_to(Path.cwd())),
                    node_count=len(graph.nodes),
                )
            )
        except Exception as e:
            result.append(
                PipelineSummary(
                    name=f.stem,
                    file=str(f),
                    error=str(e),
                )
            )
    return result


@router.get("/pipeline/{name}", response_model=PipelineGraph)
async def get_pipeline(name: str) -> PipelineGraph:
    """Return the graph for a specific pipeline."""
    for f in discover_pipelines():
        try:
            graph = parse_pipeline_to_graph(f)
            if graph.pipeline_name == name:
                return graph
        except Exception as e:
            logger.warning("parse_failed", file=f.name, error=str(e))
            continue
    raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")


@router.get("/pipeline", response_model=PipelineGraph)
async def get_first_pipeline() -> PipelineGraph:
    """Return the graph for the active pipeline, or an empty canvas.

    Python file is the source of truth. Sidecar .haute.json provides positions.
    """
    cwd = Path.cwd()

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


@router.post("/pipeline/save", response_model=SavePipelineResponse)
async def save_pipeline(body: SavePipelineRequest) -> SavePipelineResponse:
    """Save a graph: .py (source of truth) + .haute.json (positions).

    When the graph contains submodels, multiple files are written via
    ``graph_to_code_multi``.
    """
    from haute.codegen import graph_to_code, graph_to_code_multi

    graph = body.graph

    # Validate singleton node types (max 1 each)
    singletons = [
        (NodeType.API_INPUT, "API Input"),
        (NodeType.OUTPUT, "Output"),
        (NodeType.LIVE_SWITCH, "Live Switch"),
    ]
    for singleton_type, label in singletons:
        count = sum(1 for n in graph.nodes if n.data.nodeType == singleton_type)
        if count > 1:
            raise HTTPException(
                status_code=400,
                detail=f"Only one {label} node is allowed per pipeline (found {count}).",
            )

    cwd = Path.cwd()

    # Determine main pipeline .py path
    if not body.source_file:
        raise HTTPException(
            status_code=400,
            detail="source_file is required — the frontend must track"
            " and send the original pipeline file path",
        )
    py_path = (cwd / body.source_file).resolve()
    if not py_path.is_relative_to(cwd):
        raise HTTPException(
            status_code=400,
            detail="source_file must be within the project directory",
        )

    mark_self_write()

    if graph.submodels:
        # Multi-file write: main .py + submodel .py files
        files = graph_to_code_multi(
            graph,
            pipeline_name=body.name,
            description=body.description,
            preamble=body.preamble,
            source_file=body.source_file,
        )
        for rel_path, code in files.items():
            out_path = (cwd / rel_path).resolve()
            if not out_path.is_relative_to(cwd):
                continue
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(code)
    else:
        code = graph_to_code(
            graph,
            pipeline_name=body.name,
            description=body.description,
            preamble=body.preamble,
        )
        py_path.write_text(code)

    # Write sidecar .haute.json (node positions for the GUI)
    save_sidecar(py_path, graph)

    return SavePipelineResponse(
        file=str(py_path.relative_to(cwd)),
        pipeline_name=body.name,
    )


@router.post("/pipeline/run", response_model=RunPipelineResponse)
async def run_pipeline(body: RunPipelineRequest) -> RunPipelineResponse:
    """Execute the full pipeline graph and return per-node results."""
    from haute.executor import execute_graph
    from haute.graph_utils import flatten_graph

    graph = flatten_graph(body.graph)
    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        results = await asyncio.wait_for(
            asyncio.to_thread(execute_graph, graph), timeout=300.0,
        )
        return RunPipelineResponse(status="ok", results=results)
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Pipeline execution timed out (300s limit)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
                row_index=body.rowIndex,
                target_node_id=body.targetNodeId,
                column=body.column,
                row_limit=body.rowLimit,
            ),
            timeout=120.0,
        )
        return TraceResponse(
            status="ok",
            trace=trace_result_to_dict(result),
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Trace execution timed out (120s limit)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pipeline/preview", response_model=PreviewNodeResponse)
async def preview_node(body: PreviewNodeRequest) -> PreviewNodeResponse:
    """Run pipeline up to a specific node and return its output.

    Accepts an optional ``rowLimit`` (default 1000) that is pushed into
    the Polars lazy query plan so only that many rows are scanned.
    """
    from haute.executor import execute_graph
    from haute.graph_utils import flatten_graph

    graph = flatten_graph(body.graph)
    if not graph.nodes:
        raise HTTPException(status_code=400, detail="Empty graph")

    try:
        results = await asyncio.wait_for(
            asyncio.to_thread(
                execute_graph,
                graph,
                target_node_id=body.nodeId,
                row_limit=body.rowLimit,
            ),
            timeout=120.0,
        )
        node_result = results.get(body.nodeId)
        if not node_result:
            raise HTTPException(
                status_code=404,
                detail=f"Node '{body.nodeId}' not found in results",
            )

        node_map = {n.id: n for n in graph.nodes}
        timings = [
            {
                "nodeId": nid,
                "label": node_map[nid].data.label,
                "timing_ms": r.get("timing_ms", 0),
            }
            for nid, r in results.items()
            if nid in node_map
        ]

        return PreviewNodeResponse(nodeId=body.nodeId, timings=timings, **node_result)
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Preview execution timed out (120s limit)")
    except HTTPException:
        raise
    except Exception as e:
        return PreviewNodeResponse(
            nodeId=body.nodeId,
            status="error",
            error=str(e),
        )


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
            asyncio.to_thread(execute_sink, graph, sink_node_id=body.nodeId),
            timeout=300.0,
        )
        return result
    except TimeoutError:
        return SinkResponse(status="error", message="Sink execution timed out (300s limit)")
    except Exception as e:
        return SinkResponse(status="error", message=str(e))
