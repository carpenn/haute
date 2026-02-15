"""Tests for runw.schemas — Pydantic model validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from runw.schemas import (
    BrowseFilesResponse,
    ColumnInfo,
    FileItem,
    Graph,
    GraphEdge,
    GraphNode,
    GraphNodeData,
    NodeResult,
    PipelineSummary,
    PreviewNodeRequest,
    PreviewNodeResponse,
    RunPipelineRequest,
    RunPipelineResponse,
    SavePipelineRequest,
    SavePipelineResponse,
    SchemaResponse,
    SinkRequest,
    SinkResponse,
    TraceRequest,
    TraceResponse,
    TraceResultResponse,
    TraceStepResponse,
)


# ---------------------------------------------------------------------------
# Shared sub-models
# ---------------------------------------------------------------------------

class TestGraphModels:
    def test_graph_edge_requires_fields(self):
        with pytest.raises(ValidationError):
            GraphEdge()  # missing id, source, target

    def test_graph_edge_valid(self):
        e = GraphEdge(id="e1", source="a", target="b")
        assert e.source == "a"

    def test_graph_node_defaults(self):
        n = GraphNode(id="n1")
        assert n.type == "pipelineNode"
        assert n.position == {"x": 0, "y": 0}
        assert n.data.label == "Unnamed"
        assert n.data.nodeType == "transform"
        assert n.data.config == {}

    def test_graph_defaults(self):
        g = Graph()
        assert g.nodes == []
        assert g.edges == []
        assert g.pipeline_name is None

    def test_graph_with_nodes_and_edges(self):
        g = Graph(
            nodes=[GraphNode(id="a"), GraphNode(id="b")],
            edges=[GraphEdge(id="e1", source="a", target="b")],
            pipeline_name="test",
        )
        assert len(g.nodes) == 2
        assert g.edges[0].target == "b"

    def test_graph_model_dump_roundtrip(self):
        """model_dump() should produce a dict usable by execute_graph."""
        g = Graph(
            nodes=[
                GraphNode(
                    id="src",
                    data=GraphNodeData(label="Source", nodeType="dataSource", config={"path": "d.parquet"}),
                ),
            ],
            edges=[],
        )
        d = g.model_dump()
        assert d["nodes"][0]["id"] == "src"
        assert d["nodes"][0]["data"]["config"]["path"] == "d.parquet"


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class TestRequestModels:
    def test_save_pipeline_defaults(self):
        r = SavePipelineRequest(graph=Graph())
        assert r.name == "my_pipeline"
        assert r.description == ""
        assert r.preamble == ""

    def test_run_pipeline_requires_graph(self):
        with pytest.raises(ValidationError):
            RunPipelineRequest()  # missing graph

    def test_preview_node_defaults(self):
        r = PreviewNodeRequest(graph=Graph(), nodeId="n1")
        assert r.rowLimit == 1000

    def test_trace_request_defaults(self):
        r = TraceRequest(graph=Graph())
        assert r.rowIndex == 0
        assert r.targetNodeId is None
        assert r.column is None

    def test_sink_request_requires_node_id(self):
        with pytest.raises(ValidationError):
            SinkRequest(graph=Graph())  # missing nodeId


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class TestResponseModels:
    def test_save_pipeline_response(self):
        r = SavePipelineResponse(file="pipelines/test.py", pipeline_name="test")
        assert r.status == "saved"

    def test_run_pipeline_response_with_results(self):
        r = RunPipelineResponse(
            status="ok",
            results={
                "n1": NodeResult(
                    status="ok", row_count=10, column_count=2,
                    columns=[ColumnInfo(name="x", dtype="Int64")],
                ),
            },
        )
        assert r.results["n1"].row_count == 10

    def test_preview_node_response_defaults(self):
        r = PreviewNodeResponse(nodeId="n1", status="ok")
        assert r.row_count == 0
        assert r.columns == []
        assert r.error is None

    def test_sink_response_defaults(self):
        r = SinkResponse(status="ok")
        assert r.format == "parquet"
        assert r.row_count == 0

    def test_trace_response(self):
        r = TraceResponse(
            status="ok",
            trace=TraceResultResponse(
                target_node_id="t",
                row_index=0,
                steps=[],
            ),
        )
        assert r.trace.target_node_id == "t"

    def test_browse_files_response(self):
        r = BrowseFilesResponse(
            dir=".",
            items=[
                FileItem(name="data.parquet", path="data.parquet", type="file", size=1024),
                FileItem(name="subdir", path="subdir", type="directory"),
            ],
        )
        assert len(r.items) == 2
        assert r.items[0].size == 1024
        assert r.items[1].size is None

    def test_schema_response(self):
        r = SchemaResponse(
            path="data.parquet",
            columns=[ColumnInfo(name="x", dtype="Float64")],
            row_count=100,
            column_count=1,
        )
        assert r.columns[0].dtype == "Float64"

    def test_pipeline_summary(self):
        r = PipelineSummary(name="test", file="test.py")
        assert r.node_count == 0
        assert r.error is None

    def test_pipeline_summary_with_error(self):
        r = PipelineSummary(name="bad", file="bad.py", error="parse failed")
        assert r.error == "parse failed"
