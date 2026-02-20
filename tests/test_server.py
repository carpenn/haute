"""Tests for haute.server - FastAPI API endpoint integration tests."""

from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def pipeline_dir(tmp_path: Path) -> Path:
    """Create a temporary project with a root-level pipeline and sample data."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]}).write_parquet(data_dir / "input.parquet")

    code = f'''\
import polars as pl
import haute

pipeline = haute.Pipeline("test_pipeline", description="A test pipeline")


@pipeline.node(path="{data_dir / 'input.parquet'}")
def source() -> pl.DataFrame:
    """Read data."""
    return pl.scan_parquet("{data_dir / 'input.parquet'}")


@pipeline.node
def transform(source: pl.DataFrame) -> pl.DataFrame:
    """Transform."""
    return source


pipeline.connect("source", "transform")
'''
    (tmp_path / "test_pipeline.py").write_text(code)
    return tmp_path


@pytest.fixture()
def client(pipeline_dir: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient that runs with cwd set to the temp pipeline directory."""
    monkeypatch.chdir(pipeline_dir)
    # Re-import to pick up cwd change
    from haute.server import app
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /api/pipelines
# ---------------------------------------------------------------------------

class TestListPipelines:
    def test_returns_discovered_pipelines(self, client: TestClient):
        resp = client.get("/api/pipelines")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        pipe = data[0]
        assert pipe["name"] == "test_pipeline"
        assert pipe["node_count"] >= 2

    def test_includes_file_path(self, client: TestClient):
        resp = client.get("/api/pipelines")
        data = resp.json()
        assert any("test_pipeline.py" in p["file"] for p in data)


# ---------------------------------------------------------------------------
# GET /api/pipeline
# ---------------------------------------------------------------------------

class TestGetFirstPipeline:
    def test_returns_graph(self, client: TestClient):
        resp = client.get("/api/pipeline")
        assert resp.status_code == 200
        graph = resp.json()
        assert "nodes" in graph
        assert "edges" in graph
        assert len(graph["nodes"]) >= 2
        assert graph["pipeline_name"] == "test_pipeline"

    def test_empty_project_returns_empty_graph(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.chdir(tmp_path)
        from haute.server import app
        c = TestClient(app)
        resp = c.get("/api/pipeline")
        assert resp.status_code == 200
        graph = resp.json()
        assert graph["nodes"] == []


# ---------------------------------------------------------------------------
# GET /api/pipeline/{name}
# ---------------------------------------------------------------------------

class TestGetPipelineByName:
    def test_found(self, client: TestClient):
        resp = client.get("/api/pipeline/test_pipeline")
        assert resp.status_code == 200
        graph = resp.json()
        assert graph["pipeline_name"] == "test_pipeline"

    def test_not_found(self, client: TestClient):
        resp = client.get("/api/pipeline/nonexistent")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/pipeline/run
# ---------------------------------------------------------------------------

class TestRunPipeline:
    def _graph_payload(self, pipeline_dir: Path) -> dict:
        from haute.parser import parse_pipeline_file
        graph = parse_pipeline_file(pipeline_dir / "test_pipeline.py")
        return {"graph": graph}

    def test_run_returns_results(self, client: TestClient, pipeline_dir: Path):
        body = self._graph_payload(pipeline_dir)
        resp = client.post("/api/pipeline/run", json=body)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert len(data["results"]) >= 2
        # Every node should have status ok
        for nid, res in data["results"].items():
            assert res["status"] == "ok", f"Node {nid} failed: {res.get('error')}"
            assert res["row_count"] > 0

    def test_run_empty_graph_returns_400(self, client: TestClient):
        resp = client.post("/api/pipeline/run", json={"graph": {"nodes": [], "edges": []}})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/pipeline/preview
# ---------------------------------------------------------------------------

class TestPreviewNode:
    def test_preview_returns_node_data(self, client: TestClient, pipeline_dir: Path):
        from haute.parser import parse_pipeline_file
        graph = parse_pipeline_file(pipeline_dir / "test_pipeline.py")
        node_id = graph["nodes"][0]["id"]

        resp = client.post("/api/pipeline/preview", json={
            "graph": graph, "nodeId": node_id, "rowLimit": 10,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["nodeId"] == node_id
        assert data["status"] == "ok"
        assert data["row_count"] <= 10
        assert len(data["columns"]) > 0

    def test_preview_empty_graph_returns_400(self, client: TestClient):
        resp = client.post("/api/pipeline/preview", json={
            "graph": {"nodes": [], "edges": []}, "nodeId": "x",
        })
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/pipeline/trace
# ---------------------------------------------------------------------------

class TestTraceRow:
    def test_trace_returns_steps(self, client: TestClient, pipeline_dir: Path):
        from haute.parser import parse_pipeline_file
        graph = parse_pipeline_file(pipeline_dir / "test_pipeline.py")

        resp = client.post("/api/pipeline/trace", json={
            "graph": graph, "rowIndex": 0,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "trace" in data
        assert len(data["trace"]["steps"]) >= 2

    def test_trace_empty_graph_returns_400(self, client: TestClient):
        resp = client.post("/api/pipeline/trace", json={
            "graph": {"nodes": [], "edges": []},
        })
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/pipeline/save
# ---------------------------------------------------------------------------

class TestSavePipeline:
    def test_save_creates_files(self, client: TestClient, pipeline_dir: Path):
        graph = {
            "nodes": [
                {"id": "s", "type": "pipelineNode", "position": {"x": 0, "y": 0},
                 "data": {"label": "Source", "nodeType": "dataSource", "config": {"path": "d.parquet"}}},
            ],
            "edges": [],
        }
        resp = client.post("/api/pipeline/save", json={
            "name": "saved_pipe",
            "description": "Test save",
            "graph": graph,
            "source_file": "saved_pipe.py",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "saved"
        assert "saved_pipe.py" in data["file"]

        # Check files were created
        py_file = pipeline_dir / data["file"]
        assert py_file.exists()
        content = py_file.read_text()
        assert "import polars as pl" in content
        assert 'Pipeline("saved_pipe"' in content

        # Sidecar should exist too
        sidecar = py_file.with_suffix(".haute.json")
        assert sidecar.exists()


# ---------------------------------------------------------------------------
# POST /api/pipeline/sink
# ---------------------------------------------------------------------------

class TestExecuteSinkEndpoint:
    def test_sink_writes_output(self, client: TestClient, pipeline_dir: Path):
        out_path = pipeline_dir / "output" / "result.parquet"
        data_path = pipeline_dir / "data" / "input.parquet"

        graph = {
            "nodes": [
                {"id": "src", "type": "pipelineNode", "position": {"x": 0, "y": 0},
                 "data": {"label": "src", "nodeType": "dataSource",
                          "config": {"path": str(data_path)}}},
                {"id": "sink", "type": "pipelineNode", "position": {"x": 300, "y": 0},
                 "data": {"label": "sink", "nodeType": "dataSink",
                          "config": {"path": str(out_path), "format": "parquet"}}},
            ],
            "edges": [{"id": "e1", "source": "src", "target": "sink"}],
        }
        resp = client.post("/api/pipeline/sink", json={
            "graph": graph, "nodeId": "sink",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["row_count"] == 3
        assert out_path.exists()


# ---------------------------------------------------------------------------
# GET /api/files
# ---------------------------------------------------------------------------

class TestBrowseFiles:
    def test_browse_project_root(self, client: TestClient, pipeline_dir: Path):
        resp = client.get("/api/files", params={"dir": ".", "extensions": ".parquet,.py"})
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        # Should find at least the data/ dir
        names = [item["name"] for item in data["items"]]
        assert "data" in names

    def test_browse_data_dir(self, client: TestClient, pipeline_dir: Path):
        resp = client.get("/api/files", params={"dir": "data", "extensions": ".parquet"})
        assert resp.status_code == 200
        data = resp.json()
        files = [i for i in data["items"] if i["type"] == "file"]
        assert len(files) == 1
        assert files[0]["name"] == "input.parquet"
        assert files[0]["size"] > 0

    def test_browse_outside_project_returns_403(self, client: TestClient):
        resp = client.get("/api/files", params={"dir": "../../../etc"})
        assert resp.status_code == 403

    def test_browse_nonexistent_dir_returns_404(self, client: TestClient):
        resp = client.get("/api/files", params={"dir": "no_such_dir"})
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/schema
# ---------------------------------------------------------------------------

class TestGetSchema:
    def test_parquet_schema(self, client: TestClient, pipeline_dir: Path):
        resp = client.get("/api/schema", params={"path": "data/input.parquet"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 3
        assert data["column_count"] == 2
        col_names = [c["name"] for c in data["columns"]]
        assert "x" in col_names
        assert "y" in col_names
        assert len(data["preview"]) <= 5

    def test_csv_schema(self, client: TestClient, pipeline_dir: Path):
        csv_path = pipeline_dir / "data" / "test.csv"
        pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_csv(csv_path)

        resp = client.get("/api/schema", params={"path": "data/test.csv"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["column_count"] == 2

    def test_schema_outside_project_returns_403(self, client: TestClient):
        resp = client.get("/api/schema", params={"path": "../../../etc/passwd"})
        assert resp.status_code == 403

    def test_schema_not_found_returns_404(self, client: TestClient):
        resp = client.get("/api/schema", params={"path": "no_such_file.parquet"})
        assert resp.status_code == 404

    def test_unsupported_type_returns_400(self, client: TestClient, pipeline_dir: Path):
        (pipeline_dir / "data" / "test.txt").write_text("hello")
        resp = client.get("/api/schema", params={"path": "data/test.txt"})
        assert resp.status_code == 400
