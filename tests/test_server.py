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
        return {"graph": graph.model_dump()}

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
        node_id = graph.nodes[0].id

        resp = client.post("/api/pipeline/preview", json={
            "graph": graph.model_dump(), "nodeId": node_id, "rowLimit": 10,
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
            "graph": graph.model_dump(), "rowIndex": 0,
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


# ---------------------------------------------------------------------------
# Submodel routes — POST /api/submodel/create, GET /api/submodel/{name},
#                   POST /api/submodel/dissolve
# ---------------------------------------------------------------------------

@pytest.fixture()
def three_node_graph(pipeline_dir: Path) -> dict:
    """Parse the test pipeline and return its graph as a dict payload."""
    from haute.parser import parse_pipeline_file

    graph = parse_pipeline_file(pipeline_dir / "test_pipeline.py")
    return graph.model_dump()


class TestCreateSubmodel:
    def _create_payload(self, graph_dict: dict, node_ids: list[str]) -> dict:
        return {
            "name": "my_submodel",
            "node_ids": node_ids,
            "graph": graph_dict,
            "source_file": "test_pipeline.py",
            "pipeline_name": "test_pipeline",
        }

    def test_create_submodel_success(
        self, client: TestClient, pipeline_dir: Path, three_node_graph: dict,
    ):
        # Select the two nodes (source + transform) for grouping
        node_ids = [n["id"] for n in three_node_graph["nodes"]]
        assert len(node_ids) >= 2
        selected = node_ids[:2]

        payload = self._create_payload(three_node_graph, selected)
        resp = client.post("/api/submodel/create", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["submodel_file"] == "modules/my_submodel.py"
        assert data["parent_file"] == "test_pipeline.py"
        assert "nodes" in data["graph"]

        # Verify the submodel node exists in the returned graph
        returned_node_ids = {n["id"] for n in data["graph"]["nodes"]}
        assert "submodel__my_submodel" in returned_node_ids
        # Original selected nodes should be gone from parent
        for nid in selected:
            assert nid not in returned_node_ids

        # Verify files were written to disk
        assert (pipeline_dir / "modules" / "my_submodel.py").exists()
        assert (pipeline_dir / "test_pipeline.py").exists()

    def test_create_submodel_too_few_nodes_returns_400(
        self, client: TestClient, three_node_graph: dict,
    ):
        # Only 1 node — must be at least 2
        node_ids = [three_node_graph["nodes"][0]["id"]]
        payload = self._create_payload(three_node_graph, node_ids)
        resp = client.post("/api/submodel/create", json=payload)
        assert resp.status_code == 400
        assert "at least 2" in resp.json()["detail"]

    def test_create_submodel_missing_source_file_returns_400(
        self, client: TestClient, three_node_graph: dict,
    ):
        node_ids = [n["id"] for n in three_node_graph["nodes"][:2]]
        payload = {
            "name": "my_submodel",
            "node_ids": node_ids,
            "graph": three_node_graph,
            "source_file": "",
            "pipeline_name": "test_pipeline",
        }
        resp = client.post("/api/submodel/create", json=payload)
        assert resp.status_code == 400
        assert "source_file" in resp.json()["detail"]


class TestGetSubmodel:
    def test_get_submodel_success(
        self, client: TestClient, pipeline_dir: Path, three_node_graph: dict,
    ):
        # First, create a submodel
        node_ids = [n["id"] for n in three_node_graph["nodes"][:2]]
        create_resp = client.post("/api/submodel/create", json={
            "name": "lookup",
            "node_ids": node_ids,
            "graph": three_node_graph,
            "source_file": "test_pipeline.py",
            "pipeline_name": "test_pipeline",
        })
        assert create_resp.status_code == 200

        # Now fetch it
        resp = client.get("/api/submodel/lookup")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["submodel_name"]
        assert "nodes" in data["graph"]
        # The internal graph should contain the grouped nodes
        internal_ids = {n["id"] for n in data["graph"]["nodes"]}
        for nid in node_ids:
            assert nid in internal_ids

    def test_get_submodel_not_found_returns_404(self, client: TestClient):
        resp = client.get("/api/submodel/nonexistent")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()


class TestDissolveSubmodel:
    def test_dissolve_submodel_success(
        self, client: TestClient, pipeline_dir: Path, three_node_graph: dict,
    ):
        # Create a submodel first
        node_ids = [n["id"] for n in three_node_graph["nodes"][:2]]
        create_resp = client.post("/api/submodel/create", json={
            "name": "temp_group",
            "node_ids": node_ids,
            "graph": three_node_graph,
            "source_file": "test_pipeline.py",
            "pipeline_name": "test_pipeline",
        })
        assert create_resp.status_code == 200
        updated_graph = create_resp.json()["graph"]

        # Dissolve it
        resp = client.post("/api/submodel/dissolve", json={
            "submodel_name": "temp_group",
            "graph": updated_graph,
            "source_file": "test_pipeline.py",
            "pipeline_name": "test_pipeline",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

        # The flattened graph should have the original nodes back
        flat_ids = {n["id"] for n in data["graph"]["nodes"]}
        assert "submodel__temp_group" not in flat_ids
        for nid in node_ids:
            assert nid in flat_ids

        # The submodel file should be deleted
        assert not (pipeline_dir / "modules" / "temp_group.py").exists()

    def test_dissolve_nonexistent_submodel_returns_404(
        self, client: TestClient, three_node_graph: dict,
    ):
        resp = client.post("/api/submodel/dissolve", json={
            "submodel_name": "ghost",
            "graph": three_node_graph,
            "source_file": "test_pipeline.py",
            "pipeline_name": "test_pipeline",
        })
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# WebSocket — /ws/sync connect/disconnect and broadcast
# ---------------------------------------------------------------------------


class TestWebSocket:
    def test_connect_and_disconnect(self, client: TestClient):
        with client.websocket_connect("/ws/sync") as ws:
            # Connection should be accepted — sending a keep-alive message works
            ws.send_text("ping")
        # No error means connect + clean disconnect succeeded

    def test_broadcast_reaches_connected_client(
        self, client: TestClient, pipeline_dir: Path,
    ):
        """Save endpoint writes files and triggers sidecar — verify the
        full HTTP flow still works with an active WebSocket connection."""
        from haute.routes._helpers import ws_clients

        with client.websocket_connect("/ws/sync"):
            assert len(ws_clients) >= 1

            # Use a save call to exercise the full stack (which calls mark_self_write)
            graph = {
                "nodes": [
                    {"id": "s", "type": "pipelineNode",
                     "position": {"x": 0, "y": 0},
                     "data": {"label": "S", "nodeType": "dataSource",
                              "config": {"path": "d.parquet"}}},
                ],
                "edges": [],
            }
            resp = client.post("/api/pipeline/save", json={
                "name": "ws_test",
                "description": "",
                "graph": graph,
                "source_file": "ws_test.py",
            })
            assert resp.status_code == 200

        # After disconnect, client should be removed
        assert len(ws_clients) == 0


class TestBroadcast:
    def test_broadcast_removes_dead_clients(self):
        """Dead WebSocket clients should be pruned during broadcast."""
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        from haute.routes._helpers import broadcast, ws_clients

        dead_ws = MagicMock()
        dead_ws.send_text = AsyncMock(side_effect=RuntimeError("closed"))

        ws_clients.add(dead_ws)
        try:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(broadcast({"type": "test"}))
            finally:
                loop.close()
        finally:
            ws_clients.discard(dead_ws)

        assert dead_ws not in ws_clients

    def test_broadcast_delivers_to_live_client(self):
        """A live mock client should receive the broadcast message."""
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        from haute.routes._helpers import broadcast, ws_clients

        live_ws = MagicMock()
        live_ws.send_text = AsyncMock()

        ws_clients.add(live_ws)
        try:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(broadcast({"type": "graph_update", "graph": {}}))
            finally:
                loop.close()

            live_ws.send_text.assert_called_once()
            payload = live_ws.send_text.call_args[0][0]
            assert '"type": "graph_update"' in payload
        finally:
            ws_clients.discard(live_ws)


# ---------------------------------------------------------------------------
# Self-write tracking
# ---------------------------------------------------------------------------


class TestSelfWriteTracking:
    def test_mark_and_check(self):
        from haute.routes._helpers import is_self_write, mark_self_write

        mark_self_write()
        assert is_self_write() is True

    def test_expires_after_cooldown(self, monkeypatch: pytest.MonkeyPatch):
        import time as _time

        import haute.routes._helpers as helpers

        # Freeze time, mark, then advance past cooldown
        fake_time = [100.0]
        monkeypatch.setattr(_time, "monotonic", lambda: fake_time[0])

        helpers.mark_self_write()
        assert helpers.is_self_write() is True

        fake_time[0] = 101.5  # 1.5s later, past the 1.0s cooldown
        assert helpers.is_self_write() is False


# ---------------------------------------------------------------------------
# File watcher logic (unit test with mocked awatch)
# ---------------------------------------------------------------------------


class TestFileWatcher:
    def test_py_change_triggers_broadcast(
        self, pipeline_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        """A .py file change should parse and broadcast a graph_update."""
        import asyncio
        from unittest.mock import patch

        from watchfiles import Change

        monkeypatch.chdir(pipeline_dir)

        py_file = str(pipeline_dir / "test_pipeline.py")
        fake_changes = [(Change.modified, py_file)]

        async def _fake_awatch(*dirs, **kw):
            yield fake_changes

        broadcast_calls: list[dict] = []

        async def _capture_broadcast(data: dict) -> None:
            broadcast_calls.append(data)

        # awatch is imported locally inside _file_watcher via
        # ``from watchfiles import awatch``, so patch it on the watchfiles module.
        with (
            patch("watchfiles.awatch", _fake_awatch),
            patch("haute.server.broadcast", _capture_broadcast),
            patch("haute.server.is_self_write", return_value=False),
        ):
            from haute.server import _file_watcher

            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_file_watcher())
            finally:
                loop.close()

        assert len(broadcast_calls) >= 1
        assert broadcast_calls[0]["type"] == "graph_update"
        assert "graph" in broadcast_calls[0]

    def test_non_py_files_ignored(self, pipeline_dir: Path, monkeypatch: pytest.MonkeyPatch):
        """Non-.py files should be ignored by the watcher."""
        import asyncio
        from unittest.mock import patch

        from watchfiles import Change

        monkeypatch.chdir(pipeline_dir)

        fake_changes = [(Change.modified, str(pipeline_dir / "readme.txt"))]

        async def _fake_awatch(*dirs, **kw):
            yield fake_changes

        broadcast_calls: list[dict] = []

        async def _capture_broadcast(data: dict) -> None:
            broadcast_calls.append(data)

        with (
            patch("watchfiles.awatch", _fake_awatch),
            patch("haute.server.broadcast", _capture_broadcast),
            patch("haute.server.is_self_write", return_value=False),
        ):
            from haute.server import _file_watcher

            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_file_watcher())
            finally:
                loop.close()

        assert len(broadcast_calls) == 0

    def test_self_write_skips_broadcast(self, pipeline_dir: Path, monkeypatch: pytest.MonkeyPatch):
        """Changes during self-write cooldown should be skipped."""
        import asyncio
        from unittest.mock import patch

        from watchfiles import Change

        monkeypatch.chdir(pipeline_dir)

        fake_changes = [(Change.modified, str(pipeline_dir / "test_pipeline.py"))]

        async def _fake_awatch(*dirs, **kw):
            yield fake_changes

        broadcast_calls: list[dict] = []

        async def _capture_broadcast(data: dict) -> None:
            broadcast_calls.append(data)

        with (
            patch("watchfiles.awatch", _fake_awatch),
            patch("haute.server.broadcast", _capture_broadcast),
            patch("haute.server.is_self_write", return_value=True),
        ):
            from haute.server import _file_watcher

            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_file_watcher())
            finally:
                loop.close()

        assert len(broadcast_calls) == 0
