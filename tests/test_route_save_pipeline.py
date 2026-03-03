"""Tests for the SavePipelineService (routes/_save_pipeline.py).

Covers:
  - SavePipelineService.save() with simple single-file graph
  - _validate_singletons() with valid, duplicate, and missing singletons
  - _write_code() with submodel multi-file
  - _remove_stale_config_files() with stale, fresh, and empty config dirs
  - _resolve_source_file() validation
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from haute._types import GraphEdge, GraphNode, NodeData, NodeType, PipelineGraph
from haute.routes._save_pipeline import SavePipelineService
from haute.schemas import SavePipelineRequest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    nid: str,
    label: str,
    node_type: str = "transform",
    config: dict | None = None,
) -> GraphNode:
    return GraphNode(
        id=nid,
        data=NodeData(label=label, nodeType=node_type, config=config or {}),
    )


def _make_edge(src: str, tgt: str) -> GraphEdge:
    return GraphEdge(id=f"e_{src}_{tgt}", source=src, target=tgt)


def _make_graph(*nodes: GraphNode, edges: list[GraphEdge] | None = None) -> PipelineGraph:
    return PipelineGraph(nodes=list(nodes), edges=edges or [])


# ---------------------------------------------------------------------------
# _validate_singletons
# ---------------------------------------------------------------------------


class TestValidateSingletons:
    def test_valid_graph_single_api_input(self) -> None:
        """A graph with exactly one of each singleton type passes."""
        graph = _make_graph(
            _make_node("a", "Api Input", "apiInput", {"path": "data.parquet"}),
            _make_node("o", "Output", "output", {"fields": []}),
            _make_node("t", "Transform", "transform"),
        )
        # Should not raise
        SavePipelineService._validate_singletons(graph)

    def test_duplicate_api_input_raises_400(self) -> None:
        """Two API Input nodes should raise 400."""
        graph = _make_graph(
            _make_node("a1", "Api 1", "apiInput", {"path": "d1.parquet"}),
            _make_node("a2", "Api 2", "apiInput", {"path": "d2.parquet"}),
        )
        with pytest.raises(HTTPException) as exc_info:
            SavePipelineService._validate_singletons(graph)
        assert exc_info.value.status_code == 400
        assert "API Input" in exc_info.value.detail
        assert "found 2" in exc_info.value.detail

    def test_duplicate_output_raises_400(self) -> None:
        """Two Output nodes should raise 400."""
        graph = _make_graph(
            _make_node("o1", "Out 1", "output", {"fields": []}),
            _make_node("o2", "Out 2", "output", {"fields": []}),
        )
        with pytest.raises(HTTPException) as exc_info:
            SavePipelineService._validate_singletons(graph)
        assert exc_info.value.status_code == 400
        assert "Output" in exc_info.value.detail

    def test_duplicate_live_switch_raises_400(self) -> None:
        """Two Live Switch nodes should raise 400."""
        graph = _make_graph(
            _make_node("ls1", "Switch 1", "liveSwitch", {"live": "a", "batch": "b"}),
            _make_node("ls2", "Switch 2", "liveSwitch", {"live": "c", "batch": "d"}),
        )
        with pytest.raises(HTTPException) as exc_info:
            SavePipelineService._validate_singletons(graph)
        assert exc_info.value.status_code == 400
        assert "Live Switch" in exc_info.value.detail

    def test_no_singletons_passes(self) -> None:
        """A graph with only transform nodes passes validation."""
        graph = _make_graph(
            _make_node("t1", "T1", "transform"),
            _make_node("t2", "T2", "transform"),
        )
        SavePipelineService._validate_singletons(graph)

    def test_empty_graph_passes(self) -> None:
        """An empty graph passes singleton validation."""
        graph = _make_graph()
        SavePipelineService._validate_singletons(graph)


# ---------------------------------------------------------------------------
# _resolve_source_file
# ---------------------------------------------------------------------------


class TestResolveSourceFile:
    def test_empty_source_file_raises_400(self, tmp_path: Path) -> None:
        """An empty source_file string should raise 400."""
        svc = SavePipelineService(tmp_path)
        with pytest.raises(HTTPException) as exc_info:
            svc._resolve_source_file("")
        assert exc_info.value.status_code == 400
        assert "source_file" in exc_info.value.detail

    def test_valid_source_file(self, tmp_path: Path) -> None:
        """A valid relative path should resolve within the project root."""
        svc = SavePipelineService(tmp_path)
        result = svc._resolve_source_file("pipeline.py")
        assert result == (tmp_path / "pipeline.py").resolve()

    def test_traversal_raises_403(self, tmp_path: Path) -> None:
        """A path that escapes the project root should raise 403."""
        svc = SavePipelineService(tmp_path)
        with pytest.raises(HTTPException) as exc_info:
            svc._resolve_source_file("../../etc/passwd")
        assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# SavePipelineService.save() — end-to-end with simple graph
# ---------------------------------------------------------------------------


class TestSaveSimpleGraph:
    def test_save_single_file_graph(self, tmp_path: Path) -> None:
        """save() generates code, writes .py and .haute.json sidecar."""
        svc = SavePipelineService(tmp_path)
        graph = _make_graph(
            _make_node("src", "Source", "dataSource", {"path": "data.parquet"}),
            _make_node("t1", "Transform", "transform", {"code": "return source"}),
            edges=[_make_edge("src", "t1")],
        )
        body = SavePipelineRequest(
            name="my_pipeline",
            description="Test pipeline",
            graph=graph,
            source_file="my_pipeline.py",
            scenarios=["live"],
            active_scenario="live",
        )

        with patch.object(svc, "_infer_flatten_schemas"):
            result = svc.save(body)

        assert result.status == "saved"
        assert result.pipeline_name == "my_pipeline"
        assert "my_pipeline.py" in result.file

        # Verify .py file was created
        py_file = tmp_path / result.file
        assert py_file.exists()
        content = py_file.read_text()
        assert "Pipeline" in content

        # Verify sidecar was created
        sidecar = py_file.with_suffix(".haute.json")
        assert sidecar.exists()
        sidecar_data = json.loads(sidecar.read_text())
        assert "positions" in sidecar_data

    def test_save_returns_relative_file_path(self, tmp_path: Path) -> None:
        """The returned file path should be relative to project root."""
        svc = SavePipelineService(tmp_path)
        graph = _make_graph(
            _make_node("src", "Source", "dataSource", {"path": "data.parquet"}),
        )
        body = SavePipelineRequest(
            name="test_pipe",
            description="",
            graph=graph,
            source_file="test_pipe.py",
        )

        with patch.object(svc, "_infer_flatten_schemas"):
            result = svc.save(body)

        # Should be relative, not absolute
        assert not result.file.startswith("/")


# ---------------------------------------------------------------------------
# _write_code with submodels
# ---------------------------------------------------------------------------


class TestWriteCodeMultiFile:
    def test_submodel_creates_module_file(self, tmp_path: Path) -> None:
        """When graph has submodels, _write_code should create multiple files."""
        svc = SavePipelineService(tmp_path)

        main_node = _make_node("sub", "submodel__scoring", "submodel")
        graph = _make_graph(main_node)
        # Set up submodels dict so the multi-file path is taken
        graph.submodels = {
            "scoring": {
                "nodes": [
                    {"id": "s1", "data": {"label": "S1", "nodeType": "transform", "config": {}}},
                ],
                "edges": [],
            },
        }

        fake_files = {
            "main.py": "# main pipeline\nimport haute\n",
            "modules/scoring.py": "# scoring submodel\nimport haute\n",
        }

        body = SavePipelineRequest(
            name="main",
            description="",
            graph=graph,
            source_file="main.py",
        )

        with patch("haute.codegen.graph_to_code_multi", return_value=fake_files):
            svc._write_code(body, graph, tmp_path / "main.py")

        assert (tmp_path / "main.py").exists()
        assert (tmp_path / "modules" / "scoring.py").exists()
        assert "scoring submodel" in (tmp_path / "modules" / "scoring.py").read_text()

    def test_single_file_no_submodels(self, tmp_path: Path) -> None:
        """Without submodels, _write_code generates a single file via real codegen."""
        svc = SavePipelineService(tmp_path)
        graph = _make_graph(
            _make_node("src", "Source", "dataSource", {"path": "data.parquet"}),
        )
        body = SavePipelineRequest(
            name="pipe",
            description="",
            graph=graph,
            source_file="pipe.py",
        )
        py_path = tmp_path / "pipe.py"

        svc._write_code(body, graph, py_path)

        assert py_path.exists()
        code = py_path.read_text()
        assert "import haute" in code
        assert "pipe" in code  # pipeline name

    def test_submodel_path_traversal_skipped(self, tmp_path: Path) -> None:
        """Files with paths that escape project root are silently skipped."""
        svc = SavePipelineService(tmp_path)
        graph = _make_graph()
        graph.submodels = {"evil": {"nodes": [], "edges": []}}

        # Produce a file path that would escape the project root
        fake_files = {
            "main.py": "# ok\n",
            "../../etc/evil.py": "# evil\n",
        }

        body = SavePipelineRequest(
            name="main", description="", graph=graph, source_file="main.py",
        )

        with patch("haute.codegen.graph_to_code_multi", return_value=fake_files):
            svc._write_code(body, graph, tmp_path / "main.py")

        # The main file is written
        assert (tmp_path / "main.py").exists()
        # The traversal path should NOT have been written
        assert not Path("/etc/evil.py").exists()


# ---------------------------------------------------------------------------
# _remove_stale_config_files
# ---------------------------------------------------------------------------


class TestRemoveStaleConfigFiles:
    def test_removes_stale_config_file(self, tmp_path: Path) -> None:
        """Config files not corresponding to any node should be deleted."""
        svc = SavePipelineService(tmp_path)

        # Create a config file that won't be in the graph
        stale_dir = tmp_path / "config" / "factors"
        stale_dir.mkdir(parents=True)
        stale_file = stale_dir / "old_banding.json"
        stale_file.write_text("{}")

        graph = _make_graph()  # No banding nodes
        svc._write_config_files(graph)  # Populates _last_config_files (empty for no-config nodes)

        with patch("haute._config_io.NODE_TYPE_TO_FOLDER", {NodeType.BANDING: "factors"}):
            svc._remove_stale_config_files(graph)

        assert not stale_file.exists()

    def test_preserves_fresh_config_file(self, tmp_path: Path) -> None:
        """Config files that match current graph nodes should be kept."""
        svc = SavePipelineService(tmp_path)

        graph = _make_graph(
            _make_node("b1", "my_banding", "banding", {"bands": []}),
        )
        svc._write_config_files(graph)  # Writes config/factors/my_banding.json

        fresh_file = tmp_path / "config" / "factors" / "my_banding.json"
        assert fresh_file.exists(), "Config file should be written by _write_config_files"

        svc._remove_stale_config_files(graph)

        assert fresh_file.exists()

    def test_removes_empty_folder(self, tmp_path: Path) -> None:
        """Empty config type folders are removed after stale file deletion."""
        svc = SavePipelineService(tmp_path)

        config_dir = tmp_path / "config" / "factors"
        config_dir.mkdir(parents=True)
        stale_file = config_dir / "old.json"
        stale_file.write_text("{}")

        graph = _make_graph()
        svc._write_config_files(graph)  # No banding nodes → empty config files

        with patch("haute._config_io.NODE_TYPE_TO_FOLDER", {NodeType.BANDING: "factors"}):
            svc._remove_stale_config_files(graph)

        assert not config_dir.exists()
        # Config dir itself should be cleaned up too
        assert not (tmp_path / "config").exists()

    def test_no_config_dir_noop(self, tmp_path: Path) -> None:
        """If config/ doesn't exist, _remove_stale_config_files is a no-op."""
        svc = SavePipelineService(tmp_path)
        graph = _make_graph()
        svc._write_config_files(graph)

        with patch("haute._config_io.NODE_TYPE_TO_FOLDER", {NodeType.BANDING: "factors"}):
            # Should not raise
            svc._remove_stale_config_files(graph)

    def test_mixed_stale_and_fresh(self, tmp_path: Path) -> None:
        """Only stale files are removed; fresh files remain."""
        svc = SavePipelineService(tmp_path)

        # Build a graph with one banding node that produces
        # config/factors/current_banding.json via _write_config_files
        graph = _make_graph(
            _make_node("b1", "current_banding", "banding", {"bands": []}),
        )
        svc._write_config_files(graph)

        config_dir = tmp_path / "config" / "factors"
        assert config_dir.exists(), "_write_config_files should create the factors dir"

        fresh = config_dir / "current_banding.json"
        assert fresh.exists(), "Config for current_banding should exist"

        # Plant an extra stale file that doesn't correspond to any node
        stale = config_dir / "old_banding.json"
        stale.write_text("{}")

        svc._remove_stale_config_files(graph)

        assert not stale.exists()
        assert fresh.exists()
        # Folder still exists because fresh file remains
        assert config_dir.exists()


# ---------------------------------------------------------------------------
# Integration: save via HTTP endpoint (uses TestClient)
# ---------------------------------------------------------------------------


class TestSaveEndpointIntegration:
    @pytest.fixture()
    def client(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
        monkeypatch.chdir(tmp_path)
        from haute.server import app

        return TestClient(app)

    def test_save_via_http(self, client: TestClient, tmp_path: Path) -> None:
        """POST /api/pipeline/save produces a working .py file."""
        graph = {
            "nodes": [
                {
                    "id": "src",
                    "type": "pipelineNode",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "Source",
                        "nodeType": "dataSource",
                        "config": {"path": "data.parquet"},
                    },
                },
                {
                    "id": "t1",
                    "type": "pipelineNode",
                    "position": {"x": 200, "y": 0},
                    "data": {
                        "label": "Transform",
                        "nodeType": "transform",
                        "config": {},
                    },
                },
            ],
            "edges": [{"id": "e1", "source": "src", "target": "t1"}],
        }
        resp = client.post("/api/pipeline/save", json={
            "name": "saved_test",
            "description": "Integration test",
            "graph": graph,
            "source_file": "saved_test.py",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "saved"
        assert "saved_test.py" in data["file"]

    def test_save_duplicate_api_input_returns_400(self, client: TestClient) -> None:
        """Two API Input nodes should fail validation at 400."""
        graph = {
            "nodes": [
                {
                    "id": "a1",
                    "type": "pipelineNode",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "Api1",
                        "nodeType": "apiInput",
                        "config": {"path": "d.parquet"},
                    },
                },
                {
                    "id": "a2",
                    "type": "pipelineNode",
                    "position": {"x": 200, "y": 0},
                    "data": {
                        "label": "Api2",
                        "nodeType": "apiInput",
                        "config": {"path": "d2.parquet"},
                    },
                },
            ],
            "edges": [],
        }
        resp = client.post("/api/pipeline/save", json={
            "name": "bad_pipe",
            "description": "",
            "graph": graph,
            "source_file": "bad_pipe.py",
        })
        assert resp.status_code == 400
        assert "API Input" in resp.json()["detail"]

    def test_save_empty_source_file_returns_400(self, client: TestClient) -> None:
        """Empty source_file should return 400."""
        graph = {
            "nodes": [
                {
                    "id": "s",
                    "type": "pipelineNode",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "S",
                        "nodeType": "dataSource",
                        "config": {"path": "d.parquet"},
                    },
                },
            ],
            "edges": [],
        }
        resp = client.post("/api/pipeline/save", json={
            "name": "pipe",
            "description": "",
            "graph": graph,
            "source_file": "",
        })
        assert resp.status_code == 400
        assert "source_file" in resp.json()["detail"]
