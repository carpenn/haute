"""Comprehensive tests for haute.routes._helpers.

Covers:
  - validate_safe_path  — valid paths, traversal attempts, absolute paths
  - raise_node_not_found / raise_node_type_error / raise_pipeline_not_found / raise_validation_error
  - mark_self_write / is_self_write — timing-based self-write detection
  - load_sidecar / load_sidecar_positions — valid JSON, corrupt JSON, missing file
  - save_sidecar — round-trip test, scenario state
  - broadcast — WebSocket message fan-out
  - parse_pipeline_to_graph — sidecar merging
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from haute._types import GraphNode, NodeData, NodeType, PipelineGraph
from haute.routes._helpers import (
    _SELF_WRITE_COOLDOWN,
    broadcast,
    invalidate_pipeline_index,
    is_self_write,
    load_sidecar,
    load_sidecar_positions,
    mark_self_write,
    raise_node_not_found,
    raise_node_type_error,
    raise_pipeline_not_found,
    raise_validation_error,
    save_sidecar,
    validate_safe_path,
    ws_clients,
)

# ===========================================================================
# validate_safe_path
# ===========================================================================


class TestValidateSafePath:
    def test_valid_relative_path(self, tmp_path):
        sub = tmp_path / "subdir"
        sub.mkdir()
        result = validate_safe_path(tmp_path, "subdir")
        assert result == sub

    def test_valid_file_path(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("hello")
        result = validate_safe_path(tmp_path, "file.txt")
        assert result == f

    def test_traversal_attempt_raises_403(self, tmp_path):
        with pytest.raises(HTTPException) as exc_info:
            validate_safe_path(tmp_path, "../../../etc/passwd")
        assert exc_info.value.status_code == 403
        assert "outside the project root" in exc_info.value.detail

    def test_double_traversal(self, tmp_path):
        with pytest.raises(HTTPException) as exc_info:
            validate_safe_path(tmp_path, "foo/../../..")
        assert exc_info.value.status_code == 403

    def test_absolute_path_within_base(self, tmp_path):
        """Absolute path that happens to be inside base should work."""
        f = tmp_path / "inner.txt"
        f.write_text("ok")
        result = validate_safe_path(tmp_path, str(f))
        assert result == f

    def test_nested_path(self, tmp_path):
        nested = tmp_path / "a" / "b" / "c"
        nested.mkdir(parents=True)
        result = validate_safe_path(tmp_path, "a/b/c")
        assert result == nested

    def test_path_object_input(self, tmp_path):
        result = validate_safe_path(tmp_path, Path("subdir"))
        # The path may not exist but should be resolved
        assert str(result).startswith(str(tmp_path))

    def test_symlink_escape(self, tmp_path):
        """Symlink pointing outside base should be caught after resolve()."""
        outside = tmp_path.parent / "outside_target"
        outside.mkdir(exist_ok=True)
        link = tmp_path / "sneaky_link"
        try:
            link.symlink_to(outside)
        except OSError:
            pytest.skip("Cannot create symlinks in this environment")
        with pytest.raises(HTTPException) as exc_info:
            validate_safe_path(tmp_path, "sneaky_link")
        assert exc_info.value.status_code == 403


# ===========================================================================
# HTTP error helpers
# ===========================================================================


class TestRaiseNodeNotFound:
    def test_raises_404(self):
        with pytest.raises(HTTPException) as exc_info:
            raise_node_not_found("node_123")
        assert exc_info.value.status_code == 404
        assert "node_123" in exc_info.value.detail


class TestRaiseNodeTypeError:
    def test_raises_400(self):
        with pytest.raises(HTTPException) as exc_info:
            raise_node_type_error("n1", "dataSource", "transform")
        assert exc_info.value.status_code == 400
        assert "dataSource" in exc_info.value.detail
        assert "transform" in exc_info.value.detail


class TestRaisePipelineNotFound:
    def test_raises_404(self):
        with pytest.raises(HTTPException) as exc_info:
            raise_pipeline_not_found("my_pipeline")
        assert exc_info.value.status_code == 404
        assert "my_pipeline" in exc_info.value.detail


class TestRaiseValidationError:
    def test_raises_400(self):
        with pytest.raises(HTTPException) as exc_info:
            raise_validation_error("bad input")
        assert exc_info.value.status_code == 400
        assert "bad input" in exc_info.value.detail


# ===========================================================================
# mark_self_write / is_self_write
# ===========================================================================


class TestSelfWriteTracking:
    def test_mark_then_check(self):
        mark_self_write()
        assert is_self_write() is True

    def test_expires_after_cooldown(self):
        """After the cooldown window, is_self_write returns False."""
        # We can't actually wait 2+ seconds in a unit test. Instead, we patch
        # time.monotonic to simulate time passing.

        mark_self_write()
        original = time.monotonic

        # Simulate time having passed beyond cooldown
        with patch.object(time, "monotonic", return_value=original() + _SELF_WRITE_COOLDOWN + 1):
            assert is_self_write() is False


# ===========================================================================
# load_sidecar / load_sidecar_positions
# ===========================================================================


class TestLoadSidecar:
    def test_valid_json(self, tmp_path):
        py_path = tmp_path / "pipeline.py"
        py_path.write_text("")
        sidecar = tmp_path / "pipeline.haute.json"
        data = {"positions": {"a": {"x": 10, "y": 20}}, "scenarios": ["live", "test"]}
        sidecar.write_text(json.dumps(data))

        result = load_sidecar(py_path)
        assert result["positions"]["a"] == {"x": 10, "y": 20}
        assert result["scenarios"] == ["live", "test"]

    def test_missing_sidecar(self, tmp_path):
        py_path = tmp_path / "pipeline.py"
        py_path.write_text("")
        result = load_sidecar(py_path)
        assert result == {}

    def test_corrupt_json(self, tmp_path):
        py_path = tmp_path / "pipeline.py"
        py_path.write_text("")
        sidecar = tmp_path / "pipeline.haute.json"
        sidecar.write_text("{bad json content")

        result = load_sidecar(py_path)
        assert result == {}

    def test_empty_file(self, tmp_path):
        py_path = tmp_path / "pipeline.py"
        py_path.write_text("")
        sidecar = tmp_path / "pipeline.haute.json"
        sidecar.write_text("")

        result = load_sidecar(py_path)
        assert result == {}


class TestLoadSidecarPositions:
    def test_returns_positions(self, tmp_path):
        py_path = tmp_path / "pipeline.py"
        py_path.write_text("")
        sidecar = tmp_path / "pipeline.haute.json"
        sidecar.write_text(json.dumps({"positions": {"n1": {"x": 5, "y": 10}}}))

        result = load_sidecar_positions(py_path)
        assert result == {"n1": {"x": 5, "y": 10}}

    def test_no_positions_key(self, tmp_path):
        py_path = tmp_path / "pipeline.py"
        py_path.write_text("")
        sidecar = tmp_path / "pipeline.haute.json"
        sidecar.write_text(json.dumps({"scenarios": ["live"]}))

        result = load_sidecar_positions(py_path)
        assert result == {}


# ===========================================================================
# save_sidecar
# ===========================================================================


class TestSaveSidecar:
    def test_basic_save(self, tmp_path):
        py_path = tmp_path / "pipeline.py"
        graph = PipelineGraph(
            nodes=[
                GraphNode(id="a", position={"x": 100.0, "y": 200.0},
                          data=NodeData(label="A", nodeType=NodeType.DATA_SOURCE)),
            ],
            edges=[],
        )
        save_sidecar(py_path, graph)

        sidecar = tmp_path / "pipeline.haute.json"
        assert sidecar.exists()
        data = json.loads(sidecar.read_text())
        assert "positions" in data
        assert data["positions"]["A"] == {"x": 100.0, "y": 200.0}

    def test_scenario_state_saved(self, tmp_path):
        py_path = tmp_path / "pipeline.py"
        graph = PipelineGraph(
            nodes=[],
            scenarios=["live", "test_batch"],
            active_scenario="test_batch",
        )
        save_sidecar(py_path, graph)

        data = json.loads((tmp_path / "pipeline.haute.json").read_text())
        assert data["scenarios"] == ["live", "test_batch"]
        assert data["active_scenario"] == "test_batch"

    def test_default_scenario_not_saved(self, tmp_path):
        """Default scenario state (["live"], "live") is not persisted."""
        py_path = tmp_path / "pipeline.py"
        graph = PipelineGraph(nodes=[], scenarios=["live"], active_scenario="live")
        save_sidecar(py_path, graph)

        data = json.loads((tmp_path / "pipeline.haute.json").read_text())
        assert "scenarios" not in data
        assert "active_scenario" not in data

    def test_roundtrip(self, tmp_path):
        """save_sidecar then load_sidecar should produce consistent data."""
        py_path = tmp_path / "pipeline.py"
        py_path.write_text("")

        graph = PipelineGraph(
            nodes=[
                GraphNode(id="a", position={"x": 1.0, "y": 2.0},
                          data=NodeData(label="alpha", nodeType=NodeType.TRANSFORM)),
                GraphNode(id="b", position={"x": 3.0, "y": 4.0},
                          data=NodeData(label="beta", nodeType=NodeType.OUTPUT)),
            ],
            scenarios=["live", "test"],
            active_scenario="test",
        )
        save_sidecar(py_path, graph)
        loaded = load_sidecar(py_path)

        assert loaded["positions"]["alpha"] == {"x": 1.0, "y": 2.0}
        assert loaded["positions"]["beta"] == {"x": 3.0, "y": 4.0}
        assert loaded["scenarios"] == ["live", "test"]
        assert loaded["active_scenario"] == "test"

    def test_label_sanitized_as_key(self, tmp_path):
        """Position keys use sanitized label (matching parser node IDs)."""
        py_path = tmp_path / "pipeline.py"
        graph = PipelineGraph(
            nodes=[
                GraphNode(id="n1", position={"x": 10.0, "y": 20.0},
                          data=NodeData(label="My Node", nodeType=NodeType.TRANSFORM)),
            ],
        )
        save_sidecar(py_path, graph)
        from haute._types import _sanitize_func_name

        data = json.loads((tmp_path / "pipeline.haute.json").read_text())
        expected_key = _sanitize_func_name("My Node")
        assert expected_key in data["positions"]


# ===========================================================================
# broadcast
# ===========================================================================


class TestBroadcast:
    @pytest.mark.asyncio
    async def test_sends_to_all_clients(self):
        ws1 = AsyncMock()
        ws2 = AsyncMock()
        ws_clients.clear()
        ws_clients.add(ws1)
        ws_clients.add(ws2)
        try:
            await broadcast({"type": "test"})
            ws1.send_text.assert_called_once()
            ws2.send_text.assert_called_once()
            payload = json.loads(ws1.send_text.call_args[0][0])
            assert payload["type"] == "test"
        finally:
            ws_clients.clear()

    @pytest.mark.asyncio
    async def test_removes_dead_clients(self):
        live_ws = AsyncMock()
        dead_ws = AsyncMock()
        dead_ws.send_text.side_effect = Exception("connection closed")
        ws_clients.clear()
        ws_clients.add(live_ws)
        ws_clients.add(dead_ws)
        try:
            await broadcast({"type": "ping"})
            assert dead_ws not in ws_clients
            assert live_ws in ws_clients
        finally:
            ws_clients.clear()

    @pytest.mark.asyncio
    async def test_non_serializable_payload_skipped(self):
        """Payload that can't be JSON-serialized should not crash."""
        ws = AsyncMock()
        ws_clients.clear()
        ws_clients.add(ws)
        try:
            await broadcast({"bad": object()})
            ws.send_text.assert_not_called()
        finally:
            ws_clients.clear()


# ===========================================================================
# invalidate_pipeline_index
# ===========================================================================


class TestScenarioNormalization:
    """Tests for scenario normalization in parse_pipeline_to_graph."""

    def test_live_is_moved_to_first_position(self, tmp_path):
        """When sidecar has 'live' not in first position, it must be normalized to first."""
        from haute.routes._helpers import parse_pipeline_to_graph

        # Write a minimal pipeline file
        py_path = tmp_path / "pipeline.py"
        py_path.write_text(
            "import haute\n"
            "pipeline = haute.Pipeline('test')\n"
            "@pipeline.node\n"
            "def transform(df):\n"
            "    return df\n"
        )

        # Write sidecar with "live" NOT in first position
        sidecar = py_path.with_suffix(".haute.json")
        sidecar.write_text(json.dumps({
            "scenarios": ["test_batch", "live", "scenario_b"],
            "active_scenario": "live",
        }))

        graph = parse_pipeline_to_graph(py_path)

        assert graph.scenarios[0] == "live", (
            f"Expected 'live' first, got: {graph.scenarios}"
        )
        # All original scenarios must still be present
        assert set(graph.scenarios) == {"live", "test_batch", "scenario_b"}
        # No duplicates
        assert len(graph.scenarios) == 3


class TestInvalidatePipelineIndex:
    def test_clears_cache(self):
        """Calling invalidate should set module-level caches to None."""
        import haute.routes._helpers as helpers

        helpers._pipeline_index = {"old": Path("old.py")}
        helpers._module_deps = {"old": set()}
        invalidate_pipeline_index()
        assert helpers._pipeline_index is None
        assert helpers._module_deps is None
