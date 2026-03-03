"""Tests for haute.deploy._utils — get_user, get_haute_version, build_manifest."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from haute.deploy._utils import build_manifest, get_haute_version, get_user

# ---------------------------------------------------------------------------
# Helpers — minimal stubs for ResolvedDeploy and its dependencies
# ---------------------------------------------------------------------------


def _make_resolved(
    *,
    pipeline_name: str | None = "my_pipeline",
    model_name: str = "my_model",
    pipeline_file: Path = Path("/repo/main.py"),
    target: str = "databricks",
    output_fields: list[str] | None = None,
    input_node_ids: list[str] | None = None,
    output_node_id: str = "sink_1",
    artifacts: dict[str, Path] | None = None,
    input_schema: dict[str, str] | None = None,
    output_schema: dict[str, str] | None = None,
    removed_node_ids: list[str] | None = None,
    nodes_count: int = 3,
) -> MagicMock:
    """Build a minimal mock ResolvedDeploy with sensible defaults."""
    config = MagicMock()
    config.model_name = model_name
    config.pipeline_file = pipeline_file
    config.target = target
    config.output_fields = output_fields or ["premium"]

    pruned_graph = MagicMock()
    pruned_graph.pipeline_name = pipeline_name
    pruned_graph.nodes = [MagicMock() for _ in range(nodes_count)]
    pruned_graph.model_dump.return_value = {
        "nodes": [{"id": f"n{i}"} for i in range(nodes_count)],
        "edges": [],
        "pipeline_name": pipeline_name,
    }

    resolved = MagicMock()
    resolved.config = config
    resolved.pruned_graph = pruned_graph
    resolved.input_node_ids = input_node_ids or ["api_input_1"]
    resolved.output_node_id = output_node_id
    resolved.artifacts = artifacts if artifacts is not None else {
        "model.pkl": Path("/repo/artifacts/model.pkl"),
        "scaler.pkl": Path("/repo/artifacts/scaler.pkl"),
    }
    resolved.input_schema = input_schema or {"age": "int", "postcode": "str"}
    resolved.output_schema = output_schema or {"premium": "float"}
    resolved.removed_node_ids = (
        removed_node_ids if removed_node_ids is not None
        else ["train_node", "eval_node"]
    )
    return resolved


# ---------------------------------------------------------------------------
# get_user()
# ---------------------------------------------------------------------------


class TestGetUser:
    """Tests for get_user()."""

    def test_returns_string(self) -> None:
        result = get_user()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_fallback_on_key_error(self) -> None:
        with patch("haute.deploy._utils.getpass.getuser", side_effect=KeyError("no user")):
            assert get_user() == "unknown"

    def test_fallback_on_os_error(self) -> None:
        with patch("haute.deploy._utils.getpass.getuser", side_effect=OSError("no tty")):
            assert get_user() == "unknown"

    def test_returns_actual_user_when_available(self) -> None:
        with patch("haute.deploy._utils.getpass.getuser", return_value="alice"):
            assert get_user() == "alice"


# ---------------------------------------------------------------------------
# get_haute_version()
# ---------------------------------------------------------------------------


class TestGetHauteVersion:
    """Tests for get_haute_version()."""

    def test_returns_string(self) -> None:
        result = get_haute_version()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_fallback_on_package_not_found(self) -> None:
        from importlib.metadata import PackageNotFoundError

        with patch(
            "importlib.metadata.version",
            side_effect=PackageNotFoundError("haute"),
        ):
            assert get_haute_version() == "0.0.0-dev"

    def test_returns_version_when_installed(self) -> None:
        with patch("importlib.metadata.version", return_value="1.2.3"):
            assert get_haute_version() == "1.2.3"


# ---------------------------------------------------------------------------
# build_manifest()
# ---------------------------------------------------------------------------

_EXPECTED_KEYS = {
    "haute_version",
    "pipeline_name",
    "pipeline_file",
    "target",
    "created_at",
    "created_by",
    "input_node_ids",
    "output_node_id",
    "output_fields",
    "input_schema",
    "output_schema",
    "artifacts",
    "pruned_graph",
    "nodes_deployed",
    "nodes_skipped",
    "nodes_skipped_names",
}


class TestBuildManifest:
    """Tests for build_manifest()."""

    def test_all_keys_present(self) -> None:
        resolved = _make_resolved()
        manifest = build_manifest(resolved)
        assert set(manifest.keys()) == _EXPECTED_KEYS

    def test_exactly_16_keys(self) -> None:
        resolved = _make_resolved()
        manifest = build_manifest(resolved)
        assert len(manifest) == 16

    def test_pipeline_name_from_pruned_graph(self) -> None:
        resolved = _make_resolved(pipeline_name="graph_pipeline")
        manifest = build_manifest(resolved)
        assert manifest["pipeline_name"] == "graph_pipeline"

    def test_pipeline_name_falls_back_to_model_name(self) -> None:
        resolved = _make_resolved(pipeline_name=None, model_name="fallback_model")
        manifest = build_manifest(resolved)
        assert manifest["pipeline_name"] == "fallback_model"

    def test_pipeline_name_falls_back_when_empty_string(self) -> None:
        """Empty string is falsy, so it should also fall back to model_name."""
        resolved = _make_resolved(pipeline_name="", model_name="fallback_model")
        manifest = build_manifest(resolved)
        assert manifest["pipeline_name"] == "fallback_model"

    def test_pipeline_file_is_stringified(self) -> None:
        resolved = _make_resolved(pipeline_file=Path("/repo/pricing/main.py"))
        manifest = build_manifest(resolved)
        assert manifest["pipeline_file"] == "/repo/pricing/main.py"
        assert isinstance(manifest["pipeline_file"], str)

    def test_target_propagated(self) -> None:
        resolved = _make_resolved(target="container")
        manifest = build_manifest(resolved)
        assert manifest["target"] == "container"

    @patch("haute.deploy._utils.datetime")
    @patch("haute.deploy._utils.get_user", return_value="test_user")
    @patch("haute.deploy._utils.get_haute_version", return_value="2.0.0")
    def test_created_at_frozen(
        self,
        _mock_version: MagicMock,
        _mock_user: MagicMock,
        mock_dt: MagicMock,
    ) -> None:
        frozen = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
        mock_dt.now.return_value = frozen
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        resolved = _make_resolved()
        manifest = build_manifest(resolved)

        assert manifest["created_at"] == frozen.isoformat()
        assert manifest["created_by"] == "test_user"
        assert manifest["haute_version"] == "2.0.0"

    def test_created_at_is_iso_format(self) -> None:
        resolved = _make_resolved()
        manifest = build_manifest(resolved)
        # Should parse without error as ISO 8601
        parsed = datetime.fromisoformat(manifest["created_at"])
        assert parsed.tzinfo is not None, "created_at should be timezone-aware"

    def test_artifacts_are_stringified(self) -> None:
        artifacts = {
            "model.pkl": Path("/repo/artifacts/model.pkl"),
            "config.json": Path("/repo/config/config.json"),
        }
        resolved = _make_resolved(artifacts=artifacts)
        manifest = build_manifest(resolved)

        assert manifest["artifacts"] == {
            "model.pkl": "/repo/artifacts/model.pkl",
            "config.json": "/repo/config/config.json",
        }
        for value in manifest["artifacts"].values():
            assert isinstance(value, str)

    def test_empty_artifacts(self) -> None:
        resolved = _make_resolved(artifacts={})
        manifest = build_manifest(resolved)
        assert manifest["artifacts"] == {}

    def test_input_node_ids_propagated(self) -> None:
        resolved = _make_resolved(input_node_ids=["api_1", "api_2"])
        manifest = build_manifest(resolved)
        assert manifest["input_node_ids"] == ["api_1", "api_2"]

    def test_output_node_id_propagated(self) -> None:
        resolved = _make_resolved(output_node_id="output_sink")
        manifest = build_manifest(resolved)
        assert manifest["output_node_id"] == "output_sink"

    def test_schemas_propagated(self) -> None:
        resolved = _make_resolved(
            input_schema={"x": "float", "y": "int"},
            output_schema={"result": "float"},
        )
        manifest = build_manifest(resolved)
        assert manifest["input_schema"] == {"x": "float", "y": "int"}
        assert manifest["output_schema"] == {"result": "float"}

    def test_output_fields_propagated(self) -> None:
        resolved = _make_resolved(output_fields=["premium", "discount"])
        manifest = build_manifest(resolved)
        assert manifest["output_fields"] == ["premium", "discount"]

    def test_nodes_deployed_count(self) -> None:
        resolved = _make_resolved(nodes_count=5)
        manifest = build_manifest(resolved)
        assert manifest["nodes_deployed"] == 5

    def test_nodes_skipped_count_and_names(self) -> None:
        removed = ["train_node", "eval_node", "debug_node"]
        resolved = _make_resolved(removed_node_ids=removed)
        manifest = build_manifest(resolved)
        assert manifest["nodes_skipped"] == 3
        assert manifest["nodes_skipped_names"] == removed

    def test_no_removed_nodes(self) -> None:
        resolved = _make_resolved(removed_node_ids=[])
        manifest = build_manifest(resolved)
        assert manifest["nodes_skipped"] == 0
        assert manifest["nodes_skipped_names"] == []

    def test_pruned_graph_is_model_dump(self) -> None:
        resolved = _make_resolved(pipeline_name="test_pipe", nodes_count=2)
        manifest = build_manifest(resolved)
        expected = {
            "nodes": [{"id": "n0"}, {"id": "n1"}],
            "edges": [],
            "pipeline_name": "test_pipe",
        }
        assert manifest["pruned_graph"] == expected
        resolved.pruned_graph.model_dump.assert_called_once()
