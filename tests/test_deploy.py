"""Tests for the haute.deploy package.

Tests the target-agnostic layers (pruner, bundler, schema, scorer, validators,
config). MLflow-specific tests are integration-level and require mlflow installed.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from haute.parser import parse_pipeline_file

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PIPELINE_FILE = Path("main.py")


@pytest.fixture()
def full_graph() -> dict:
    """Parse the example pipeline into a React Flow graph."""
    return parse_pipeline_file(PIPELINE_FILE)


# ---------------------------------------------------------------------------
# Pruner tests
# ---------------------------------------------------------------------------


class TestPruner:
    def test_find_output_node(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node

        output_id = find_output_node(full_graph)
        assert output_id == "output"

    def test_find_deploy_input_nodes(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_deploy_input_nodes

        inputs = find_deploy_input_nodes(full_graph)
        assert inputs == ["policies"]

    def test_prune_for_deploy(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node, prune_for_deploy

        output_id = find_output_node(full_graph)
        pruned, kept, removed = prune_for_deploy(full_graph, output_id)

        kept_set = set(kept)
        removed_set = set(removed)

        # policies, frequency_model, severity_model, calculate_premium, output
        assert "policies" in kept_set
        assert "output" in kept_set
        assert "frequency_model" in kept_set
        assert "severity_model" in kept_set
        assert "calculate_premium" in kept_set

        # Training data nodes should be pruned
        assert "claims" in removed_set
        assert "exposure" in removed_set

        # Sink nodes should be pruned
        assert "frequency_write" in removed_set
        assert "severity_write" in removed_set

        # Pruned graph should have fewer nodes
        assert len(pruned["nodes"]) < len(full_graph["nodes"])
        assert len(pruned["nodes"]) == len(kept)

    def test_prune_missing_output_raises(self, full_graph: dict) -> None:
        from haute.deploy._pruner import prune_for_deploy

        with pytest.raises(ValueError, match="not found in graph"):
            prune_for_deploy(full_graph, "nonexistent_node")

    def test_find_output_no_output_raises(self) -> None:
        from haute.deploy._pruner import find_output_node

        graph = {"nodes": [
            {"id": "a", "data": {"nodeType": "dataSource", "config": {}}},
        ]}
        with pytest.raises(ValueError, match="No output node"):
            find_output_node(graph)


# ---------------------------------------------------------------------------
# Bundler tests
# ---------------------------------------------------------------------------


class TestBundler:
    def test_collect_artifacts(self, full_graph: dict) -> None:
        from haute.deploy._bundler import collect_artifacts
        from haute.deploy._pruner import find_output_node, prune_for_deploy

        output_id = find_output_node(full_graph)
        pruned, _kept, _removed = prune_for_deploy(full_graph, output_id)

        artifacts = collect_artifacts(pruned, ["policies"], PIPELINE_FILE.parent)

        # Should find the catboost model files
        artifact_names = set(artifacts.keys())
        assert any("freq" in name for name in artifact_names), (
            f"Expected freq model in {artifact_names}"
        )
        assert any("sev" in name for name in artifact_names), (
            f"Expected sev model in {artifact_names}"
        )

        # All artifact paths should exist
        for name, path in artifacts.items():
            assert path.is_file(), f"Artifact {name} not found at {path}"

    def test_missing_artifact_raises(self) -> None:
        from haute.deploy._bundler import collect_artifacts

        graph = {"nodes": [
            {
                "id": "ext1",
                "data": {
                    "nodeType": "externalFile",
                    "config": {"path": "nonexistent/model.pkl"},
                },
            },
        ]}

        with pytest.raises(FileNotFoundError, match="Artifact not found"):
            collect_artifacts(graph, [], Path("."))


# ---------------------------------------------------------------------------
# Scorer tests
# ---------------------------------------------------------------------------


class TestScorer:
    def test_score_single_row(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node, prune_for_deploy
        from haute.deploy._scorer import score_graph

        output_id = find_output_node(full_graph)
        pruned, _kept, _removed = prune_for_deploy(full_graph, output_id)

        sample = pl.read_parquet("data/policies.parquet", n_rows=1)
        result = score_graph(
            graph=pruned,
            input_df=sample,
            input_node_ids=["policies"],
            output_node_id=output_id,
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) >= 1
        assert len(result.columns) > 0

    def test_score_multiple_rows(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node, prune_for_deploy
        from haute.deploy._scorer import score_graph

        output_id = find_output_node(full_graph)
        pruned, _kept, _removed = prune_for_deploy(full_graph, output_id)

        sample = pl.read_parquet("data/policies.parquet", n_rows=5)
        result = score_graph(
            graph=pruned,
            input_df=sample,
            input_node_ids=["policies"],
            output_node_id=output_id,
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 5


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------


class TestSchema:
    def test_infer_input_schema(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node, prune_for_deploy
        from haute.deploy._schema import infer_input_schema

        output_id = find_output_node(full_graph)
        pruned, _kept, _removed = prune_for_deploy(full_graph, output_id)

        schema = infer_input_schema(pruned, "policies")

        assert isinstance(schema, dict)
        assert len(schema) > 0
        assert "VehPower" in schema
        assert "Area" in schema

    def test_infer_output_schema(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node, prune_for_deploy
        from haute.deploy._schema import infer_output_schema

        output_id = find_output_node(full_graph)
        pruned, _kept, _removed = prune_for_deploy(full_graph, output_id)

        schema = infer_output_schema(pruned, output_id, ["policies"])

        assert isinstance(schema, dict)
        assert len(schema) > 0


# ---------------------------------------------------------------------------
# Validator tests
# ---------------------------------------------------------------------------


class TestValidators:
    def test_validate_passes_for_good_config(self, full_graph: dict) -> None:
        from haute.deploy._bundler import collect_artifacts
        from haute.deploy._config import DeployConfig, ResolvedDeploy
        from haute.deploy._pruner import find_output_node, prune_for_deploy
        from haute.deploy._schema import infer_input_schema, infer_output_schema
        from haute.deploy._validators import validate_deploy

        output_id = find_output_node(full_graph)
        pruned, _kept, removed = prune_for_deploy(full_graph, output_id)
        artifacts = collect_artifacts(pruned, ["policies"], PIPELINE_FILE.parent)
        input_schema = infer_input_schema(pruned, "policies")
        output_schema = infer_output_schema(pruned, output_id, ["policies"])

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="test-model",
        )
        resolved = ResolvedDeploy(
            config=config,
            full_graph=full_graph,
            pruned_graph=pruned,
            input_node_ids=["policies"],
            output_node_id=output_id,
            artifacts=artifacts,
            input_schema=input_schema,
            output_schema=output_schema,
            removed_node_ids=removed,
        )

        errors = validate_deploy(resolved)
        assert errors == [], f"Unexpected validation errors: {errors}"

    def test_score_test_quotes(self, full_graph: dict) -> None:
        from haute.deploy._bundler import collect_artifacts
        from haute.deploy._config import DeployConfig, ResolvedDeploy
        from haute.deploy._pruner import find_output_node, prune_for_deploy
        from haute.deploy._schema import infer_input_schema, infer_output_schema
        from haute.deploy._validators import score_test_quotes

        output_id = find_output_node(full_graph)
        pruned, _kept, removed = prune_for_deploy(full_graph, output_id)
        artifacts = collect_artifacts(pruned, ["policies"], PIPELINE_FILE.parent)
        input_schema = infer_input_schema(pruned, "policies")
        output_schema = infer_output_schema(pruned, output_id, ["policies"])

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="test-model",
            test_quotes_dir=Path("test_quotes"),
        )
        resolved = ResolvedDeploy(
            config=config,
            full_graph=full_graph,
            pruned_graph=pruned,
            input_node_ids=["policies"],
            output_node_id=output_id,
            artifacts=artifacts,
            input_schema=input_schema,
            output_schema=output_schema,
            removed_node_ids=removed,
        )

        results = score_test_quotes(resolved)
        assert len(results) > 0, "Expected at least one test quote file"
        for r in results:
            assert r["status"] == "ok", f"Test quote {r['file']} failed: {r['error']}"
            assert r["rows"] > 0


# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------


class TestConfig:
    def test_from_toml(self) -> None:
        from haute.deploy._config import DeployConfig

        config = DeployConfig.from_toml(Path("haute.toml"))
        assert config.model_name == "motor-pricing"
        assert config.pipeline_file == Path("main.py")
        assert config.databricks.experiment_name == "/Shared/haute/motor-pricing"
        assert config.databricks.serving_workload_size == "Small"

    def test_override(self) -> None:
        from haute.deploy._config import DeployConfig

        config = DeployConfig.from_toml(Path("haute.toml"))
        overridden = config.override(model_name="custom-name")
        assert overridden.model_name == "custom-name"
        assert config.model_name == "motor-pricing"  # original unchanged

    def test_resolve_config(self) -> None:
        from haute.deploy._config import DeployConfig, resolve_config

        config = DeployConfig.from_toml(Path("haute.toml"))
        resolved = resolve_config(config)

        assert resolved.output_node_id == "output"
        assert "policies" in resolved.input_node_ids
        assert len(resolved.input_schema) > 0
        assert len(resolved.output_schema) > 0
        assert len(resolved.artifacts) > 0
        assert len(resolved.removed_node_ids) > 0


# ---------------------------------------------------------------------------
# Parser deploy_input round-trip test
# ---------------------------------------------------------------------------


class TestDatabricksTracking:
    """Regression tests: deploy must target Databricks, not local MLflow."""

    def test_deploy_sets_tracking_uri(self) -> None:
        """deploy_to_mlflow() must call mlflow.set_tracking_uri('databricks')."""
        from unittest.mock import MagicMock, patch

        from haute.deploy._config import DeployConfig, ResolvedDeploy
        from haute.deploy._mlflow import deploy_to_mlflow

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="test-model",
        )
        resolved = ResolvedDeploy(
            config=config,
            full_graph={"nodes": [], "edges": []},
            pruned_graph={"nodes": [], "edges": []},
            input_node_ids=["policies"],
            output_node_id="output",
            artifacts={},
            input_schema={"col": "Int64"},
            output_schema={"col": "Int64"},
        )

        with patch("mlflow.set_tracking_uri") as mock_set_tracking, \
             patch("mlflow.set_registry_uri") as mock_set_registry, \
             patch("mlflow.set_experiment"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_dict"), \
             patch("mlflow.pyfunc.log_model"), \
             patch("mlflow.tracking.MlflowClient") as mock_client, \
             patch("haute.deploy._mlflow._ensure_experiment_directory"), \
             patch("haute.deploy._mlflow._build_signature"), \
             patch("haute.deploy._mlflow._create_or_update_serving_endpoint"):
            mock_client.return_value.search_model_versions.return_value = []
            mock_run.return_value.__enter__ = MagicMock()
            mock_run.return_value.__exit__ = MagicMock(return_value=False)

            deploy_to_mlflow(resolved)

            mock_set_tracking.assert_called_once_with("databricks")
            mock_set_registry.assert_called_once_with("databricks-uc")

    def test_deploy_uses_uc_model_name(self) -> None:
        """Model must be registered with catalog.schema.model_name format."""
        from unittest.mock import MagicMock, patch

        from haute.deploy._config import DatabricksConfig, DeployConfig, ResolvedDeploy
        from haute.deploy._mlflow import deploy_to_mlflow

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            databricks=DatabricksConfig(catalog="workspace", schema="default"),
        )
        resolved = ResolvedDeploy(
            config=config,
            full_graph={"nodes": [], "edges": []},
            pruned_graph={"nodes": [], "edges": []},
            input_node_ids=["policies"],
            output_node_id="output",
            artifacts={},
            input_schema={"col": "Int64"},
            output_schema={"col": "Int64"},
        )

        with patch("mlflow.set_tracking_uri"), \
             patch("mlflow.set_registry_uri"), \
             patch("mlflow.set_experiment"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_dict"), \
             patch("mlflow.pyfunc.log_model") as mock_log_model, \
             patch("mlflow.tracking.MlflowClient") as mock_client, \
             patch("haute.deploy._mlflow._ensure_experiment_directory"), \
             patch("haute.deploy._mlflow._build_signature"), \
             patch("haute.deploy._mlflow._create_or_update_serving_endpoint"):
            mock_client.return_value.search_model_versions.return_value = []
            mock_run.return_value.__enter__ = MagicMock()
            mock_run.return_value.__exit__ = MagicMock(return_value=False)

            result = deploy_to_mlflow(resolved)

            # Verify the UC three-level namespace was used in log_model
            log_call = mock_log_model.call_args
            assert log_call.kwargs["registered_model_name"] == "workspace.default.my-model"

            # Verify the model URI uses the UC name
            assert "workspace.default.my-model" in result.model_uri

    def test_ensure_experiment_directory_called(self) -> None:
        """Experiment parent directories must be created before set_experiment."""
        from unittest.mock import MagicMock, patch

        from haute.deploy._config import DatabricksConfig, DeployConfig, ResolvedDeploy
        from haute.deploy._mlflow import deploy_to_mlflow

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="test-model",
            databricks=DatabricksConfig(experiment_name="/Shared/haute/test"),
        )
        resolved = ResolvedDeploy(
            config=config,
            full_graph={"nodes": [], "edges": []},
            pruned_graph={"nodes": [], "edges": []},
            input_node_ids=["policies"],
            output_node_id="output",
            artifacts={},
            input_schema={"col": "Int64"},
            output_schema={"col": "Int64"},
        )

        with patch("mlflow.set_tracking_uri"), \
             patch("mlflow.set_registry_uri"), \
             patch("mlflow.set_experiment"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_dict"), \
             patch("mlflow.pyfunc.log_model"), \
             patch("mlflow.tracking.MlflowClient") as mock_client, \
             patch("haute.deploy._mlflow._ensure_experiment_directory") as mock_ensure, \
             patch("haute.deploy._mlflow._build_signature"), \
             patch("haute.deploy._mlflow._create_or_update_serving_endpoint"):
            mock_client.return_value.search_model_versions.return_value = []
            mock_run.return_value.__enter__ = MagicMock()
            mock_run.return_value.__exit__ = MagicMock(return_value=False)

            deploy_to_mlflow(resolved)

            mock_ensure.assert_called_once_with("/Shared/haute/test")

    def test_ensure_experiment_directory_skips_top_level(self) -> None:
        """Top-level experiment paths (parent is '/') should not trigger mkdirs."""
        from unittest.mock import patch

        from haute.deploy._mlflow import _ensure_experiment_directory

        with patch("databricks.sdk.WorkspaceClient") as mock_ws_cls:
            _ensure_experiment_directory("/top_level_experiment")
            mock_ws_cls.return_value.workspace.mkdirs.assert_not_called()

    def test_ensure_experiment_directory_creates_parent(self) -> None:
        """Nested experiment paths should trigger mkdirs on the parent."""
        from unittest.mock import patch

        from haute.deploy._mlflow import _ensure_experiment_directory

        with patch("databricks.sdk.WorkspaceClient") as mock_ws_cls:
            _ensure_experiment_directory("/Shared/haute/my-experiment")
            mock_ws_cls.return_value.workspace.mkdirs.assert_called_once_with(
                "/Shared/haute"
            )


class TestServingEndpoint:
    """Regression tests: deploy must create/update the Databricks serving endpoint."""

    def test_deploy_calls_create_or_update_endpoint(self) -> None:
        """deploy_to_mlflow() must call _create_or_update_serving_endpoint."""
        from unittest.mock import MagicMock, patch

        from haute.deploy._config import DatabricksConfig, DeployConfig, ResolvedDeploy
        from haute.deploy._mlflow import deploy_to_mlflow

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            endpoint_name="my-endpoint",
            databricks=DatabricksConfig(catalog="ws", schema="default"),
        )
        resolved = ResolvedDeploy(
            config=config,
            full_graph={"nodes": [], "edges": []},
            pruned_graph={"nodes": [], "edges": []},
            input_node_ids=["policies"],
            output_node_id="output",
            artifacts={},
            input_schema={"col": "Int64"},
            output_schema={"col": "Int64"},
        )

        with patch("mlflow.set_tracking_uri"), \
             patch("mlflow.set_registry_uri"), \
             patch("mlflow.set_experiment"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_dict"), \
             patch("mlflow.pyfunc.log_model"), \
             patch("mlflow.tracking.MlflowClient") as mock_client, \
             patch("haute.deploy._mlflow._ensure_experiment_directory"), \
             patch("haute.deploy._mlflow._build_signature"), \
             patch("haute.deploy._mlflow._create_or_update_serving_endpoint") as mock_ep:
            mock_client.return_value.search_model_versions.return_value = []
            mock_run.return_value.__enter__ = MagicMock()
            mock_run.return_value.__exit__ = MagicMock(return_value=False)
            mock_ep.return_value = "https://host/serving-endpoints/my-endpoint/invocations"

            result = deploy_to_mlflow(resolved)

            mock_ep.assert_called_once_with(
                config=config,
                uc_model_name="ws.default.my-model",
                model_version=1,
            )
            assert result.endpoint_url == (
                "https://host/serving-endpoints/my-endpoint/invocations"
            )

    def test_endpoint_returns_none_when_no_endpoint_name(self) -> None:
        """If endpoint_name is not set, _create_or_update_serving_endpoint returns None."""
        from haute.deploy._config import DeployConfig
        from haute.deploy._mlflow import _create_or_update_serving_endpoint

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            endpoint_name=None,
        )
        result = _create_or_update_serving_endpoint(
            config=config, uc_model_name="main.pricing.my-model", model_version=1
        )
        assert result is None

    def test_endpoint_creates_new_endpoint(self) -> None:
        """When endpoint doesn't exist, it should be created."""
        from unittest.mock import patch

        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import _create_or_update_serving_endpoint

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            endpoint_name="my-endpoint",
            databricks=DatabricksConfig(
                serving_workload_size="Small",
                serving_scale_to_zero=True,
            ),
        )

        with patch("databricks.sdk.WorkspaceClient") as mock_ws_cls, \
             patch.dict("os.environ", {"DATABRICKS_HOST": "https://myhost"}):
            mock_ws = mock_ws_cls.return_value
            from databricks.sdk.errors import NotFound
            mock_ws.serving_endpoints.get.side_effect = NotFound("not found")

            url = _create_or_update_serving_endpoint(
                config=config,
                uc_model_name="main.pricing.my-model",
                model_version=2,
            )

            mock_ws.serving_endpoints.create.assert_called_once()
            assert url == "https://myhost/serving-endpoints/my-endpoint/invocations"

    def test_endpoint_updates_existing_endpoint(self) -> None:
        """When endpoint already exists, it should update the config."""
        from unittest.mock import MagicMock, patch

        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import _create_or_update_serving_endpoint

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            endpoint_name="my-endpoint",
            databricks=DatabricksConfig(
                serving_workload_size="Medium",
                serving_scale_to_zero=False,
            ),
        )

        with patch("databricks.sdk.WorkspaceClient") as mock_ws_cls, \
             patch.dict("os.environ", {"DATABRICKS_HOST": "https://myhost"}):
            mock_ws = mock_ws_cls.return_value
            mock_ws.serving_endpoints.get.return_value = MagicMock()

            url = _create_or_update_serving_endpoint(
                config=config,
                uc_model_name="main.pricing.my-model",
                model_version=3,
            )

            mock_ws.serving_endpoints.update_config.assert_called_once()
            mock_ws.serving_endpoints.create.assert_not_called()
            assert url == "https://myhost/serving-endpoints/my-endpoint/invocations"


class TestModelsFromCode:
    """Regression tests: deploy must use models-from-code, not CloudPickle."""

    def test_model_code_path_is_string(self) -> None:
        """_MODEL_CODE_PATH must be a str file path, not a Python object."""
        from haute.deploy._mlflow import _MODEL_CODE_PATH

        assert isinstance(_MODEL_CODE_PATH, str)
        assert _MODEL_CODE_PATH.endswith("_model_code.py")

    def test_model_code_file_exists(self) -> None:
        """The models-from-code script must exist on disk."""
        from pathlib import Path

        from haute.deploy._mlflow import _MODEL_CODE_PATH

        assert Path(_MODEL_CODE_PATH).is_file()

    def test_model_code_contains_set_model(self) -> None:
        """The script must call set_model() so MLflow can discover the model."""
        from pathlib import Path

        from haute.deploy._mlflow import _MODEL_CODE_PATH

        source = Path(_MODEL_CODE_PATH).read_text()
        assert "set_model(" in source

    def test_log_model_receives_file_path(self) -> None:
        """log_model(python_model=...) must receive a file path, not an object."""
        from unittest.mock import MagicMock, patch

        from haute.deploy._config import DeployConfig, ResolvedDeploy
        from haute.deploy._mlflow import deploy_to_mlflow

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="test-model",
        )
        resolved = ResolvedDeploy(
            config=config,
            full_graph={"nodes": [], "edges": []},
            pruned_graph={"nodes": [], "edges": []},
            input_node_ids=["policies"],
            output_node_id="output",
            artifacts={},
            input_schema={"col": "Int64"},
            output_schema={"col": "Int64"},
        )

        with patch("mlflow.set_tracking_uri"), \
             patch("mlflow.set_registry_uri"), \
             patch("mlflow.set_experiment"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_dict"), \
             patch("mlflow.pyfunc.log_model") as mock_log, \
             patch("mlflow.tracking.MlflowClient") as mock_client, \
             patch("haute.deploy._mlflow._ensure_experiment_directory"), \
             patch("haute.deploy._mlflow._build_signature"), \
             patch("haute.deploy._mlflow._create_or_update_serving_endpoint"):
            mock_client.return_value.search_model_versions.return_value = []
            mock_run.return_value.__enter__ = MagicMock()
            mock_run.return_value.__exit__ = MagicMock(return_value=False)

            deploy_to_mlflow(resolved)

            log_call = mock_log.call_args
            python_model_arg = log_call.kwargs["python_model"]
            assert isinstance(python_model_arg, str), (
                f"Expected file path (str), got {type(python_model_arg)}"
            )
            assert python_model_arg.endswith("_model_code.py")


class TestHauteModel:
    def test_inherits_from_python_model(self) -> None:
        """HauteModel must inherit from mlflow.pyfunc.PythonModel (MLflow 3.x)."""
        from mlflow.pyfunc import PythonModel

        from haute.deploy._model_code import HauteModel

        assert issubclass(HauteModel, PythonModel)


class TestParserDeployInput:
    def test_deploy_input_preserved_in_config(self) -> None:
        """deploy_input=True in decorator should appear in parsed config."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        policies_node = None
        for n in graph["nodes"]:
            if n["id"] == "policies":
                policies_node = n
                break

        assert policies_node is not None, "policies node not found"
        config = policies_node["data"]["config"]
        assert config.get("deploy_input") is True, (
            f"deploy_input not in config: {config}"
        )
