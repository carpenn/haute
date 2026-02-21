"""Tests for the haute.deploy package.

Tests the target-agnostic layers (pruner, bundler, schema, scorer, validators,
config). MLflow-specific tests are integration-level and require mlflow installed.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from haute._types import PipelineGraph
from haute.parser import parse_pipeline_file

if TYPE_CHECKING:
    from haute.deploy._config import DeployConfig, ResolvedDeploy
from tests.conftest import make_graph as _g

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path("tests/fixtures")
PIPELINE_FILE = FIXTURE_DIR / "pipeline.py"
DATA_DIR = FIXTURE_DIR / "data"


@pytest.fixture()
def full_graph() -> PipelineGraph:
    """Parse the fixture pipeline into a PipelineGraph."""
    return parse_pipeline_file(PIPELINE_FILE)


@dataclass
class MLflowMocks:
    """Named references to every mock in the MLflow deploy patch block."""

    set_tracking_uri: MagicMock
    set_registry_uri: MagicMock
    set_experiment: MagicMock
    start_run: MagicMock
    log_dict: MagicMock
    log_model: MagicMock
    client: MagicMock
    check_connectivity: MagicMock
    build_signature: MagicMock
    create_or_update_endpoint: MagicMock


@contextmanager
def mock_mlflow_deploy():
    """Patch all 10 MLflow/deploy targets used by deploy_to_mlflow().

    Yields an MLflowMocks dataclass so callers can assert on specific mocks.
    """
    with (
        patch("mlflow.set_tracking_uri") as m_tracking,
        patch("mlflow.set_registry_uri") as m_registry,
        patch("mlflow.set_experiment") as m_experiment,
        patch("mlflow.start_run") as m_run,
        patch("mlflow.log_dict") as m_log_dict,
        patch("mlflow.pyfunc.log_model") as m_log_model,
        patch("mlflow.tracking.MlflowClient") as m_client,
        patch("haute.deploy._mlflow._check_databricks_connectivity") as m_conn,
        patch("haute.deploy._mlflow._build_signature") as m_sig,
        patch("haute.deploy._mlflow._create_or_update_serving_endpoint") as m_ep,
    ):
        m_client.return_value.search_model_versions.return_value = []
        m_run.return_value.__enter__ = MagicMock()
        m_run.return_value.__exit__ = MagicMock(return_value=False)

        yield MLflowMocks(
            set_tracking_uri=m_tracking,
            set_registry_uri=m_registry,
            set_experiment=m_experiment,
            start_run=m_run,
            log_dict=m_log_dict,
            log_model=m_log_model,
            client=m_client,
            check_connectivity=m_conn,
            build_signature=m_sig,
            create_or_update_endpoint=m_ep,
        )


def _make_resolved(
    config: DeployConfig | None = None, **overrides: object,
) -> ResolvedDeploy:
    """Build a lightweight ResolvedDeploy with sensible defaults.

    Accepts either a pre-built DeployConfig or keyword overrides applied to a
    minimal config.  All graph/schema fields default to empty so tests that
    only exercise the MLflow API layer don't need to build real graphs.
    """
    from haute.deploy._config import DeployConfig, ResolvedDeploy

    if config is None:
        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="test-model",
        )

    defaults = dict(
        config=config,
        full_graph=PipelineGraph(),
        pruned_graph=PipelineGraph(),
        input_node_ids=["policies"],
        output_node_id="output",
        artifacts={},
        input_schema={"col": "Int64"},
        output_schema={"col": "Int64"},
    )
    defaults.update(overrides)
    return ResolvedDeploy(**defaults)


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
        assert inputs == ["quotes"]

    def test_prune_for_deploy(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node, prune_for_deploy

        output_id = find_output_node(full_graph)
        pruned, kept, removed = prune_for_deploy(full_graph, output_id)

        kept_set = set(kept)
        removed_set = set(removed)

        # Live scoring path: quotes → policies (switch) → area_lookup → output
        assert "quotes" in kept_set
        assert "policies" in kept_set
        assert "output" in kept_set

        # Batch source pruned (liveSwitch keeps only live branch)
        assert "batch_quotes" in removed_set

        # Sink nodes should be pruned
        assert "results_write" in removed_set

        # Pruned graph should have fewer nodes
        assert len(pruned.nodes) < len(full_graph.nodes)
        assert len(pruned.nodes) == len(kept)

    def test_prune_drops_batch_branch_at_live_switch(self) -> None:
        """liveSwitch nodes should only keep the live (first) input branch."""
        from haute.deploy._pruner import prune_for_deploy

        graph = _g({
            "nodes": [
                {"id": "live_src", "data": {"label": "live_src", "nodeType": "apiInput", "config": {"path": "d.json"}}},
                {"id": "batch_src", "data": {"label": "batch_src", "nodeType": "dataSource", "config": {"path": "d.parquet"}}},
                {"id": "switch", "data": {"label": "switch", "nodeType": "liveSwitch", "config": {"mode": "live", "inputs": ["live_src", "batch_src"]}}},
                {"id": "score", "data": {"label": "score", "nodeType": "transform", "config": {}}},
                {"id": "out", "data": {"label": "out", "nodeType": "output", "config": {}}},
            ],
            "edges": [
                {"id": "e1", "source": "live_src", "target": "switch"},
                {"id": "e2", "source": "batch_src", "target": "switch"},
                {"id": "e3", "source": "switch", "target": "score"},
                {"id": "e4", "source": "score", "target": "out"},
            ],
        })
        pruned, kept, removed = prune_for_deploy(graph, "out")
        kept_set = set(kept)
        assert "live_src" in kept_set
        assert "switch" in kept_set
        assert "score" in kept_set
        assert "out" in kept_set
        assert "batch_src" in set(removed)

    def test_prune_missing_output_raises(self, full_graph: dict) -> None:
        from haute.deploy._pruner import prune_for_deploy

        with pytest.raises(ValueError, match="not found.*graph"):
            prune_for_deploy(full_graph, "nonexistent_node")

    def test_find_output_no_output_raises(self) -> None:
        from haute.deploy._pruner import find_output_node

        graph = _g({
            "nodes": [
                {"id": "a", "data": {"nodeType": "dataSource", "config": {}}},
            ]
        })
        with pytest.raises(ValueError, match="[Nn]o output node"):
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

        artifacts = collect_artifacts(pruned, ["quotes"], FIXTURE_DIR)

        # All artifact paths should exist
        for name, path in artifacts.items():
            assert path.is_file(), f"Artifact {name} not found at {path}"

    def test_missing_artifact_raises(self) -> None:
        from haute.deploy._bundler import collect_artifacts

        graph = _g({
            "nodes": [
                {
                    "id": "ext1",
                    "data": {
                        "nodeType": "externalFile",
                        "config": {"path": "nonexistent/model.pkl"},
                    },
                },
            ]
        })

        with pytest.raises(FileNotFoundError, match="[Aa]rtifact.*not found"):
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

        sample = pl.read_parquet(DATA_DIR / "policies.parquet", n_rows=1)
        result = score_graph(
            graph=pruned,
            input_df=sample,
            input_node_ids=["quotes"],
            output_node_id=output_id,
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1
        # The pipeline adds area_factor and premium columns
        assert "premium" in result.columns
        assert "area_factor" in result.columns
        # All output values should be non-null for valid input
        assert result["premium"].null_count() == 0

    def test_score_multiple_rows(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node, prune_for_deploy
        from haute.deploy._scorer import score_graph

        output_id = find_output_node(full_graph)
        pruned, _kept, _removed = prune_for_deploy(full_graph, output_id)

        sample = pl.read_parquet(DATA_DIR / "policies.parquet", n_rows=5)
        result = score_graph(
            graph=pruned,
            input_df=sample,
            input_node_ids=["quotes"],
            output_node_id=output_id,
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 5
        # Verify computed columns are present and populated
        assert "premium" in result.columns
        assert "area_factor" in result.columns
        assert result["premium"].null_count() == 0
        # Input columns should survive through to output
        assert "VehPower" in result.columns
        assert "Area" in result.columns


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------


class TestSchema:
    def test_infer_input_schema(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node, prune_for_deploy
        from haute.deploy._schema import infer_input_schema

        output_id = find_output_node(full_graph)
        pruned, _kept, _removed = prune_for_deploy(full_graph, output_id)

        schema = infer_input_schema(pruned, "quotes")

        assert isinstance(schema, dict)
        assert len(schema) > 0
        assert "VehPower" in schema
        assert "Area" in schema

    def test_infer_output_schema(self, full_graph: dict) -> None:
        from haute.deploy._pruner import find_output_node, prune_for_deploy
        from haute.deploy._schema import infer_output_schema

        output_id = find_output_node(full_graph)
        pruned, _kept, _removed = prune_for_deploy(full_graph, output_id)

        schema = infer_output_schema(pruned, output_id, ["quotes"])

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
        artifacts = collect_artifacts(pruned, ["quotes"], FIXTURE_DIR)
        input_schema = infer_input_schema(pruned, "quotes")
        output_schema = infer_output_schema(pruned, output_id, ["quotes"])

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="test-model",
        )
        resolved = ResolvedDeploy(
            config=config,
            full_graph=full_graph,
            pruned_graph=pruned,
            input_node_ids=["quotes"],
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
        artifacts = collect_artifacts(pruned, ["quotes"], FIXTURE_DIR)
        input_schema = infer_input_schema(pruned, "quotes")
        output_schema = infer_output_schema(pruned, output_id, ["quotes"])

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="test-model",
            test_quotes_dir=FIXTURE_DIR / "quotes",
        )
        resolved = ResolvedDeploy(
            config=config,
            full_graph=full_graph,
            pruned_graph=pruned,
            input_node_ids=["quotes"],
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
        """Integration test — resolves via haute.toml which references main.py."""
        from haute.deploy._config import DeployConfig, resolve_config

        config = DeployConfig.from_toml(Path("haute.toml"))
        resolved = resolve_config(config)

        assert resolved.output_node_id == "output"
        assert "quotes" in resolved.input_node_ids
        assert len(resolved.input_schema) > 0
        assert len(resolved.output_schema) > 0
        assert len(resolved.removed_node_ids) > 0


# ---------------------------------------------------------------------------
# Pure deploy logic — extracted functions
# ---------------------------------------------------------------------------


class TestBuildUcModelName:
    """Tests for the pure UC model name construction."""

    def test_default_config(self) -> None:
        from haute.deploy._config import DeployConfig
        from haute.deploy._mlflow import build_uc_model_name

        config = DeployConfig(pipeline_file=PIPELINE_FILE, model_name="my-model")
        # Default DatabricksConfig: catalog="main", schema="pricing"
        assert build_uc_model_name(config) == "main.pricing.my-model"

    def test_custom_catalog_schema(self) -> None:
        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import build_uc_model_name

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            databricks=DatabricksConfig(catalog="workspace", schema="pricing"),
        )
        assert build_uc_model_name(config) == "workspace.pricing.my-model"

    def test_suffix_appended(self) -> None:
        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import build_uc_model_name

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            endpoint_suffix="-staging",
            databricks=DatabricksConfig(catalog="ws", schema="default"),
        )
        assert build_uc_model_name(config) == "ws.default.my-model-staging"

    def test_no_suffix(self) -> None:
        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import build_uc_model_name

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            databricks=DatabricksConfig(catalog="ws", schema="default"),
        )
        assert build_uc_model_name(config) == "ws.default.my-model"


class TestBuildExperimentName:
    """Tests for the pure experiment name construction."""

    def test_default_experiment(self) -> None:
        from haute.deploy._config import DeployConfig
        from haute.deploy._mlflow import build_experiment_name

        config = DeployConfig(pipeline_file=PIPELINE_FILE, model_name="my-model")
        assert build_experiment_name(config) == config.databricks.experiment_name

    def test_suffix_appended(self) -> None:
        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import build_experiment_name

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            endpoint_suffix="-staging",
            databricks=DatabricksConfig(experiment_name="/Shared/haute/test"),
        )
        assert build_experiment_name(config) == "/Shared/haute/test-staging"

    def test_no_suffix(self) -> None:
        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import build_experiment_name

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            databricks=DatabricksConfig(experiment_name="/Shared/haute/test"),
        )
        assert build_experiment_name(config) == "/Shared/haute/test"


# ---------------------------------------------------------------------------
# Parser apiInput round-trip test
# ---------------------------------------------------------------------------


class TestDatabricksTracking:
    """Regression tests: deploy must target Databricks, not local MLflow."""

    def test_deploy_sets_tracking_uri(self) -> None:
        """deploy_to_mlflow() must call mlflow.set_tracking_uri('databricks')."""
        from haute.deploy._mlflow import DeployResult, deploy_to_mlflow

        resolved = _make_resolved()

        with mock_mlflow_deploy() as mocks:
            result = deploy_to_mlflow(resolved)

            mocks.set_tracking_uri.assert_called_once_with("databricks")
            mocks.set_registry_uri.assert_called_once_with("databricks-uc")
            # Behavioral: result is a well-formed DeployResult
            assert isinstance(result, DeployResult)
            assert result.model_name == "test-model"
            assert result.model_version >= 1
            assert result.manifest_path.name == "deploy_manifest.json"

    def test_deploy_uses_uc_model_name(self) -> None:
        """Model must be registered with catalog.schema.model_name format."""
        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import deploy_to_mlflow

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            databricks=DatabricksConfig(catalog="workspace", schema="default"),
        )
        resolved = _make_resolved(config)

        with mock_mlflow_deploy() as mocks:
            result = deploy_to_mlflow(resolved)

            # Verify the UC three-level namespace was used in log_model
            log_call = mocks.log_model.call_args
            assert log_call.kwargs["registered_model_name"] == "workspace.default.my-model"

            # Verify the model URI uses the UC name
            assert "workspace.default.my-model" in result.model_uri

    def test_experiment_name_includes_suffix_for_staging(self) -> None:
        """Staging deploys must use a suffixed experiment and model name."""
        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import deploy_to_mlflow

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="test-model",
            endpoint_suffix="-staging",
            databricks=DatabricksConfig(
                experiment_name="/Shared/haute/test",
                catalog="workspace",
                schema="default",
            ),
        )
        resolved = _make_resolved(config)

        with mock_mlflow_deploy() as mocks:
            deploy_to_mlflow(resolved)

            mocks.set_experiment.assert_called_once_with("/Shared/haute/test-staging")
            # Model must also be registered with the suffix
            log_call = mocks.log_model.call_args
            assert (
                log_call.kwargs["registered_model_name"]
                == "workspace.default.test-model-staging"
            )


class TestServingEndpoint:
    """Regression tests: deploy must create/update the Databricks serving endpoint."""

    def test_deploy_calls_create_or_update_endpoint(self) -> None:
        """deploy_to_mlflow() must call _create_or_update_serving_endpoint."""
        from haute.deploy._config import DatabricksConfig, DeployConfig
        from haute.deploy._mlflow import deploy_to_mlflow

        config = DeployConfig(
            pipeline_file=PIPELINE_FILE,
            model_name="my-model",
            endpoint_name="my-endpoint",
            databricks=DatabricksConfig(catalog="ws", schema="default"),
        )
        resolved = _make_resolved(config)

        with mock_mlflow_deploy() as mocks:
            mocks.create_or_update_endpoint.return_value = (
                "https://host/serving-endpoints/my-endpoint/invocations"
            )

            result = deploy_to_mlflow(resolved)

            mocks.create_or_update_endpoint.assert_called_once_with(
                config=config,
                uc_model_name="ws.default.my-model",
                model_version=1,
            )
            assert result.endpoint_url == "https://host/serving-endpoints/my-endpoint/invocations"

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

        with (
            patch("databricks.sdk.WorkspaceClient") as mock_ws_cls,
            patch.dict("os.environ", {
                "DATABRICKS_RATING_HOST": "https://myhost",
                "DATABRICKS_RATING_TOKEN": "test-token",
            }),
        ):
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

        with (
            patch("databricks.sdk.WorkspaceClient") as mock_ws_cls,
            patch.dict("os.environ", {
                "DATABRICKS_RATING_HOST": "https://myhost",
                "DATABRICKS_RATING_TOKEN": "test-token",
            }),
        ):
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
        from haute.deploy._mlflow import deploy_to_mlflow

        resolved = _make_resolved()

        with mock_mlflow_deploy() as mocks:
            deploy_to_mlflow(resolved)

            log_call = mocks.log_model.call_args
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


class TestParserApiInput:
    def test_api_input_node_type(self) -> None:
        """api_input=True in decorator should produce apiInput nodeType."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        quotes_node = None
        for n in graph.nodes:
            if n.id == "quotes":
                quotes_node = n
                break

        assert quotes_node is not None, "quotes node not found"
        assert quotes_node.data.nodeType == "apiInput", (
            f"Expected apiInput nodeType, got: {quotes_node.data.nodeType}"
        )

    def test_live_switch_node_type(self) -> None:
        """live_switch=True in decorator should produce liveSwitch nodeType."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        policies_node = None
        for n in graph.nodes:
            if n.id == "policies":
                policies_node = n
                break

        assert policies_node is not None, "policies node not found"
        assert policies_node.data.nodeType == "liveSwitch", (
            f"Expected liveSwitch nodeType, got: {policies_node.data.nodeType}"
        )
        config = policies_node.data.config
        assert config["mode"] == "live"
        assert config["inputs"] == ["quotes", "batch_quotes"]
