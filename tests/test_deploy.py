"""Tests for the haute.deploy package.

Tests the target-agnostic layers (pruner, bundler, schema, scorer, validators,
config). MLflow-specific tests are integration-level and require mlflow installed.
"""

from __future__ import annotations

import json
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
        assert any("freq" in name for name in artifact_names), f"Expected freq model in {artifact_names}"
        assert any("sev" in name for name in artifact_names), f"Expected sev model in {artifact_names}"

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
