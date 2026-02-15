"""MLflow deployment target — log model + create Databricks serving endpoint."""

from __future__ import annotations

import getpass
import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from haute.deploy._config import ResolvedDeploy


@dataclass
class DeployResult:
    """Returned by deploy_to_mlflow()."""

    model_name: str
    model_version: int
    model_uri: str
    endpoint_url: str | None
    manifest_path: Path


def deploy_to_mlflow(resolved: ResolvedDeploy) -> DeployResult:
    """Deploy a resolved pipeline to MLflow + Databricks Model Serving.

    Steps:
        1. Build deployment manifest JSON
        2. Log HauteModel as mlflow.pyfunc with artifacts + signature
        3. Register model version in MLflow Model Registry
        4. Return DeployResult with model URI and endpoint URL

    Args:
        resolved: Fully resolved deployment config (from ``resolve_config()``).

    Returns:
        DeployResult with model URI, version, and endpoint URL.
    """
    import mlflow

    config = resolved.config
    model_name = config.model_name

    # 1. Build deployment manifest
    manifest = _build_manifest(resolved)

    # 2. Write manifest to a temp file
    with tempfile.TemporaryDirectory() as tmpdir:
        manifest_path = Path(tmpdir) / "deploy_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))

        # 3. Build artifact dict for mlflow.pyfunc.log_model
        artifacts: dict[str, str] = {
            "deploy_manifest": str(manifest_path),
        }
        for artifact_name, artifact_path in resolved.artifacts.items():
            artifacts[artifact_name] = str(artifact_path)

        # 4. Build MLflow model signature
        signature = _build_signature(resolved)

        # 5. Set experiment if configured
        experiment_name = config.databricks.experiment_name
        mlflow.set_experiment(experiment_name)

        # 6. Log the model
        with mlflow.start_run(run_name=f"deploy-{model_name}"):
            mlflow.log_dict(manifest, "deploy_manifest.json")

            mlflow.pyfunc.log_model(
                artifact_path="model",
                python_model=_get_model_instance(),
                artifacts=artifacts,
                signature=signature,
                pip_requirements=_pip_requirements(resolved),
                registered_model_name=model_name,
            )

        # 7. Get the registered model version
        client = mlflow.tracking.MlflowClient()
        versions = client.search_model_versions(f"name='{model_name}'")
        if versions:
            latest_version = max(v.version for v in versions)
        else:
            latest_version = 1

        model_uri = f"models:/{model_name}/{latest_version}"

    return DeployResult(
        model_name=model_name,
        model_version=int(latest_version),
        model_uri=model_uri,
        endpoint_url=None,
        manifest_path=manifest_path,
    )


def get_deploy_status(model_name: str) -> dict[str, str | int]:
    """Query MLflow Model Registry for current model versions and status.

    Returns:
        Dict with keys: model_name, latest_version, latest_stage, status.
    """
    import mlflow

    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")

    if not versions:
        return {
            "model_name": model_name,
            "latest_version": 0,
            "status": "not_found",
        }

    latest = max(versions, key=lambda v: int(v.version))
    return {
        "model_name": model_name,
        "latest_version": int(latest.version),
        "latest_stage": getattr(latest, "current_stage", "None"),
        "status": latest.status,
        "run_id": latest.run_id,
    }


def _get_model_instance() -> object:
    """Import and return a HauteModel instance.

    Deferred import to avoid loading mlflow at module level.
    """
    from haute.deploy._model import HauteModel
    return HauteModel()


def _build_manifest(resolved: ResolvedDeploy) -> dict:
    """Build the deployment manifest dict."""
    import haute

    config = resolved.config
    return {
        "haute_version": haute.__version__,
        "pipeline_name": resolved.pruned_graph.get("pipeline_name", config.model_name),
        "pipeline_file": str(config.pipeline_file),
        "created_at": datetime.now(UTC).isoformat(),
        "created_by": _get_user(),
        "input_nodes": resolved.input_node_ids,
        "output_node": resolved.output_node_id,
        "output_fields": config.output_fields,
        "input_schema": resolved.input_schema,
        "output_schema": resolved.output_schema,
        "artifacts": {
            name: str(path) for name, path in resolved.artifacts.items()
        },
        "graph": resolved.pruned_graph,
        "nodes_deployed": len(resolved.pruned_graph.get("nodes", [])),
        "nodes_skipped": len(resolved.removed_node_ids),
        "nodes_skipped_names": resolved.removed_node_ids,
    }


def _build_signature(resolved: ResolvedDeploy) -> object:
    """Build an MLflow ModelSignature from resolved schemas."""
    from mlflow.models import ModelSignature
    from mlflow.types import ColSpec, DataType, Schema

    dtype_map = {
        "Int8": DataType.integer,
        "Int16": DataType.integer,
        "Int32": DataType.integer,
        "Int64": DataType.long,
        "UInt8": DataType.integer,
        "UInt16": DataType.integer,
        "UInt32": DataType.long,
        "UInt64": DataType.long,
        "Float32": DataType.float,
        "Float64": DataType.double,
        "String": DataType.string,
        "Utf8": DataType.string,
        "Boolean": DataType.boolean,
        "Date": DataType.datetime,
        "Datetime": DataType.datetime,
    }

    def _to_colspecs(schema: dict[str, str]) -> list[ColSpec]:
        specs = []
        for col_name, dtype_str in schema.items():
            # Handle parameterized types like Datetime('us', 'UTC')
            base_type = dtype_str.split("(")[0] if "(" in dtype_str else dtype_str
            mlflow_type = dtype_map.get(base_type, DataType.string)
            specs.append(ColSpec(type=mlflow_type, name=col_name))
        return specs

    input_schema = Schema(_to_colspecs(resolved.input_schema))
    output_schema = Schema(_to_colspecs(resolved.output_schema))
    return ModelSignature(inputs=input_schema, outputs=output_schema)


def _pip_requirements(resolved: ResolvedDeploy) -> list[str]:
    """Build pip requirements for the deployed model."""
    import haute

    reqs = [
        f"haute=={haute.__version__}",
        "polars>=1.0.0",
    ]

    # Check if catboost is used
    for node in resolved.pruned_graph.get("nodes", []):
        config = node.get("data", {}).get("config", {})
        if config.get("fileType") == "catboost":
            reqs.append("catboost>=1.2.8")
            break

    return reqs


def _get_user() -> str:
    """Get the current user's name."""
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        return "unknown"
