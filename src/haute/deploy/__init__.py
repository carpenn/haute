"""Haute Deploy — package a pipeline as a live scoring API.

Public API::

    from haute.deploy import deploy, DeployConfig, DeployResult

    config = DeployConfig.from_toml(Path("haute.toml"))
    result = deploy(config)
    print(result.model_uri)
"""

from haute.deploy._config import DatabricksConfig, DeployConfig, ResolvedDeploy, resolve_config
from haute.deploy._mlflow import DeployResult, deploy_to_mlflow, get_deploy_status

__all__ = [
    "DatabricksConfig",
    "DeployConfig",
    "DeployResult",
    "ResolvedDeploy",
    "deploy",
    "deploy_to_mlflow",
    "get_deploy_status",
    "resolve_config",
]


def deploy(config: DeployConfig) -> DeployResult:
    """Resolve config and deploy to the configured target.

    Currently only supports ``target="databricks"`` (MLflow-backed).
    """
    resolved = resolve_config(config)
    return deploy_to_mlflow(resolved)
