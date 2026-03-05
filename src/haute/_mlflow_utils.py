"""Shared MLflow helpers used by _mlflow_io, _optimiser_io, and routes/mlflow.

Eliminates duplication of:
  - ``resolve_version()``: resolve "latest" to a concrete version number
  - ``search_versions()``: safely quote model name and search
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mlflow.entities.model_registry import ModelVersion
    from mlflow.tracking import MlflowClient


def search_versions(
    client: MlflowClient,
    model_name: str,
) -> list[ModelVersion]:
    """Search model versions, safely quoting the model name."""
    safe_name = model_name.replace("'", "\\'")
    return client.search_model_versions(f"name='{safe_name}'")  # type: ignore[return-value]


def resolve_version(
    client: Any,
    model_name: str,
    version: str,
) -> str:
    """Resolve ``"latest"`` or empty version to a concrete version number.

    Raises:
        ValueError: if no versions are found for the model.
    """
    if version and version != "latest":
        return version

    versions = search_versions(client, model_name)
    if not versions:
        raise ValueError(
            f"No versions found for registered model '{model_name}'."
        )
    sorted_versions = sorted(versions, key=lambda v: int(v.version), reverse=True)
    return sorted_versions[0].version
