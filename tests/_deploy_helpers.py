"""Shared factory for building ResolvedDeploy instances in deploy tests."""

from __future__ import annotations

from pathlib import Path

from haute.deploy._config import DeployConfig, ResolvedDeploy
from haute.graph_utils import PipelineGraph


FIXTURE_DIR = Path("tests/fixtures")
DEFAULT_PIPELINE_FILE = FIXTURE_DIR / "pipeline.py"

_SENTINEL = object()


def make_resolved_deploy(
    config: DeployConfig | None = None,
    **overrides: object,
) -> ResolvedDeploy:
    """Build a lightweight ResolvedDeploy with sensible defaults.

    Accepts either a pre-built DeployConfig or keyword overrides applied to a
    minimal config.  All graph/schema fields default to empty so tests that
    only exercise the MLflow API layer don't need to build real graphs.

    Supports all deploy test patterns:
    - ``make_resolved_deploy()`` — bare defaults
    - ``make_resolved_deploy(input_schema={...})`` — override specific fields
    - ``make_resolved_deploy(config=custom_cfg)`` — supply a pre-built config

    When ``config`` is None, DeployConfig fields can be passed as keyword
    arguments (``pipeline_file``, ``model_name``, ``target``, ``output_fields``,
    ``container``) and they'll be extracted from ``overrides`` before building
    the config.  All other kwargs become ResolvedDeploy field overrides.
    """
    if config is None:
        config_kwargs: dict[str, object] = {
            "pipeline_file": overrides.pop("pipeline_file", DEFAULT_PIPELINE_FILE),
            "model_name": overrides.pop("model_name", "test-model"),
            "target": overrides.pop("target", "databricks"),
        }
        # Only pass optional config fields when explicitly provided.
        output_fields = overrides.pop("output_fields", _SENTINEL)
        if output_fields is not _SENTINEL:
            config_kwargs["output_fields"] = output_fields
        container = overrides.pop("container", _SENTINEL)
        if container is not _SENTINEL:
            config_kwargs["container"] = container
        config = DeployConfig(**config_kwargs)

    defaults: dict[str, object] = {
        "config": config,
        "full_graph": PipelineGraph(),
        "pruned_graph": PipelineGraph(),
        "input_node_ids": ["policies"],
        "output_node_id": "output",
        "artifacts": {},
        "input_schema": {"col": "Int64"},
        "output_schema": {"col": "Int64"},
    }
    defaults.update(overrides)
    return ResolvedDeploy(**defaults)
