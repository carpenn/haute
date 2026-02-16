"""Deploy configuration — user input, TOML loading, and resolution."""

from __future__ import annotations

import copy
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from haute.deploy._pruner import (
    find_deploy_input_nodes,
    find_output_node,
    find_source_nodes,
    prune_for_deploy,
)


@dataclass
class DatabricksConfig:
    """Typed Databricks-specific settings from [deploy.databricks] in haute.toml."""

    experiment_name: str = "/Shared/haute/default"
    catalog: str = "main"
    schema: str = "pricing"
    serving_workload_size: str = "Small"
    serving_scale_to_zero: bool = True


@dataclass
class SafetyConfig:
    """Thresholds and gates from [safety] in haute.toml."""

    impact_dataset: str = ""
    max_single_quote_change_pct: float = 25.0
    max_avg_change_pct: float = 10.0
    block_on_threshold_breach: bool = True
    min_approvers: int = 2


@dataclass
class CIConfig:
    """CI/CD settings from [ci] in haute.toml."""

    provider: str = "github"
    staging_endpoint_suffix: str = "-staging"
    production_require_approval: bool = True
    production_min_approvers: int = 2


@dataclass
class DeployConfig:
    """User-provided deployment configuration (from haute.toml + CLI)."""

    pipeline_file: Path
    model_name: str
    target: str = "databricks"
    endpoint_name: str | None = None
    endpoint_suffix: str | None = None
    output_fields: list[str] | None = None
    test_quotes_dir: Path | None = None
    databricks: DatabricksConfig = field(default_factory=DatabricksConfig)
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    ci: CIConfig = field(default_factory=CIConfig)

    @property
    def effective_endpoint_name(self) -> str | None:
        """Endpoint name with optional suffix applied (e.g. for staging).

        Returns None when endpoint_name is unset and no suffix is given,
        which signals that no serving endpoint should be created.
        """
        if self.endpoint_name is None and self.endpoint_suffix is None:
            return None
        base = self.endpoint_name or self.model_name
        if self.endpoint_suffix:
            return base + self.endpoint_suffix
        return base

    @classmethod
    def from_toml(cls, path: Path) -> DeployConfig:
        """Load from haute.toml, merging [project], [deploy], [safety], [ci]."""
        import tomllib

        text = path.read_text()
        data = tomllib.loads(text)

        project = data.get("project", {})
        deploy = data.get("deploy", {})
        db_raw = deploy.get("databricks", {})
        tq = data.get("test_quotes", {})
        safety_raw = data.get("safety", {})
        approval_raw = safety_raw.get("approval", {})
        ci_raw = data.get("ci", {})
        ci_staging = ci_raw.get("staging", {})
        ci_prod = ci_raw.get("production", {})

        pipeline_file = Path(project.get("pipeline", "main.py"))
        model_name = deploy.get("model_name", project.get("name", pipeline_file.stem))
        endpoint_name = deploy.get("endpoint_name")
        target = deploy.get("target", "databricks")

        output_fields_raw = deploy.get("output_fields")
        output_fields = list(output_fields_raw) if output_fields_raw else None

        tq_dir = Path(tq["dir"]) if tq.get("dir") else None

        db_config = DatabricksConfig(
            experiment_name=db_raw.get("experiment_name", "/Shared/haute/default"),
            catalog=db_raw.get("catalog", "main"),
            schema=db_raw.get("schema", "pricing"),
            serving_workload_size=db_raw.get("serving_workload_size", "Small"),
            serving_scale_to_zero=db_raw.get("serving_scale_to_zero", True),
        )

        safety_config = SafetyConfig(
            impact_dataset=safety_raw.get("impact_dataset", ""),
            max_single_quote_change_pct=safety_raw.get("max_single_quote_change_pct", 25.0),
            max_avg_change_pct=safety_raw.get("max_avg_change_pct", 10.0),
            block_on_threshold_breach=safety_raw.get("block_on_threshold_breach", True),
            min_approvers=approval_raw.get("min_approvers", 2),
        )

        ci_config = CIConfig(
            provider=ci_raw.get("provider", "github"),
            staging_endpoint_suffix=ci_staging.get("endpoint_suffix", "-staging"),
            production_require_approval=ci_prod.get("require_approval", True),
            production_min_approvers=ci_prod.get("min_approvers", 2),
        )

        config = cls(
            pipeline_file=pipeline_file,
            model_name=model_name,
            target=target,
            endpoint_name=endpoint_name,
            output_fields=output_fields,
            test_quotes_dir=tq_dir,
            databricks=db_config,
            safety=safety_config,
            ci=ci_config,
        )

        return _apply_env_overrides(config)

    def override(self, **cli_kwargs: Any) -> DeployConfig:
        """Return a copy with non-None CLI flags applied over TOML values."""
        c = copy.copy(self)
        for key, val in cli_kwargs.items():
            if val is not None and hasattr(c, key):
                setattr(c, key, val)
        return c


def _apply_env_overrides(config: DeployConfig) -> DeployConfig:
    """Apply HAUTE_ prefixed env vars over TOML values.

    Resolution order (highest wins):
      1. CLI flags          (applied later via .override())
      2. Environment vars   HAUTE_MODEL_NAME=foo
      3. haute.toml         model_name = "foo"
    """
    env_map: dict[str, tuple[str, type]] = {
        "HAUTE_MODEL_NAME": ("model_name", str),
        "HAUTE_ENDPOINT_NAME": ("endpoint_name", str),
        "HAUTE_TARGET": ("target", str),
        "HAUTE_SERVING_WORKLOAD_SIZE": ("databricks.serving_workload_size", str),
        "HAUTE_SERVING_SCALE_TO_ZERO": ("databricks.serving_scale_to_zero", bool),
    }
    for env_key, (attr_path, attr_type) in env_map.items():
        val = os.environ.get(env_key)
        if val is None:
            continue
        if "." in attr_path:
            obj_name, field_name = attr_path.split(".", 1)
            obj = getattr(config, obj_name)
            if attr_type is bool:
                setattr(obj, field_name, val.lower() in ("true", "1", "yes"))
            else:
                setattr(obj, field_name, val)
        else:
            if attr_type is bool:
                setattr(config, attr_path, val.lower() in ("true", "1", "yes"))
            else:
                setattr(config, attr_path, val)
    return config


@dataclass
class ResolvedDeploy:
    """Computed state after resolving a DeployConfig against a parsed pipeline.

    Created by ``resolve_config()`` — never constructed directly.
    """

    config: DeployConfig
    full_graph: dict
    pruned_graph: dict
    input_node_ids: list[str]
    output_node_id: str
    artifacts: dict[str, Path]
    input_schema: dict[str, str]
    output_schema: dict[str, str]
    removed_node_ids: list[str] = field(default_factory=list)


def _load_env(project_root: Path) -> None:
    """Load .env file into os.environ if it exists (simple key=value parsing)."""
    env_path = project_root / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        os.environ.setdefault(key, value)


def resolve_config(config: DeployConfig) -> ResolvedDeploy:
    """Parse pipeline, prune graph, detect I/O nodes, collect artifacts, infer schemas.

    This is the main resolution function that converts user-provided
    ``DeployConfig`` into a fully resolved ``ResolvedDeploy`` ready for
    deployment.
    """
    from haute.deploy._bundler import collect_artifacts
    from haute.deploy._schema import infer_input_schema, infer_output_schema
    from haute.parser import parse_pipeline_file

    # Load .env for Databricks credentials
    # Pipeline is in project root by default; handle both root and subdir cases
    project_root = config.pipeline_file.resolve().parent
    _load_env(project_root)

    # Parse the pipeline
    full_graph = parse_pipeline_file(config.pipeline_file)
    if not full_graph.get("nodes"):
        raise ValueError(f"No nodes found in {config.pipeline_file}")

    # Find output node
    output_node_id = find_output_node(full_graph)

    # Prune to scoring path
    pruned_graph, _kept_ids, removed_ids = prune_for_deploy(full_graph, output_node_id)

    # Find input nodes
    deploy_inputs = find_deploy_input_nodes(pruned_graph)
    if not deploy_inputs:
        # Fallback: use the single source node in the pruned graph
        all_sources = find_source_nodes(pruned_graph)
        if len(all_sources) == 1:
            deploy_inputs = all_sources
        elif len(all_sources) == 0:
            raise ValueError("No source nodes in the pruned graph.")
        else:
            raise ValueError(
                f"Multiple source nodes in pruned graph ({all_sources}) but none "
                "marked deploy_input=True. Mark one with "
                "@pipeline.node(path=..., deploy_input=True)."
            )

    # Collect artifacts
    pipeline_dir = config.pipeline_file.parent
    artifacts = collect_artifacts(pruned_graph, deploy_inputs, pipeline_dir)

    # Infer schemas
    input_schema = infer_input_schema(pruned_graph, deploy_inputs[0])
    output_schema = infer_output_schema(pruned_graph, output_node_id, deploy_inputs)

    return ResolvedDeploy(
        config=config,
        full_graph=full_graph,
        pruned_graph=pruned_graph,
        input_node_ids=deploy_inputs,
        output_node_id=output_node_id,
        artifacts=artifacts,
        input_schema=input_schema,
        output_schema=output_schema,
        removed_node_ids=removed_ids,
    )
