"""Tests for DeployConfig - new [safety], [ci] sections, env overrides, endpoint suffix."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from haute.deploy._config import CIConfig, DeployConfig, SafetyConfig


@pytest.fixture()
def toml_file(tmp_path: Path) -> Path:
    """Write a haute.toml with all sections and return the path."""
    content = """\
[project]
name = "motor-pricing"
pipeline = "main.py"

[deploy]
target = "databricks"
model_name = "motor-pricing"
endpoint_name = "motor-pricing"

[deploy.databricks]
experiment_name = "/Shared/haute/motor-pricing"
catalog = "main"
schema = "pricing"
serving_workload_size = "Small"
serving_scale_to_zero = true

[test_quotes]
dir = "tests/quotes"

[safety]
impact_dataset = "data/portfolio.parquet"
max_single_quote_change_pct = 20.0
max_avg_change_pct = 5.0
block_on_threshold_breach = true

[safety.approval]
min_approvers = 3

[ci]
provider = "github"

[ci.staging]
endpoint_suffix = "-stg"

[ci.production]
require_approval = true
min_approvers = 4
"""
    p = tmp_path / "haute.toml"
    p.write_text(content)
    return p


class TestFromToml:
    def test_loads_safety_section(self, toml_file: Path) -> None:
        config = DeployConfig.from_toml(toml_file)
        assert config.safety.impact_dataset == "data/portfolio.parquet"
        assert config.safety.max_single_quote_change_pct == 20.0
        assert config.safety.max_avg_change_pct == 5.0
        assert config.safety.block_on_threshold_breach is True
        assert config.safety.min_approvers == 3

    def test_loads_ci_section(self, toml_file: Path) -> None:
        config = DeployConfig.from_toml(toml_file)
        assert config.ci.provider == "github"
        assert config.ci.staging_endpoint_suffix == "-stg"
        assert config.ci.production_require_approval is True
        assert config.ci.production_min_approvers == 4

    def test_loads_target(self, toml_file: Path) -> None:
        config = DeployConfig.from_toml(toml_file)
        assert config.target == "databricks"

    def test_defaults_when_sections_missing(self, tmp_path: Path) -> None:
        content = """\
[project]
name = "simple"
pipeline = "main.py"

[deploy]
model_name = "simple"
"""
        p = tmp_path / "haute.toml"
        p.write_text(content)
        config = DeployConfig.from_toml(p)
        assert isinstance(config.safety, SafetyConfig)
        assert isinstance(config.ci, CIConfig)
        assert config.safety.min_approvers == 2
        assert config.ci.provider == "github"


class TestEffectiveEndpointName:
    def test_no_suffix(self) -> None:
        config = DeployConfig(
            pipeline_file=Path("main.py"),
            model_name="motor",
            endpoint_name="motor",
        )
        assert config.effective_endpoint_name == "motor"

    def test_with_suffix(self) -> None:
        config = DeployConfig(
            pipeline_file=Path("main.py"),
            model_name="motor",
            endpoint_name="motor",
            endpoint_suffix="-staging",
        )
        assert config.effective_endpoint_name == "motor-staging"

    def test_none_when_no_name_and_no_suffix(self) -> None:
        config = DeployConfig(
            pipeline_file=Path("main.py"),
            model_name="motor",
            endpoint_name=None,
        )
        assert config.effective_endpoint_name is None

    def test_suffix_falls_back_to_model_name(self) -> None:
        config = DeployConfig(
            pipeline_file=Path("main.py"),
            model_name="motor",
            endpoint_name=None,
            endpoint_suffix="-staging",
        )
        assert config.effective_endpoint_name == "motor-staging"


class TestEnvOverrides:
    def test_haute_model_name_override(
        self, toml_file: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("HAUTE_MODEL_NAME", "overridden-name")
        config = DeployConfig.from_toml(toml_file)
        assert config.model_name == "overridden-name"

    def test_haute_target_override(
        self, toml_file: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("HAUTE_TARGET", "docker")
        config = DeployConfig.from_toml(toml_file)
        assert config.target == "docker"

    def test_haute_nested_override(
        self, toml_file: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("HAUTE_SERVING_WORKLOAD_SIZE", "Large")
        config = DeployConfig.from_toml(toml_file)
        assert config.databricks.serving_workload_size == "Large"

    def test_haute_bool_override(
        self, toml_file: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("HAUTE_SERVING_SCALE_TO_ZERO", "false")
        config = DeployConfig.from_toml(toml_file)
        assert config.databricks.serving_scale_to_zero is False

    def test_no_env_no_override(self, toml_file: Path) -> None:
        # Ensure HAUTE_ vars are not set
        for key in ("HAUTE_MODEL_NAME", "HAUTE_TARGET"):
            os.environ.pop(key, None)
        config = DeployConfig.from_toml(toml_file)
        assert config.model_name == "motor-pricing"
        assert config.target == "databricks"
