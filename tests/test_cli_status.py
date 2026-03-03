"""Tests for haute.cli._status — the ``haute status`` command."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from haute.cli import cli


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def toml_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "haute.toml").write_text(
        '[project]\nname = "t"\npipeline = "main.py"\n'
        '[deploy]\nmodel_name = "motor-pricing"\n',
    )
    return tmp_path


class TestStatus:
    def test_no_model_name_loads_from_toml(
        self, runner: CliRunner, toml_project: Path,
    ) -> None:
        mock_info = {
            "model_name": "motor-pricing",
            "latest_version": 3,
            "latest_stage": "Production",
            "status": "READY",
            "run_id": "abc123",
        }
        with patch("haute.deploy._mlflow.get_deploy_status", return_value=mock_info):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0, result.output
        assert "motor-pricing" in result.output
        assert "3" in result.output
        assert "Production" in result.output

    def test_explicit_model_name(self, runner: CliRunner) -> None:
        mock_info = {
            "model_name": "custom-model",
            "latest_version": 1,
            "status": "READY",
        }
        with patch("haute.deploy._mlflow.get_deploy_status", return_value=mock_info):
            result = runner.invoke(cli, ["status", "custom-model"])
        assert result.exit_code == 0, result.output
        assert "custom-model" in result.output

    def test_version_only_flag(
        self, runner: CliRunner, toml_project: Path,
    ) -> None:
        mock_info = {"latest_version": 5}
        with patch("haute.deploy._mlflow.get_deploy_status", return_value=mock_info):
            result = runner.invoke(cli, ["status", "--version-only"])
        assert result.exit_code == 0
        # Output includes config loading message + version
        assert "5" in result.output.strip().splitlines()[-1]

    def test_model_not_found(
        self, runner: CliRunner, toml_project: Path,
    ) -> None:
        mock_info = {"status": "not_found"}
        with patch("haute.deploy._mlflow.get_deploy_status", return_value=mock_info):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "not found" in result.output.lower()

    def test_mlflow_import_error(
        self, runner: CliRunner, toml_project: Path,
    ) -> None:
        """Simulated ImportError when mlflow is missing."""
        with patch(
            "haute.deploy._mlflow.get_deploy_status",
            side_effect=ImportError("No module named 'mlflow'"),
        ):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 1
        assert "mlflow" in result.output.lower()

    def test_missing_optional_fields(
        self, runner: CliRunner, toml_project: Path,
    ) -> None:
        """Status display should handle missing optional keys gracefully."""
        mock_info = {
            "model_name": "motor-pricing",
            "latest_version": 1,
            "status": "READY",
            # no latest_stage, no run_id
        }
        with patch("haute.deploy._mlflow.get_deploy_status", return_value=mock_info):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0, result.output
        assert "N/A" in result.output
