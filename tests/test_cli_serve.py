"""Tests for haute.cli._serve — the ``haute serve`` command."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from haute.cli import cli


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


class TestServe:
    def test_prod_mode_no_static_fails(
        self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Prod mode without static dir should fail."""
        monkeypatch.chdir(tmp_path)

        with patch("haute.cli._serve._find_frontend_dir", return_value=None), \
             patch("haute.server.STATIC_DIR", tmp_path / "nonexistent"):
            result = runner.invoke(cli, ["serve", "--no-browser"])

        assert result.exit_code == 1
        assert "frontend" in result.output.lower() or "npm" in result.output.lower()

    def test_prod_mode_with_static_dir(
        self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Prod mode should serve from built static directory."""
        monkeypatch.chdir(tmp_path)
        static = tmp_path / "static"
        static.mkdir()

        with patch("haute.cli._serve._find_frontend_dir", return_value=None), \
             patch("haute.server.STATIC_DIR", static), \
             patch("uvicorn.run") as mock_run:
            result = runner.invoke(cli, ["serve", "--no-browser"])

        assert result.exit_code == 0, result.output
        mock_run.assert_called_once()

    def test_custom_host_and_port(
        self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        static = tmp_path / "static"
        static.mkdir()

        with patch("haute.cli._serve._find_frontend_dir", return_value=None), \
             patch("haute.server.STATIC_DIR", static), \
             patch("uvicorn.run") as mock_run:
            result = runner.invoke(
                cli, ["serve", "--no-browser", "--host", "0.0.0.0", "--port", "9000"],
            )

        assert result.exit_code == 0, result.output
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["host"] == "0.0.0.0"
        assert call_kwargs["port"] == 9000

    def test_dev_mode_starts_vite_and_uvicorn(
        self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Dev mode should start Vite subprocess + uvicorn."""
        monkeypatch.chdir(tmp_path)
        fe = tmp_path / "frontend"
        fe.mkdir()
        (fe / "package.json").write_text("{}")
        (fe / "node_modules").mkdir()

        mock_proc = MagicMock()

        with patch("haute.cli._serve._find_frontend_dir", return_value=fe), \
             patch("subprocess.Popen", return_value=mock_proc) as mock_popen, \
             patch("uvicorn.run"), \
             patch("signal.signal"):
            runner.invoke(cli, ["serve", "--no-browser"])

        mock_popen.assert_called_once()
        assert "npm" in mock_popen.call_args[0][0] or mock_popen.call_args[0][0][0] == "npm"
