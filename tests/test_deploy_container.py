"""Tests for haute.deploy._container — Docker build/push helpers.

Covers:
  - _check_docker_available: success and CalledProcessError
  - _docker_build: success (returncode 0) and failure (returncode != 0)
  - _git_sha_short: success and exception fallback
  - _update_service: NotImplementedError for unsupported platform

All subprocess calls are mocked — no Docker or git required.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestCheckDockerAvailable:
    """Tests for _check_docker_available()."""

    def test_success_when_docker_is_present(self) -> None:
        """No exception when 'docker info' succeeds."""
        from haute.deploy._container import _check_docker_available

        with patch("haute.deploy._container.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            # Should not raise
            _check_docker_available()

        mock_run.assert_called_once_with(
            ["docker", "info"],
            check=True,
            capture_output=True,
        )

    def test_raises_runtime_error_when_docker_missing(self) -> None:
        """RuntimeError when 'docker info' raises CalledProcessError."""
        from haute.deploy._container import _check_docker_available

        with patch("haute.deploy._container.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "docker info")

            with pytest.raises(RuntimeError, match="Docker is not available"):
                _check_docker_available()


class TestDockerBuild:
    """Tests for _docker_build()."""

    def test_success_when_returncode_zero(self, tmp_path: Path) -> None:
        """No exception when docker build returns 0."""
        from haute.deploy._container import _docker_build

        with patch("haute.deploy._container.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="")

            # Should not raise
            _docker_build(tmp_path, "test-image:latest")

        mock_run.assert_called_once_with(
            ["docker", "build", "-t", "test-image:latest", "."],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )

    def test_raises_runtime_error_on_failure(self, tmp_path: Path) -> None:
        """RuntimeError when docker build returns non-zero."""
        from haute.deploy._container import _docker_build

        with patch("haute.deploy._container.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stderr="ERROR: Dockerfile parse error",
            )

            with pytest.raises(RuntimeError, match="Docker build failed"):
                _docker_build(tmp_path, "test-image:latest")


class TestGitShaShort:
    """Tests for _git_sha_short()."""

    def test_returns_sha_on_success(self) -> None:
        """Returns the trimmed stdout when git succeeds."""
        from haute.deploy._container import _git_sha_short

        with patch("haute.deploy._container.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="abc1234\n")

            result = _git_sha_short()

        assert result == "abc1234"

    def test_returns_local_on_exception(self) -> None:
        """Falls back to 'local' when git is unavailable or not a repo."""
        from haute.deploy._container import _git_sha_short

        with patch("haute.deploy._container.subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError("git not found")

            result = _git_sha_short()

        assert result == "local"


class TestUpdateService:
    """Tests for _update_service()."""

    def test_raises_not_implemented_for_unsupported_platform(self) -> None:
        """Any platform target should raise NotImplementedError (not yet built)."""
        from haute.deploy._container import _update_service

        with pytest.raises(NotImplementedError, match="not yet implemented"):
            _update_service(
                target="azure-container-apps",
                image_tag="myregistry/model:abc1234",
                resolved=MagicMock(),
            )
