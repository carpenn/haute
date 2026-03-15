"""Shared git helpers for test_git.py and test_git_routes.py."""

from __future__ import annotations

import subprocess
from pathlib import Path


def git_run(cwd: Path, *args: str) -> str:
    """Run a raw git command for test setup."""
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def init_repo(path: Path, *, user: str = "Test User") -> Path:
    """Create a fresh git repo with an initial commit on 'main'."""
    git_run(path, "init", "-b", "main")
    git_run(path, "config", "user.name", user)
    git_run(path, "config", "user.email", "test@example.com")
    (path / "README.md").write_text("# Test\n")
    git_run(path, "add", ".")
    git_run(path, "commit", "-m", "Initial commit")
    return path
