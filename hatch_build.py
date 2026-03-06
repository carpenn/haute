"""Hatchling custom build hook — builds the frontend before packaging.

This runs automatically during ``hatch build``, ``uv build``, or
``pip install .`` so that ``src/haute/static/`` always contains the
latest compiled frontend assets.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class FrontendBuildHook(BuildHookInterface):
    PLUGIN_NAME = "frontend-build"

    def initialize(self, version: str, build_data: dict) -> None:  # noqa: ARG002
        frontend_dir = Path(self.root) / "frontend"
        if not frontend_dir.exists():
            # Source dist or CI without frontend — skip
            return

        static_dir = Path(self.root) / "src" / "haute" / "static"

        # Install deps if node_modules is missing
        node_modules = frontend_dir / "node_modules"
        if not node_modules.exists():
            self._run(["npm", "ci", "--prefer-offline"], cwd=frontend_dir)

        # Build frontend → src/haute/static/
        self._run(["npm", "run", "build"], cwd=frontend_dir)

        # Sanity check
        index_html = static_dir / "index.html"
        if not index_html.exists():
            msg = f"Frontend build did not produce {index_html}"
            raise RuntimeError(msg)

    def _run(self, cmd: list[str], cwd: Path) -> None:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(result.stdout, file=sys.stderr)
            print(result.stderr, file=sys.stderr)
            msg = f"Command failed: {' '.join(cmd)}"
            raise RuntimeError(msg)
