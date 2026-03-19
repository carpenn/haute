"""Hatchling custom build hook — builds the frontend before packaging.

This runs automatically during ``hatch build``, ``uv build``, or
``pip install .`` so that ``src/haute/static/`` always contains the
latest compiled frontend assets.
"""

from __future__ import annotations

import os
import shutil
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
            self._run([self._npm(), "ci", "--prefer-offline"], cwd=frontend_dir)

        # Build frontend → src/haute/static/
        self._run([self._npm(), "run", "build"], cwd=frontend_dir)

        # Sanity check
        index_html = static_dir / "index.html"
        if not index_html.exists():
            msg = f"Frontend build did not produce {index_html}"
            raise RuntimeError(msg)

    @staticmethod
    def _npm() -> str:
        """Return the npm executable, resolving common Windows install paths.

        Duplicates logic from ``haute.cli._helpers._npm`` because this build
        hook runs outside the installed package (hatchling context).
        """
        found = shutil.which("npm")
        if found:
            return found
        if sys.platform == "win32":
            candidate = Path(r"C:\Program Files\nodejs\npm.cmd")
            if candidate.exists():
                return str(candidate)
        msg = "npm not found on PATH. Install Node.js from https://nodejs.org"
        raise RuntimeError(msg)

    @staticmethod
    def _node_env() -> dict[str, str] | None:
        """Return env with Node.js on PATH, or *None* if already available."""
        if shutil.which("node"):
            return None
        if sys.platform == "win32":
            nodejs_dir = Path(r"C:\Program Files\nodejs")
            if (nodejs_dir / "node.exe").exists():
                env = os.environ.copy()
                env["PATH"] = f"{nodejs_dir};{env.get('PATH', '')}"
                return env
        return None

    def _run(self, cmd: list[str], cwd: Path) -> None:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            env=self._node_env(),
        )
        if result.returncode != 0:
            print(result.stdout, file=sys.stderr)
            print(result.stderr, file=sys.stderr)
            msg = f"Command failed: {' '.join(cmd)}"
            raise RuntimeError(msg)
