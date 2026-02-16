"""Pipeline file discovery — shared by CLI and server."""

from __future__ import annotations

from pathlib import Path

_SKIP = {"__init__.py", "setup.py", "conftest.py"}


def discover_pipelines(root: Path | None = None) -> list[Path]:
    """Find ``.py`` files in *root* that contain ``haute.Pipeline``.

    Parameters
    ----------
    root:
        Directory to scan.  Defaults to ``Path.cwd()``.

    Returns
    -------
    list[Path]
        Sorted list of matching files.
    """
    if root is None:
        root = Path.cwd()

    found: list[Path] = []
    for f in sorted(root.glob("*.py")):
        if f.name in _SKIP:
            continue
        try:
            text = f.read_text(errors="replace")
        except OSError:
            continue
        if "haute.Pipeline" in text:
            found.append(f)

    return found
