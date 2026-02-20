"""Security sandbox for user-code execution and file deserialization.

Provides:
- ``SAFE_GLOBALS`` — restricted builtins for ``exec()`` that block
  dangerous operations (``__import__``, ``open``, ``eval``, etc.)
  while preserving everything Polars transform code needs.
- ``safe_unpickle(path)`` — a ``RestrictedUnpickler`` that only allows
  known-safe classes (numpy, sklearn, catboost, etc.).
- ``validate_project_path(path)`` — ensures a path resolves inside the
  project root directory, preventing directory-traversal attacks.
"""

from __future__ import annotations

import builtins
import pickle
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Project-root path validation
# ---------------------------------------------------------------------------

_PROJECT_ROOT: Path | None = None


def _get_project_root() -> Path:
    """Return the cached project root (cwd at import time)."""
    global _PROJECT_ROOT
    if _PROJECT_ROOT is None:
        _PROJECT_ROOT = Path.cwd().resolve()
    return _PROJECT_ROOT


def set_project_root(root: Path) -> None:
    """Override the project root (used by tests and CLI)."""
    global _PROJECT_ROOT
    _PROJECT_ROOT = root.resolve()


def validate_project_path(path: str | Path) -> Path:
    """Resolve *path* and verify it is inside the project root.

    Raises:
        ValueError: If the path escapes the project directory.
    """
    resolved = Path(path).resolve()
    root = _get_project_root()
    if not resolved.is_relative_to(root):
        raise ValueError(
            f"Path '{path}' resolves to '{resolved}' which is outside "
            f"the project root '{root}'"
        )
    return resolved


# ---------------------------------------------------------------------------
# Restricted builtins for exec()
# ---------------------------------------------------------------------------

# Builtins that allow arbitrary code execution or system access.
_BLOCKED_BUILTINS = frozenset({
    "__import__",
    "breakpoint",
    "compile",
    "eval",
    "exec",
    "globals",
    "locals",
    "open",
    "input",
    "memoryview",
})

_SAFE_BUILTINS: dict[str, Any] = {
    name: getattr(builtins, name)
    for name in dir(builtins)
    if not name.startswith("_") and name not in _BLOCKED_BUILTINS
}
# Keep __builtins__ itself pointing to the restricted set so nested
# lookups (e.g. list comprehensions) work correctly.
_SAFE_BUILTINS["__builtins__"] = _SAFE_BUILTINS


def safe_globals(**extra: Any) -> dict[str, Any]:
    """Build a restricted global namespace for ``exec()``.

    Includes safe builtins + any extra bindings (e.g. ``pl=polars``).
    Blocks ``__import__``, ``open``, ``eval``, ``exec``, ``compile``,
    ``breakpoint``, ``globals``, ``locals``, and ``input``.
    """
    ns = dict(_SAFE_BUILTINS)
    ns.update(extra)
    return ns


# ---------------------------------------------------------------------------
# Restricted unpickler
# ---------------------------------------------------------------------------

# Allowlisted (module, qualname) prefixes for pickle deserialization.
# These cover the common model/data types used in pricing pipelines.
_ALLOWED_PICKLE_PREFIXES: list[tuple[str, ...]] = [
    ("numpy",),
    ("sklearn",),
    ("scipy",),
    ("catboost",),
    ("xgboost",),
    ("lightgbm",),
    ("pandas",),
    ("polars",),
    ("collections",),
    ("builtins",),
    ("_codecs",),
    ("copyreg",),
    ("datetime",),
]


class _RestrictedUnpickler(pickle.Unpickler):
    """Unpickler that only allows known-safe classes.

    Prevents arbitrary code execution via crafted pickle payloads
    while still supporting common ML model and data formats.
    """

    def find_class(self, module: str, name: str) -> Any:
        for prefix in _ALLOWED_PICKLE_PREFIXES:
            if module.startswith(prefix[0]):
                return super().find_class(module, name)
        raise pickle.UnpicklingError(
            f"Blocked unpickling of {module}.{name} — "
            f"class not in the allowlist. If this is a legitimate model "
            f"class, add its module to _ALLOWED_PICKLE_PREFIXES in "
            f"src/haute/_sandbox.py"
        )


def safe_unpickle(path: str | Path) -> Any:
    """Deserialize a pickle file using the restricted unpickler.

    Also validates the path is within the project root.
    """
    validated = validate_project_path(path)
    with open(validated, "rb") as f:
        return _RestrictedUnpickler(f).load()
