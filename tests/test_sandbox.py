"""Tests for the security sandbox (_sandbox.py)."""

from __future__ import annotations

import pickle
import tempfile
from pathlib import Path

import pytest

from haute._sandbox import (
    safe_globals,
    safe_unpickle,
    set_project_root,
    validate_project_path,
)


class TestSafeGlobals:
    """Verify restricted builtins block dangerous operations."""

    def test_polars_operations_work(self):
        """Normal Polars code should execute fine."""
        import polars as pl

        ns = safe_globals(pl=pl)
        local = {}
        exec("result = [1, 2, 3]", ns, local)
        assert local["result"] == [1, 2, 3]

    def test_builtins_available(self):
        """Common builtins like len, range, sorted, etc. should work."""
        ns = safe_globals()
        local = {}
        exec("result = len(sorted(range(5)))", ns, local)
        assert local["result"] == 5

    def test_list_comprehension_works(self):
        """List comprehensions need __builtins__ to resolve names."""
        ns = safe_globals()
        local = {}
        exec("result = [x * 2 for x in range(3)]", ns, local)
        assert local["result"] == [0, 2, 4]

    def test_import_blocked(self):
        """__import__ should be blocked."""
        ns = safe_globals()
        with pytest.raises(NameError):
            exec("__import__('os')", ns, {})

    def test_open_blocked(self):
        """open() should be blocked."""
        ns = safe_globals()
        with pytest.raises(NameError):
            exec("open('/etc/passwd')", ns, {})

    def test_eval_blocked(self):
        """eval() should be blocked."""
        ns = safe_globals()
        with pytest.raises(NameError):
            exec("eval('1+1')", ns, {})

    def test_exec_blocked(self):
        """Nested exec() should be blocked."""
        ns = safe_globals()
        with pytest.raises(NameError):
            exec("exec('x=1')", ns, {})

    def test_compile_blocked(self):
        """compile() should be blocked."""
        ns = safe_globals()
        with pytest.raises(NameError):
            exec("compile('x=1', '<str>', 'exec')", ns, {})

    def test_breakpoint_blocked(self):
        """breakpoint() should be blocked."""
        ns = safe_globals()
        with pytest.raises(NameError):
            exec("breakpoint()", ns, {})


class TestValidateProjectPath:
    """Verify path validation catches directory traversal."""

    def test_path_inside_root(self, tmp_path: Path):
        set_project_root(tmp_path)
        f = tmp_path / "data.parquet"
        f.touch()
        assert validate_project_path(str(f)) == f

    def test_path_outside_root_raises(self, tmp_path: Path):
        set_project_root(tmp_path / "subdir")
        with pytest.raises(ValueError, match="outside the project root"):
            validate_project_path("/etc/passwd")

    def test_traversal_attack_blocked(self, tmp_path: Path):
        set_project_root(tmp_path)
        with pytest.raises(ValueError, match="outside the project root"):
            validate_project_path(str(tmp_path / ".." / ".." / "etc" / "passwd"))


class TestSafeUnpickle:
    """Verify restricted unpickler blocks dangerous payloads."""

    def test_safe_object_loads(self, tmp_path: Path):
        """A plain dict should unpickle fine."""
        set_project_root(tmp_path)
        f = tmp_path / "safe.pkl"
        f.write_bytes(pickle.dumps({"key": "value", "nums": [1, 2, 3]}))
        result = safe_unpickle(str(f))
        assert result == {"key": "value", "nums": [1, 2, 3]}

    def test_os_system_blocked(self, tmp_path: Path):
        """A pickle payload calling os.system should be blocked."""
        set_project_root(tmp_path)
        f = tmp_path / "evil.pkl"
        # Properly crafted payload via __reduce__ → os.system("echo pwned")
        payload = (
            b"\x80\x04\x95%\x00\x00\x00\x00\x00\x00\x00"
            b"\x8c\x05posix\x94\x8c\x06system\x94\x93\x94"
            b"\x8c\necho pwned\x94\x85\x94R\x94."
        )
        f.write_bytes(payload)
        with pytest.raises(pickle.UnpicklingError, match="not in the allowlist"):
            safe_unpickle(str(f))

    def test_path_outside_root_blocked(self, tmp_path: Path):
        """Pickle loading should fail if path is outside root."""
        set_project_root(tmp_path / "safe_dir")
        f = tmp_path / "outside.pkl"
        f.write_bytes(pickle.dumps(42))
        with pytest.raises(ValueError, match="outside the project root"):
            safe_unpickle(str(f))
