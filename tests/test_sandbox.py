"""Tests for the security sandbox (_sandbox.py)."""

from __future__ import annotations

import pickle
from pathlib import Path

import pytest

from haute._sandbox import (
    UnsafeCodeError,
    safe_globals,
    safe_joblib_load,
    safe_unpickle,
    set_project_root,
    validate_project_path,
    validate_user_code,
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
        with pytest.raises(ValueError, match="outside.*project root"):
            validate_project_path("/etc/passwd")

    def test_traversal_attack_blocked(self, tmp_path: Path):
        set_project_root(tmp_path)
        with pytest.raises(ValueError, match="outside.*project root"):
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
        with pytest.raises(pickle.UnpicklingError, match="not in.*allowlist"):
            safe_unpickle(str(f))

    def test_path_outside_root_blocked(self, tmp_path: Path):
        """Pickle loading should fail if path is outside root."""
        set_project_root(tmp_path / "safe_dir")
        f = tmp_path / "outside.pkl"
        f.write_bytes(pickle.dumps(42))
        with pytest.raises(ValueError, match="outside.*project root"):
            safe_unpickle(str(f))


class TestSafeJoblibLoad:
    """Verify joblib loading goes through the restricted unpickler."""

    def test_safe_object_loads(self, tmp_path: Path):
        """A plain numpy array saved with joblib should load fine."""
        import joblib
        import numpy as np

        set_project_root(tmp_path)
        f = tmp_path / "safe.joblib"
        data = {"weights": np.array([1.0, 2.0, 3.0]), "bias": 0.5}
        joblib.dump(data, str(f))
        result = safe_joblib_load(str(f))
        assert result["bias"] == 0.5
        np.testing.assert_array_equal(result["weights"], [1.0, 2.0, 3.0])

    def test_safe_sklearn_model_loads(self, tmp_path: Path):
        """A sklearn model saved with joblib should load fine."""
        import joblib
        from sklearn.linear_model import LinearRegression

        set_project_root(tmp_path)
        f = tmp_path / "model.joblib"
        model = LinearRegression()
        joblib.dump(model, str(f))
        result = safe_joblib_load(str(f))
        assert isinstance(result, LinearRegression)

    def test_malicious_joblib_blocked(self, tmp_path: Path):
        """A joblib file containing os.system should be blocked."""
        import joblib

        set_project_root(tmp_path)
        f = tmp_path / "evil.joblib"

        # Create a malicious object that would exec on unpickle
        class _Evil:
            def __reduce__(self):
                import os
                return (os.system, ("echo pwned",))

        joblib.dump(_Evil(), str(f))
        with pytest.raises(pickle.UnpicklingError, match="not in.*allowlist"):
            safe_joblib_load(str(f))

    def test_subprocess_payload_blocked(self, tmp_path: Path):
        """A joblib file trying to use subprocess should be blocked."""
        import joblib

        set_project_root(tmp_path)
        f = tmp_path / "evil2.joblib"

        class _Evil:
            def __reduce__(self):
                import subprocess
                return (subprocess.call, (["echo", "pwned"],))

        joblib.dump(_Evil(), str(f))
        with pytest.raises(pickle.UnpicklingError, match="not in.*allowlist"):
            safe_joblib_load(str(f))

    def test_path_outside_root_blocked(self, tmp_path: Path):
        """Joblib loading should fail if path is outside root."""
        import joblib

        set_project_root(tmp_path / "safe_dir")
        f = tmp_path / "outside.joblib"
        joblib.dump(42, str(f))
        with pytest.raises(ValueError, match="outside.*project root"):
            safe_joblib_load(str(f))

    def test_restriction_does_not_leak_across_calls(self, tmp_path: Path):
        """Verify the monkey-patch is restored after safe_joblib_load."""
        import joblib
        from joblib.numpy_pickle import NumpyUnpickler

        original_find_class = NumpyUnpickler.find_class
        set_project_root(tmp_path)
        f = tmp_path / "test.joblib"
        joblib.dump([1, 2, 3], str(f))
        safe_joblib_load(str(f))
        # find_class should be restored to original after the call
        assert NumpyUnpickler.find_class is original_find_class

    def test_restriction_restored_on_error(self, tmp_path: Path):
        """Verify monkey-patch is restored even if loading fails."""
        import joblib
        from joblib.numpy_pickle import NumpyUnpickler

        original_find_class = NumpyUnpickler.find_class
        set_project_root(tmp_path)
        f = tmp_path / "evil.joblib"

        class _Evil:
            def __reduce__(self):
                import os
                return (os.system, ("echo pwned",))

        joblib.dump(_Evil(), str(f))
        with pytest.raises(pickle.UnpicklingError):
            safe_joblib_load(str(f))
        # find_class must be restored even after the error
        assert NumpyUnpickler.find_class is original_find_class


class TestValidateUserCode:
    """Verify AST-level code validation blocks sandbox escape vectors."""

    # ------- Legitimate Polars code should pass -------

    def test_polars_chain_passes(self):
        """Standard Polars method chain is allowed."""
        validate_user_code('.filter(pl.col("age") > 25).select("name", "age")')

    def test_polars_with_columns_passes(self):
        """with_columns expression is allowed."""
        validate_user_code(
            'df.with_columns(\n'
            '    premium=pl.col("base") * pl.col("factor")\n'
            ')'
        )

    def test_polars_join_passes(self):
        """join expression is allowed."""
        validate_user_code(
            'claims.join(exposure, on="IDpol", how="left")'
        )

    def test_assignment_passes(self):
        """Variable assignment is allowed."""
        validate_user_code('df = claims.filter(pl.col("amount") > 0)')

    def test_list_comprehension_passes(self):
        """List comprehensions are allowed."""
        validate_user_code('cols = [c for c in df.columns if c != "id"]')

    def test_f_string_passes(self):
        """f-strings are allowed."""
        validate_user_code('label = f"col_{i}"')

    def test_function_def_passes(self):
        """Regular (non-async) function definitions are allowed."""
        validate_user_code("def helper(x):\n    return x * 2")

    def test_lambda_passes(self):
        """Lambda expressions are allowed."""
        validate_user_code("fn = lambda x: x * 2")

    def test_safe_dunder_passes(self):
        """Dunders not in the block list are allowed (e.g. __name__)."""
        # __name__ is not in _BLOCKED_ATTRS — it's harmless
        validate_user_code('x = "hello".__len__()')

    def test_syntax_error_passes_through(self):
        """SyntaxError code should not raise UnsafeCodeError — let exec() handle it."""
        validate_user_code("df = (((")  # broken syntax, no UnsafeCodeError

    def test_empty_code_passes(self):
        """Empty string should pass."""
        validate_user_code("")

    # ------- Dunder access blocked -------

    def test_subclasses_blocked(self):
        """__subclasses__() is the classic sandbox escape."""
        with pytest.raises(UnsafeCodeError, match="__subclasses__"):
            validate_user_code("().__class__.__bases__[0].__subclasses__()")

    def test_class_blocked(self):
        """__class__ access is blocked."""
        with pytest.raises(UnsafeCodeError, match="__class__"):
            validate_user_code('"".__class__')

    def test_bases_blocked(self):
        """__bases__ access is blocked."""
        with pytest.raises(UnsafeCodeError, match="__bases__"):
            validate_user_code("object.__bases__")

    def test_mro_blocked(self):
        """__mro__ access is blocked."""
        with pytest.raises(UnsafeCodeError, match="__mro__"):
            validate_user_code("object.__mro__")

    def test_globals_attr_blocked(self):
        """__globals__ access on function objects is blocked."""
        with pytest.raises(UnsafeCodeError, match="__globals__"):
            validate_user_code("func.__globals__")

    def test_code_attr_blocked(self):
        """__code__ access on function objects is blocked."""
        with pytest.raises(UnsafeCodeError, match="__code__"):
            validate_user_code("func.__code__")

    def test_dict_blocked(self):
        """__dict__ access is blocked."""
        with pytest.raises(UnsafeCodeError, match="__dict__"):
            validate_user_code("obj.__dict__")

    def test_builtins_attr_blocked(self):
        """__builtins__ access is blocked."""
        with pytest.raises(UnsafeCodeError, match="__builtins__"):
            validate_user_code("x.__builtins__")

    def test_import_attr_blocked(self):
        """__import__ attribute access is blocked."""
        with pytest.raises(UnsafeCodeError, match="__import__"):
            validate_user_code("x.__import__")

    def test_reduce_blocked(self):
        """__reduce__ access is blocked (pickle exploit vector)."""
        with pytest.raises(UnsafeCodeError, match="__reduce__"):
            validate_user_code("obj.__reduce__()")

    # ------- Reflection calls blocked -------

    def test_getattr_blocked(self):
        """getattr() is blocked."""
        with pytest.raises(UnsafeCodeError, match="getattr"):
            validate_user_code('getattr(obj, "__class__")')

    def test_setattr_blocked(self):
        """setattr() is blocked."""
        with pytest.raises(UnsafeCodeError, match="setattr"):
            validate_user_code('setattr(obj, "x", 1)')

    def test_delattr_blocked(self):
        """delattr() is blocked."""
        with pytest.raises(UnsafeCodeError, match="delattr"):
            validate_user_code('delattr(obj, "x")')

    def test_type_blocked(self):
        """type() is blocked (can create classes dynamically)."""
        with pytest.raises(UnsafeCodeError, match="type"):
            validate_user_code('type("Evil", (object,), {})')

    def test_vars_blocked(self):
        """vars() is blocked."""
        with pytest.raises(UnsafeCodeError, match="vars"):
            validate_user_code("vars(obj)")

    def test_dir_blocked(self):
        """dir() is blocked."""
        with pytest.raises(UnsafeCodeError, match="dir"):
            validate_user_code("dir(obj)")

    def test_hasattr_blocked(self):
        """hasattr() is blocked."""
        with pytest.raises(UnsafeCodeError, match="hasattr"):
            validate_user_code('hasattr(obj, "__class__")')

    def test_eval_call_blocked(self):
        """eval() call is blocked at AST level."""
        with pytest.raises(UnsafeCodeError, match="eval"):
            validate_user_code('eval("1+1")')

    def test_exec_call_blocked(self):
        """exec() call is blocked at AST level."""
        with pytest.raises(UnsafeCodeError, match="exec"):
            validate_user_code('exec("x=1")')

    def test_open_call_blocked(self):
        """open() call is blocked at AST level."""
        with pytest.raises(UnsafeCodeError, match="open"):
            validate_user_code('open("/etc/passwd")')

    def test_compile_call_blocked(self):
        """compile() call is blocked at AST level."""
        with pytest.raises(UnsafeCodeError, match="compile"):
            validate_user_code('compile("x=1", "<>", "exec")')

    def test_super_blocked(self):
        """super() is blocked."""
        with pytest.raises(UnsafeCodeError, match="super"):
            validate_user_code("super().__init__()")

    # ------- Imports blocked -------

    def test_import_blocked(self):
        """import statements are blocked."""
        with pytest.raises(UnsafeCodeError, match="import"):
            validate_user_code("import os")

    def test_from_import_blocked(self):
        """from...import statements are blocked."""
        with pytest.raises(UnsafeCodeError, match="import"):
            validate_user_code("from os import system")

    # ------- Class / async / scope escaping blocked -------

    def test_class_def_blocked(self):
        """class definitions are blocked."""
        with pytest.raises(UnsafeCodeError, match="class"):
            validate_user_code("class Evil:\n    pass")

    def test_async_def_blocked(self):
        """async function definitions are blocked."""
        with pytest.raises(UnsafeCodeError, match="async"):
            validate_user_code("async def exploit():\n    pass")

    def test_global_blocked(self):
        """global statements are blocked."""
        with pytest.raises(UnsafeCodeError, match="global"):
            validate_user_code("global x")

    def test_nonlocal_blocked(self):
        """nonlocal statements are blocked."""
        with pytest.raises(UnsafeCodeError, match="nonlocal"):
            validate_user_code("def f():\n    nonlocal x")

    # ------- Known sandbox escape patterns -------

    def test_classic_subclasses_escape(self):
        """The classic CPython sandbox escape via __subclasses__ is blocked."""
        code = (
            "[c for c in ().__class__.__bases__[0].__subclasses__() "
            "if c.__name__ == 'catch_warnings'][0]()._module.__builtins__"
        )
        with pytest.raises(UnsafeCodeError):
            validate_user_code(code)

    def test_getattr_based_escape(self):
        """getattr-based escape route is blocked."""
        with pytest.raises(UnsafeCodeError):
            validate_user_code(
                'getattr(getattr("", "__class__"), "__bases__")'
            )

    def test_type_metaclass_escape(self):
        """type() dynamic class creation is blocked."""
        with pytest.raises(UnsafeCodeError):
            validate_user_code(
                'type("X", (object,), {"__init__": lambda s: None})'
            )
