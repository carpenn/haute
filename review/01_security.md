# Security Issues

Three path traversal vulnerabilities identified. All are straightforward fixes.

---

## S1: Path traversal in `optimiser.py` via `str.startswith` check

**File:** `src/haute/routes/optimiser.py:207`
**Severity:** HIGH
**Agent:** Routes (8)

The output path safety check uses `str(out).startswith(str(base))` which is subtly broken. If the base is `/home/user/project` and someone crafts `output_path = "../project2/evil.json"`, the resolved path `/home/user/project2/evil.json` starts with `/home/user/project` and **passes the check**. The `is_relative_to()` method exists precisely to avoid this class of bug.

**Fix:** Replace with `validate_safe_path()` from `_helpers.py`, which uses `.resolve()` + `.is_relative_to()` correctly. This is a one-line change.

```python
# Before (broken)
if not str(out).startswith(str(base)):
    raise HTTPException(403, ...)

# After (correct)
out = validate_safe_path(base, body.output_path)
```

---

## S2: Path traversal in `get_submodel` endpoint

**File:** `src/haute/routes/submodel.py:84`
**Severity:** HIGH
**Agent:** Secondary Routes (9)

The `name` URL parameter is used directly to construct a file path without any validation:

```python
sm_path = cwd / "modules" / f"{name}.py"
```

A request to `GET /api/submodel/../../etc/passwd` would resolve to an arbitrary path. Although `sm_path.is_file()` provides some protection, this is inconsistent with the rest of the codebase which uses `validate_safe_path()`.

**Fix:** Add `validate_safe_path(cwd / "modules", f"{name}.py")` or add a name validation similar to `_validate_module_name` from `utility.py`.

---

## S3: Path traversal in `dissolve_submodel` endpoint

**File:** `src/haute/routes/submodel.py:145`
**Severity:** HIGH
**Agent:** Secondary Routes (9)

The `dissolve_submodel` endpoint resolves `body.source_file` and writes to it without calling `validate_safe_path`:

```python
py_path = (cwd / body.source_file).resolve()
code = graph_to_code(...)
py_path.write_text(code)
```

There is no `is_relative_to` check on `py_path` before `write_text`. Compare with `_save_pipeline.py` which correctly calls `validate_safe_path(self._root, source_file)`.

**Fix:** Add `py_path = validate_safe_path(cwd, body.source_file)` before writing.
