# Error Handling & Validation

---

## E1: Git routes missing general exception handling

**File:** `src/haute/routes/git.py` (all 9 handlers)
**Severity:** HIGH
**Agent:** Error Handling (19)

Every handler only catches `GitError`. Any non-`GitError` exception (OSError, UnicodeDecodeError, subprocess.TimeoutExpired) propagates unhandled to the middleware, returning a generic `{"detail": "Internal server error"}` with zero actionable information. Every other route module uses `except Exception: raise HTTPException(500, str(e))`.

**Fix:** Add `except Exception as e: raise HTTPException(500, detail=str(e))` after each `except GitError` block.

---

## E2: `useSchemaFetch.ts` swallows errors silently

**File:** `frontend/src/hooks/useSchemaFetch.ts:22-24`
**Severity:** MEDIUM
**Agent:** Error Handling (19)

Schema fetch failure silently sets schema to `null` with no user feedback. Users see the schema disappear with no explanation.

**Fix:** Add `error` state and surface it in `DataSourceEditor` and `ApiInputEditor`.

---

## E3: Error `detail` strings leak internal Python exception messages

**Files:** `routes/databricks.py:87`, `routes/optimiser.py`, and others using `raise HTTPException(500, detail=str(e))`
**Severity:** MEDIUM
**Agent:** Error Handling (19)

`str(e)` from `except Exception` can include internal paths, tracebacks, or module names. May expose credential-adjacent information (Databricks SDK errors).

**Fix:** Follow the `files.py:116-118` pattern: `detail="Operation failed. Check the server logs for details."` and log the actual error at `logger.error`.

---

## E4: Utility syntax errors returned as 200 OK

**File:** `src/haute/routes/utility.py:113-120`
**Severity:** LOW-MEDIUM
**Agent:** Error Handling (19)

Syntax errors in utility files return 200 OK with `status: "error"` in body, rather than 4xx. Makes automated failure detection harder.

**Fix:** Return `HTTPException(400, detail=err_msg)` with error_line in the body.

---

## E5: Deploy schema cache corruption silently ignored

**File:** `src/haute/deploy/_schema.py:83-84`
**Severity:** LOW
**Agent:** Error Handling (19)

```python
except Exception:
    pass  # corrupt cache -- recompute
```

Should at least `logger.warning("corrupt_schema_cache")`.

---

## E6: Git route error handler has zero logging

**File:** `src/haute/routes/git.py:54-58`
**Severity:** LOW
**Agent:** Error Handling (19)

`_handle_git_error` converts to HTTPException without logging. Should `logger.warning("git_error", detail=str(e))`.

---

## E7: Frontend RAM estimate failure not surfaced to user

**File:** `frontend/src/panels/ModellingConfig.tsx:72`
**Severity:** LOW
**Agent:** Error Handling (19)

Failure logged to console only. Users may unknowingly train with inadequate resources.

---

## E8: Node execution failure logged as warning, not error

**File:** `src/haute/_execute_lazy.py:433`
**Severity:** LOW
**Agent:** Error Handling (19)

`logger.warning` when `logger.error` would be more appropriate for a node execution failure.

---

## E9: `.env` fallback parser doesn't handle quoted values

**File:** `src/haute/deploy/_config.py:369-377`
**Severity:** LOW
**Agent:** Deploy (17)

When `python-dotenv` is not installed, the fallback parser does `value = value.strip()` but doesn't strip surrounding quotes. `KEY="value"` sets the env var to `"value"` (with quotes).

**Fix:** `value = value.strip("'\"")`

---

## E10: Codegen unknown node type falls through silently

**File:** `src/haute/codegen.py:716`
**Severity:** LOW
**Agent:** Codegen (2)

`_generate_node_code` falls back to `_gen_transform` for unknown node types without logging a warning. If a new NodeType is added but not registered, it silently becomes a transform.

**Fix:** Add `logger.warning("unknown_node_type_fallback", ...)`.

---

## E11: `_apply_ratebook` silently skips entries missing `__factor_group__`

**File:** `src/haute/executor.py:810-828`
**Severity:** LOW
**Agent:** Optimisation (16)

Entries where `factor_col` is missing are silently dropped. A corrupted artifact produces an empty rating table with no warning.

**Fix:** Add warning log when entries are skipped.

---

## E12: Optimiser hardcoded save path — multiple nodes overwrite each other

**File:** `frontend/src/panels/OptimiserConfig.tsx:172`
**Severity:** LOW-MEDIUM
**Agent:** Optimisation (16)

```typescript
output_path: `output/optimiser_result.json`,
```

All optimiser nodes save to the same file. Should incorporate node label: `output/optimiser_${slugify(nodeLabel)}.json`.
