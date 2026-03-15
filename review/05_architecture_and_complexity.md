# Architecture & Complexity

Large files, structural concerns, and opportunities for decomposition.

---

## A1: `executor.py` at 1,247 lines — extract node builders

**File:** `src/haute/executor.py`
**Severity:** MEDIUM-HIGH
**Agent:** Executor (3)

Contains preamble compilation, user code execution, instance resolution, 17 node builders (~380 lines), optimiser apply helpers, preview cache, execute_graph, _eager_execute, execute_sink. The builders are already in a registry pattern, making extraction mechanical.

**Recommendation:** Extract the 17 node builders + `_NODE_BUILDERS` + `NodeBuildContext` into `_builders.py`. Reduces `executor.py` to ~850 lines focused on orchestration.

---

## A2: `_json_flatten.py` at 1,322 lines — monolith with 6 flatten strategies

**File:** `src/haute/_json_flatten.py`
**Severity:** MEDIUM
**Agent:** Data Sources (14)

Has accumulated 6 different flatten/write strategies. `read_json_flat()` (codegen runtime) uses a different (older, slower) strategy than `build_json_cache()` (GUI button). Users get different behavior depending on the path.

**Recommendation:** Split into `_json_flatten/_schema.py`, `_json_flatten/_writer.py`, `_json_flatten/_cache.py`. Unify the flattening strategy.

---

## A3: `_parser_helpers.py` at 840 lines

**File:** `src/haute/_parser_helpers.py`
**Severity:** LOW-MEDIUM
**Agent:** Parser (1)

Grab-bag of AST helpers, code extraction (3 functions for 3 node types), graph building, meta extraction, config resolution, node config building (120-line if/elif chain), and text utilities.

**Recommendation:** Could split into `_parser_ast.py`, `_parser_code_extract.py`, `_parser_graph.py`. But the module is internally well-organized with section headers. Only worth splitting if it continues to grow.

---

## A4: `OptimiserConfig.tsx` at 690 lines — monolith

**File:** `frontend/src/panels/OptimiserConfig.tsx`
**Severity:** MEDIUM
**Agent:** Node Editors (11), Optimisation (16)

Handles mode toggle, data input, objective, constraints, ratebook factors, solver tuning, advanced settings, staleness detection, solve/save/MLflow actions, progress, error, and results display.

**Recommendation:** Split like `ModellingConfig.tsx` was: `OptimiserObjectiveConfig`, `OptimiserRatebookConfig`, `OptimiserSolverConfig`, `OptimiserActionsAndResults`.

---

## A5: `_shared.tsx` at 675 lines — mixed concerns

**File:** `frontend/src/panels/editors/_shared.tsx`
**Severity:** MEDIUM
**Agent:** Node Editors (11)

Contains types, style constants, MlflowStatusBadge, FileBrowser (~130 lines), SchemaPreview (~75 lines), full CodeMirror 6 editor with theme (~340 lines), and InputSourcesBar.

**Recommendation:** Split into `_types.ts`, `CodeEditor.tsx`, `FileBrowser.tsx`, `SchemaPreview.tsx`, `InputSourcesBar.tsx`.

---

## A6: `GitPanel.tsx` at 639 lines

**File:** `frontend/src/panels/GitPanel.tsx`
**Severity:** LOW-MEDIUM
**Agent:** Frontend Toolbar (13)

7 nearly-identical async action handlers following the same `setLoading/try/catch/finally` pattern. The `timeAgo` function is defined inside the component (recreated every render). The confirmation dialog should be a shared `ConfirmDialog`.

---

## A7: Deploy `_intercept` is a 143-line nested function

**File:** `src/haute/deploy/_scorer.py:78-221`
**Severity:** MEDIUM
**Agent:** Deploy (17)

5 different node-type branches, each defining closures with default-argument capture. Has grown organically.

**Recommendation:** Refactor into separate handler functions per node type, with `_intercept` as a thin dispatch:
```python
handlers = {NodeType.API_INPUT: _intercept_api_input, ...}
```

---

## A8: Dual graph model creates divergence risk

**Files:** `src/haute/pipeline.py` (Pipeline class) vs `src/haute/_types.py` (PipelineGraph)
**Severity:** MEDIUM
**Agent:** Pipeline Core (6)

`Pipeline` has its own `_topo_order()`, `run()`, `score()` that duplicate the executor's graph traversal with different semantics. `Pipeline.to_graph()` infers node types using a simplistic heuristic completely different from the parser's `_infer_node_type`. The two paths could drift.

---

## A9: Exception hierarchy inconsistent

**Files:** `executor.py:60`, `_git.py:40`, `_json_flatten.py:60`
**Severity:** LOW-MEDIUM
**Agent:** Error Handling (19)

`PreambleError`, `GitError`, and `JsonCacheCancelledError` inherit directly from `Exception`, not `HauteError`. Cannot catch "all haute errors" with `except HauteError`.

**Fix:** One-line change per exception class to inherit from `HauteError`.

---

## A10: `FlowEditor` (App.tsx) is a god component

**File:** `frontend/src/App.tsx` (493 lines)
**Severity:** LOW-MEDIUM
**Agent:** Frontend State (12)

11 refs, 15+ UI store selectors, 8 custom hooks. The refs form an "app spine" (`graphRef`, `preambleRef`, `pipelineNameRef`, `sourceFileRef`, `submodelsRef`, `nodeIdCounter`, `lastSavedRef`) that should move into a dedicated store or `usePipelineState` hook.

---

## A11: `JobStore` uses `dict[str, Any]` with no typed schema

**File:** `src/haute/routes/_job_store.py`
**Severity:** LOW-MEDIUM
**Agent:** Schemas (20)

All jobs stored as `dict[str, Any]`. A typo in a key name silently returns `None`. A `TrainingJob` and `OptimiserJob` TypedDict or dataclass would eliminate this.

---

## A12: Zero Pydantic validators in 90+ models

**File:** `src/haute/schemas.py`
**Severity:** LOW-MEDIUM
**Agent:** Schemas (20)

No `@field_validator`, `@model_validator`, no `Field(ge=..., le=..., min_length=...)`. All 90+ models rely entirely on basic type coercion. `status` fields are `str` everywhere instead of `Literal["ok", "error", "running"]`.

---

## A13: Parallel dispatch tables must stay in sync

**Files:** `codegen.py` (`_CODEGEN_BUILDERS`), `executor.py` (`_NODE_BUILDERS`), `_parser_helpers.py` (`_infer_node_type` + `_build_node_config`)
**Severity:** LOW-MEDIUM
**Agents:** Parser (1), Codegen (2), Pipeline Core (6)

Three separate dispatch mechanisms for the same node types. No compile-time or test-time check that they cover the same set.

**Fix:** Add a test asserting `set(_CODEGEN_BUILDERS.keys()) == set(_NODE_BUILDERS.keys())` (minus types that only exist in one).

---

## A14: No cycle detection in executor path

**File:** `src/haute/_topo.py`
**Severity:** LOW
**Agent:** Pipeline Core (6)

`topo_sort_ids` uses Kahn's algorithm which silently drops cycle nodes. `Pipeline._topo_order()` checks for this, but `_prepare_graph()` does not. A cycle in the GUI causes some nodes to silently disappear.

---

## A15: Container Dockerfile doesn't pin dependency versions

**File:** `src/haute/deploy/_container.py:300`
**Severity:** LOW-MEDIUM
**Agent:** Deploy (17)

Generated `pip install haute polars fastapi uvicorn` has no version pins. Builds are not reproducible. The MLflow path correctly pins versions.

---

## A16: Frontend types are entirely manual mirrors of backend

**Agents:** Schemas (20)
**Severity:** LOW (accepted architectural decision)

No code generation, no OpenAPI schema export, no shared contract. Every backend schema change requires a manual frontend update with no automated drift detection. `PipelineGraph` in the frontend doesn't include `preserved_blocks` that the backend has.
