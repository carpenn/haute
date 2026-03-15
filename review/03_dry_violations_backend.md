# DRY Violations — Backend

Systemic repetition across the Python codebase, ordered by impact.

---

## D1: Deploy scorer duplicates executor builder logic for 4 node types

**File:** `src/haute/deploy/_scorer.py:78-221`
**Severity:** HIGH
**Agent:** Executor (3)

The `_intercept` function re-implements logic for `EXTERNAL_FILE`, `OPTIMISER_APPLY`, `MODEL_SCORE`, and `DATA_SOURCE` node types. Each is 20-30 lines closely mirroring the executor builders but with artifact path remapping. When executor builders change, deploy scorer intercepts get out of sync. The deploy scorer's MODEL_SCORE intercept already doesn't pass `preamble_ns` to `_exec_user_code`, while the executor builder does.

**Recommendation:** Refactor executor builders to accept an optional `path_override` parameter, or make `ModelScorer` accept a local-path loading mode, so `_scorer.py` can delegate instead of reimplementing.

---

## D2: `_sanitize_func_name(ctx.node.data.label)` repeated 17 times

**File:** `src/haute/executor.py` (lines 301, 326, 349, 373, 400, 409, 439, 454, 477, 512, 543, 551, 599, 607, 642, 668, 674)
**Severity:** MEDIUM-HIGH
**Agent:** Executor (3)

Every single builder starts with the same two boilerplate lines:
```python
config = ctx.node.data.config
func_name = _sanitize_func_name(ctx.node.data.label)
```

**Fix:** Add `func_name` and `config` as computed properties on `NodeBuildContext`:
```python
@property
def func_name(self) -> str:
    return _sanitize_func_name(self.node.data.label)

@property
def config(self) -> dict[str, Any]:
    return self.node.data.config
```

---

## D3: Function building duplicated between `_execute_lazy()` and `_build_funcs()`

**File:** `src/haute/_execute_lazy.py:157-171` vs `270-306`
**Severity:** MEDIUM
**Agent:** Executor (3)

`_execute_lazy()` builds node functions inline with a hand-rolled loop that is functionally identical to `_build_funcs()` but with subtle differences (doesn't pass `row_limit`, conditionally includes `node_map` and `preamble_ns`). If a new builder kwarg is added, it must be added in both places.

**Fix:** Have `_execute_lazy()` call `_build_funcs()` with `row_limit=None`.

---

## D4: `selected_columns` filtering duplicated between lazy and eager paths

**File:** `src/haute/_execute_lazy.py:208-213` and `421-426`
**Severity:** MEDIUM
**Agent:** Pipeline Core (6)

Both do the same thing: read config, check validity, apply select. If the logic changes, both need updating.

**Fix:** Extract a `_apply_selected_columns(frame, config)` helper.

---

## D5: "Job completed" guard repeated 5 times

**Files:** `src/haute/routes/optimiser.py:87,115,194,239`, `src/haute/routes/modelling.py:165`
**Severity:** MEDIUM
**Agent:** Routes (8)

```python
job = _store.require_job(body.job_id)
if job.get("status") != "completed":
    raise HTTPException(400, f"Job '{body.job_id}' is not completed ...")
```

**Fix:** Add `require_completed_job()` method to `JobStore`.

---

## D6: `result_dict` construction duplicated in `_solve_online` / `_solve_ratebook`

**File:** `src/haute/routes/_optimiser_service.py:119-148` and `228-257`
**Severity:** MEDIUM
**Agent:** Optimisation (16)

8+ identical keys, same convergence warning logic, same `job.update()` structure. ~30 lines duplicated.

**Fix:** Extract `_finalize_solve_result()` helper.

---

## D7: Dataclass-to-Pydantic manual copying in git routes (29 field assignments)

**File:** `src/haute/routes/git.py` (7 response constructors)
**Severity:** MEDIUM
**Agent:** Secondary Routes (9)

`_git.py` defines dataclasses and `schemas.py` defines near-identical Pydantic models. The route handlers manually copy fields between them. If a field is added to one but not the other, the mismatch is silent.

**Fix:** Option A: Use `model_validate(dataclasses.asdict(result))`. Option B: Eliminate the dataclasses and have `_git.py` return Pydantic models directly.

---

## D8: `_find_cbm_artifact` and `_find_rsglm_artifact` are near-identical

**File:** `src/haute/_mlflow_io.py:195-228`
**Severity:** LOW-MEDIUM
**Agent:** Model Scoring (5)

Differ only in file extension and error message. ~15 lines duplicated.

**Fix:** Extract `_find_artifact_by_extension(client, run_id, ext, label)`.

---

## D9: Classification probability handling duplicated between eager and batch paths

**Files:** `src/haute/_mlflow_io.py:494-502`, `src/haute/_model_scorer.py:266-277`
**Severity:** MEDIUM
**Agent:** Model Scoring (5)

Both contain the same `predict_proba` + `ndim == 2` + `[:, 1]` + `with_columns` pattern. If multi-class support is added, both need updating.

**Fix:** Unify into `_append_predictions(df, scoring_model, x_data, output_col, task)`.

---

## D10: Four identical passthrough codegen builders

**File:** `src/haute/codegen.py:548-596`
**Severity:** MEDIUM
**Agent:** Codegen (2)

`_gen_scenario_expander`, `_gen_optimiser`, `_gen_optimiser_apply`, `_gen_modelling` are line-for-line identical except for the template and config keys tuple.

**Fix:** Create `_gen_simple_passthrough(template, config_keys)` factory.

---

## D11: `cache_info` / atomic write patterns duplicated across I/O files

**Files:** `src/haute/_databricks_io.py:183-204`, `src/haute/_json_flatten.py:1207-1226,1301-1321`
**Severity:** MEDIUM
**Agent:** Data Sources (14)

The "read parquet metadata and return TypedDict" pattern is duplicated 3 times. The "write to `.tmp`, then atomic rename" pattern is duplicated 6+ times across `_databricks_io.py` and `_json_flatten.py`.

**Fix:** Extract `_parquet_metadata(path)` helper and an `AtomicParquetWriter` context manager into `_polars_utils.py`.

---

## D12: `NodeResult` and `PreviewNodeResponse` share 10 identical fields

**File:** `src/haute/schemas.py`
**Severity:** LOW-MEDIUM
**Agent:** Schemas (20)

`PreviewNodeResponse` adds `node_id`, `timings`, `memory`, `node_statuses` on top. `NodeResult` should be a base model with inheritance. The frontend's `NodeResult` type is actually shaped like `PreviewNodeResponse` — a schema mismatch.

---

## D13: `LogExperimentResponse` and `OptimiserMlflowLogResponse` identical

**File:** `src/haute/schemas.py`
**Severity:** LOW
**Agent:** Schemas (20)

Same fields: `status`, `backend`, `experiment_name`, `run_id`, `run_url`, `tracking_uri`, `error`. Should be a shared `MlflowLogResponse` base class.

---

## D14: Transport dispatch pattern duplicated in `_smoke.py` and `_impact.py`

**Files:** `src/haute/cli/_smoke.py:47-64`, `src/haute/cli/_impact.py:83-105`
**Severity:** LOW-MEDIUM
**Agent:** CLI (7)

Both implement the same three-way dispatch on `config.target`. When a new target is added, both files plus `deploy/__init__.py` need updating.

---

## D15: `_build_manifest` wrappers + dead code in deploy modules

**Files:** `src/haute/deploy/_mlflow.py:207-211`, `src/haute/deploy/_container.py:391-395`
**Severity:** LOW
**Agent:** Deploy (17)

One-line wrappers around `_utils.build_manifest()` that add no value. Also dead code: `_safe_user()` and `_get_haute_version()` in `_container.py` are never called.

---

## D16: `_find_modelling_node` and `_find_optimiser_node` near-identical

**Files:** `src/haute/routes/_train_service.py:73-79`, `src/haute/routes/_optimiser_service.py:50-57`
**Severity:** LOW
**Agent:** Routes (8)

Both: lookup node by ID, check nodeType, raise if wrong. Differ only in the expected `NodeType`.

**Fix:** Single `_find_typed_node(graph, node_id, expected_type, type_label)` helper.

---

## D17: Submodel graph-building duplicated between `_submodel_ops.py` and `_parser_submodels.py`

**Files:** `src/haute/routes/_submodel_ops.py:80-146`, `src/haute/_parser_submodels.py:134-215`
**Severity:** LOW-MEDIUM
**Agent:** Secondary Routes (9)

Both build submodel placeholder nodes with identical structure, rewire cross-boundary edges with the same handle naming convention. A change to the submodel data format needs updating in two places.

---

## D18: Three near-identical except clauses in optimiser background thread

**File:** `src/haute/routes/_optimiser_service.py:553-576`
**Severity:** LOW
**Agent:** Routes (8)

Three clauses differ only in prefix string and category. Could be a single handler with a lookup dict.

---

## D19: `_compile_node_code` test helper duplicated

**Files:** `tests/test_codegen.py:37`, `tests/test_codegen_builders.py:19`
**Severity:** LOW
**Agent:** Codegen (2)

Identical function copy-pasted. Move to conftest.
