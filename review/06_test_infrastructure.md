# Test Infrastructure & Gaps

Test DRY violations, organizational issues, and coverage gaps.

---

## T1: Git test helpers duplicated identically in 2 files

**Files:** `tests/test_git.py:43-62`, `tests/test_git_routes.py:15-31`
**Severity:** HIGH (for test DRY)
**Agent:** Test Infrastructure (18)

`_git(cwd, *args)` subprocess wrapper and `_init_repo(path, *, user)` are copy-pasted with identical signatures and bodies.

**Fix:** Extract to `conftest.py` or `tests/_git_helpers.py`.

---

## T2: `_make_resolved()` duplicated across 4 deploy test files

**Files:** `test_deploy.py:92`, `test_deploy_internals.py:37`, `test_deploy_utils.py:33`, `test_deploy_container.py:27`
**Severity:** HIGH (for test DRY)
**Agents:** Deploy (17), Test Infrastructure (18)

Functionally identical factory for `ResolvedDeploy`. High divergence risk.

**Fix:** Shared fixture in conftest with flexible defaults.

---

## T3: `CliRunner()` fixture duplicated across 8 CLI test files

**Files:** All 8 `test_cli_*.py` files
**Severity:** MEDIUM
**Agent:** Test Infrastructure (18)

Identical one-liner fixture: `return CliRunner()`.

**Fix:** Move to conftest.py.

---

## T4: `TestClient(app)` fixture duplicated across 10 route test files

**Severity:** MEDIUM
**Agent:** Test Infrastructure (18)

Each route test file defines its own `client` fixture. A shared conftest fixture with a parameter for `raise_server_exceptions` would consolidate.

---

## T5: Frontend has dual test directory structure with duplicates

**Severity:** MEDIUM-HIGH
**Agent:** Test Infrastructure (18)

Two parallel structures: legacy (`src/__tests__/editors/`) and colocated (`src/panels/__tests__/`). Some components have tests in BOTH:
- `ModelScoreEditor` — tests in both locations
- `OptimiserApplyEditor` — tests in both locations
- `useMlflowBrowser` — tests in both locations
- `useUIStore` — tests in both locations (the colocated version is more complete, has git panel exclusion tests)

**Fix:** Pick one pattern (colocated recommended), migrate, delete duplicates.

---

## T6: `_widen_sandbox` fixture duplicated

**Files:** `tests/conftest.py:13` (as `_widen_sandbox_root`), `tests/test_io.py:97` (as `_widen_sandbox`)
**Severity:** LOW
**Agent:** Test Infrastructure (18)

Identical logic with different names. `test_io.py` should use the conftest version.

---

## T7: `_make_node()` / `_make_edge()` / `_make_graph()` reinvented in 5 test files

**Files:** `test_route_save_pipeline.py`, `test_scenario_expander.py`, `test_node_builder.py`, `test_optimiser_apply.py`, `test_deploy_utils.py`
**Severity:** LOW-MEDIUM
**Agent:** Test Infrastructure (18)

Conftest already provides `make_node` and `make_graph`. A more flexible factory could serve most use cases.

---

## T8: Frontend `vi.mock("../../api/client")` repeated 22+ times

**Severity:** LOW
**Agent:** Test Infrastructure (18)

Each test independently mocks the API client with varying shapes. A shared mock factory would reduce this.

---

## T9: Zero RustyStats/pyfunc test coverage in `_mlflow_io.py`

**Severity:** MEDIUM
**Agent:** Model Scoring (5)

- `_find_rsglm_artifact()` — 0 tests
- `_find_model_artifact()` — only referenced as a mock
- `_load_rustystats_model()` — 0 unit tests
- `load_local_model()` for `.rsglm` — 0 tests
- `_prepare_predict_frame()` with `flavor="rustystats"` — 0 tests

---

## T10: No dedicated sink tests

**Severity:** MEDIUM
**Agent:** Data Sources (14)

No `test_sink*` files exist. `execute_sink()` is exercised through integration tests but edge cases (streaming failure fallback, concurrent config mutation, partial-write cleanup) are untested.

---

## T11: No dispatch table coverage parity test

**Severity:** LOW-MEDIUM
**Agents:** Codegen (2), Pipeline Core (6)

No test asserts `set(_CODEGEN_BUILDERS.keys()) == set(_NODE_BUILDERS.keys())`. Adding one would catch drift when new node types are added.

---

## T12: Only 17 uses of `pytest.mark.parametrize` across 2,692 tests

**Severity:** LOW
**Agent:** Test Infrastructure (18)

Many test classes repeat the same structure with different inputs. Executor, banding, and sandbox tests could benefit from more parametrization.

---

## T13: Frontend coverage thresholds at 40% vs Python's 85%

**Severity:** LOW-MEDIUM
**Agent:** Test Infrastructure (18)

The gap is significant. Consider raising frontend thresholds incrementally.

---

## T14: TOML schema drift risk — no sync test with dataclasses

**File:** `src/haute/deploy/_config.py:92-117`
**Severity:** LOW
**Agent:** Deploy (17)

`_VALID_TOML_SCHEMA` dict must stay in sync with config dataclass fields. No automated check.

**Fix:** Add a test introspecting dataclass fields and asserting they match schema keys.
