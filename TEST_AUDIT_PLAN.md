# Test Suite Audit — Full Implementation Plan

**Date:** 2026-03-03
**Suite:** 70+ files, ~30,500 lines, ~2,000 tests
**Overall rating:** Adequate, trending Superficial in specific areas
**Estimated regression-catching:** ~60-65% current → ~85-90% after fixes

---

## Priority 1: CRITICAL (8 issues) — Silent data corruption risk

### C1. `_build_signature` type mapping never verified
- **File:** `tests/test_deploy_internals.py:1315-1381`
- **Tests:** `test_basic_types`, `test_parameterized_datetime_type`, `test_all_numeric_types`, `test_unknown_dtype_falls_back_to_string`
- **Problem:** All 4 tests assert only `isinstance(sig, ModelSignature)`. A mismap where every Polars dtype becomes `string` passes.
- **Fix:** Assert `{col.name: col.type for col in sig.inputs.inputs}` maps each dtype correctly. See Rewrite 1 in audit.

### C2. `HauteModel.predict` never tested end-to-end with a real graph
- **File:** `tests/test_deploy_internals.py:148-219`
- **Tests:** `test_predict_pandas_round_trip`, `test_predict_passes_output_fields`, `test_predict_passes_artifact_paths`
- **Problem:** All 3 tests mock `score_graph`. The production Databricks code path is untested.
- **Fix:** Add a new test that builds a minimal real graph (1 source + 1 transform + 1 output), calls `load_context` with real artifacts on `tmp_path`, then calls `predict()` and asserts real output values.

### C3. `score_graph` never tested with wrong input types
- **File:** `tests/test_executor.py` and `tests/test_deploy_internals.py`
- **Problem:** All scoring tests use correctly-typed DataFrames. No test where input has string-where-float-expected, nulls where non-null expected, or missing columns.
- **Fix:** Add 3 new tests: (a) wrong dtype input, (b) unexpected nulls, (c) missing column. Assert appropriate error/behavior.

### C4. `infer_output_schema` tested only for existence
- **File:** `tests/test_deploy.py:504-514`
- **Problem:** `assert isinstance(schema, dict)` and `len(schema) > 0`. No column names or types verified.
- **Fix:** Assert specific column names and their inferred types against the fixture pipeline's known output.

### C5. Metric computation tests reimplement the formula
- **File:** `tests/test_modelling.py:196-205`
- **Tests:** `test_core_metric` (parametrized)
- **Problem:** Expected values computed with same arithmetic as implementation. Bug in both would be invisible.
- **Fix:** Use `sklearn.metrics` as independent oracle. See Rewrite 2 in audit.

### C6. Metrics asserted as truthy dicts across 6 tests
- **File:** `tests/test_modelling.py:477, 565, 579, 595, 726, 840`
- **Tests:** `test_with_weight_column`, `test_poisson_loss`, `test_tweedie_loss`, `test_offset_column`, `test_monotone_constraint_training`, `test_training_job_includes_shap`
- **Problem:** `assert result.metrics` passes for `{"gini": 0.0}` or `{"gini": float("nan")}`.
- **Fix:** In each test, assert specific metrics are present, finite, and in plausible ranges:
  ```python
  assert "gini" in result.metrics
  assert np.isfinite(result.metrics["gini"])
  assert 0.0 < result.metrics["gini"] <= 1.0  # known good data should have predictive power
  ```

### C7. 8 production node types have zero round-trip coverage
- **File:** `tests/test_parser_roundtrip.py:17-19`
- **Problem:** `modelScore`, `externalFile`, `liveSwitch`, `modelling`, `optimiser`, `optimiserApply`, `scenarioExpander`, `submodel` all excluded.
- **Fix:** Add deterministic (not Hypothesis) round-trip tests for each type with minimal config. Follow the `TestEdgeCases` pattern already in the file.

### C8. Codegen function bodies never executed (~20 tests)
- **File:** `tests/test_codegen.py`, `tests/test_codegen_builders.py`, `tests/test_model_score_codegen.py`
- **Problem:** `compile()` only checks syntax. A function body that references undefined names passes. Banding/expander/optimiser builders only check config path in decorator, never the body.
- **Fix:** For at minimum the 5 core node types (banding, dataSource, apiInput, output, modelScore), add tests that `exec()` the generated code against real data and verify output. See Rewrite 3.

---

## Priority 2: HIGH (22 issues) — Regressions slip through undetected

### H1. `test_model_scorer.py` — 5 routing tests discard return value
- **File:** `tests/test_model_scorer.py:104-198`
- **Tests:** `test_live_scenario_uses_eager`, `test_non_live_scenario_uses_batched`, `test_feature_intersection`, `test_empty_input_doesnt_crash`, `test_user_code_applied_after_scoring`
- **Fix:** In each test, capture the return value of `scorer.score(lf)` and assert the output DataFrame has expected columns and plausible values.

### H2. `test_mlflow_log.py` — entire class is mock transcription
- **File:** `tests/test_mlflow_log.py:53-410` (13 tests)
- **Fix:** Add 1-2 integration tests against a real local MLflow backend (`mlflow.set_tracking_uri(f"file://{tmp_path}")`) that verify experiment creation, param/metric logging, and artifact presence on disk. Keep 2-3 of the mock tests for edge cases (Databricks URI, error paths).

### H3. Broad `Exception` catches in `test_executor.py` (4 instances)
- **File:** `tests/test_executor.py:317, 997, 1005, 1040`
- **Fix:** Replace `pytest.raises(Exception)` with specific types: `ValueError` for unknown node type, `FileNotFoundError` for missing file, etc.

### H4. `test_deploy_internals.py:623` — union exception catch
- **File:** `tests/test_deploy_internals.py:623`
- **Fix:** Replace `pytest.raises((RuntimeError, ValueError))` with the single correct exception type.

### H5. `test_deploy_utils.py:142-280` — `TestBuildManifest` uses MagicMock
- **Fix:** Replace `MagicMock()` with real `ResolvedDeploy` objects (follow `test_container.py` pattern).

### H6. `test_cli_helpers.py:83-119` — `Path` patched wholesale
- **Fix:** Use `monkeypatch.chdir(child)` with real filesystem instead of mocking `Path`.

### H7. `test_cli_smoke.py:178-198` — `_smoke_http` patched entirely
- **Fix:** Write tests for the real `_smoke_http` function using `responses` or `httpretty` to mock HTTP, not by replacing the entire function.

### H8. `test_modelling_routes.py:533` — `builtins.__import__` patched
- **Fix:** Replace with `patch("haute.routes.modelling.mlflow", side_effect=ImportError)`.

### H9. Six-layer patch stacks in `test_deploy_internals.py:1095-1168`
- **Fix:** Replace with fixture pipelines that structurally provoke error conditions (e.g., pipeline with no source nodes) instead of patching each internal function.

### H10. `test_route_save_pipeline.py:210-288` — `graph_to_code` patched
- **Fix:** Let codegen run against simple fixture graphs. Assert written files contain valid Python via `compile()` at minimum.

### H11. `test_route_save_pipeline.py:295-388` — private `_last_config_files` set directly
- **Fix:** Call `svc.save()` with a fixture body to populate `_last_config_files`, then test stale removal.

### H12. `compute_ave_per_feature` untested (~130 lines)
- **File:** `src/haute/_metrics.py`
- **Fix:** Add tests for `_ave_numeric_bins`, `_ave_categorical_bins`: NaN bin path, `max_categories` lumping, `__MISSING__` rename.

### H13. `_assign_group_split` hash fallback untested
- **File:** `src/haute/_split.py`
- **Fix:** Add test where all groups hash to train (small group count with specific seed), verify fallback forces at least one group to test.

### H14. WebSocket broadcast with multiple concurrent clients
- **File:** `tests/test_server.py`
- **Fix:** Test broadcast with 2 mock clients (one live, one disconnected). Assert live client receives, dead client is cleaned up.

### H15. `liveSwitch` with empty `input_scenario_map`
- **File:** `tests/test_executor.py`
- **Fix:** Add test with `liveSwitch` node where `input_scenario_map = {}`. Verify fallback behavior or appropriate error.

### H16. Banding null row assertion missing
- **File:** `tests/test_banding.py:133-143`
- **Fix:** Add `assert bands[1] == "dflt"` (or whatever the null-handling contract is).

### H17. `_prepare_predict_frame` mocked in batch scoring
- **File:** `tests/test_model_scorer.py:253`
- **Fix:** Add 1 test where `_prepare_predict_frame` runs for real with null values and mixed types.

### H18. `test_optimiser_apply.py:457` — `time.sleep(0.05)` flaky on WSL2
- **Fix:** Replace with `os.utime(path, (future_time, future_time))` to explicitly set mtime.

### H19. `test_databricks_io.py:327` — conditional assertion
- **Fix:** Remove the `if cache_dir.exists()` guard. Assert `not any(tmp_path.rglob("*.tmp"))`.

### H20. Disjunctive `or "Error"` assertions in CLI tests (5 instances)
- **File:** `tests/test_cli.py:314, 321, 333`, `tests/test_cli_train.py:97`, `tests/test_cli_deploy.py:142`
- **Fix:** Replace each `or "Error"` with the specific expected text.

### H21. `test_optimiser_routes.py:787` — race condition accepts two status codes
- **Fix:** Mock the job store to guarantee a deterministic "running" state before the frontier call.

### H22. `test_modelling_routes.py:129` — tautological OR assertion on progress
- **Fix:** Either mock the training to be slow enough to catch progress, or test progress callback directly in `test_modelling.py` with a controlled progress function that records all calls.

---

## Priority 3: MEDIUM (30 issues) — Noise, false confidence, maintenance tax

### M1. `test_json_cache_routes.py` — 14 tests, zero domain logic
- **Fix:** Collapse to ~5 parametrized route-wiring tests. Add 3-4 integration tests with real flatten/cache logic.

### M2. Delete no-assertion tests (5 instances)
- `test_route_helpers.py:147` — fix with module global reset or delete
- `test_server.py:1041` — add assertion about side effects or delete
- `test_deploy_internals.py:960` — add assertion or delete
- `test_deploy_internals.py:967` — assert `load_dotenv` was called
- `test_sandbox.py:291` — add explicit assertion about expected behavior

### M3. Delete tests of language/framework behavior (7 instances)
- `test_types.py:97` (`test_is_exception`) — delete
- `test_types.py:100` (`test_can_be_raised_and_caught`) — delete
- `test_scenario_expander.py:24` (`test_enum_value`) — delete
- `test_execute_lazy.py:263` (`test_named_tuple_fields`) — delete
- `test_deploy_container.py:110` (`test_raises_not_implemented`) — delete
- `test_container.py:219` (`test_all_platform_targets_listed`) — delete
- `test_deploy_internals.py:958` (`test_inherits_from_python_model`) — delete

### M4. Fix implementation-mirroring assertions (8 instances)
- `test_types.py:37-89` — keep only `test_no_unexpected_members`, delete 3 others
- `test_job_store.py:24` — replace `len == 12` with `isalnum()` and `len >= 8`
- `test_pipeline.py:234` — replace `x == 280` with `nodes[1].x > nodes[0].x`
- `test_deploy_utils.py:150` — delete `test_exactly_16_keys`
- `test_mlflow_log.py:208` — replace count with named artifact checks
- `test_mlflow_log.py:167` — same
- `test_route_helpers.py:311` — use `_sanitize_func_name()` in assertion
- `test_modelling_codegen.py:41-120` — delete string-match tests (keep execution tests)

### M5. Fix tautological property tests (3 instances)
- `test_property.py:57` — replace with `isidentifier()` + idempotency
- `test_property.py:117` — replace with subset + ordering properties
- `test_property.py:335` — expand `_KNOWN_TYPE_KWARGS` to all node types + conflicts

### M6. Reduce `test_schemas.py` from 20 to ~4 tests
- Keep only tests that verify non-obvious behaviors (validators, computed fields)
- Delete pure default-value assertions

### M7. Strengthen weak assertions (8 instances)
- `test_deploy_config.py:69-84` — delete redundant `isinstance` lines
- `test_deploy_utils.py:73` — add format validation for user string
- `test_deploy_utils.py:99` — validate version format with `packaging.version.Version`
- `test_config_validation.py:21` — replace subset with equality check
- `test_e2e.py:71` — replace `len > 100` with `compile()`
- `test_submodel.py:136` — assert specific expected dict keys
- `test_submodel.py:145` — add `compile()` for submodel code
- `test_model_card.py:24` — add well-formedness check and verify XSS string is absent

### M8. Fix feature tests that don't assert the feature (5 instances)
- `test_cli_deploy.py:181` — assert suffix in mock call args or output
- `test_cli_smoke.py:155` — same
- `test_cli_smoke.py:84` — assert poll count and "polling" in output
- `test_cli_lint.py:94` — assert "main.py" in error output
- `test_safety.py` — add more fixture pipelines or document limitation

### M9. Delete redundant private function tests (5 sets)
- `test_optimiser_apply.py:21-22` — `_apply_online`, `_apply_ratebook` (covered by executor tests)
- `test_execute_lazy.py:282-327` — `_build_funcs` (covered by `TestExecuteEagerCore`)
- `test_deploy_internals.py:920-949` — `_remap_artifact` (should test through `score_graph`)
- `test_pipeline.py:122-165` — `_topo_order` (should test through `pipeline.run()`)
- `test_model_score_executor.py:398-476` — `_score_eager`, `_resolve_artifact_local` (covered by executor tests)

### M10. `test_modelling.py:214-220` — Gini random test with useless tolerance
- **Fix:** Use fixed seed value, assert exact value within tight tolerance.

---

## Priority 4: LOW (28 issues) — Cleanup for suite health

### L1. Fix temp directory leaks (2 files)
- `test_parser_roundtrip.py:531` — use `TemporaryDirectory()` context manager
- `test_property.py:258` — same

### L2. Remove `_strip_upstream_prefix` helper in `test_parser_roundtrip.py:97-120`
- Replace with assertion that accounts for known post-round-trip form

### L3. Add `_store` isolation fixture for `test_optimiser_routes.py`
- Autouse fixture that asserts/clears `_store.jobs` before each test

### L4. Fix camelCase vs snake_case inconsistency in `test_rating_step.py:248-270`
- Use the same key format as production (camelCase)

### L5. Fix hardcoded JSON byte comparison in `test_server.py:1051-1076`
- Parse response body as JSON and compare dicts, not byte strings

### L6. Move test-only helpers out of production code
- `_set_fetch_progress`/`_clear_fetch_progress` from `_databricks_io.py`
- `_set_flatten_progress`/`_clear_flatten_progress` from `_json_flatten.py`
- Move to `tests/conftest.py` or `tests/_test_helpers.py`

### L7. Fix sentinel string splitting in `test_scaffold.py`
- Assert `len(parts) == 2` before indexing

### L8. DRY: Parametrize copy-paste tests
- `test_rating_step.py:151-207` — 4 combine tests → 1 parametrized
- `test_databricks_io.py:272-330` — extract mock connector fixture
- `test_container.py:228-261` — delete duplicate of `test_deploy_dispatch.py`

### L9. Fix `test_parser_helpers.py:88-93` — `ast.dump` format dependency
- Assert return type and non-emptiness, not string content

### L10. Fix `test_scaffold.py:258` — string count for YAML structure
- Parse YAML and iterate `doc["jobs"].values()` to check `timeout-minutes`

### L11. `test_modelling.py:123-129` — deterministic split with vague tolerance
- Replace `750 < train_n < 850` with exact value for seed 42

### L12. `test_trace.py:35-42` — `isinstance(str)` without value
- Assert exact stringified value, not just type

### L13. `test_trace.py:230-250` — tests private `_cache.fingerprint`
- Replace with behavioral test: assert both trace results correct, optionally time the second call

### L14. `test_sandbox.py:209-240` — tests monkey-patch mechanism, not security contract
- Replace with: "after safe_joblib_load, subsequent joblib.load of safe object succeeds"

### L15. `test_sandbox.py:154-164` — `isinstance(LinearRegression)` without checking model params
- Add `assert result.get_params() == model.get_params()`

### L16. `test_sandbox.py:47-81` — `TestSafeGlobals` not comprehensive
- Assert `safe_globals()["__builtins__"]` contains exactly the expected allowlist keys

### L17. `test_json_flatten.py:572-577` — `pytest.raises(Exception)` with no post-failure assertion
- Specify `IsADirectoryError` and assert temp file cleaned up

### L18. `test_json_flatten.py:838-845` — `test_stops_after_max_samples` doesn't verify early stop
- Add corrupted records past position 5 that would raise if read

### L19. `test_preserve_markers.py:143-152` — `test_unmatched_start_marker_ignored` missing node check
- Add `assert len(graph.nodes) == 1`

### L20. `test_preserve_markers.py:345` — double roundtrip tests equality but not content
- Add `assert "TODAY = date.today()" in code2`

### L21. `test_modelling.py:500-516` — progress callback assertion weak
- Assert progress values are monotonically increasing and in [0, 1]

### L22. `test_modelling.py:823-840` — SHAP assertions check count, not content
- Assert feature names are correct and x1 has higher SHAP than x2

### L23. `test_parser_helpers.py:836-854` — misleading test name
- Rename `test_external_config_with_transform_code` to `test_factors_path_infers_banding_type`

### L24. `test_parser_helpers.py:856-863` — patches `load_node_config` to return `{}`
- Test is asserting `dict.pop()` happened. Delete or merge into a more meaningful config test.

### L25. `test_parser_helpers.py:370-395` — `_build_rf_nodes` never checks nodeType
- Add `assert nodes[0].data.nodeType == "dataSource"`

### L26. `test_parser_helpers.py:192-195` — submodel detector under-tested
- Add negative tests: `@other_object.node`, `@submodel.connect`, bare `@submodel`

### L27. `test_parser_internals.py:429-431` — fallback parse uses `>= 2` not exact
- Change to `assert len(graph.nodes) == 2` and name-check extracted nodes

### L28. `test_parser_internals.py:534-545` — roundtrip doesn't check transform code
- Add assertion that transform node's `code` config survived the roundtrip

---

## Implementation Order

1. **Critical (C1-C8):** Do first. These protect against production data corruption.
2. **High (H1-H22):** Do second. These close the biggest regression gaps.
3. **Medium (M1-M10):** Do third. These clean up noise and false confidence.
4. **Low (L1-L28):** Do last. These improve maintainability and long-term health.

Estimated total effort: ~3-4 focused sessions for Critical+High, ~2 sessions for Medium+Low.
