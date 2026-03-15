# Bugs & Correctness Issues

All confirmed bugs or high-confidence correctness concerns, ordered by severity.

---

## B1: Deploy bundler silently skips registered-model MODEL_SCORE nodes

**File:** `src/haute/deploy/_bundler.py:65-76`
**Severity:** HIGH
**Agent:** Model Scoring (5)

`collect_artifacts()` only checks `config.get("run_id")` and `config.get("artifact_path")`. If a MODEL_SCORE node uses `sourceType: "registered"`, the bundler silently skips it (the `if not run_id or not artifact_path: continue` guard). The artifact is never bundled. At deploy time, the model would fail or try to download from MLflow at runtime (which may not be accessible from the serving environment).

**Fix:** Resolve registered models to their `run_id` using `resolve_version()` + `client.get_model_version()` before downloading, exactly like `load_mlflow_model()` does.

---

## B2: Codegen triple-quote injection in descriptions

**File:** `src/haute/codegen.py` (all template strings)
**Severity:** MEDIUM-HIGH
**Agent:** Codegen (2)

All templates use `"""{description}"""` for docstrings. If a node description contains `"""`, the generated docstring becomes syntactically invalid. The generated `.py` file will have a syntax error and the file watcher / parser will fail to re-parse it, breaking the bidirectional sync loop.

**Fix:** Sanitize description in `_common_node_fields`: replace `"""` with `"`, or escape triple quotes.

---

## B3: Codegen unescaped curly braces in user-controlled paths

**File:** `src/haute/codegen.py` (multiple `.format()` calls)
**Severity:** MEDIUM-HIGH
**Agent:** Codegen (2)

Many templates use `.format()` with user-controlled values. If a config value contains `{` or `}` (e.g., a file path like `data/{year}/input.parquet`), the `.format()` call will raise `KeyError` or `IndexError`. Crash on save for any user whose file paths contain curly braces.

**Fix:** Escape user-controlled string values going into `.format()` (`{` -> `{{`, `}` -> `}}`), or switch to f-strings with variables.

---

## B4: Codegen DATA_SOURCE missing JSON/JSONL templates

**File:** `src/haute/codegen.py:342-371`
**Severity:** MEDIUM-HIGH
**Agent:** Data Sources (14)

`_gen_data_source()` only generates `scan_csv` or `scan_parquet` templates. It checks `path.endswith(".csv")` and falls through to parquet for everything else. If a user configures a DATA_SOURCE with a `.json` or `.jsonl` file, the codegen produces `pl.scan_parquet("data.json")` which would fail at runtime. The executor's `_build_data_source()` correctly uses `read_source(path)` which handles all extensions, so the GUI preview works fine but the generated `.py` file breaks.

**Fix:** Add `.json` and `.jsonl` extension handling in `_gen_data_source()`, routing to `read_source()` or the appropriate Polars call.

---

## B5: Parser `_extract_connect_calls` inverted receiver check

**File:** `src/haute/_parser_helpers.py:440-445`
**Severity:** MEDIUM
**Agent:** Parser (1)

```python
if isinstance(func.value, ast.Name) and func.value.id != receiver:
    continue
```

This guard only rejects calls when the callee IS an `ast.Name` AND the name doesn't match. If the callee is NOT an `ast.Name` (e.g., `some.module.pipeline.connect("a", "b")`), the check passes through. The intent is to only accept calls on the named receiver, but this accepts anything that isn't a simple `ast.Name` with the wrong name.

**Fix:** Change to positive match: `if not (isinstance(func.value, ast.Name) and func.value.id == receiver): continue`

---

## B6: Parser `extract_submodel_calls` doesn't check receiver name

**File:** `src/haute/_parser_submodels.py:40-44`
**Severity:** MEDIUM
**Agent:** Parser (1)

Any `<name>.submodel("...")` call at module level would be picked up, not just `pipeline.submodel()`. The test file documents this gap explicitly.

**Fix:** Add `func.value.id == "pipeline"` check.

---

## B7: `selected_columns` missing from TypedDicts

**File:** `src/haute/_types.py`
**Severity:** MEDIUM
**Agents:** Codegen (2), Config I/O (4), Error Handling (19) — all three independently found this

`selected_columns` is actively used by `_execute_lazy.py:422`, `_parser_helpers.py:593`, and `codegen.py:683`, but is NOT declared in any TypedDict. The `_config_validation.py` module flags it as "unrecognized" for every node type that uses it, producing false-positive warnings on every parse.

**Fix:** Add `selected_columns: list[str]` to every TypedDict where it can appear (at minimum `TransformConfig`, `DataSourceConfig`, `DataSinkConfig`, `ModelScoreConfig`), or add it to a universal keys set.

---

## B8: `MODEL_SCORE_CONFIG_KEYS` uses snake_case but TypedDict uses camelCase

**File:** `src/haute/_types.py:357-361`
**Severity:** MEDIUM
**Agent:** Schemas (20)

The config key tuple has `"source_type"` but `ModelScoreConfig` TypedDict has `sourceType`. The parser at `_parser_helpers.py:509` has a manual workaround (`config_key = "sourceType" if key == "source_type" else key`). Adding a new snake_case key without a translation will silently store the wrong key name. Similarly, `OPTIMISER_CONFIG_KEYS` has phantom keys `"data_input"` and `"banding_source"` that don't appear in the `OptimiserConfig` TypedDict.

**Fix:** Align the config key tuples with the TypedDict field names.

---

## B9: `haute status` uses wrong catalog/schema defaults

**File:** `src/haute/deploy/_mlflow.py:166-170`
**Severity:** MEDIUM
**Agent:** Deploy (17)

`get_deploy_status()` defaults to `catalog="main", schema="default"`, but the typical config uses `schema="pricing"`. When called from the CLI, only `model_name` is passed, so the UC model name would be `main.default.<name>` instead of `main.pricing.<name>`. The CLI should load the full `DeployConfig` and pass the configured catalog/schema.

---

## B10: Frontend double opacity on trace-dimmed nodes

**File:** `frontend/src/hooks/useTracing.ts:129-152`
**Severity:** MEDIUM
**Agent:** React Flow (10)

Trace dimming is applied in two places simultaneously:
1. `data._traceDimmed = dimmed` -> PipelineNode sets `opacity: 0.3`
2. `style.opacity = dimmed ? 0.4 : 1` -> ReactFlow node wrapper

Result: effective opacity is 0.3 * 0.4 = 0.12 (almost invisible). Almost certainly unintentional.

**Fix:** Remove one of the two opacity applications. Either delete `style.opacity` (lines 145-149) or remove `_traceDimmed` from node data.

---

## B11: Frontend SubmodelDialog missing Escape key handler

**File:** `frontend/src/components/SubmodelDialog.tsx`
**Severity:** LOW-MEDIUM
**Agent:** Frontend Toolbar (13)

Unlike `RenameDialog` and `KeyboardShortcuts`, `SubmodelDialog` has no `useEffect` for Escape key. Users cannot press Escape to close it.

**Fix:** Add Escape key handler matching the pattern in `RenameDialog`.

---

## B12: Frontend scenario slug duplication

**File:** `frontend/src/components/Toolbar.tsx:101` vs `frontend/src/stores/useSettingsStore.ts:100`
**Severity:** LOW-MEDIUM
**Agent:** Frontend Toolbar (13)

The slug transformation (`name.trim().toLowerCase().replace(/\s+/g, "_")`) is computed independently in both the Toolbar and the store's `addScenario`. If either changes without the other, the active scenario reference will mismatch.

**Fix:** Have `addScenario` return the normalized name, or call `setActiveScenario` without transformation.

---

## B13: Rating `_apply_rating_table` crashes on non-numeric defaultValue

**File:** `src/haute/_rating.py:152-153`
**Severity:** LOW-MEDIUM
**Agent:** Rating (15)

If `defaultValue` is `"N/A"` or any non-numeric string, `float(str(default_raw))` raises `ValueError`. The banding side handles this gracefully, but rating does not.

**Fix:** Wrap in try/except `(ValueError, TypeError)`, defaulting to `None`.

---

## B14: Rating `_apply_rating_table` potential row-duplication

**File:** `src/haute/_rating.py:129-149`
**Severity:** LOW-MEDIUM
**Agent:** Rating (15)

If entries contain duplicate factor combinations, the left join produces duplicate rows (fan-out). No deduplication or uniqueness check exists. Users can manually edit config JSON files to trigger this.

**Fix:** Add `lookup = lookup.unique(subset=factors, keep="last")` before the join.

---

## B15: Rating extra columns from entries pollute main frame

**File:** `src/haute/_rating.py:149`
**Severity:** LOW
**Agent:** Rating (15)

If entries contain extra keys beyond factor names and `"value"`, those columns are brought into the main frame by the join. Only `"value"` is explicitly handled.

**Fix:** Select only needed columns before join: `lookup = lookup.select([*factors, "value"])`

---

## B16: Git routes block the event loop

**File:** `src/haute/routes/git.py` (all handlers)
**Severity:** MEDIUM
**Agent:** Routes (8)

All git route handlers are `async def` but call synchronous blocking git operations directly. In FastAPI, `async def` handlers run on the main event loop, so a slow `git push` or `git pull` blocks ALL other requests.

**Fix:** Change `async def` to `def` for all git route handlers (FastAPI auto-threads `def` handlers).

---

## B17: `LRUCache` can't distinguish None values from cache misses

**File:** `src/haute/_lru_cache.py:62` and `src/haute/_io.py:77-79`
**Severity:** LOW
**Agent:** Data Sources (14)

`_object_cache.get(key)` returns `None` both for missing keys and for keys whose value is `None`. A JSON file containing `null` at the top level would be re-loaded on every call.

**Fix:** Use a sentinel pattern: `_MISSING = object()`, check `if value is _MISSING`.

---

## B18: `score_from_config()` uses CWD-relative path resolution

**File:** `src/haute/_model_scorer.py:194`
**Severity:** LOW-MEDIUM
**Agent:** Config I/O (4)

Reads config via `Path(config).read_text()` relative to `Path.cwd()`. If the working directory changes (tests, deploy, subprocess), the config file won't be found. The parser correctly uses `base_dir`, but codegen-generated code bypasses this.

**Fix:** Have generated code resolve relative to `__file__`: `config_dir = Path(__file__).parent / config`.

---

## B19: Codegen sink templates bypass `safe_sink`

**File:** `src/haute/codegen.py:155-169`
**Severity:** LOW-MEDIUM
**Agent:** Data Sources (14)

Templates hardcode `{first}.collect(engine="streaming").write_parquet("{path}")` but the executor uses `safe_sink()` with graceful fallback. Running the generated `.py` directly uses a less robust write path.

**Fix:** Template should delegate to `from haute._polars_utils import safe_sink`.

---

## B20: Frontend `bumpGraphVersion` fires on every drag

**File:** `frontend/src/App.tsx:145`
**Severity:** LOW-MEDIUM
**Agent:** Frontend State (12)

```typescript
useEffect(() => {
    graphRef.current = { nodes, edges }
    bumpGraphVersion()
}, [nodes, edges, bumpGraphVersion])
```

This bumps `graphVersion` on every drag, selection change, or position update — not just structural changes. Column cache and preview cache are invalidated far more often than necessary.

---

## B21: `_resolve_node_config` mutates decorator_kwargs via `.pop()`

**File:** `src/haute/_parser_helpers.py:817`
**Severity:** LOW
**Agent:** Parser (1)

The input dict is mutated in place. Currently safe because the dict is not reused, but fragile for future changes.

---

## B22: Frontend `DataSourceEditor` and `ExternalFileEditor` stale state risk

**Files:** `frontend/src/panels/editors/DataSourceEditor.tsx:22`, `ExternalFileEditor.tsx:22-23`
**Severity:** LOW
**Agent:** Node Editors (11)

These editors use `useState` initialized from `config` props. If config is updated externally (undo/redo, WebSocket sync), the local state won't update. Other editors correctly read from `config` directly.

**Fix:** Follow the `ModelScoreEditor.tsx` pattern — read from config directly, use `onUpdate` to mutate.

---

## B23: Frontend `BandingRulesGrid` uses index-based keys

**File:** `frontend/src/panels/editors/banding/BandingRulesGrid.tsx:33,87`
**Severity:** LOW
**Agent:** Node Editors (11)

Uses `key={i}` for rule rows. When rules are reordered or deleted, React may reuse DOM nodes incorrectly, causing input values to appear in wrong rows.

**Fix:** Generate stable keys for rules (e.g., add a `_id` field when rules are created).

---

## B24: Rating type comment claims "subtract"/"divide" but they're not implemented

**File:** `src/haute/_types.py:140`
**Severity:** LOW
**Agent:** Rating (15)

The TypedDict comment says `# "multiply" | "add" | "subtract" | "divide"` but `_combine_rating_columns()` only implements `multiply`, `add`, `min`, `max`. The `else` branch silently defaults to `multiply`.

**Fix:** Update the comment to match reality: `# "multiply" | "add" | "min" | "max"`.
