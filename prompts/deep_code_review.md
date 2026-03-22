# Deep Code Review — Haute Library

You are performing an exhaustive code review of the **haute** insurance pricing
pipeline framework (`src/haute/`). This is safety-critical software — a subtle
bug can mean incorrect premiums for thousands of customers. Your job is to find
every real bug, not style nits.

## Scope

Review the **library only** (`src/haute/`, `frontend/src/`). Ignore `main.py`,
test fixtures, and anything in `.venv/`/`node_modules/`.

## How to run

Use **two review passes executed in parallel via background agents (all Opus)**.

### Pass 1 — File-level scan (~44 agents)

Split every source file across agents grouped by subsystem. Each agent reads
all files in its group and reports bugs with exact `file:line` references.

| Group | Files |
|-------|-------|
| Parser & config | `parser.py`, `_parser_helpers.py`, `_parser_regex.py`, `_parser_submodels.py`, `_config_io.py`, `_config_validation.py` |
| Pipeline & executor | `pipeline.py`, `executor.py`, `_execute_lazy.py`, `_builders.py`, `_node_builder.py` |
| Types, schemas, graph | `schemas.py`, `_types.py`, `_flatten.py`, `_json_flatten.py`, `_topo.py`, `_submodel_graph.py`, `graph_utils.py` |
| Caching | `_cache.py`, `_fingerprint_cache.py`, `_lru_cache.py` |
| I/O | `_io.py`, `_databricks_io.py`, `_mlflow_io.py`, `_mlflow_utils.py`, `_optimiser_io.py` |
| Rating & scoring | `_rating.py`, `_model_scorer.py`, `_polars_utils.py` |
| Codegen & sandbox | `codegen.py`, `_sandbox.py`, `_scaffold.py` |
| Modelling (algorithms) | `modelling/__init__.py`, `modelling/_algorithms.py`, `_metrics.py`, `_rustystats.py`, `_split.py`, `_training_job.py`, `_result_types.py` |
| Modelling (export) | `modelling/_export.py`, `_charts.py`, `_mlflow_log.py`, `_model_card.py` |
| Server & routes (1/3) | `server.py`, `routes/__init__.py`, `routes/_helpers.py`, `routes/_save_pipeline.py`, `routes/_job_store.py` |
| Server & routes (2/3) | `routes/pipeline.py`, `routes/modelling.py`, `routes/_train_service.py`, `routes/submodel.py`, `routes/_submodel_ops.py` |
| Server & routes (3/3) | `routes/optimiser.py`, `routes/_optimiser_service.py`, `routes/databricks.py`, `routes/files.py`, `routes/git.py`, `routes/json_cache.py`, `routes/mlflow.py`, `routes/utility.py` |
| CLI | `cli/__init__.py`, `cli/_helpers.py`, `cli/_deploy.py`, `cli/_impact.py`, `cli/_init_cmd.py`, `cli/_lint.py`, `cli/_run.py`, `cli/_serve.py`, `cli/_smoke.py`, `cli/_status.py`, `cli/_train.py` |
| Deploy (1/2) | `deploy/__init__.py`, `deploy/_bundler.py`, `deploy/_config.py`, `deploy/_container.py`, `deploy/_impact.py`, `deploy/_mlflow.py` |
| Deploy (2/2) | `deploy/_model_code.py`, `deploy/_pruner.py`, `deploy/_schema.py`, `deploy/_scorer.py`, `deploy/_utils.py`, `deploy/_validators.py` |
| Misc backend | `__init__.py`, `_ram_estimate.py`, `_logging.py`, `_git.py`, `trace.py`, `discovery.py` |
| Frontend entry & app | `main.tsx`, `App.tsx`, `setupTests.ts` |
| Frontend API & types | `api/client.ts`, `api/types.ts`, `types/banding.ts`, `types/node.ts`, `types/trace.ts` |
| Frontend stores | `stores/useNodeResultsStore.ts`, `stores/useSettingsStore.ts`, `stores/useToastStore.ts`, `stores/useUIStore.ts` |
| Frontend hooks (1/2) | `hooks/useWebSocketSync.ts`, `hooks/usePipelineAPI.ts`, `hooks/useNodeHandlers.ts`, `hooks/useUndoRedo.ts`, `hooks/useKeyboardShortcuts.ts`, `hooks/useBackgroundJobs.ts`, `hooks/useJobPolling.ts` |
| Frontend hooks (2/2) | `hooks/useClickOutside.ts`, `hooks/useConstraintHandlers.ts`, `hooks/useDataInputColumns.ts`, `hooks/useDragResize.ts`, `hooks/useEdgeHandlers.ts`, `hooks/useMlflowBrowser.ts`, `hooks/useSchemaFetch.ts`, `hooks/useSubmodelNavigation.ts`, `hooks/useTracing.ts` |
| Frontend components (1/2) | `components/BreadcrumbBar.tsx`, `components/BreakdownDropdown.tsx`, `components/CacheFetchButton.tsx`, `components/ColumnTable.tsx`, `components/ContextMenu.tsx`, `components/ErrorBoundary.tsx` |
| Frontend components (2/2) | `components/KeyboardShortcuts.tsx`, `components/ModalShell.tsx`, `components/NodeSearch.tsx`, `components/PolarsIcon.tsx`, `components/RenameDialog.tsx`, `components/SubmodelDialog.tsx`, `components/Toast.tsx`, `components/ToggleButtonGroup.tsx`, `components/Toolbar.tsx`, `components/form/ConfigCheckbox.tsx`, `components/form/ConfigInput.tsx`, `components/form/ConfigSelect.tsx`, `components/form/EditorLabel.tsx`, `components/form/index.ts` |
| Frontend nodes | `nodes/PipelineNode.tsx`, `nodes/SubmodelNode.tsx`, `nodes/SubmodelPortNode.tsx` |
| Frontend panels (1/3) | `panels/DataPreview.tsx`, `panels/GitPanel.tsx`, `panels/ImportsPanel.tsx`, `panels/ModellingConfig.tsx`, `panels/ModellingPreview.tsx` |
| Frontend panels (2/3) | `panels/NodePalette.tsx`, `panels/NodePanel.tsx`, `panels/OptimiserConfig.tsx`, `panels/OptimiserDataPreview.tsx`, `panels/OptimiserPreview.tsx` |
| Frontend panels (3/3) | `panels/PanelHeader.tsx`, `panels/PanelShell.tsx`, `panels/TracePanel.tsx`, `panels/UtilityPanel.tsx` |
| Frontend editors (1/3) | `panels/editors/index.ts`, `panels/editors/_shared.tsx`, `panels/editors/ApiInputEditor.tsx`, `panels/editors/BandingEditor.tsx`, `panels/editors/ColumnsTab.tsx`, `panels/editors/ConstantEditor.tsx` |
| Frontend editors (2/3) | `panels/editors/DataSourceEditor.tsx`, `panels/editors/ExternalFileEditor.tsx`, `panels/editors/LiveSwitchEditor.tsx`, `panels/editors/MlflowModelPicker.tsx`, `panels/editors/ModelScoreEditor.tsx`, `panels/editors/OptimiserApplyEditor.tsx` |
| Frontend editors (3/3) | `panels/editors/OutputEditor.tsx`, `panels/editors/SinkEditor.tsx`, `panels/editors/SubmodelEditor.tsx`, `panels/editors/TransformEditor.tsx`, `panels/editors/_DatabricksSelector.tsx`, `panels/editors/banding/BandingRulesGrid.tsx`, `panels/editors/banding/bandingUtils.ts`, `panels/editors/banding/index.ts`, `panels/editors/rating/OneWayEditor.tsx`, `panels/editors/rating/StatsFooter.tsx`, `panels/editors/rating/TwoWayGrid.tsx`, `panels/editors/rating/ratingTableUtils.ts`, `panels/editors/rating/index.ts` |
| Frontend modelling (1/2) | `panels/modelling/AveTab.tsx`, `panels/modelling/FeatureAndAlgorithmConfig.tsx`, `panels/modelling/FeatureBrowser.tsx`, `panels/modelling/FeatureImportance.tsx`, `panels/modelling/FeaturesTab.tsx`, `panels/modelling/GLMCoefficientsTab.tsx`, `panels/modelling/GLMFactorConfig.tsx`, `panels/modelling/GLMRegularizationConfig.tsx`, `panels/modelling/GLMRelativitiesTab.tsx`, `panels/modelling/GLMTargetConfig.tsx` |
| Frontend modelling (2/2) | `panels/modelling/LiftTab.tsx`, `panels/modelling/LossChart.tsx`, `panels/modelling/LossTab.tsx`, `panels/modelling/MlflowExportSection.tsx`, `panels/modelling/PdpTab.tsx`, `panels/modelling/ResidualsTab.tsx`, `panels/modelling/SplitAndMetricsConfig.tsx`, `panels/modelling/SummaryTab.tsx`, `panels/modelling/TargetAndTaskConfig.tsx`, `panels/modelling/TrainingActionsAndResults.tsx`, `panels/modelling/TrainingProgress.tsx`, `panels/modelling/styles.ts` |
| Frontend utils | `utils/banding.ts`, `utils/buildGraph.ts`, `utils/chartHelpers.ts`, `utils/color.ts`, `utils/configField.ts`, `utils/dtypeColors.ts`, `utils/formatBytes.ts`, `utils/formatTime.ts`, `utils/formatValue.ts`, `utils/graphHelpers.ts`, `utils/hoverHandlers.ts`, `utils/layout.ts`, `utils/makePreviewData.ts`, `utils/nodeTypes.ts`, `utils/sanitizeName.ts`, `utils/validateConfigRefs.ts` |
| CI & build | `pyproject.toml`, `.github/workflows/ci.yml`, `.github/workflows/docs.yml`, `frontend/vite.config.ts`, `frontend/vitest.config.ts`, `frontend/eslint.config.js` |

Each agent prompt should say:

> You are reviewing safety-critical insurance pricing software. Read ALL files
> in your group completely. Report ONLY actual bugs — not style preferences.
> For each issue give the exact file path, line number, what the bug is, and
> severity (critical / high / medium / low). Focus on:
> - Incorrect calculations or silent data corruption
> - Null/NaN/Inf propagation
> - Type coercion that loses information
> - Security (injection, path traversal, sandbox escape)
> - Race conditions and thread safety
> - Cache staleness / invalidation failures
> - Edge cases with empty inputs, zero values, missing keys

### Pass 2 — Mechanism-level trace (~52 agents)

Each agent traces **one end-to-end mechanism** across multiple files. This
catches cross-file bugs that Pass 1 misses (e.g., feature-name mismatch
between model training and scoring).

| # | Mechanism | Key files to trace |
|---|-----------|-------------------|
| 1 | Model scoring pipeline | `_model_scorer.py` → `_mlflow_io.py` → `_builders.py` → `deploy/_scorer.py` |
| 2 | Rating table lookup | `_rating.py` → `_builders.py` → `_config_io.py` → `codegen.py` |
| 3 | Graph execution order | `_topo.py` → `_types.py` → `_execute_lazy.py` → `executor.py` → `pipeline.py` |
| 4 | LiveSwitch mechanism | `_builders.py` → `_execute_lazy.py` → `deploy/_pruner.py` |
| 5 | Submodel flatten/collapse | `_submodel_graph.py` → `_flatten.py` → `routes/_submodel_ops.py` |
| 6 | Config save/load roundtrip | `_config_io.py` → `_save_pipeline.py` → `parser.py` → `codegen.py` |
| 7 | Optimiser solve + apply | `routes/_optimiser_service.py` → `_optimiser_io.py` → `_builders.py` |
| 8 | Scenario expander | `_builders.py` → `_execute_lazy.py` → instance node mechanism |
| 9 | Deploy bundle + score | `deploy/_pruner.py` → `_bundler.py` → `_scorer.py` → `_model_code.py` |
| 10 | Training job lifecycle | `_training_job.py` → `_split.py` → `_algorithms.py` → `_rustystats.py` → `_metrics.py` |
| 11 | WebSocket graph sync | `server.py` → `routes/_helpers.py` → `_save_pipeline.py` → frontend `useWebSocketSync.ts` |
| 12 | Polars lazy execution | `_execute_lazy.py` → `_builders.py` → `_polars_utils.py` → `executor.py` |
| 13 | Data source loading | `_builders.py` → `_io.py` → `_databricks_io.py` → `_json_flatten.py` |
| 14 | Parser roundtrip fidelity | Every node type: `codegen.py` → `parser.py` → same graph? |
| 15 | Frontend ↔ backend types | `api/types.ts` vs `schemas.py` vs `_types.py` |
| 16 | Column propagation | `_execute_lazy.py` → `executor.py` → frontend `usePipelineAPI.ts` → `useNodeResultsStore.ts` |
| 17 | GLM training + prediction | `_rustystats.py` → `_mlflow_io.py` → `_model_scorer.py` |
| 18 | CatBoost training + scoring | `_algorithms.py` → `_mlflow_io.py` → `_model_scorer.py` |
| 19 | Preview/trace execution | `executor.py` → `trace.py` → `_fingerprint_cache.py` |
| 20 | Banding codegen roundtrip | `_rating.py` → `_builders.py` → `codegen.py` → parser |
| 21 | Rating step codegen roundtrip | Same path, focusing on multi-table and combine operations |
| 22 | Data sink write | `_polars_utils.py` → `_builders.py` → `_execute_lazy.py` |
| 23 | User code sandbox | `_sandbox.py` → `executor.py` → `_builders.py` → `codegen.py` |
| 24 | MLflow integration | `_mlflow_io.py` → `_mlflow_utils.py` → `_mlflow_log.py` → `deploy/_mlflow.py` |
| 25 | Frontend undo/redo | `useUndoRedo.ts` → `useNodeHandlers.ts` → `useKeyboardShortcuts.ts` |
| 26 | Frontend node editors | All 16 editor files (`ApiInputEditor`, `BandingEditor`, `ColumnsTab`, `ConstantEditor`, `DataSourceEditor`, `ExternalFileEditor`, `LiveSwitchEditor`, `MlflowModelPicker`, `ModelScoreEditor`, `OptimiserApplyEditor`, `OutputEditor`, `SinkEditor`, `SubmodelEditor`, `TransformEditor`, `_DatabricksSelector`, `_shared`) — type preservation, stale state |
| 27 | Frontend modelling config | All modelling panel files vs `_training_job.py` constructor |
| 28 | Optimiser config + apply | `OptimiserConfig.tsx` → `_optimiser_service.py` → `_builders.py` |
| 29 | External file loading | `_io.py` → `_sandbox.py` → `_builders.py` → `graph_utils.py` |
| 30 | Instance node mechanism | `_builders.py` → `_types.py` → `_execute_lazy.py` |
| 31 | Databricks integration | `_databricks_io.py` → `routes/databricks.py` → `_DatabricksSelector.tsx` |
| 32 | Chart/diagnostic accuracy | `_charts.py` → `_metrics.py` → frontend modelling tabs |
| 33 | Error handling + recovery | All routes, executor, training service, optimiser service |
| 34 | Memory management | `_execute_lazy.py`, `_ram_estimate.py`, `executor.py`, `_model_scorer.py` |
| 35 | JSON flatten schema | `_json_flatten.py` end-to-end |
| 36 | Frontend store consistency | All Zustand stores + hooks that consume them |
| 37 | Numeric precision | Rating multiplication chains, Float32/64 boundaries, optimiser |
| 38 | Constant + output nodes | `_builders.py` → `codegen.py` → `deploy/_schema.py` |
| 39 | Training service lifecycle | `routes/_train_service.py` → `routes/_job_store.py` → `routes/modelling.py` → `modelling/_training_job.py` |
| 40 | Codegen injection safety | Every f-string/template in `codegen.py` that interpolates config values |
| 41 | Deploy config + validation | `deploy/_config.py` → `deploy/_validators.py` → `deploy/_utils.py` → `deploy/_container.py` → `deploy/_impact.py` |
| 42 | File & utility routes | `routes/files.py` → `routes/utility.py` → `routes/json_cache.py` → `routes/git.py` |
| 43 | Frontend job polling | `hooks/useBackgroundJobs.ts` → `hooks/useJobPolling.ts` → `stores/useToastStore.ts` → `stores/useUIStore.ts` |
| 44 | Frontend data input flow | `hooks/useDataInputColumns.ts` → `hooks/useSchemaFetch.ts` → `panels/editors/DataSourceEditor.tsx` → `panels/editors/_DatabricksSelector.tsx` |
| 45 | Frontend submodel navigation | `hooks/useSubmodelNavigation.ts` → `components/BreadcrumbBar.tsx` → `nodes/SubmodelNode.tsx` → `nodes/SubmodelPortNode.tsx` → `components/SubmodelDialog.tsx` |
| 46 | Frontend edge + constraint handling | `hooks/useEdgeHandlers.ts` → `hooks/useConstraintHandlers.ts` → `utils/buildGraph.ts` → `utils/graphHelpers.ts` |
| 47 | Frontend tracing flow | `hooks/useTracing.ts` → `panels/TracePanel.tsx` → `types/trace.ts` → backend `trace.py` |
| 48 | Frontend settings + layout | `stores/useSettingsStore.ts` → `stores/useUIStore.ts` → `hooks/useDragResize.ts` → `utils/layout.ts` → `panels/PanelShell.tsx` → `panels/PanelHeader.tsx` |
| 49 | Rating editor roundtrip | `panels/editors/rating/OneWayEditor.tsx` → `rating/TwoWayGrid.tsx` → `rating/ratingTableUtils.ts` → `rating/StatsFooter.tsx` → backend `_rating.py` |
| 50 | Banding editor roundtrip | `panels/editors/banding/BandingRulesGrid.tsx` → `banding/bandingUtils.ts` → `panels/editors/BandingEditor.tsx` → `utils/banding.ts` → backend `_rating.py` |
| 51 | MLflow browser + model picker | `hooks/useMlflowBrowser.ts` → `panels/editors/MlflowModelPicker.tsx` → `panels/modelling/MlflowExportSection.tsx` → `routes/mlflow.py` |
| 52 | CLI command coverage | `cli/__init__.py` → `cli/_helpers.py` → all CLI commands (`_deploy.py`, `_impact.py`, `_init_cmd.py`, `_lint.py`, `_run.py`, `_serve.py`, `_smoke.py`, `_status.py`, `_train.py`) |

Each agent prompt should say:

> Trace this mechanism end-to-end across all listed files. Read each file
> thoroughly. At every boundary between files, verify: Are types preserved?
> Are nulls handled? Can row order change? Are column names consistent?
> What happens with zero-row inputs? What happens under concurrency?
> Report ONLY confirmed bugs with exact file:line references.

## Output format

After all agents complete, compile a single report organised by severity:

```
## CRITICAL — Incorrect pricing / production outage
## HIGH — Deploy safety / data loss
## MEDIUM — Display errors / configuration bugs
## LOW — Edge cases / cosmetic
## CLEAN — Mechanisms verified bug-free
```

For each bug: file:line, one-line description, and a "Why it matters" sentence
tied to insurance pricing impact.

## What NOT to report

- Style preferences (naming, formatting, missing docstrings)
- Theoretical issues that require conditions that can't happen in practice
- Issues in `main.py` (it's a toy example, not the library)
- Issues in test files (unless a test is actively asserting wrong behaviour)
