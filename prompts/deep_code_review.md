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

### Pass 1 — File-level scan (~36 agents)

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
| Modelling (algorithms) | `modelling/_algorithms.py`, `_metrics.py`, `_rustystats.py`, `_split.py`, `_training_job.py`, `_result_types.py` |
| Modelling (export) | `modelling/_export.py`, `_charts.py`, `_mlflow_log.py`, `_model_card.py` |
| Server & routes | `server.py`, `routes/*` (split across 3 agents) |
| CLI | `cli/*` |
| Deploy | `deploy/*` |
| Misc backend | `_ram_estimate.py`, `_logging.py`, `_git.py`, `trace.py`, `discovery.py` |
| Frontend stores & API | `api/`, `stores/`, `types/` |
| Frontend hooks | `hooks/*` (split across 2 agents) |
| Frontend components | `components/*` |
| Frontend nodes | `nodes/*` |
| Frontend panels | `panels/*` (split across 3 agents) |
| Frontend editors | `panels/editors/*` (split across 3 agents) |
| Frontend modelling | `panels/modelling/*` (split across 2 agents) |
| Frontend utils | `utils/*` |
| CI & build | `pyproject.toml`, `.github/workflows/*`, `vite/vitest config`, `eslint` |

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

### Pass 2 — Mechanism-level trace (~40 agents)

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
| 26 | Frontend node editors | All 11 editor files — type preservation, stale state |
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
| 39 | Feature engineering | `utility/features.py` → `main.py` data flow |
| 40 | Codegen injection safety | Every f-string/template in `codegen.py` that interpolates config values |

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
- Issues already documented in `CLAUDE.md` or marked with `# TODO`
