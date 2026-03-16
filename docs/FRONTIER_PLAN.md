# Efficient Frontier Selection — Implementation Plan

## Overview

Transform the optimiser workflow from "solve → optionally run frontier separately" to "solve + frontier upfront → explore trade-offs → select a point → save/log".

The frontier is computed as part of the solve background job. The analyst sees an interactive chart of frontier points, clicks one to see details, and saves/logs that specific point's lambdas.

---

## Design Decisions

| Decision | Choice |
|---|---|
| Frontier computation | Upfront, in the same background thread as solve |
| Frontier type | 1D sweep only (vary one constraint, hold others fixed) |
| Sweep range | User-configurable: min, max, number of steps (in config panel) |
| price-contour changes | None — `solver.solve()` + `solver.frontier()` called sequentially |
| Selection mechanism | `POST /frontier/select` re-solves at point's lambdas, swaps `job.solve_result` |
| Save/Log location | Frontier detail card in bottom panel (removed from config panel) |
| No-frontier fallback | Summary tab has Save/Log when no constraints exist |
| Frontier state | Zustand store (survives panel unmount) |
| MLflow provenance | Frontier CSV artifact + `frontier.selected_point_index` tag |
| Ratebook frontier | Deferred (RatebookOptimiser has no `frontier()` method) |

---

## Phase 1: Backend Schemas

**File: `src/haute/schemas.py`**

1. Add `OptimiserFrontierSelectRequest`:
   - `job_id: str`
   - `point_index: int`

2. Add `OptimiserFrontierSelectResponse`:
   - `status: str`
   - `total_objective: float`
   - `constraints: dict[str, float]`
   - `baseline_objective: float`
   - `baseline_constraints: dict[str, float]`
   - `lambdas: dict[str, float]`
   - `converged: bool`

3. Add optional `frontier: OptimiserFrontierResponse | None` to `OptimiserStatusResponse` so frontier data arrives inline with the solve status poll (no separate API call needed).

4. Add frontier config fields to `OptimiserSolveRequest` (or rely on node config):
   - `frontier_min: float` (default 0.80 — fraction of baseline)
   - `frontier_max: float` (default 1.10)
   - `frontier_steps: int` (default 15)

---

## Phase 2: Backend Service — Frontier in Solve Thread

**File: `src/haute/routes/_optimiser_service.py`**

After `solver.solve(quote_grid)` completes in the background thread:

```
1. Build solve_result dict (existing code)
2. If constraints exist:
   a. Compute threshold_ranges from config (frontier_min/max as fractions of baseline)
   b. Call solver.frontier(quote_grid, threshold_ranges=ranges, n_points_per_dim=frontier_steps)
   c. Store frontier_result.points.to_dicts() on the job
3. If frontier() raises, log warning and continue (non-fatal)
4. atomic_update job with solve_result + frontier_data
```

The frontier config (min, max, steps) comes from the node's `config` dict, which already supports `frontier_enabled`, `frontier_points_per_dim`, and `frontier_threshold_ranges` fields in `_types.py`.

---

## Phase 3: Backend Endpoint — `/frontier/select`

**File: `src/haute/routes/optimiser.py`**

New `POST /api/optimiser/frontier/select`:

```
1. Retrieve completed job (solver, quote_grid, frontier_data)
2. Validate point_index is in range
3. Extract lambdas from frontier point (lambda_{name} columns)
4. Re-solve: solver.solve(quote_grid, lambdas=selected_lambdas)
5. Swap job.solve_result via atomic_update
6. Store original_solve_result (first time only, for revert)
7. Store selected_frontier_point index
8. Return new objective/constraints/lambdas
```

**Also modify:**

- `solve_status` endpoint: include `frontier` data in response when job is completed
- `mlflow_log` endpoint: log `frontier.csv` artifact + provenance tags (`frontier.selected_point_index`, `frontier.n_points`)
- `save_result` endpoint: include `frontier_selection` metadata in saved JSON

---

## Phase 4: Frontend Types & API Client

**File: `frontend/src/api/types.ts`**

Add `FrontierSelectResponse` type matching the backend schema.

**File: `frontend/src/api/client.ts`**

Add:
```typescript
selectFrontierPoint(payload: { job_id: string; point_index: number }): Promise<FrontierSelectResponse>
```

---

## Phase 5: Frontend Store — Frontier State

**File: `frontend/src/stores/useNodeResultsStore.ts`**

Extend `CachedSolveResult`:
```typescript
{
  result: SolveResult           // swapped on select
  originalResult: SolveResult   // preserved for revert
  jobId: string
  configHash: string
  constraints: Record<string, Record<string, number>>
  nodeLabel: string
  frontier: {                   // NEW — null if no constraints or computation failed
    points: Record<string, unknown>[]
    n_points: number
    constraint_names: string[]
  } | null
  selectedPointIndex: number | null  // NEW
}
```

New actions:
- `selectFrontierPoint(nodeId, pointIndex | null)` — updates selection state
- `updateFrontierAfterSelect(nodeId, pointIndex, selectResult)` — swaps result metrics

Modify:
- `completeSolveJob` — extracts `frontier` from result, stores `originalResult`

---

## Phase 6: Frontend UI — OptimiserPreview Restructure

**File: `frontend/src/panels/OptimiserPreview.tsx`**

### Tab changes
- Tabs: **Frontier** | Summary | Convergence (frontier is default when frontier data exists)
- When no frontier data (no constraints / computation failed): Summary is default

### Frontier tab layout

```
┌─────────────────────────────────────────────────────────────────────────┐
│ [Target] Node Label  "15 points"  [Frontier] [Summary] [Convergence] [v]│
├───────────────────────────────────────┬─────────────────────────────────┤
│                                       │  Point 7 of 15    [◀] [▶]      │
│  Scatter chart (SVG)                  │                                 │
│  Y: objective                         │  OBJECTIVE                      │
│  X: constraint (dropdown if >1)       │  Margin: 1,234,567             │
│                                       │  vs Baseline: +3.2%            │
│      ●  ●  ● [◉] ●  ●  ●           │                                 │
│              ^selected                │  CONSTRAINTS                    │
│  [◎ = current solve]                 │  ● Retention: 92.1%  (min 90%) │
│                                       │  ● Loss ratio: 68.3% (max 70%) │
│  Plot: Obj vs [Retention ▾]          │                                 │
│                                       │  LAMBDAS                        │
│                                       │  retention: 0.0342              │
│                                       │                                 │
│                                       │  ─────────────────────────────  │
│                                       │  [Save Result]  [Log to MLflow] │
└───────────────────────────────────────┴─────────────────────────────────┘
```

### Interactions
- **Click** chart point → detail card populates, calls `/frontier/select`
- **Chevron stepper** in detail card ("Point 7 of 15 ◀ ▶")
- **Current-solve marker** — distinct ring on the chart showing initial solve result
- **Constraint dropdown** — when multiple constraints, pick which one is on X-axis

### Component extraction
- `optimiser/FrontierTab.tsx` — main frontier view (chart + detail card)
- `optimiser/FrontierChart.tsx` — interactive SVG scatter (reuses `buildScales`/`ChartGrid` from OptimiserDataPreview)
- `optimiser/PointDetailCard.tsx` — right-side detail with metrics + action buttons
- `optimiser/SummaryTab.tsx` — extracted from existing summary code
- `optimiser/ConvergenceTab.tsx` — extracted from existing convergence code

---

## Phase 7: Frontend UI — Config Panel Changes

**File: `frontend/src/panels/OptimiserConfig.tsx`**

1. **Add frontier config fields** (in an "Efficient Frontier" section):
   - Min (default 0.80) — lower bound as fraction of baseline
   - Max (default 1.10) — upper bound as fraction of baseline
   - Steps (default 15) — number of points

2. **Remove Save/Log buttons** from the config panel results section. These move to the frontier detail card.

3. **No-frontier fallback**: When there are no constraints, Save/Log buttons remain on the Summary tab in the bottom panel (since there's no frontier to show).

---

## Phase 8: Tests

### Backend (~10 tests)

**File: `tests/test_optimiser_routes.py`**

| Test | What it verifies |
|---|---|
| `test_solve_status_includes_frontier` | Completed solve status has `frontier` with points |
| `test_solve_no_constraints_no_frontier` | No constraints → `frontier` is null |
| `test_solve_frontier_failure_non_fatal` | `solver.frontier()` raises → solve still completes, `frontier` null |
| `test_frontier_select_swaps_result` | Select point → subsequent save uses new lambdas |
| `test_frontier_select_out_of_range` | point_index >= n_points → 400 |
| `test_frontier_select_negative_index` | point_index < 0 → 400 |
| `test_frontier_select_no_frontier` | No frontier data on job → 400 |
| `test_frontier_select_missing_job` | Bad job_id → 404 |
| `test_mlflow_log_frontier_artifact` | MLflow log includes `frontier.csv` |
| `test_mlflow_log_frontier_tags` | After select, MLflow log has `frontier.selected_point_index` tag |

### Frontend (~10 tests)

**File: `frontend/src/panels/__tests__/OptimiserPreview.test.tsx`**

| Test | What it verifies |
|---|---|
| `test_frontier_tab_default_when_data_exists` | Frontier tab is active by default |
| `test_frontier_chart_renders_points` | SVG circles rendered for each point |
| `test_click_point_selects_it` | Click circle → selectedPointIndex updates |
| `test_detail_card_shows_metrics` | Selected point → objective, constraints, lambdas shown |
| `test_save_button_in_detail_card` | Save button calls selectFrontierPoint then saveOptimiser |
| `test_no_frontier_shows_summary` | When frontier null → summary tab default |
| `test_chevron_navigation` | ◀ ▶ step through points |

**File: `frontend/src/panels/__tests__/OptimiserConfig.test.tsx`**

| Test | What it verifies |
|---|---|
| `test_frontier_config_fields_render` | Min, max, steps inputs appear |
| `test_save_buttons_removed_from_config` | No Save/MLflow buttons in config panel |

**File: `frontend/src/stores/__tests__/` (new or extend)**

| Test | What it verifies |
|---|---|
| `test_completeSolveJob_extracts_frontier` | Frontier stored on CachedSolveResult |
| `test_selectFrontierPoint_updates_index` | Selection state changes |
| `test_updateFrontierAfterSelect_swaps_metrics` | Result fields update |

---

## Sequencing

```
Phase 1 (schemas)          ─┐
Phase 2 (service)           ├── Backend (can develop in parallel with Phase 4-5)
Phase 3 (endpoints)        ─┘
Phase 4 (frontend types)   ─┐
Phase 5 (frontend store)    ├── Frontend foundation
Phase 6 (preview UI)        │
Phase 7 (config UI)        ─┘
Phase 8 (tests)             ── After all above
```

Phases 1-3 and 4-5 are independent and can be developed in parallel.

---

## Out of Scope (Future)

- Ratebook frontier (needs `RatebookOptimiser.frontier()` in price-contour)
- Compare mode (pin two frontier points side-by-side)
- Factor table diff view for ratebook points
- Adaptive frontier sampling (refine interesting regions)
- Frontier across pipeline configurations (model A vs model B)
