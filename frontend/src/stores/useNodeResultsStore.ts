/**
 * Zustand store for node computation results — previews, optimiser solves, training runs.
 *
 * Results are keyed by node ID and survive panel unmount/remount. This is the
 * core persistence layer that prevents losing expensive computation results
 * when clicking away from a node and back.
 *
 * Cache invalidation:
 *   - Previews: keyed on (nodeId, graphVersion). Stale entries are still
 *     returned for instant display but flagged via getPreview().
 *   - Solve/train results: keyed on (nodeId, configHash). A config change
 *     doesn't delete the old result — it's kept with a staleness flag so
 *     the panel can show "config changed since last run".
 */
import { create } from "zustand"
import type { PreviewData } from "../panels/DataPreview"
import type { SolveResult, OptimiserPreviewData } from "../panels/OptimiserPreview"
import type { ColumnInfo } from "../types/node"

// ─── Types ───────────────────────────────────────────────────────

export type SolveProgress = {
  status: string
  progress: number
  message: string
  elapsed_seconds: number
  result?: SolveResult
}

export type TrainResult = {
  status: string
  metrics: Record<string, number>
  feature_importance: { feature: string; importance: number }[]
  model_path: string
  train_rows: number
  test_rows: number  // validation rows (kept as test_rows for backward compat)
  holdout_rows?: number
  holdout_metrics?: Record<string, number>
  diagnostics_set?: string  // "train" | "validation" | "holdout"
  features?: string[]
  cat_features?: string[]
  error?: string
  best_iteration?: number | null
  loss_history?: { iteration: number; [key: string]: number }[]
  double_lift?: { decile: number; actual: number; predicted: number; count: number }[]
  shap_summary?: { feature: string; mean_abs_shap: number }[]
  feature_importance_loss?: { feature: string; importance: number }[]
  cv_results?: { mean_metrics: Record<string, number>; std_metrics: Record<string, number>; n_folds: number } | null
  ave_per_feature?: { feature: string; type: string; bins: { label: string; exposure: number; avg_actual: number; avg_predicted: number }[] }[]
  residuals_histogram?: { bin_center: number; count: number; weighted_count: number }[]
  residuals_stats?: { mean: number; std: number; skew: number; min: number; max: number }
  actual_vs_predicted?: { actual: number; predicted: number; weight: number }[]
  lorenz_curve?: { cum_weight_frac: number; cum_actual_frac: number }[]
  lorenz_curve_perfect?: { cum_weight_frac: number; cum_actual_frac: number }[]
  pdp_data?: { feature: string; type: string; grid: { value: number | string; avg_prediction: number }[] }[]
  warning?: string | null
  total_source_rows?: number | null
  // GLM-specific
  glm_coefficients?: { feature: string; coefficient: number; std_error: number; z_value: number; p_value: number; significance: string }[]
  glm_relativities?: { feature: string; relativity: number; ci_lower?: number; ci_upper?: number }[]
  glm_fit_statistics?: Record<string, number>
  glm_regularization_path?: { selected_alpha?: number; n_nonzero?: number }
}

export type TrainProgress = {
  status: string
  progress: number
  message: string
  iteration: number
  total_iterations: number
  train_loss: Record<string, number>
  elapsed_seconds: number
  result?: TrainResult
  warning?: string | null
}

interface CachedPreview {
  data: PreviewData
  graphVersion: number
}

interface CachedSolveResult {
  result: SolveResult
  jobId: string
  configHash: string
  /** Constraint config snapshot for OptimiserPreview */
  constraints: Record<string, Record<string, number>>
  nodeLabel: string
}

interface ActiveSolveJob {
  jobId: string
  nodeId: string
  nodeLabel: string
  progress: SolveProgress | null
  error: string | null
  /** Constraint config snapshot for OptimiserPreview */
  constraints: Record<string, Record<string, number>>
  configHash: string
}

interface CachedTrainResult {
  result: TrainResult
  jobId: string
  configHash: string
}

interface ActiveTrainJob {
  jobId: string
  nodeId: string
  nodeLabel: string
  progress: TrainProgress | null
  error: string | null
  configHash: string
}

// ─── Config hashing ──────────────────────────────────────────────

/** Fast djb2 string hash — good enough for staleness detection. */
function djb2(s: string): string {
  let hash = 5381
  for (let i = 0; i < s.length; i++) {
    hash = ((hash << 5) + hash + s.charCodeAt(i)) | 0
  }
  return (hash >>> 0).toString(36)
}

export function hashConfig(config: Record<string, unknown>): string {
  // Strip internal keys that don't affect computation
  const { _nodeId, _columns, _schemaWarnings, ...rest } = config
  void _nodeId; void _columns; void _schemaWarnings
  return djb2(JSON.stringify(rest))
}

// ─── Store ───────────────────────────────────────────────────────

interface NodeResultsState {
  // Preview cache
  previews: Record<string, CachedPreview>

  // Optimiser
  solveResults: Record<string, CachedSolveResult>
  solveJobs: Record<string, ActiveSolveJob>

  // Training
  trainResults: Record<string, CachedTrainResult>
  trainJobs: Record<string, ActiveTrainJob>

  // Column cache — keyed by source node ID, cached across panel mounts
  columnCache: Record<string, { columns: ColumnInfo[]; graphVersion: number }>

  // Graph version — bumped on any node/edge change
  graphVersion: number

  // ── Column cache actions ──
  setColumns: (sourceNodeId: string, columns: ColumnInfo[], graphVersion: number) => void
  getColumns: (sourceNodeId: string) => { columns: ColumnInfo[]; fresh: boolean } | null

  // ── Preview actions ──
  setPreview: (nodeId: string, data: PreviewData, graphVersion: number) => void
  /** Returns cached preview, or null if no entry exists. Caller checks graphVersion for staleness. */
  getPreview: (nodeId: string) => CachedPreview | null
  bumpGraphVersion: () => void

  // ── Optimiser actions ──
  startSolveJob: (nodeId: string, jobId: string, nodeLabel: string, constraints: Record<string, Record<string, number>>, configHash: string) => void
  updateSolveProgress: (nodeId: string, progress: SolveProgress) => void
  completeSolveJob: (nodeId: string, result: SolveResult) => void
  failSolveJob: (nodeId: string, error: string) => void

  // ── Training actions ──
  startTrainJob: (nodeId: string, jobId: string, nodeLabel: string, configHash: string) => void
  updateTrainProgress: (nodeId: string, progress: TrainProgress) => void
  completeTrainJob: (nodeId: string, result: TrainResult) => void
  failTrainJob: (nodeId: string, error: string) => void

  // ── Derived helpers ──
  /** Build OptimiserPreviewData for a node (from completed result or null). */
  getOptimiserPreview: (nodeId: string) => OptimiserPreviewData | null
  /** Return completed training result for a node, or null. */
  getModellingPreview: (nodeId: string) => { result: TrainResult; jobId: string; nodeLabel: string; configHash: string } | null

  // ── Cleanup ──
  clearNode: (nodeId: string) => void
}

const useNodeResultsStore = create<NodeResultsState>()((set, get) => ({
  previews: {},
  columnCache: {},
  solveResults: {},
  solveJobs: {},
  trainResults: {},
  trainJobs: {},
  graphVersion: 0,

  // ── Column cache ──

  setColumns: (sourceNodeId, columns, graphVersion) =>
    set((s) => ({
      columnCache: { ...s.columnCache, [sourceNodeId]: { columns, graphVersion } },
    })),

  getColumns: (sourceNodeId) => {
    const entry = get().columnCache[sourceNodeId]
    if (!entry) return null
    return { columns: entry.columns, fresh: entry.graphVersion === get().graphVersion }
  },

  // ── Preview ──

  setPreview: (nodeId, data, graphVersion) =>
    set((s) => ({
      previews: { ...s.previews, [nodeId]: { data, graphVersion } },
    })),

  getPreview: (nodeId) => get().previews[nodeId] ?? null,

  bumpGraphVersion: () => set((s) => ({ graphVersion: s.graphVersion + 1 })),

  // ── Optimiser ──

  startSolveJob: (nodeId, jobId, nodeLabel, constraints, configHash) =>
    set((s) => ({
      solveJobs: {
        ...s.solveJobs,
        [nodeId]: { jobId, nodeId, nodeLabel, progress: null, error: null, constraints, configHash },
      },
    })),

  updateSolveProgress: (nodeId, progress) =>
    set((s) => {
      const job = s.solveJobs[nodeId]
      if (!job) return s
      return {
        solveJobs: { ...s.solveJobs, [nodeId]: { ...job, progress } },
      }
    }),

  completeSolveJob: (nodeId, result) =>
    set((s) => {
      const job = s.solveJobs[nodeId]
      if (!job) return s
      const { [nodeId]: _, ...remainingJobs } = s.solveJobs
      return {
        solveJobs: remainingJobs,
        solveResults: {
          ...s.solveResults,
          [nodeId]: {
            result,
            jobId: job.jobId,
            configHash: job.configHash,
            constraints: job.constraints,
            nodeLabel: job.nodeLabel,
          },
        },
      }
    }),

  failSolveJob: (nodeId, error) =>
    set((s) => {
      const job = s.solveJobs[nodeId]
      if (!job) return s
      return {
        solveJobs: {
          ...s.solveJobs,
          [nodeId]: { ...job, progress: null, error },
        },
      }
    }),

  // ── Training ──

  startTrainJob: (nodeId, jobId, nodeLabel, configHash) =>
    set((s) => ({
      trainJobs: {
        ...s.trainJobs,
        [nodeId]: { jobId, nodeId, nodeLabel, progress: null, error: null, configHash },
      },
    })),

  updateTrainProgress: (nodeId, progress) =>
    set((s) => {
      const job = s.trainJobs[nodeId]
      if (!job) return s
      return {
        trainJobs: { ...s.trainJobs, [nodeId]: { ...job, progress } },
      }
    }),

  completeTrainJob: (nodeId, result) =>
    set((s) => {
      const job = s.trainJobs[nodeId]
      // Support direct completion (no active job, e.g. sync error/result)
      const { [nodeId]: _, ...remainingJobs } = s.trainJobs
      return {
        trainJobs: remainingJobs,
        trainResults: {
          ...s.trainResults,
          [nodeId]: {
            result,
            jobId: job?.jobId ?? "",
            configHash: job?.configHash ?? "",
          },
        },
      }
    }),

  failTrainJob: (nodeId, error) =>
    set((s) => {
      const job = s.trainJobs[nodeId]
      if (!job) return s
      return {
        trainJobs: {
          ...s.trainJobs,
          [nodeId]: { ...job, progress: null, error },
        },
      }
    }),

  // ── Derived ──

  getOptimiserPreview: (nodeId) => {
    const cached = get().solveResults[nodeId]
    if (!cached) return null
    return {
      result: cached.result,
      jobId: cached.jobId,
      constraints: cached.constraints,
      nodeLabel: cached.nodeLabel,
    }
  },

  getModellingPreview: (nodeId) => {
    const cached = get().trainResults[nodeId]
    if (!cached) return null
    // Only show completed (non-error) results in the preview panel
    if (cached.result.status === "error") return null
    const job = get().trainJobs[nodeId]
    return {
      result: cached.result,
      jobId: cached.jobId,
      nodeLabel: job?.nodeLabel ?? "Model",
      configHash: cached.configHash,
    }
  },

  // ── Cleanup ──

  clearNode: (nodeId) =>
    set((s) => {
      const { [nodeId]: _p, ...previews } = s.previews
      const { [nodeId]: _cc, ...columnCache } = s.columnCache
      const { [nodeId]: _sr, ...solveResults } = s.solveResults
      const { [nodeId]: _sj, ...solveJobs } = s.solveJobs
      const { [nodeId]: _tr, ...trainResults } = s.trainResults
      const { [nodeId]: _tj, ...trainJobs } = s.trainJobs
      return { previews, columnCache, solveResults, solveJobs, trainResults, trainJobs }
    }),
}))

export default useNodeResultsStore
