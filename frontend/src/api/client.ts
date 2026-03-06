/**
 * Typed API client for the Haute backend.
 *
 * Centralizes all fetch() calls with:
 * - Typed request/response interfaces
 * - AbortController support for request cancellation
 * - Configurable timeouts
 * - Consistent error handling via ApiError
 */

import type { Node, Edge } from "@xyflow/react"
import type {
  PipelineGraph,
  SavePipelineResponse,
  TraceResponse,
  NodeResult,
  SinkResponse,
  SubmodelCreateResponse,
  SubmodelGraphResponse,
  DissolveSubmodelResponse,
  SchemaResult,
} from "./types"

export class ApiError extends Error {
  status: number
  detail?: string

  constructor(message: string, status: number, detail?: string) {
    super(message)
    this.name = "ApiError"
    this.status = status
    this.detail = detail
  }
}

async function request<T>(
  url: string,
  options: RequestInit & { timeout?: number } = {},
): Promise<T> {
  const { timeout = 30_000, signal: externalSignal, ...fetchOptions } = options

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeout)

  // If an external signal is provided, abort our controller when it fires
  if (externalSignal) {
    if (externalSignal.aborted) {
      controller.abort()
    } else {
      externalSignal.addEventListener("abort", () => controller.abort(), { once: true })
    }
  }

  try {
    const res = await fetch(url, { ...fetchOptions, signal: controller.signal })
    if (!res.ok) {
      let detail: string | undefined
      try {
        const body = await res.json()
        detail = body.detail || JSON.stringify(body)
      } catch {
        detail = res.statusText
      }
      throw new ApiError(`HTTP ${res.status}`, res.status, detail)
    }
    return await res.json() as T
  } finally {
    clearTimeout(timeoutId)
  }
}

function post<T>(url: string, body: unknown, options: { signal?: AbortSignal; timeout?: number } = {}): Promise<T> {
  return request<T>(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    ...options,
  })
}

function del<T>(url: string, options: { signal?: AbortSignal; timeout?: number } = {}): Promise<T> {
  return request<T>(url, { method: "DELETE", ...options })
}

/** Graph payload accepted by most pipeline endpoints. */
type GraphPayload = { nodes: Node[]; edges: Edge[]; submodels?: Record<string, unknown>; preamble?: string }

// ---------------------------------------------------------------------------
// Pipeline endpoints
// ---------------------------------------------------------------------------

export function loadPipeline(options?: { signal?: AbortSignal }): Promise<PipelineGraph> {
  return request<PipelineGraph>("/api/pipeline", options).catch((err) => {
    if (err instanceof ApiError && err.status === 404) {
      return { nodes: [], edges: [] } as PipelineGraph
    }
    throw err
  })
}

export function previewNode(
  graph: GraphPayload,
  nodeId: string,
  rowLimit: number,
  scenario?: string,
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<NodeResult & { node_id: string }> {
  return post("/api/pipeline/preview", {
    graph, node_id: nodeId, row_limit: rowLimit, scenario: scenario ?? "live",
  }, {
    timeout: 120_000,
    ...options,
  })
}

export function savePipeline(
  payload: {
    name: string
    description: string
    graph: GraphPayload
    preamble: string
    source_file: string
    scenarios?: string[]
    active_scenario?: string
  },
  options?: { signal?: AbortSignal },
): Promise<SavePipelineResponse> {
  return post("/api/pipeline/save", payload, options)
}

export function traceCell(
  payload: {
    graph: GraphPayload
    row_index: number
    target_node_id: string
    column?: string | null
    row_limit?: number
    scenario?: string
  },
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<TraceResponse> {
  return post("/api/pipeline/trace", payload, { timeout: 120_000, ...options })
}

export function executeSink(
  graph: GraphPayload,
  nodeId: string,
  scenario?: string,
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<SinkResponse> {
  return post("/api/pipeline/sink", { graph, node_id: nodeId, scenario: scenario ?? "live" }, { timeout: 300_000, ...options })
}

// ---------------------------------------------------------------------------
// Submodel endpoints
// ---------------------------------------------------------------------------

export function createSubmodel(
  payload: {
    name: string
    node_ids: string[]
    graph: GraphPayload
    preamble: string
    source_file: string
    pipeline_name: string
  },
  options?: { signal?: AbortSignal },
): Promise<SubmodelCreateResponse> {
  return post("/api/submodel/create", payload, options)
}

export function loadSubmodel(
  name: string,
  options?: { signal?: AbortSignal },
): Promise<SubmodelGraphResponse> {
  return request<SubmodelGraphResponse>(
    `/api/submodel/${encodeURIComponent(name)}`,
    options,
  )
}

export function dissolveSubmodel(
  payload: {
    submodel_name: string
    graph: GraphPayload
    preamble: string
    source_file: string
    pipeline_name: string
  },
  options?: { signal?: AbortSignal },
): Promise<DissolveSubmodelResponse> {
  return post("/api/submodel/dissolve", payload, options)
}

// ---------------------------------------------------------------------------
// Schema endpoints
// ---------------------------------------------------------------------------

export function fetchSchema(
  path: string,
  options?: { signal?: AbortSignal },
): Promise<SchemaResult> {
  return request<SchemaResult>(`/api/schema?path=${encodeURIComponent(path)}`, options)
}

export function fetchDatabricksSchema(
  table: string,
  options?: { signal?: AbortSignal },
): Promise<SchemaResult> {
  return request<SchemaResult>(`/api/schema/databricks?table=${encodeURIComponent(table)}`, options)
}

// ---------------------------------------------------------------------------
// Modelling endpoints
// ---------------------------------------------------------------------------

export function checkMlflow(
  options?: { signal?: AbortSignal },
): Promise<{ mlflow_installed?: boolean; backend?: string; databricks_host?: string }> {
  return request("/api/modelling/mlflow/check", options)
}

export function getTrainStatus<T = unknown>(
  jobId: string,
  options?: { signal?: AbortSignal },
): Promise<T> {
  return request<T>(`/api/modelling/train/status/${encodeURIComponent(jobId)}`, options)
}

export function trainModel(
  payload: { graph: GraphPayload; node_id: string; scenario?: string },
  options?: { signal?: AbortSignal },
): Promise<Record<string, unknown>> {
  return post("/api/modelling/train", { ...payload, scenario: payload.scenario ?? "live" }, options)
}

export type TrainEstimate = {
  total_rows?: number | null
  safe_row_limit?: number | null
  estimated_mb: number
  training_mb: number
  available_mb: number
  bytes_per_row: number
  was_downsampled: boolean
  warning?: string | null
  // GPU VRAM estimation (only populated when task_type is GPU)
  gpu_vram_estimated_mb?: number | null
  gpu_vram_available_mb?: number | null
  gpu_warning?: string | null
}

export function estimateTrainingRam(
  payload: { graph: GraphPayload; node_id: string },
  options?: { signal?: AbortSignal },
): Promise<TrainEstimate> {
  return post("/api/modelling/estimate", payload, { timeout: 30_000, ...options })
}

export function exportTraining(
  payload: { graph: GraphPayload; node_id: string; data_path: string },
  options?: { signal?: AbortSignal },
): Promise<{ script?: string }> {
  return post("/api/modelling/export", payload, options)
}

export function logToMlflow(
  payload: { job_id: string; experiment_name?: string | null; model_name?: string | null },
  options?: { signal?: AbortSignal },
): Promise<{ status: string; backend?: string; experiment_name?: string; run_id?: string; run_url?: string | null; tracking_uri?: string; error?: string }> {
  return post("/api/modelling/mlflow/log", payload, { timeout: 120_000, ...options })
}

// ---------------------------------------------------------------------------
// Optimiser endpoints
// ---------------------------------------------------------------------------

export function solveOptimiser(
  payload: { graph: GraphPayload; node_id: string },
  options?: { signal?: AbortSignal },
): Promise<{ status: string; job_id?: string; error?: string }> {
  return post("/api/optimiser/solve", payload, { timeout: 300_000, ...options })
}

export function getOptimiserStatus<T = unknown>(
  jobId: string,
  options?: { signal?: AbortSignal },
): Promise<T> {
  return request<T>(`/api/optimiser/solve/status/${encodeURIComponent(jobId)}`, options)
}

export function applyOptimiser(
  payload: { job_id: string },
  options?: { signal?: AbortSignal },
): Promise<{ status: string; total_objective?: number; constraints?: Record<string, number>; preview?: Record<string, unknown>[]; row_count?: number; error?: string }> {
  return post("/api/optimiser/apply", payload, { timeout: 120_000, ...options })
}

export function saveOptimiser(
  payload: { job_id: string; output_path: string },
  options?: { signal?: AbortSignal },
): Promise<{ status: string; path?: string; message?: string }> {
  return post("/api/optimiser/save", payload, options)
}

export function logOptimiserToMlflow(
  payload: { job_id: string; experiment_name?: string; model_name?: string | null },
  options?: { signal?: AbortSignal },
): Promise<{ status: string; backend?: string; experiment_name?: string; run_id?: string; run_url?: string | null; tracking_uri?: string; error?: string }> {
  return post("/api/optimiser/mlflow/log", payload, options)
}

export function runFrontier(
  payload: { job_id: string; threshold_ranges: Record<string, [number, number]>; n_points_per_dim?: number },
  options?: { signal?: AbortSignal },
): Promise<{ status: string; points: Record<string, unknown>[]; n_points: number; constraint_names: string[] }> {
  return post("/api/optimiser/frontier", payload, { timeout: 120_000, ...options })
}

// ---------------------------------------------------------------------------
// Databricks endpoints
// ---------------------------------------------------------------------------

export function getWarehouses(
  options?: { signal?: AbortSignal },
): Promise<{ warehouses?: { id: string; name: string; http_path: string; state: string; size: string }[] }> {
  return request("/api/databricks/warehouses", options)
}

export function getCatalogs(
  options?: { signal?: AbortSignal },
): Promise<{ catalogs?: { name: string; comment: string }[] }> {
  return request("/api/databricks/catalogs", options)
}

export function getSchemas(
  catalog: string,
  options?: { signal?: AbortSignal },
): Promise<{ schemas?: { name: string; comment: string }[] }> {
  return request(`/api/databricks/schemas?catalog=${encodeURIComponent(catalog)}`, options)
}

export function getTables(
  catalog: string,
  schema: string,
  options?: { signal?: AbortSignal },
): Promise<{ tables?: { name: string; full_name: string; table_type: string; comment: string }[] }> {
  return request(`/api/databricks/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`, options)
}

export function getCacheStatus(
  table: string,
  options?: { signal?: AbortSignal },
): Promise<{ cached: boolean; path?: string; table: string; row_count: number; column_count: number; size_bytes: number; fetched_at: number }> {
  return request(`/api/databricks/cache?table=${encodeURIComponent(table)}`, options)
}

export function getFetchProgress(
  table: string,
  options?: { signal?: AbortSignal },
): Promise<{ active: boolean; rows?: number; elapsed?: number }> {
  return request(`/api/databricks/fetch/progress?table=${encodeURIComponent(table)}`, options)
}

export function fetchDatabricksData(
  payload: { table: string; http_path?: string; query?: string },
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<Record<string, unknown>> {
  return post("/api/databricks/fetch", payload, { timeout: 300_000, ...options })
}

export function deleteCache(
  table: string,
  options?: { signal?: AbortSignal },
): Promise<{ cached: boolean; path?: string; table: string; row_count: number; column_count: number; size_bytes: number; fetched_at: number }> {
  return del(`/api/databricks/cache?table=${encodeURIComponent(table)}`, options)
}

// ---------------------------------------------------------------------------
// JSON cache endpoints
// ---------------------------------------------------------------------------

export function buildJsonCache(
  payload: { path: string; config_path?: string },
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<Record<string, unknown>> {
  return post("/api/json-cache/build", payload, { timeout: 1_800_000, ...options })
}

export function getJsonCacheProgress(
  path: string,
  options?: { signal?: AbortSignal },
): Promise<{ active: boolean; rows?: number; elapsed?: number }> {
  return request(`/api/json-cache/progress?path=${encodeURIComponent(path)}`, options)
}

export function getJsonCacheStatus(
  path: string,
  options?: { signal?: AbortSignal },
): Promise<{ cached: boolean; path?: string; data_path: string; row_count: number; column_count: number; size_bytes: number; cached_at: number }> {
  return request(`/api/json-cache/status?path=${encodeURIComponent(path)}`, options)
}

export function deleteJsonCache(
  path: string,
  options?: { signal?: AbortSignal },
): Promise<{ cached: boolean; data_path: string }> {
  return del(`/api/json-cache?path=${encodeURIComponent(path)}`, options)
}

// ---------------------------------------------------------------------------
// MLflow endpoints (used by ModelScoreEditor + OptimiserApplyEditor)
// ---------------------------------------------------------------------------

export function getExperiments(
  options?: { signal?: AbortSignal },
): Promise<{ experiment_id: string; name: string }[]> {
  return request("/api/mlflow/experiments", options)
}

export function getRuns(
  experimentId: string,
  artifactFilter?: string,
  options?: { signal?: AbortSignal },
): Promise<{ run_id: string; run_name: string; metrics: Record<string, number>; artifacts: string[] }[]> {
  const params = new URLSearchParams({ experiment_id: experimentId })
  if (artifactFilter) params.set("artifact_filter", artifactFilter)
  return request(`/api/mlflow/runs?${params.toString()}`, options)
}

export function getModels(
  options?: { signal?: AbortSignal },
): Promise<{ name: string; latest_versions: { version: string; status: string; run_id: string }[] }[]> {
  return request("/api/mlflow/models", options)
}

export function getModelVersions(
  modelName: string,
  options?: { signal?: AbortSignal },
): Promise<{ version: string; run_id: string; status: string; description: string }[]> {
  return request(`/api/mlflow/model-versions?model_name=${encodeURIComponent(modelName)}`, options)
}

// ---------------------------------------------------------------------------
// File browsing
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Utility endpoints
// ---------------------------------------------------------------------------

export type UtilityFile = { name: string; module: string }

export type UtilityWriteResult = {
  status: string
  name: string
  module: string
  import_line: string
  error?: string | null
  error_line?: number | null
}

export function listUtilityFiles(
  options?: { signal?: AbortSignal },
): Promise<{ files: UtilityFile[] }> {
  return request("/api/utility", options)
}

export function readUtilityFile(
  module: string,
  options?: { signal?: AbortSignal },
): Promise<{ name: string; module: string; content: string }> {
  return request(`/api/utility/${encodeURIComponent(module)}`, options)
}

export function createUtilityFile(
  payload: { name: string; content?: string },
  options?: { signal?: AbortSignal },
): Promise<UtilityWriteResult> {
  return post("/api/utility", payload, options)
}

export function updateUtilityFile(
  module: string,
  content: string,
  options?: { signal?: AbortSignal },
): Promise<UtilityWriteResult> {
  return request(`/api/utility/${encodeURIComponent(module)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content }),
    ...options,
  })
}

export function deleteUtilityFile(
  module: string,
  options?: { signal?: AbortSignal },
): Promise<{ status: string; module: string }> {
  return del(`/api/utility/${encodeURIComponent(module)}`, options)
}

// ---------------------------------------------------------------------------
// File browsing
// ---------------------------------------------------------------------------

export function listFiles(
  dir: string,
  extensions?: string,
  options?: { signal?: AbortSignal },
): Promise<{ items?: { name: string; path: string; type: "file" | "directory"; size?: number }[] }> {
  const params = new URLSearchParams({ dir })
  if (extensions) params.set("extensions", extensions)
  return request(`/api/files?${params.toString()}`, options)
}

// ---------------------------------------------------------------------------
// Git endpoints
// ---------------------------------------------------------------------------

export type GitStatus = {
  branch: string
  is_main: boolean
  is_read_only: boolean
  changed_files: string[]
  main_ahead: boolean
  main_ahead_by: number
  main_last_updated: string | null
}

export type GitBranch = {
  name: string
  is_yours: boolean
  is_current: boolean
  is_archived: boolean
  last_commit_time: string
  commit_count: number
}

export type GitHistoryEntry = {
  sha: string
  short_sha: string
  message: string
  timestamp: string
  files_changed: string[]
}

export function getGitStatus(
  options?: { signal?: AbortSignal },
): Promise<GitStatus> {
  return request("/api/git/status", options)
}

export function listGitBranches(
  options?: { signal?: AbortSignal },
): Promise<{ current: string; branches: GitBranch[] }> {
  return request("/api/git/branches", options)
}

export function createGitBranch(
  description: string,
  options?: { signal?: AbortSignal },
): Promise<{ branch: string }> {
  return post("/api/git/branches", { description }, options)
}

export function switchGitBranch(
  branch: string,
  options?: { signal?: AbortSignal },
): Promise<{ status: string; branch: string }> {
  return post("/api/git/switch", { branch }, options)
}

export function gitSave(
  options?: { signal?: AbortSignal },
): Promise<{ commit_sha: string; message: string; timestamp: string }> {
  return post("/api/git/save", {}, options)
}

export function gitSubmit(
  options?: { signal?: AbortSignal },
): Promise<{ compare_url: string | null; branch: string }> {
  return post("/api/git/submit", {}, options)
}

export function getGitHistory(
  limit?: number,
  options?: { signal?: AbortSignal },
): Promise<{ entries: GitHistoryEntry[] }> {
  const params = limit ? `?limit=${limit}` : ""
  return request(`/api/git/history${params}`, options)
}

export function gitRevert(
  sha: string,
  options?: { signal?: AbortSignal },
): Promise<{ backup_tag: string; reverted_to: string }> {
  return post("/api/git/revert", { sha }, options)
}

export function gitPull(
  options?: { signal?: AbortSignal },
): Promise<{ success: boolean; conflict: boolean; conflict_message: string | null; commits_pulled: number }> {
  return post("/api/git/pull", {}, options)
}

export function gitArchiveBranch(
  branch: string,
  options?: { signal?: AbortSignal },
): Promise<{ archived_as: string }> {
  return post("/api/git/archive", { branch }, options)
}

export function gitDeleteBranch(
  branch: string,
  options?: { signal?: AbortSignal },
): Promise<{ status: string; branch: string }> {
  return request("/api/git/branches", {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ branch }),
    ...options,
  })
}
