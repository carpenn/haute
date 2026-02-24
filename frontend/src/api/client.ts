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
  RunPipelineResponse,
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
type GraphPayload = { nodes: Node[]; edges: Edge[]; submodels?: Record<string, unknown> }

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
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<NodeResult & { node_id: string }> {
  return post("/api/pipeline/preview", { graph, node_id: nodeId, row_limit: rowLimit }, {
    timeout: 120_000,
    ...options,
  })
}

export function runPipeline(
  graph: GraphPayload,
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<RunPipelineResponse> {
  return post("/api/pipeline/run", { graph }, { timeout: 300_000, ...options })
}

export function savePipeline(
  payload: {
    name: string
    description: string
    graph: GraphPayload
    preamble: string
    source_file: string
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
  },
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<TraceResponse> {
  return post("/api/pipeline/trace", payload, { timeout: 120_000, ...options })
}

export function executeSink(
  graph: GraphPayload,
  nodeId: string,
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<SinkResponse> {
  return post("/api/pipeline/sink", { graph, node_id: nodeId }, { timeout: 300_000, ...options })
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
  payload: { graph: GraphPayload; node_id: string },
  options?: { signal?: AbortSignal },
): Promise<Record<string, unknown>> {
  return post("/api/modelling/train", payload, options)
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
  return post("/api/modelling/mlflow/log", payload, options)
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
): Promise<{ active?: boolean; rows?: number; elapsed?: number }> {
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
// MLflow endpoints (used by ModelScoreEditor)
// ---------------------------------------------------------------------------

export function getExperiments(
  options?: { signal?: AbortSignal },
): Promise<{ experiment_id: string; name: string }[]> {
  return request("/api/mlflow/experiments", options)
}

export function getRuns(
  experimentId: string,
  options?: { signal?: AbortSignal },
): Promise<{ run_id: string; run_name: string; metrics: Record<string, number>; artifacts: string[] }[]> {
  return request(`/api/mlflow/runs?experiment_id=${encodeURIComponent(experimentId)}`, options)
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

export function listFiles(
  dir: string,
  extensions?: string,
  options?: { signal?: AbortSignal },
): Promise<{ items?: { name: string; path: string; type: "file" | "directory"; size?: number }[] }> {
  const params = new URLSearchParams({ dir })
  if (extensions) params.set("extensions", extensions)
  return request(`/api/files?${params.toString()}`, options)
}
