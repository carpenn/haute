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
  TraceResult,
  NodeResult,
  SinkResponse,
  SubmodelCreateResponse,
  SubmodelGraphResponse,
  DissolveSubmodelResponse,
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
): Promise<NodeResult & { nodeId: string }> {
  return post("/api/pipeline/preview", { graph, nodeId, rowLimit }, {
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
    rowIndex: number
    targetNodeId: string
    column?: string | null
    rowLimit?: number
  },
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<TraceResult> {
  return post("/api/pipeline/trace", payload, { timeout: 120_000, ...options })
}

export function executeSink(
  graph: GraphPayload,
  nodeId: string,
  options?: { signal?: AbortSignal; timeout?: number },
): Promise<SinkResponse> {
  return post("/api/pipeline/sink", { graph, nodeId }, { timeout: 300_000, ...options })
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
