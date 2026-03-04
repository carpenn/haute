/** Shared API response/request types for the Haute backend. */

// Re-export canonical types from their source locations
export type { ColumnInfo } from "../types/node"
export type { TraceResult, TraceStep, TraceSchemaDiff } from "../types/trace"

export interface PipelineGraph {
  nodes: import("@xyflow/react").Node[]
  edges: import("@xyflow/react").Edge[]
  pipeline_name?: string
  pipeline_description?: string
  preamble?: string
  source_file?: string
  submodels?: Record<string, unknown>
  warning?: string
  scenarios?: string[]
  active_scenario?: string
}

export interface SchemaWarning {
  column: string
  status: string
}

export interface NodeResult {
  status: string
  row_count?: number
  column_count?: number
  columns?: { name: string; dtype: string }[]
  preview?: Record<string, unknown>[]
  error?: string | null
  error_line?: number | null
  timing_ms?: number
  memory_bytes?: number
  timings?: NodeTiming[]
  memory?: NodeMemory[]
  schema_warnings?: SchemaWarning[]
  node_statuses?: Record<string, string>
}

export interface NodeTiming {
  node_id: string
  label: string
  timing_ms: number
}

export interface NodeMemory {
  node_id: string
  label: string
  memory_bytes: number
}

export interface SavePipelineResponse {
  file: string
  pipeline_name: string
}

export interface SubmodelCreateResponse {
  graph?: PipelineGraph
}

export interface SubmodelGraphResponse {
  graph?: {
    nodes: import("@xyflow/react").Node[]
    edges: import("@xyflow/react").Edge[]
  }
}

export interface DissolveSubmodelResponse {
  graph?: PipelineGraph
}

/** HTTP response envelope for /api/pipeline/trace (wraps TraceResult). */
export interface TraceResponse {
  status: string
  trace?: import("../types/trace").TraceResult
  error?: string
}

export interface SinkResponse {
  status: string
  message?: string
  row_count?: number
  path?: string
  format?: string
}

/** Schema info returned by /api/schema and /api/schema/databricks. */
export interface SchemaResult {
  path: string
  columns: { name: string; dtype: string }[]
  row_count: number
  column_count: number
  preview: Record<string, unknown>[]
}
