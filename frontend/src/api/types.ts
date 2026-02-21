/** Shared API response/request types for the Haute backend. */

export interface PipelineGraph {
  nodes: import("@xyflow/react").Node[]
  edges: import("@xyflow/react").Edge[]
  pipeline_name?: string
  pipeline_description?: string
  preamble?: string
  source_file?: string
  submodels?: Record<string, unknown>
  warning?: string
}

export interface SchemaWarning {
  column: string
  status: string
}

export interface NodeResult {
  status: string
  row_count?: number
  column_count?: number
  columns?: ColumnInfo[]
  preview?: Record<string, unknown>[]
  error?: string | null
  timing_ms?: number
  timings?: NodeTiming[]
  schema_warnings?: SchemaWarning[]
}

export interface ColumnInfo {
  name: string
  dtype: string
}

export interface NodeTiming {
  nodeId: string
  label: string
  timing_ms: number
}

export interface RunPipelineResponse {
  status: string
  results: Record<string, NodeResult>
}

export interface SavePipelineResponse {
  file: string
  pipeline_name: string
}

export interface TraceStep {
  node_id: string
  label: string
  node_type: string
  input_schema: ColumnInfo[]
  output_schema: ColumnInfo[]
  schema_diff: {
    columns_added: string[]
    columns_removed: string[]
    columns_modified: string[]
    columns_passed: string[]
  }
  row_values: Record<string, unknown>
}

export interface TraceResult {
  status: string
  trace?: {
    target_node_id: string
    column: string | null
    output_value: unknown
    steps: TraceStep[]
    row_id_column: string | null
    row_id_value: unknown
    total_nodes_in_pipeline: number
    nodes_in_trace: number
    execution_ms: number
  }
  error?: string
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

export interface SinkResponse {
  status: string
  message?: string
  rows_written?: number
  path?: string
}
