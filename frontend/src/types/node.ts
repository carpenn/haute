/** Shared node data shape used across hooks and components. */

import type { NodeTypeValue } from "../utils/nodeTypes"

export interface ColumnInfo {
  name: string
  dtype: string
}

/**
 * Base data shape for all Haute pipeline nodes.
 *
 * ReactFlow's Node.data is typed as Record<string, any>. This interface gives
 * typed access to the fields the app actually uses, avoiding scattered
 * `as Record<string, unknown>` casts.
 */
export interface HauteNodeData {
  label: string
  nodeType: NodeTypeValue | string
  description?: string
  config?: Record<string, unknown>
  code?: string
  func_name?: string
  /** Runtime columns from last preview/run — set by usePipelineAPI */
  _columns?: ColumnInfo[]
  /** Full column set before selected_columns filtering — set by usePipelineAPI */
  _availableColumns?: ColumnInfo[]
  /** Schema warnings from last preview — set by usePipelineAPI */
  _schemaWarnings?: { column: string; status: string }[]
  /** Node execution status — set by useTracing */
  _status?: "ok" | "error" | "running"
  _traceActive?: boolean
  _traceDimmed?: boolean
  _traceValue?: unknown
}

/** Type-narrow a ReactFlow node's data to HauteNodeData. */
export function nodeData(node: { data: Record<string, unknown> }): HauteNodeData {
  return node.data as unknown as HauteNodeData
}
