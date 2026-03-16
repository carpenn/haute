/**
 * Validates config fields that reference other node IDs.
 *
 * Scans all nodes' config for fields known to contain node references
 * (data_input, banding_source, instanceOf) and flags any that point to
 * non-existent node IDs. Returns human-readable warnings.
 */
import type { Node } from "@xyflow/react"

/** Config keys that store node ID references. */
const NODE_REF_FIELDS = ["data_input", "banding_source", "instanceOf"] as const

export interface ConfigRefWarning {
  nodeId: string
  nodeLabel: string
  field: string
  referencedId: string
}

export function validateConfigRefs(nodes: Node[]): ConfigRefWarning[] {
  const nodeIds = new Set(nodes.map((n) => n.id))
  const warnings: ConfigRefWarning[] = []

  for (const node of nodes) {
    const config = (node.data?.config ?? {}) as Record<string, unknown>
    const label = (node.data?.label as string) || node.id

    for (const field of NODE_REF_FIELDS) {
      const ref = config[field]
      if (typeof ref === "string" && ref && !nodeIds.has(ref)) {
        warnings.push({ nodeId: node.id, nodeLabel: label, field, referencedId: ref })
      }
    }
  }

  return warnings
}

export function formatConfigRefWarnings(warnings: ConfigRefWarning[]): string {
  if (warnings.length === 0) return ""
  if (warnings.length === 1) {
    const w = warnings[0]
    return `"${w.nodeLabel}" references missing node "${w.referencedId}" in ${w.field}`
  }
  return `${warnings.length} nodes have broken references to missing nodes`
}
