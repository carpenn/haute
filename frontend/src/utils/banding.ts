import type { BandingFactor } from "../types/banding"
import type { SimpleNode } from "../panels/editors/_shared"
import { NODE_TYPES } from "./nodeTypes"

/** Collect assignment values from a list of banding factors into level sets. */
function collectLevels(
  factors: BandingFactor[],
  target: Record<string, Set<string>>,
): void {
  for (const f of factors) {
    if (!f.outputColumn) continue
    if (!target[f.outputColumn]) target[f.outputColumn] = new Set()
    for (const r of f.rules || []) {
      const a = (r as Record<string, string>).assignment
      if (a) target[f.outputColumn].add(a)
    }
  }
}

/** Convert level sets to sorted arrays, dropping empty entries. */
function levelSetsToRecord(sets: Record<string, Set<string>>): Record<string, string[]> {
  const levels: Record<string, string[]> = {}
  for (const [col, s] of Object.entries(sets)) {
    if (s.size > 0) levels[col] = [...s]
  }
  return levels
}

/** Parse factors from a banding node's config, or return null if invalid. */
function parseBandingFactors(node: SimpleNode): BandingFactor[] | null {
  if (node.data.nodeType !== NODE_TYPES.BANDING) return null
  const cfg = (node.data.config || {}) as Record<string, unknown>
  const factors = cfg.factors as BandingFactor[] | undefined
  return Array.isArray(factors) ? factors : null
}

/** Extract factor column -> level names from a single banding node. */
export function extractBandingLevelsForNode(
  allNodes: SimpleNode[],
  nodeId: string,
): Record<string, string[]> {
  const node = allNodes.find(n => n.id === nodeId)
  if (!node) return {}
  const factors = parseBandingFactors(node)
  if (!factors) return {}
  const sets: Record<string, Set<string>> = {}
  collectLevels(factors, sets)
  return levelSetsToRecord(sets)
}

/** Extract factor column -> level names from ALL banding nodes. */
export function extractBandingLevels(
  allNodes: SimpleNode[],
): Record<string, string[]> {
  const sets: Record<string, Set<string>> = {}
  for (const n of allNodes) {
    const factors = parseBandingFactors(n)
    if (!factors) continue
    collectLevels(factors, sets)
  }
  return levelSetsToRecord(sets)
}
