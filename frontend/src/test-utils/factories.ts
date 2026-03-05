/**
 * Shared test factories for creating Node and Edge objects.
 *
 * Eliminates duplicate `makeNode` / `makeEdge` helpers across 8+ test files.
 */

import type { Node, Edge } from "@xyflow/react"
import type { SimpleNode, SimpleEdge } from "../panels/editors/_shared"

// ---------------------------------------------------------------------------
// makeNode — React Flow Node
// ---------------------------------------------------------------------------

/**
 * Create a React Flow `Node` with sensible defaults.
 *
 * @param id        – Node ID (required).
 * @param nodeType  – The application node type stored in `data.nodeType`.
 *                    Defaults to `"transform"`.
 * @param overrides – Any additional fields to merge (position, data, etc.).
 */
export function makeNode(
  id: string,
  nodeType = "transform",
  overrides: Partial<Node> = {},
): Node {
  const { data: dataOverrides, ...rest } = overrides
  return {
    id,
    position: { x: 0, y: 0 },
    type: nodeType,
    data: {
      label: `Node ${id}`,
      nodeType,
      config: {},
      ...dataOverrides,
    },
    ...rest,
  } as Node
}

// ---------------------------------------------------------------------------
// makeEdge — React Flow Edge
// ---------------------------------------------------------------------------

/**
 * Create a React Flow `Edge` with sensible defaults.
 *
 * @param id        – Edge ID. If omitted, derived as `"e_{source}_{target}"`.
 * @param source    – Source node ID.
 * @param target    – Target node ID.
 * @param overrides – Any additional fields to merge.
 */
export function makeEdge(
  source: string,
  target: string,
  overrides: Partial<Edge> & { id?: string } = {},
): Edge {
  const { id = `e_${source}_${target}`, ...rest } = overrides
  return { id, source, target, ...rest } as Edge
}

// ---------------------------------------------------------------------------
// makeSimpleNode — Lightweight SimpleNode (used by buildGraph, NodePanel)
// ---------------------------------------------------------------------------

/**
 * Create a `SimpleNode` (the lighter type used in graph-building utils).
 *
 * @param id        – Node ID (required).
 * @param nodeType  – Application node type. Defaults to `"transform"`.
 * @param overrides – Partial overrides including `type`, `config`, or any
 *                    extra `data` fields.
 */
export function makeSimpleNode(
  id: string,
  nodeType = "transform",
  overrides: {
    type?: string
    config?: Record<string, unknown>
    description?: string
    [key: string]: unknown
  } = {},
): SimpleNode {
  const { type, config, description = "", ...extraData } = overrides
  return {
    id,
    type,
    data: {
      label: `Node ${id}`,
      description,
      nodeType,
      config,
      ...extraData,
    },
  }
}

// ---------------------------------------------------------------------------
// makeSimpleEdge — Lightweight SimpleEdge
// ---------------------------------------------------------------------------

/**
 * Create a `SimpleEdge`.
 */
export function makeSimpleEdge(
  id: string,
  source: string,
  target: string,
): SimpleEdge {
  return { id, source, target }
}
