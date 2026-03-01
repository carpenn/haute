import type { Node, Edge } from "@xyflow/react"
import type { SimpleNode, SimpleEdge } from "../panels/editors/_shared"

/** Build the graph payload expected by backend API calls. */
export function buildGraph(
  allNodes: SimpleNode[],
  edges: SimpleEdge[],
  submodels?: Record<string, unknown>,
  preamble?: string,
) {
  return {
    nodes: allNodes.map((n) => ({
      id: n.id,
      type: n.type || n.data.nodeType,
      data: n.data,
      position: { x: 0, y: 0 },
    })),
    edges,
    submodels,
    preamble,
  }
}

/** Resolve graph payload from ref objects (parentGraph takes priority). */
export function resolveGraphFromRefs(
  graphRef: React.MutableRefObject<{ nodes: Node[]; edges: Edge[] }>,
  parentGraphRef: React.MutableRefObject<{ nodes: Node[]; edges: Edge[]; submodels: Record<string, unknown> } | null>,
  submodelsRef: React.MutableRefObject<Record<string, unknown>>,
  preambleRef: React.MutableRefObject<string>,
) {
  return parentGraphRef.current
    ? { nodes: parentGraphRef.current.nodes, edges: parentGraphRef.current.edges, submodels: parentGraphRef.current.submodels, preamble: preambleRef.current }
    : { nodes: graphRef.current.nodes, edges: graphRef.current.edges, submodels: submodelsRef.current, preamble: preambleRef.current }
}
