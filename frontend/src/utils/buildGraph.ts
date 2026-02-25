import type { SimpleNode, SimpleEdge } from "../panels/editors/_shared"

/** Build the graph payload expected by backend API calls. */
export function buildGraph(
  allNodes: SimpleNode[],
  edges: SimpleEdge[],
  submodels?: Record<string, unknown>,
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
  }
}
