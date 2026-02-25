import { describe, it, expect } from "vitest"
import { buildGraph } from "../../utils/buildGraph"
import type { SimpleNode, SimpleEdge } from "../../panels/editors/_shared"

// ─── Helpers ─────────────────────────────────────────────────────

function makeNode(
  id: string,
  nodeType: string,
  overrides?: { type?: string; config?: Record<string, unknown> },
): SimpleNode {
  return {
    id,
    type: overrides?.type,
    data: {
      label: `Node ${id}`,
      description: "",
      nodeType,
      config: overrides?.config,
    },
  }
}

function makeEdge(id: string, source: string, target: string): SimpleEdge {
  return { id, source, target }
}

// ─── Tests ───────────────────────────────────────────────────────

describe("buildGraph", () => {
  it("builds correct graph shape from nodes and edges", () => {
    const nodes: SimpleNode[] = [
      makeNode("n1", "dataSource", { type: "custom" }),
      makeNode("n2", "transform", { type: "custom" }),
    ]
    const edges: SimpleEdge[] = [makeEdge("e1", "n1", "n2")]

    const result = buildGraph(nodes, edges)

    expect(result.nodes).toHaveLength(2)
    expect(result.edges).toBe(edges)

    // Each node should have id, type, data, and a position
    expect(result.nodes[0]).toEqual({
      id: "n1",
      type: "custom",
      data: nodes[0].data,
      position: { x: 0, y: 0 },
    })
    expect(result.nodes[1]).toEqual({
      id: "n2",
      type: "custom",
      data: nodes[1].data,
      position: { x: 0, y: 0 },
    })
  })

  it("handles empty nodes array", () => {
    const result = buildGraph([], [])

    expect(result.nodes).toEqual([])
    expect(result.edges).toEqual([])
    expect(result.submodels).toBeUndefined()
  })

  it("maps node type from data.nodeType when type is undefined", () => {
    const nodes: SimpleNode[] = [
      makeNode("n1", "banding"),  // no type override -> n.type is undefined
    ]

    const result = buildGraph(nodes, [])

    // Should fall back to n.data.nodeType
    expect(result.nodes[0].type).toBe("banding")
  })

  it("prefers n.type over n.data.nodeType when both are present", () => {
    const nodes: SimpleNode[] = [
      makeNode("n1", "transform", { type: "custom" }),
    ]

    const result = buildGraph(nodes, [])

    expect(result.nodes[0].type).toBe("custom")
  })

  it("passes submodels through", () => {
    const submodels = {
      "sub1": { nodes: [], edges: [] },
      "sub2": { nodes: [], edges: [] },
    }

    const result = buildGraph([], [], submodels)

    expect(result.submodels).toBe(submodels)
  })

  it("handles undefined submodels", () => {
    const result = buildGraph([], [])

    expect(result.submodels).toBeUndefined()
  })

  it("preserves all data fields on mapped nodes", () => {
    const nodes: SimpleNode[] = [
      {
        id: "n1",
        type: "custom",
        data: {
          label: "My Source",
          description: "Loads data",
          nodeType: "dataSource",
          config: { path: "/data/input.csv" },
          extraField: "should be preserved",
        },
      },
    ]

    const result = buildGraph(nodes, [])
    const mapped = result.nodes[0]

    expect(mapped.data.label).toBe("My Source")
    expect(mapped.data.description).toBe("Loads data")
    expect(mapped.data.config).toEqual({ path: "/data/input.csv" })
    expect(mapped.data.extraField).toBe("should be preserved")
  })

  it("always sets position to {x: 0, y: 0}", () => {
    const nodes: SimpleNode[] = [
      makeNode("n1", "transform", { type: "custom" }),
      makeNode("n2", "output", { type: "custom" }),
      makeNode("n3", "banding"),
    ]

    const result = buildGraph(nodes, [])

    for (const node of result.nodes) {
      expect(node.position).toEqual({ x: 0, y: 0 })
    }
  })
})
