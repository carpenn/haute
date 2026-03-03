/**
 * Tests for ELK-based graph layout utility.
 *
 * Tests: position assignment from ELK output, fallback for missing nodes,
 * empty input handling, edge passthrough to ELK.
 */
import { describe, it, expect, vi, beforeEach } from "vitest"
import type { Node, Edge } from "@xyflow/react"

// ── Mock ELK ────────────────────────────────────────────────────

const mockLayout = vi.fn()

vi.mock("elkjs/lib/elk.bundled.js", () => {
  return {
    default: class ELK {
      layout = mockLayout
    },
  }
})

// Import AFTER the mock is registered
const { getLayoutedElements } = await import("../../utils/layout")

// ── Helpers ─────────────────────────────────────────────────────

function makeNode(id: string, x = 0, y = 0): Node {
  return { id, position: { x, y }, data: {} } as Node
}

function makeEdge(id: string, source: string, target: string): Edge {
  return { id, source, target } as Edge
}

beforeEach(() => {
  mockLayout.mockReset()
})

// ── Tests ───────────────────────────────────────────────────────

describe("getLayoutedElements", () => {
  it("assigns positions from ELK output to nodes", async () => {
    const nodes = [makeNode("a", 0, 0), makeNode("b", 0, 0)]
    const edges = [makeEdge("e1", "a", "b")]

    mockLayout.mockResolvedValue({
      children: [
        { id: "a", x: 100, y: 200 },
        { id: "b", x: 300, y: 400 },
      ],
    })

    const result = await getLayoutedElements(nodes, edges)

    expect(result[0].position).toEqual({ x: 100, y: 200 })
    expect(result[1].position).toEqual({ x: 300, y: 400 })
  })

  it("preserves original position for nodes not in ELK output", async () => {
    const nodes = [makeNode("a", 10, 20), makeNode("b", 30, 40)]

    mockLayout.mockResolvedValue({
      children: [{ id: "a", x: 100, y: 200 }],
      // "b" is missing from ELK output
    })

    const result = await getLayoutedElements(nodes, [])

    expect(result[0].position).toEqual({ x: 100, y: 200 })
    expect(result[1].position).toEqual({ x: 30, y: 40 })
  })

  it("returns empty array for empty input", async () => {
    mockLayout.mockResolvedValue({ children: [] })

    const result = await getLayoutedElements([], [])

    expect(result).toEqual([])
  })

  it("passes edges to ELK in the correct format", async () => {
    const nodes = [makeNode("a"), makeNode("b"), makeNode("c")]
    const edges = [makeEdge("e1", "a", "b"), makeEdge("e2", "b", "c")]

    mockLayout.mockResolvedValue({ children: [] })

    await getLayoutedElements(nodes, edges)

    const elkGraph = mockLayout.mock.calls[0][0]
    expect(elkGraph.edges).toEqual([
      { id: "e1", sources: ["a"], targets: ["b"] },
      { id: "e2", sources: ["b"], targets: ["c"] },
    ])
  })

  it("passes nodes to ELK with fixed width/height", async () => {
    const nodes = [makeNode("a"), makeNode("b")]

    mockLayout.mockResolvedValue({ children: [] })

    await getLayoutedElements(nodes, [])

    const elkGraph = mockLayout.mock.calls[0][0]
    expect(elkGraph.children).toEqual([
      { id: "a", width: 240, height: 70 },
      { id: "b", width: 240, height: 70 },
    ])
  })

  it("defaults to x=0, y=0 when ELK returns undefined coordinates", async () => {
    const nodes = [makeNode("a", 99, 99)]

    mockLayout.mockResolvedValue({
      children: [{ id: "a", x: undefined, y: undefined }],
    })

    const result = await getLayoutedElements(nodes, [])

    expect(result[0].position).toEqual({ x: 0, y: 0 })
  })

  it("handles ELK returning null children array", async () => {
    const nodes = [makeNode("a", 5, 10)]

    mockLayout.mockResolvedValue({ children: null })

    const result = await getLayoutedElements(nodes, [])

    // No ELK positions available, should keep original
    expect(result[0].position).toEqual({ x: 5, y: 10 })
  })
})
