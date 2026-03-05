/**
 * Tests for ELK-based graph layout utility.
 *
 * Tests: position assignment from ELK output, fallback for missing nodes,
 * empty input handling, edge passthrough to ELK.
 */
import { describe, it, expect, vi, beforeEach } from "vitest"
import { makeNode, makeEdge } from "../../test-utils/factories"

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

beforeEach(() => {
  mockLayout.mockReset()
})

// ── Tests ───────────────────────────────────────────────────────

describe("getLayoutedElements", () => {
  it("assigns positions from ELK output to nodes", async () => {
    const nodes = [makeNode("a"), makeNode("b")]
    const edges = [makeEdge("a", "b", { id: "e1" })]

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
    const nodes = [makeNode("a", "transform", { position: { x: 10, y: 20 } }), makeNode("b", "transform", { position: { x: 30, y: 40 } })]

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
    const edges = [makeEdge("a", "b", { id: "e1" }), makeEdge("b", "c", { id: "e2" })]

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
    const nodes = [makeNode("a", "transform", { position: { x: 99, y: 99 } })]

    mockLayout.mockResolvedValue({
      children: [{ id: "a", x: undefined, y: undefined }],
    })

    const result = await getLayoutedElements(nodes, [])

    expect(result[0].position).toEqual({ x: 0, y: 0 })
  })

  it("handles ELK returning null children array", async () => {
    const nodes = [makeNode("a", "transform", { position: { x: 5, y: 10 } })]

    mockLayout.mockResolvedValue({ children: null })

    const result = await getLayoutedElements(nodes, [])

    // No ELK positions available, should keep original
    expect(result[0].position).toEqual({ x: 5, y: 10 })
  })

  it("snaps nodes with nearly-equal y to the same y", async () => {
    const nodes = [makeNode("a"), makeNode("b"), makeNode("c")]

    mockLayout.mockResolvedValue({
      children: [
        { id: "a", x: 0, y: 100 },
        { id: "b", x: 200, y: 108 },
        { id: "c", x: 400, y: 103 },
      ],
    })

    const result = await getLayoutedElements(nodes, [])

    // All three y-values within threshold (20) → snapped to median (103)
    expect(result[0].position.y).toBe(103)
    expect(result[1].position.y).toBe(103)
    expect(result[2].position.y).toBe(103)
  })

  it("snaps nodes with nearly-equal x to the same x", async () => {
    const nodes = [makeNode("a"), makeNode("b")]

    mockLayout.mockResolvedValue({
      children: [
        { id: "a", x: 300, y: 0 },
        { id: "b", x: 305, y: 150 },
      ],
    })

    const result = await getLayoutedElements(nodes, [])

    expect(result[0].position.x).toBe(result[1].position.x)
  })

  it("does not snap nodes that are far apart", async () => {
    const nodes = [makeNode("a"), makeNode("b")]

    mockLayout.mockResolvedValue({
      children: [
        { id: "a", x: 0, y: 100 },
        { id: "b", x: 0, y: 250 },
      ],
    })

    const result = await getLayoutedElements(nodes, [])

    expect(result[0].position.y).toBe(100)
    expect(result[1].position.y).toBe(250)
  })

  it("creates separate clusters for distinct groups", async () => {
    const nodes = [makeNode("a"), makeNode("b"), makeNode("c"), makeNode("d")]

    mockLayout.mockResolvedValue({
      children: [
        { id: "a", x: 0, y: 100 },
        { id: "b", x: 200, y: 105 },
        { id: "c", x: 0, y: 300 },
        { id: "d", x: 200, y: 308 },
      ],
    })

    const result = await getLayoutedElements(nodes, [])

    // Cluster 1: a,b → snapped together
    expect(result[0].position.y).toBe(result[1].position.y)
    // Cluster 2: c,d → snapped together
    expect(result[2].position.y).toBe(result[3].position.y)
    // Clusters are still distinct
    expect(result[0].position.y).not.toBe(result[2].position.y)
  })
})
