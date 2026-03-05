import { describe, it, expect } from "vitest"
import { makeNode, makeEdge, makeSimpleNode, makeSimpleEdge } from "../factories"

describe("makeNode", () => {
  it("creates a node with defaults", () => {
    const node = makeNode("n1")
    expect(node.id).toBe("n1")
    expect(node.position).toEqual({ x: 0, y: 0 })
    expect(node.type).toBe("transform")
    expect(node.data.label).toBe("Node n1")
    expect(node.data.nodeType).toBe("transform")
    expect(node.data.config).toEqual({})
  })

  it("accepts custom nodeType", () => {
    const node = makeNode("n2", "dataSource")
    expect(node.type).toBe("dataSource")
    expect(node.data.nodeType).toBe("dataSource")
  })

  it("accepts position override", () => {
    const node = makeNode("n3", "transform", { position: { x: 10, y: 20 } })
    expect(node.position).toEqual({ x: 10, y: 20 })
  })

  it("accepts data overrides that merge with defaults", () => {
    const node = makeNode("n4", "transform", { data: { label: "Custom" } })
    expect(node.data.label).toBe("Custom")
    expect(node.data.nodeType).toBe("transform")
  })

  it("each call returns a distinct object", () => {
    const a = makeNode("a")
    const b = makeNode("a")
    expect(a).not.toBe(b)
    expect(a).toEqual(b)
  })
})

describe("makeEdge", () => {
  it("creates an edge with auto-generated id", () => {
    const edge = makeEdge("n1", "n2")
    expect(edge.id).toBe("e_n1_n2")
    expect(edge.source).toBe("n1")
    expect(edge.target).toBe("n2")
  })

  it("accepts explicit id override", () => {
    const edge = makeEdge("a", "b", { id: "custom_id" })
    expect(edge.id).toBe("custom_id")
    expect(edge.source).toBe("a")
    expect(edge.target).toBe("b")
  })

  it("accepts additional edge properties", () => {
    const edge = makeEdge("a", "b", { animated: true })
    expect(edge.animated).toBe(true)
  })
})

describe("makeSimpleNode", () => {
  it("creates a SimpleNode with defaults", () => {
    const node = makeSimpleNode("s1")
    expect(node.id).toBe("s1")
    expect(node.data.label).toBe("Node s1")
    expect(node.data.description).toBe("")
    expect(node.data.nodeType).toBe("transform")
    expect(node.type).toBeUndefined()
  })

  it("accepts type override", () => {
    const node = makeSimpleNode("s2", "banding", { type: "custom" })
    expect(node.type).toBe("custom")
    expect(node.data.nodeType).toBe("banding")
  })

  it("accepts config override", () => {
    const node = makeSimpleNode("s3", "dataSource", { config: { path: "/data.csv" } })
    expect(node.data.config).toEqual({ path: "/data.csv" })
  })

  it("accepts description override", () => {
    const node = makeSimpleNode("s4", "transform", { description: "A node" })
    expect(node.data.description).toBe("A node")
  })
})

describe("makeSimpleEdge", () => {
  it("creates a SimpleEdge with given id, source, target", () => {
    const edge = makeSimpleEdge("e1", "a", "b")
    expect(edge.id).toBe("e1")
    expect(edge.source).toBe("a")
    expect(edge.target).toBe("b")
  })
})
