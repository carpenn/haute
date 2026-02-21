import { describe, it, expect } from "vitest"
import { NODE_TYPES, SINGLETON_TYPES, nodeTypeIcons, nodeTypeColors, nodeTypeLabels } from "../nodeTypes"

describe("NODE_TYPES", () => {
  it("contains all expected node types", () => {
    expect(NODE_TYPES.API_INPUT).toBe("apiInput")
    expect(NODE_TYPES.DATA_SOURCE).toBe("dataSource")
    expect(NODE_TYPES.TRANSFORM).toBe("transform")
    expect(NODE_TYPES.MODEL_SCORE).toBe("modelScore")
    expect(NODE_TYPES.BANDING).toBe("banding")
    expect(NODE_TYPES.RATING_STEP).toBe("ratingStep")
    expect(NODE_TYPES.OUTPUT).toBe("output")
    expect(NODE_TYPES.DATA_SINK).toBe("dataSink")
    expect(NODE_TYPES.EXTERNAL_FILE).toBe("externalFile")
    expect(NODE_TYPES.LIVE_SWITCH).toBe("liveSwitch")
    expect(NODE_TYPES.SUBMODEL).toBe("submodel")
    expect(NODE_TYPES.SUBMODEL_PORT).toBe("submodelPort")
  })

  it("has exactly 12 node types", () => {
    expect(Object.keys(NODE_TYPES)).toHaveLength(12)
  })
})

describe("SINGLETON_TYPES", () => {
  it("contains apiInput, output, and liveSwitch", () => {
    expect(SINGLETON_TYPES.has(NODE_TYPES.API_INPUT)).toBe(true)
    expect(SINGLETON_TYPES.has(NODE_TYPES.OUTPUT)).toBe(true)
    expect(SINGLETON_TYPES.has(NODE_TYPES.LIVE_SWITCH)).toBe(true)
  })

  it("does not contain non-singleton types", () => {
    expect(SINGLETON_TYPES.has(NODE_TYPES.TRANSFORM)).toBe(false)
    expect(SINGLETON_TYPES.has(NODE_TYPES.DATA_SOURCE)).toBe(false)
  })

  it("has exactly 3 entries", () => {
    expect(SINGLETON_TYPES.size).toBe(3)
  })
})

describe("nodeTypeIcons", () => {
  it("has an icon for every node type", () => {
    for (const value of Object.values(NODE_TYPES)) {
      expect(nodeTypeIcons[value]).toBeDefined()
    }
  })
})

describe("nodeTypeColors", () => {
  it("has a color for every node type", () => {
    for (const value of Object.values(NODE_TYPES)) {
      expect(nodeTypeColors[value]).toBeDefined()
      expect(nodeTypeColors[value]).toMatch(/^#[0-9a-f]{6}$/i)
    }
  })
})

describe("nodeTypeLabels", () => {
  it("has a label for every node type", () => {
    for (const value of Object.values(NODE_TYPES)) {
      expect(nodeTypeLabels[value]).toBeDefined()
      expect(nodeTypeLabels[value].length).toBeGreaterThan(0)
    }
  })
})
