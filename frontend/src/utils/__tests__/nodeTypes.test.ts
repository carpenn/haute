import { describe, it, expect } from "vitest"
import { NODE_TYPES, NODE_TYPE_META, SINGLETON_TYPES, SOURCE_ONLY_TYPES, SINK_ONLY_TYPES, nodeTypeIcons, nodeTypeColors, nodeTypeLabels } from "../nodeTypes"

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
    expect(NODE_TYPES.MODELLING).toBe("modelling")
    expect(NODE_TYPES.SUBMODEL).toBe("submodel")
    expect(NODE_TYPES.SUBMODEL_PORT).toBe("submodelPort")
  })

  it("has exactly 13 node types", () => {
    expect(Object.keys(NODE_TYPES)).toHaveLength(13)
  })
})

describe("NODE_TYPE_META", () => {
  it("has metadata for every node type", () => {
    for (const value of Object.values(NODE_TYPES)) {
      const meta = NODE_TYPE_META[value]
      expect(meta).toBeDefined()
      expect(meta.icon).toBeDefined()
      expect(meta.color).toMatch(/^#[0-9a-f]{6}$/i)
      expect(meta.label.length).toBeGreaterThan(0)
    }
  })

  it("has exactly one entry per NODE_TYPES value", () => {
    expect(Object.keys(NODE_TYPE_META)).toHaveLength(Object.keys(NODE_TYPES).length)
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

describe("SOURCE_ONLY_TYPES", () => {
  it("contains dataSource and apiInput", () => {
    expect(SOURCE_ONLY_TYPES.has(NODE_TYPES.DATA_SOURCE)).toBe(true)
    expect(SOURCE_ONLY_TYPES.has(NODE_TYPES.API_INPUT)).toBe(true)
  })

  it("does not contain non-source types", () => {
    expect(SOURCE_ONLY_TYPES.has(NODE_TYPES.TRANSFORM)).toBe(false)
    expect(SOURCE_ONLY_TYPES.has(NODE_TYPES.OUTPUT)).toBe(false)
    expect(SOURCE_ONLY_TYPES.has(NODE_TYPES.DATA_SINK)).toBe(false)
  })

  it("has exactly 2 entries", () => {
    expect(SOURCE_ONLY_TYPES.size).toBe(2)
  })
})

describe("SINK_ONLY_TYPES", () => {
  it("contains output, dataSink, and modelling", () => {
    expect(SINK_ONLY_TYPES.has(NODE_TYPES.OUTPUT)).toBe(true)
    expect(SINK_ONLY_TYPES.has(NODE_TYPES.DATA_SINK)).toBe(true)
    expect(SINK_ONLY_TYPES.has(NODE_TYPES.MODELLING)).toBe(true)
  })

  it("does not contain non-sink types", () => {
    expect(SINK_ONLY_TYPES.has(NODE_TYPES.TRANSFORM)).toBe(false)
    expect(SINK_ONLY_TYPES.has(NODE_TYPES.DATA_SOURCE)).toBe(false)
    expect(SINK_ONLY_TYPES.has(NODE_TYPES.API_INPUT)).toBe(false)
  })

  it("has exactly 3 entries", () => {
    expect(SINK_ONLY_TYPES.size).toBe(3)
  })
})

describe("derived lookups (backward compat)", () => {
  it("nodeTypeIcons has an icon for every node type", () => {
    for (const value of Object.values(NODE_TYPES)) {
      expect(nodeTypeIcons[value]).toBeDefined()
    }
  })

  it("nodeTypeColors has a valid hex color for every node type", () => {
    for (const value of Object.values(NODE_TYPES)) {
      expect(nodeTypeColors[value]).toBeDefined()
      expect(nodeTypeColors[value]).toMatch(/^#[0-9a-f]{6}$/i)
    }
  })

  it("nodeTypeLabels has a non-empty label for every node type", () => {
    for (const value of Object.values(NODE_TYPES)) {
      expect(nodeTypeLabels[value]).toBeDefined()
      expect(nodeTypeLabels[value].length).toBeGreaterThan(0)
    }
  })
})
