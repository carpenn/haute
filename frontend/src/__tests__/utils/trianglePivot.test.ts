import { describe, it, expect } from "vitest"
import { buildTrianglePivot } from "../../utils/trianglePivot"
import type { PivotResult } from "../../utils/trianglePivot"

describe("buildTrianglePivot", () => {
  const sampleRows = [
    { origin: "2020", dev: "12", value: 100 },
    { origin: "2020", dev: "24", value: 200 },
    { origin: "2021", dev: "12", value: 150 },
    { origin: "2021", dev: "24", value: 250 },
    // Second row for same (origin, dev) pair — should be summed
    { origin: "2020", dev: "12", value: 50 },
  ]

  it("produces correct sorted origins and developments", () => {
    const result = buildTrianglePivot(sampleRows, "origin", "dev", "value")
    expect(result.origins).toEqual(["2020", "2021"])
    expect(result.developments).toEqual(["12", "24"])
  })

  it("sums values for the same (origin, dev) pair", () => {
    const result = buildTrianglePivot(sampleRows, "origin", "dev", "value")
    // 2020 / 12 has two rows: 100 + 50 = 150
    expect(result.cells.get("2020")?.get("12")).toBe(150)
    expect(result.cells.get("2020")?.get("24")).toBe(200)
    expect(result.cells.get("2021")?.get("12")).toBe(150)
    expect(result.cells.get("2021")?.get("24")).toBe(250)
  })

  it("leaves missing combinations absent from the cell map", () => {
    const result = buildTrianglePivot(sampleRows, "origin", "dev", "value")
    // No (2021, 36) entry in the data
    expect(result.cells.get("2021")?.get("36")).toBeUndefined()
  })

  it("returns empty result for empty input", () => {
    const result = buildTrianglePivot([], "origin", "dev", "value")
    expect(result.origins).toEqual([])
    expect(result.developments).toEqual([])
    expect(result.cells.size).toBe(0)
  })

  it("treats null/undefined field values as empty string key", () => {
    const rows = [
      { origin: null, dev: "12", value: 10 },
      { origin: undefined, dev: "12", value: 20 },
    ]
    const result = buildTrianglePivot(rows as Record<string, unknown>[], "origin", "dev", "value")
    expect(result.origins).toContain("")
    expect(result.cells.get("")?.get("12")).toBe(30)
  })

  it("treats non-numeric values as 0 (does not crash)", () => {
    const rows = [
      { origin: "2020", dev: "12", value: "not-a-number" },
      { origin: "2020", dev: "12", value: 10 },
    ]
    const result = buildTrianglePivot(rows as Record<string, unknown>[], "origin", "dev", "value")
    expect(result.cells.get("2020")?.get("12")).toBe(10)
  })

  it("handles string numeric values by converting them", () => {
    const rows = [
      { origin: "2020", dev: "12", value: "100" },
      { origin: "2020", dev: "12", value: "50.5" },
    ]
    const result = buildTrianglePivot(rows as Record<string, unknown>[], "origin", "dev", "value")
    expect(result.cells.get("2020")?.get("12")).toBeCloseTo(150.5)
  })

  it("sorts numeric keys numerically, not lexicographically", () => {
    const rows = [
      { origin: "10", dev: "1", value: 1 },
      { origin: "2", dev: "10", value: 1 },
      { origin: "1", dev: "2", value: 1 },
    ]
    const result = buildTrianglePivot(rows, "origin", "dev", "value")
    expect(result.origins).toEqual(["1", "2", "10"])
    expect(result.developments).toEqual(["1", "2", "10"])
  })

  it("sorts non-numeric keys lexicographically", () => {
    const rows = [
      { origin: "zebra", dev: "alpha", value: 1 },
      { origin: "alpha", dev: "zebra", value: 1 },
    ]
    const result = buildTrianglePivot(rows, "origin", "dev", "value")
    expect(result.origins).toEqual(["alpha", "zebra"])
    expect(result.developments).toEqual(["alpha", "zebra"])
  })

  it("correctly returns PivotResult type", () => {
    const result: PivotResult = buildTrianglePivot(sampleRows, "origin", "dev", "value")
    expect(Array.isArray(result.origins)).toBe(true)
    expect(Array.isArray(result.developments)).toBe(true)
    expect(result.cells).toBeInstanceOf(Map)
  })
})
