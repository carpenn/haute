/**
 * Pure logic tests for rating table utility functions.
 *
 * Tests: normaliseRatingTables, relativityColor, relativityTextColor,
 * tableStats, buildCartesianEntries
 */
import { describe, it, expect } from "vitest"
import {
  normaliseRatingTables,
  relativityColor,
  relativityTextColor,
  tableStats,
  buildCartesianEntries,
} from "../../panels/editors/rating/ratingTableUtils"

// ─── normaliseRatingTables ───────────────────────────────────────

describe("normaliseRatingTables", () => {
  it("returns existing tables when present", () => {
    const tables = [{ name: "T1", factors: ["age"], outputColumn: "af", defaultValue: "1.0", entries: [] }]
    const result = normaliseRatingTables({ tables })
    expect(result).toBe(tables)
  })

  it("returns default table when tables is undefined", () => {
    const result = normaliseRatingTables({})
    expect(result).toHaveLength(1)
    expect(result[0].name).toBe("Table 1")
    expect(result[0].factors).toEqual([])
    expect(result[0].defaultValue).toBe("1.0")
  })

  it("returns default table when tables is empty array", () => {
    const result = normaliseRatingTables({ tables: [] })
    expect(result).toHaveLength(1)
    expect(result[0].name).toBe("Table 1")
  })

  it("returns default table when tables is non-array", () => {
    const result = normaliseRatingTables({ tables: "not an array" })
    expect(result).toHaveLength(1)
  })
})

// ─── relativityColor ─────────────────────────────────────────────

describe("relativityColor", () => {
  it("returns transparent for NaN", () => {
    expect(relativityColor(NaN)).toBe("transparent")
  })

  it("returns transparent for value of exactly 1.0", () => {
    expect(relativityColor(1.0)).toBe("transparent")
  })

  it("returns transparent for values very close to 1.0", () => {
    expect(relativityColor(1.004)).toBe("transparent")
    expect(relativityColor(0.996)).toBe("transparent")
  })

  it("returns red-tinted color for values above 1.005", () => {
    const color = relativityColor(1.1)
    expect(color).toContain("rgba(239, 68, 68")
  })

  it("returns blue-tinted color for values below 0.995", () => {
    const color = relativityColor(0.8)
    expect(color).toContain("rgba(59, 130, 246")
  })

  it("increases alpha with larger deviation", () => {
    const low = relativityColor(1.05)
    const high = relativityColor(1.4)
    // Extract alpha values
    const alphaLow = parseFloat(low.match(/[\d.]+\)$/)?.[0] ?? "0")
    const alphaHigh = parseFloat(high.match(/[\d.]+\)$/)?.[0] ?? "0")
    expect(alphaHigh).toBeGreaterThan(alphaLow)
  })

  it("caps alpha at max deviation (0.5)", () => {
    const color1 = relativityColor(1.5)
    const color2 = relativityColor(2.0)
    // Both should have same max alpha since t is capped at 1
    expect(color1).toBe(color2)
  })
})

// ─── relativityTextColor ──────────────────────────────────────────

describe("relativityTextColor", () => {
  it("returns secondary text color for NaN", () => {
    expect(relativityTextColor(NaN)).toBe("var(--text-secondary)")
  })

  it("returns red for values above 1.005", () => {
    expect(relativityTextColor(1.1)).toBe("#dc2626")
  })

  it("returns blue for values below 0.995", () => {
    expect(relativityTextColor(0.8)).toBe("#2563eb")
  })

  it("returns green for values at 1.0", () => {
    expect(relativityTextColor(1.0)).toBe("#10b981")
  })

  it("returns green for values within ±0.005 of 1.0", () => {
    expect(relativityTextColor(1.003)).toBe("#10b981")
    expect(relativityTextColor(0.997)).toBe("#10b981")
  })
})

// ─── tableStats ──────────────────────────────────────────────────

describe("tableStats", () => {
  it("returns null for empty entries", () => {
    expect(tableStats([])).toBeNull()
  })

  it("returns null when no entries have numeric values", () => {
    expect(tableStats([{ age: "young" }])).toBeNull()
  })

  it("computes correct stats for numeric values", () => {
    const entries = [
      { age: "young", value: 1.1 },
      { age: "mid", value: 1.0 },
      { age: "old", value: 0.9 },
    ]
    const stats = tableStats(entries)
    expect(stats).not.toBeNull()
    expect(stats!.min).toBe(0.9)
    expect(stats!.max).toBe(1.1)
    expect(stats!.avg).toBeCloseTo(1.0, 5)
    expect(stats!.count).toBe(3)
  })

  it("parses string values", () => {
    const entries = [{ value: "1.5" }, { value: "2.5" }]
    const stats = tableStats(entries)
    expect(stats).not.toBeNull()
    expect(stats!.min).toBe(1.5)
    expect(stats!.max).toBe(2.5)
  })

  it("ignores non-numeric string values", () => {
    const entries = [{ value: "abc" }, { value: 2.0 }]
    const stats = tableStats(entries)
    expect(stats).not.toBeNull()
    expect(stats!.count).toBe(1)
    expect(stats!.min).toBe(2.0)
  })

  it("handles single entry", () => {
    const stats = tableStats([{ value: 3.14 }])
    expect(stats).not.toBeNull()
    expect(stats!.min).toBe(3.14)
    expect(stats!.max).toBe(3.14)
    expect(stats!.avg).toBe(3.14)
    expect(stats!.count).toBe(1)
  })

  it("handles entries with missing value key", () => {
    const entries = [{ age: "young" }, { value: 1.5 }]
    const stats = tableStats(entries)
    expect(stats!.count).toBe(1)
  })
})

// ─── buildCartesianEntries ────────────────────────────────────────

describe("buildCartesianEntries", () => {
  const bandingLevels = {
    age_band: ["young", "mid", "old"],
    region: ["north", "south"],
  }

  it("returns empty array for zero factors", () => {
    expect(buildCartesianEntries([], bandingLevels, [], null)).toEqual([])
  })

  it("returns existing entries when a factor has no levels", () => {
    const existing = [{ unknown: "x", value: 1.0 }]
    const result = buildCartesianEntries(["unknown_factor"], bandingLevels, existing, null)
    expect(result).toBe(existing)
  })

  it("builds 1-way cartesian product", () => {
    const result = buildCartesianEntries(["age_band"], bandingLevels, [], "1.0")
    expect(result).toHaveLength(3)
    expect(result.map(e => e.age_band)).toEqual(["young", "mid", "old"])
    expect(result.every(e => e.value === 1.0)).toBe(true)
  })

  it("builds 2-way cartesian product", () => {
    const result = buildCartesianEntries(["age_band", "region"], bandingLevels, [], "1.0")
    expect(result).toHaveLength(6) // 3 * 2
    // Check all combinations exist
    const combos = result.map(e => `${e.age_band}|${e.region}`)
    expect(combos).toContain("young|north")
    expect(combos).toContain("old|south")
  })

  it("preserves existing values", () => {
    const existing = [{ age_band: "young", value: 1.5 }]
    const result = buildCartesianEntries(["age_band"], bandingLevels, existing, "1.0")
    const youngEntry = result.find(e => e.age_band === "young")
    expect(youngEntry?.value).toBe(1.5)
    // Other entries get default
    const midEntry = result.find(e => e.age_band === "mid")
    expect(midEntry?.value).toBe(1.0)
  })

  it("uses 1.0 as default when defaultValue is null", () => {
    const result = buildCartesianEntries(["age_band"], bandingLevels, [], null)
    expect(result.every(e => e.value === 1.0)).toBe(true)
  })

  it("uses 1.0 as default when defaultValue is empty string", () => {
    const result = buildCartesianEntries(["age_band"], bandingLevels, [], "")
    expect(result.every(e => e.value === 1.0)).toBe(true)
  })

  it("parses defaultValue as number", () => {
    const result = buildCartesianEntries(["age_band"], bandingLevels, [], "2.5")
    expect(result.every(e => e.value === 2.5)).toBe(true)
  })

  it("preserves string existing values as numbers", () => {
    const existing = [{ age_band: "young", value: "1.3" }]
    const result = buildCartesianEntries(["age_band"], bandingLevels, existing, "1.0")
    const youngEntry = result.find(e => e.age_band === "young")
    expect(youngEntry?.value).toBe(1.3)
  })

  it("handles 3-way product", () => {
    const levels3 = { ...bandingLevels, size: ["small", "large"] }
    const result = buildCartesianEntries(["age_band", "region", "size"], levels3, [], "1.0")
    expect(result).toHaveLength(12) // 3 * 2 * 2
  })
})
