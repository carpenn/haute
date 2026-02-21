import { describe, it, expect } from "vitest"
import { formatValue, formatValueCompact } from "../formatValue"

describe("formatValue", () => {
  it("formats null as 'null'", () => {
    expect(formatValue(null)).toBe("null")
  })

  it("formats undefined as 'null'", () => {
    expect(formatValue(undefined)).toBe("null")
  })

  it("formats integers with locale separators", () => {
    const result = formatValue(1000)
    // toLocaleString is locale-dependent; just verify it's a string representation of 1000
    expect(result).toContain("1")
    expect(result).toContain("000")
  })

  it("formats small integers without separators", () => {
    expect(formatValue(42)).toBe("42")
  })

  it("formats zero", () => {
    expect(formatValue(0)).toBe("0")
  })

  it("formats negative integers", () => {
    const result = formatValue(-5)
    expect(result).toContain("5")
    expect(result).toContain("-")
  })

  it("formats floats with up to 4 fraction digits by default", () => {
    const result = formatValue(3.14159)
    expect(result).toContain("3")
    // Should be truncated/rounded to at most 4 fraction digits
    expect(result.replace(/[^0-9]/g, "").length).toBeLessThanOrEqual(6)
  })

  it("respects custom maxFractionDigits", () => {
    const result = formatValue(1.23456789, 2)
    expect(result).toContain("1")
    // With maxFractionDigits=2, should have at most 2 decimal places
    const parts = result.split(/[.,]/)
    if (parts.length > 1) {
      const lastPart = parts[parts.length - 1]
      expect(lastPart.length).toBeLessThanOrEqual(2)
    }
  })

  it("formats strings via String()", () => {
    expect(formatValue("hello")).toBe("hello")
  })

  it("formats booleans via String()", () => {
    expect(formatValue(true)).toBe("true")
    expect(formatValue(false)).toBe("false")
  })

  it("formats objects via String()", () => {
    expect(formatValue({})).toBe("[object Object]")
  })

  it("formats arrays via String()", () => {
    expect(formatValue([1, 2, 3])).toBe("1,2,3")
  })
})

describe("formatValueCompact", () => {
  it("returns short values unchanged", () => {
    expect(formatValueCompact("short")).toBe("short")
    expect(formatValueCompact(42)).toBe("42")
    expect(formatValueCompact(null)).toBe("null")
  })

  it("truncates values longer than 20 characters", () => {
    const result = formatValueCompact("a]very long string that exceeds twenty chars")
    expect(result.length).toBe(19) // 18 chars + ellipsis
    expect(result).toMatch(/\u2026$/) // ends with ellipsis
  })

  it("does not truncate exactly 20 character values", () => {
    const result = formatValueCompact("12345678901234567890")
    expect(result).toBe("12345678901234567890")
    expect(result.length).toBe(20)
  })

  it("truncates 21+ character values", () => {
    const result = formatValueCompact("123456789012345678901")
    expect(result.length).toBe(19)
    expect(result.endsWith("\u2026")).toBe(true)
  })
})
