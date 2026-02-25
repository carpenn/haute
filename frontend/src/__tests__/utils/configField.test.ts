/**
 * Tests for configField — a type-safe config accessor that uses nullish
 * coalescing (`??`) to distinguish null/undefined from falsy values like
 * 0, false, and "".
 */
import { describe, it, expect } from "vitest"
import { configField } from "../../utils/configField"

describe("configField", () => {
  // ── Nullish values fall back ─────────────────────────────────────

  it("returns fallback when key is undefined (missing from config)", () => {
    const config: Record<string, unknown> = {}
    expect(configField(config, "missing", "default")).toBe("default")
  })

  it("returns fallback when value is explicitly undefined", () => {
    const config: Record<string, unknown> = { key: undefined }
    expect(configField(config, "key", "default")).toBe("default")
  })

  it("returns fallback when value is null (null ?? fallback = fallback)", () => {
    const config: Record<string, unknown> = { key: null }
    expect(configField(config, "key", "default")).toBe("default")
  })

  // ── Present values are returned (including falsy ones) ───────────

  it("returns string value when present", () => {
    const config: Record<string, unknown> = { name: "Alice" }
    expect(configField(config, "name", "fallback")).toBe("Alice")
  })

  it("returns empty string (NOT fallback) — key ?? vs || difference", () => {
    const config: Record<string, unknown> = { name: "" }
    expect(configField(config, "name", "fallback")).toBe("")
  })

  it("returns 0 (NOT fallback) — key ?? vs || difference", () => {
    const config: Record<string, unknown> = { count: 0 }
    expect(configField(config, "count", 42)).toBe(0)
  })

  it("returns false (NOT fallback) — key ?? vs || difference", () => {
    const config: Record<string, unknown> = { enabled: false }
    expect(configField(config, "enabled", true)).toBe(false)
  })

  // ── Complex values ───────────────────────────────────────────────

  it("returns array value", () => {
    const arr = [1, 2, 3]
    const config: Record<string, unknown> = { items: arr }
    expect(configField(config, "items", [])).toBe(arr)
  })

  it("returns object value", () => {
    const obj = { a: 1, b: 2 }
    const config: Record<string, unknown> = { settings: obj }
    expect(configField(config, "settings", {})).toBe(obj)
  })

  // ── Fallback type preservation ───────────────────────────────────

  it("preserves string fallback type", () => {
    const config: Record<string, unknown> = {}
    const result: string = configField(config, "x", "default")
    expect(result).toBe("default")
    expect(typeof result).toBe("string")
  })

  it("preserves number fallback type", () => {
    const config: Record<string, unknown> = {}
    const result: number = configField(config, "x", 99)
    expect(result).toBe(99)
    expect(typeof result).toBe("number")
  })

  it("preserves boolean fallback type", () => {
    const config: Record<string, unknown> = {}
    const result: boolean = configField(config, "x", true)
    expect(result).toBe(true)
    expect(typeof result).toBe("boolean")
  })

  it("preserves array fallback type", () => {
    const config: Record<string, unknown> = {}
    const fallback = ["a", "b"]
    const result: string[] = configField(config, "x", fallback)
    expect(result).toBe(fallback)
    expect(Array.isArray(result)).toBe(true)
  })
})
