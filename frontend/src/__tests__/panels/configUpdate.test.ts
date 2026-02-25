/**
 * Tests for the config update merge semantics used by NodePanel's handleConfigUpdate.
 *
 * handleConfigUpdate is defined inside the NodePanel component as:
 *
 *   const handleConfigUpdate = (keyOrUpdates: string | Record<string, unknown>, value?: unknown) => {
 *     const newConfig = typeof keyOrUpdates === "string"
 *       ? { ...config, [keyOrUpdates]: value }
 *       : { ...config, ...keyOrUpdates }
 *     if (onUpdateNode) {
 *       onUpdateNode(node.id, { ...node.data, config: newConfig })
 *     }
 *   }
 *
 * We extract and test the merge logic directly since it's a pure transformation.
 */
import { describe, it, expect } from "vitest"

// ── Extract the merge logic from handleConfigUpdate ──

/**
 * Replicates the merge logic from NodePanel.handleConfigUpdate.
 * Given the current config and the update arguments, returns the merged config.
 */
function mergeConfig(
  config: Record<string, unknown>,
  keyOrUpdates: string | Record<string, unknown>,
  value?: unknown,
): Record<string, unknown> {
  return typeof keyOrUpdates === "string"
    ? { ...config, [keyOrUpdates]: value }
    : { ...config, ...keyOrUpdates }
}

// ── Tests ────────────────────────────────────────────────────────

describe("handleConfigUpdate merge semantics", () => {
  it("string key: merges a single key-value pair into config", () => {
    const config = { existing: "value", count: 5 }
    const result = mergeConfig(config, "newKey", "newValue")

    expect(result).toEqual({ existing: "value", count: 5, newKey: "newValue" })
  })

  it("object: merges multiple keys at once", () => {
    const config = { a: 1 }
    const result = mergeConfig(config, { b: 2, c: 3 })

    expect(result).toEqual({ a: 1, b: 2, c: 3 })
  })

  it("overwrites existing keys with string update", () => {
    const config = { solver: "old", tolerance: 0.01 }
    const result = mergeConfig(config, "solver", "new")

    expect(result).toEqual({ solver: "new", tolerance: 0.01 })
  })

  it("overwrites existing keys with object update", () => {
    const config = { solver: "old", tolerance: 0.01, iterations: 100 }
    const result = mergeConfig(config, { solver: "new", iterations: 200 })

    expect(result).toEqual({ solver: "new", tolerance: 0.01, iterations: 200 })
  })

  it("preserves keys not in update (string mode)", () => {
    const config = { a: 1, b: 2, c: 3 }
    const result = mergeConfig(config, "b", 99)

    expect(result.a).toBe(1)
    expect(result.b).toBe(99)
    expect(result.c).toBe(3)
  })

  it("preserves keys not in update (object mode)", () => {
    const config = { a: 1, b: 2, c: 3 }
    const result = mergeConfig(config, { b: 99 })

    expect(result.a).toBe(1)
    expect(result.b).toBe(99)
    expect(result.c).toBe(3)
  })

  it("handles empty config with string update", () => {
    const result = mergeConfig({}, "key", "value")
    expect(result).toEqual({ key: "value" })
  })

  it("handles empty config with object update", () => {
    const result = mergeConfig({}, { a: 1, b: 2 })
    expect(result).toEqual({ a: 1, b: 2 })
  })

  it("handles empty object update (no-op)", () => {
    const config = { a: 1, b: 2 }
    const result = mergeConfig(config, {})
    expect(result).toEqual({ a: 1, b: 2 })
  })

  it("does not mutate the original config", () => {
    const config = { a: 1, b: 2 }
    const frozen = { ...config }
    mergeConfig(config, "a", 99)

    expect(config).toEqual(frozen)
  })

  it("handles nested objects as values", () => {
    const config = { constraints: { premium: { min: 0 } } }
    const result = mergeConfig(config, "constraints", { premium: { min: 10, max: 100 } })

    expect(result.constraints).toEqual({ premium: { min: 10, max: 100 } })
  })

  it("handles null and undefined values", () => {
    const config = { a: "exists" }
    const withNull = mergeConfig(config, "a", null)
    expect(withNull.a).toBeNull()

    const withUndefined = mergeConfig(config, "a", undefined)
    expect(withUndefined.a).toBeUndefined()
  })

  it("string update: value parameter is used literally even if falsy", () => {
    const config = { count: 5 }
    expect(mergeConfig(config, "count", 0)).toEqual({ count: 0 })
    expect(mergeConfig(config, "count", "")).toEqual({ count: "" })
    expect(mergeConfig(config, "count", false)).toEqual({ count: false })
  })
})
