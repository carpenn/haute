import { describe, it, expect } from "vitest"
import { getDtypeColor } from "../../utils/dtypeColors"

describe("getDtypeColor", () => {
  // ── Each mapped dtype returns the correct color ────────────────

  it("returns blue for Int64", () => {
    expect(getDtypeColor("Int64")).toBe("text-blue-400")
  })

  it("returns blue for Int32", () => {
    expect(getDtypeColor("Int32")).toBe("text-blue-400")
  })

  it("returns emerald for Float64", () => {
    expect(getDtypeColor("Float64")).toBe("text-emerald-400")
  })

  it("returns emerald for Float32", () => {
    expect(getDtypeColor("Float32")).toBe("text-emerald-400")
  })

  it("returns amber for String", () => {
    expect(getDtypeColor("String")).toBe("text-amber-400")
  })

  it("returns amber for Utf8", () => {
    expect(getDtypeColor("Utf8")).toBe("text-amber-400")
  })

  it("returns purple for Boolean", () => {
    expect(getDtypeColor("Boolean")).toBe("text-purple-400")
  })

  it("returns rose for Date", () => {
    expect(getDtypeColor("Date")).toBe("text-rose-400")
  })

  it("returns rose for Datetime", () => {
    expect(getDtypeColor("Datetime")).toBe("text-rose-400")
  })

  // ── Partial match (includes-based matching) ────────────────────

  it("matches partial dtype string (e.g. Polars.Int64)", () => {
    expect(getDtypeColor("Polars.Int64")).toBe("text-blue-400")
  })

  // ── Fallback for unknown / empty dtypes ────────────────────────

  it("returns fallback for unknown dtype", () => {
    expect(getDtypeColor("Complex128")).toBe("text-slate-400")
  })

  it("returns fallback for empty string", () => {
    expect(getDtypeColor("")).toBe("text-slate-400")
  })
})
