import { describe, it, expect } from "vitest"
import { makePreviewData } from "../makePreviewData"

describe("makePreviewData", () => {
  it("returns defaults when no overrides", () => {
    const result = makePreviewData("n1", "Source")
    expect(result).toEqual({
      nodeId: "n1",
      nodeLabel: "Source",
      status: "ok",
      row_count: 0,
      column_count: 0,
      columns: [],
      preview: [],
      error: null,
    })
  })

  it("applies overrides", () => {
    const result = makePreviewData("n1", "Source", {
      status: "error",
      error: "Something broke",
      row_count: 42,
    })
    expect(result.status).toBe("error")
    expect(result.error).toBe("Something broke")
    expect(result.row_count).toBe(42)
    // Defaults still present for non-overridden fields
    expect(result.columns).toEqual([])
    expect(result.preview).toEqual([])
  })

  it("preserves nodeId and nodeLabel even with overrides", () => {
    const result = makePreviewData("n1", "My Node", { status: "loading" })
    expect(result.nodeId).toBe("n1")
    expect(result.nodeLabel).toBe("My Node")
  })

  it("loading status", () => {
    const result = makePreviewData("n1", "X", { status: "loading" })
    expect(result.status).toBe("loading")
  })
})
