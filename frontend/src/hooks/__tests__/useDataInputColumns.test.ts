import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { renderHook, cleanup, waitFor } from "@testing-library/react"
import { useDataInputColumns } from "../useDataInputColumns"
import useNodeResultsStore from "../../stores/useNodeResultsStore"

vi.mock("../../api/client", () => ({
  previewNode: vi.fn(),
}))

vi.mock("../../utils/buildGraph", () => ({
  buildGraph: vi.fn(() => ({ nodes: [], edges: [], preamble: "" })),
}))

import { previewNode } from "../../api/client"
const mockPreview = vi.mocked(previewNode)

const sampleColumns = [
  { name: "age", dtype: "i64" },
  { name: "premium", dtype: "f64" },
]

const nodes = [
  { id: "ds1", data: { label: "DS1", description: "", nodeType: "dataSource" } },
  { id: "t1", data: { label: "T1", description: "", nodeType: "transform" } },
]

const edges = [{ id: "e1", source: "ds1", target: "t1" }]

describe("useDataInputColumns", () => {
  beforeEach(() => {
    useNodeResultsStore.setState({
      columnCache: {},
      graphVersion: 0,
    })
    mockPreview.mockReset()
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
  })

  it("returns empty array when dataInput is empty", () => {
    const { result } = renderHook(() => useDataInputColumns("", nodes, edges))
    expect(result.current).toEqual([])
  })

  it("fetches columns from API when no cache exists", async () => {
    mockPreview.mockResolvedValue({ status: "ok", columns: sampleColumns })
    const { result } = renderHook(() => useDataInputColumns("ds1", nodes, edges))
    await waitFor(() => expect(result.current).toHaveLength(2))
    expect(result.current[0].name).toBe("age")
    expect(result.current[1].name).toBe("premium")
  })

  it("uses cached columns immediately when available", () => {
    useNodeResultsStore.setState({
      columnCache: {
        ds1: { columns: sampleColumns, graphVersion: 0 },
      },
      graphVersion: 0,
    })
    const { result } = renderHook(() => useDataInputColumns("ds1", nodes, edges))
    // Cached columns should be available synchronously
    expect(result.current).toHaveLength(2)
    expect(result.current[0].name).toBe("age")
  })

  it("skips API call when cache is fresh (same graphVersion)", () => {
    useNodeResultsStore.setState({
      columnCache: {
        ds1: { columns: sampleColumns, graphVersion: 5 },
      },
      graphVersion: 5,
    })
    renderHook(() => useDataInputColumns("ds1", nodes, edges))
    // Should NOT call previewNode since cache is fresh
    expect(mockPreview).not.toHaveBeenCalled()
  })

  it("refetches when cache is stale (different graphVersion)", async () => {
    useNodeResultsStore.setState({
      columnCache: {
        ds1: { columns: [{ name: "old", dtype: "f64" }], graphVersion: 1 },
      },
      graphVersion: 5,
    })
    const freshCols = [{ name: "new_col", dtype: "str" }]
    mockPreview.mockResolvedValue({ status: "ok", columns: freshCols })
    const { result } = renderHook(() => useDataInputColumns("ds1", nodes, edges))
    // Shows stale cache first
    expect(result.current[0].name).toBe("old")
    // Then updates with fresh data
    await waitFor(() => expect(result.current[0].name).toBe("new_col"))
  })

  it("handles API error gracefully with no cache", async () => {
    mockPreview.mockRejectedValue(new Error("Network error"))
    const { result } = renderHook(() => useDataInputColumns("ds1", nodes, edges))
    await waitFor(() => expect(mockPreview).toHaveBeenCalled())
    // Falls back to empty
    expect(result.current).toEqual([])
  })
})
