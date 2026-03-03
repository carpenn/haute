import { describe, it, expect, vi, beforeEach } from "vitest"
import { renderHook, act, waitFor } from "@testing-library/react"
import { useSchemaFetch } from "../useSchemaFetch"
import { fetchSchema } from "../../api/client"

vi.mock("../../api/client", () => ({
  fetchSchema: vi.fn(),
}))

const mockFetchSchema = fetchSchema as ReturnType<typeof vi.fn>

const fakeSchema = {
  path: "data.csv",
  columns: [{ name: "col1", dtype: "Utf8" }],
  row_count: 10,
  column_count: 1,
  preview: [],
}

describe("useSchemaFetch", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("no initialPath: not loading, schema is null", () => {
    const { result } = renderHook(() => useSchemaFetch())
    expect(result.current.loading).toBe(false)
    expect(result.current.schema).toBeNull()
  })

  it("initialPath triggers auto-fetch and sets schema on success", async () => {
    mockFetchSchema.mockResolvedValue(fakeSchema)
    const { result } = renderHook(() => useSchemaFetch("data.csv"))
    await waitFor(() => {
      expect(result.current.loading).toBe(false)
    })
    expect(result.current.schema).toEqual(fakeSchema)
    expect(mockFetchSchema).toHaveBeenCalledWith("data.csv")
  })

  it("fetch failure sets schema to null, loading to false", async () => {
    mockFetchSchema.mockRejectedValue(new Error("fail"))
    const { result } = renderHook(() => useSchemaFetch("bad.csv"))
    await waitFor(() => {
      expect(result.current.loading).toBe(false)
    })
    expect(result.current.schema).toBeNull()
  })

  it("fetchForPath triggers manual fetch", async () => {
    mockFetchSchema.mockResolvedValue(fakeSchema)
    const { result } = renderHook(() => useSchemaFetch())
    act(() => {
      result.current.fetchForPath("manual.csv")
    })
    await waitFor(() => {
      expect(result.current.loading).toBe(false)
    })
    expect(result.current.schema).toEqual(fakeSchema)
    expect(mockFetchSchema).toHaveBeenCalledWith("manual.csv")
  })

  it("loading state is true while fetch is pending", async () => {
    let resolvePromise: (value: unknown) => void
    mockFetchSchema.mockReturnValue(new Promise((r) => { resolvePromise = r }))
    const { result } = renderHook(() => useSchemaFetch("pending.csv"))
    expect(result.current.loading).toBe(true)
    await act(async () => {
      resolvePromise!(fakeSchema)
    })
    expect(result.current.loading).toBe(false)
  })
})
