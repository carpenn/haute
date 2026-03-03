import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { renderHook, cleanup, act, waitFor } from "@testing-library/react"
import type { Node, Edge } from "@xyflow/react"
import usePipelineAPI from "../usePipelineAPI"
import useToastStore from "../../stores/useToastStore"
import useSettingsStore from "../../stores/useSettingsStore"
import useUIStore from "../../stores/useUIStore"
import useNodeResultsStore from "../../stores/useNodeResultsStore"

vi.mock("../../api/client", () => ({
  loadPipeline: vi.fn(),
  previewNode: vi.fn(),
  runPipeline: vi.fn(),
  savePipeline: vi.fn(),
  ApiError: class ApiError extends Error {},
}))

vi.mock("../../utils/buildGraph", () => ({
  resolveGraphFromRefs: vi.fn(() => ({ nodes: [], edges: [], preamble: "" })),
}))

vi.mock("../../utils/makePreviewData", () => ({
  makePreviewData: vi.fn((nodeId: string, label: string, opts: Record<string, unknown>) => ({
    nodeId,
    nodeLabel: label,
    status: opts.status || "ok",
    row_count: opts.row_count ?? 0,
    column_count: opts.column_count ?? 0,
    columns: opts.columns ?? [],
    preview: opts.preview ?? [],
    error: opts.error ?? null,
    timing_ms: opts.timing_ms ?? 0,
    memory_bytes: opts.memory_bytes ?? 0,
    timings: opts.timings ?? [],
    memory: opts.memory ?? [],
    schema_warnings: opts.schema_warnings ?? [],
  })),
}))

import { loadPipeline, previewNode, runPipeline, savePipeline } from "../../api/client"
const mockLoad = vi.mocked(loadPipeline)
const mockPreview = vi.mocked(previewNode)
const mockRun = vi.mocked(runPipeline)
const mockSave = vi.mocked(savePipeline)

function makeNode(id: string): Node {
  return { id, position: { x: 0, y: 0 }, data: { label: `Node ${id}`, nodeType: "transform" } } as Node
}

function makeParams(overrides: Partial<Parameters<typeof usePipelineAPI>[0]> = {}) {
  return {
    selectedNode: null as Node | null,
    graphRef: { current: { nodes: [] as Node[], edges: [] as Edge[] } },
    parentGraphRef: { current: null },
    submodelsRef: { current: {} },
    setNodes: vi.fn(),
    setNodesRaw: vi.fn(),
    setEdgesRaw: vi.fn(),
    setPreamble: vi.fn(),
    preambleRef: { current: "" },
    pipelineNameRef: { current: "test" },
    sourceFileRef: { current: "test.py" },
    lastSavedRef: { current: "" },
    nodeIdCounter: { current: 0 },
    ...overrides,
  }
}

describe("usePipelineAPI", () => {
  beforeEach(() => {
    vi.useRealTimers()
    useToastStore.setState({ toasts: [], _toastCounter: 0 })
    useSettingsStore.setState({ rowLimit: 1000, activeScenario: "live", scenarios: ["live"] })
    useUIStore.setState({ dirty: false })
    useNodeResultsStore.setState({ previews: {}, graphVersion: 0, columnCache: {} })
    mockLoad.mockReset()
    mockPreview.mockReset()
    mockRun.mockReset()
    mockSave.mockReset()
  })

  afterEach(() => {
    vi.useRealTimers()
    cleanup()
    vi.restoreAllMocks()
  })

  it("loads pipeline on mount and sets loading to false", async () => {
    mockLoad.mockResolvedValue({
      nodes: [makeNode("n1")],
      edges: [],
      preamble: "import polars as pl",
      pipeline_name: "pricing",
    })
    const params = makeParams()
    const { result } = renderHook(() => usePipelineAPI(params))
    expect(result.current.loading).toBe(true)
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(params.setNodesRaw).toHaveBeenCalled()
    expect(params.setEdgesRaw).toHaveBeenCalled()
    expect(params.setPreamble).toHaveBeenCalledWith("import polars as pl")
  })

  it("shows toast on load failure", async () => {
    mockLoad.mockRejectedValue(new Error("Server down"))
    const params = makeParams()
    renderHook(() => usePipelineAPI(params))
    await waitFor(() => {
      const toasts = useToastStore.getState().toasts
      expect(toasts.some((t) => t.type === "error" && t.text.includes("Server down"))).toBe(true)
    })
  })

  it("handleRun does nothing with empty graph", async () => {
    mockLoad.mockResolvedValue({ nodes: [], edges: [] })
    const params = makeParams()
    params.graphRef.current = { nodes: [], edges: [] }
    const { result } = renderHook(() => usePipelineAPI(params))
    await waitFor(() => expect(result.current.loading).toBe(false))
    act(() => {
      result.current.handleRun()
    })
    expect(mockRun).not.toHaveBeenCalled()
  })

  it("handleRun calls runPipeline and sets status to Done", async () => {
    mockLoad.mockResolvedValue({ nodes: [], edges: [] })
    const n1 = makeNode("n1")
    mockRun.mockResolvedValue({
      results: {
        n1: { status: "ok", columns: [{ name: "a", dtype: "f64" }], preview: [], row_count: 10, column_count: 1, timing_ms: 42, memory_bytes: 100 },
      },
    })
    const params = makeParams({ selectedNode: n1 })
    params.graphRef.current = { nodes: [n1], edges: [] }
    const { result } = renderHook(() => usePipelineAPI(params))
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(async () => {
      result.current.handleRun()
    })
    await waitFor(() => expect(result.current.runStatus).toBe("Done"))
  })

  it("handleRun shows toast on error", async () => {
    mockLoad.mockResolvedValue({ nodes: [], edges: [] })
    mockRun.mockRejectedValue(new Error("Run failed"))
    const params = makeParams()
    params.graphRef.current = { nodes: [makeNode("n1")], edges: [] }
    const { result } = renderHook(() => usePipelineAPI(params))
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(async () => {
      result.current.handleRun()
    })
    await waitFor(() => expect(result.current.runStatus).toBe("Error"))
    const toasts = useToastStore.getState().toasts
    expect(toasts.some((t) => t.text.includes("Run failed"))).toBe(true)
  })

  it("handleSave calls savePipeline and shows success toast", async () => {
    mockLoad.mockResolvedValue({ nodes: [], edges: [] })
    mockSave.mockResolvedValue({ file: "pricing.py" })
    const params = makeParams()
    params.graphRef.current = { nodes: [makeNode("n1")], edges: [] }
    const { result } = renderHook(() => usePipelineAPI(params))
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(async () => {
      result.current.handleSave()
    })
    await waitFor(() => {
      const toasts = useToastStore.getState().toasts
      expect(toasts.some((t) => t.type === "success" && t.text.includes("pricing.py"))).toBe(true)
    })
    expect(useUIStore.getState().dirty).toBe(false)
  })

  it("handleSave shows error toast on failure", async () => {
    mockLoad.mockResolvedValue({ nodes: [], edges: [] })
    mockSave.mockRejectedValue(new Error("disk full"))
    const params = makeParams()
    params.graphRef.current = { nodes: [], edges: [] }
    const { result } = renderHook(() => usePipelineAPI(params))
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(async () => {
      result.current.handleSave()
    })
    await waitFor(() => {
      const toasts = useToastStore.getState().toasts
      expect(toasts.some((t) => t.type === "error")).toBe(true)
    })
  })

  it("loads scenarios from backend", async () => {
    mockLoad.mockResolvedValue({
      nodes: [],
      edges: [],
      scenarios: ["live", "test_scenario"],
      active_scenario: "test_scenario",
    })
    const params = makeParams()
    renderHook(() => usePipelineAPI(params))
    await waitFor(() => {
      expect(useSettingsStore.getState().scenarios).toEqual(["live", "test_scenario"])
      expect(useSettingsStore.getState().activeScenario).toBe("test_scenario")
    })
  })

  it("setPreviewData can be set externally", async () => {
    mockLoad.mockResolvedValue({ nodes: [], edges: [] })
    const params = makeParams()
    const { result } = renderHook(() => usePipelineAPI(params))
    await waitFor(() => expect(result.current.loading).toBe(false))
    act(() => {
      result.current.setPreviewData(null)
    })
    expect(result.current.previewData).toBeNull()
  })

  it("initial nodeStatuses is empty", async () => {
    mockLoad.mockResolvedValue({ nodes: [], edges: [] })
    const params = makeParams()
    const { result } = renderHook(() => usePipelineAPI(params))
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.nodeStatuses).toEqual({})
  })

  it("fetchPreview sets loading preview then calls API", async () => {
    mockLoad.mockResolvedValue({ nodes: [], edges: [] })
    mockPreview.mockResolvedValue({
      status: "ok",
      columns: [{ name: "a", dtype: "f64" }],
      preview: [{ a: 1 }],
      row_count: 1,
      column_count: 1,
    })
    const params = makeParams()
    const { result } = renderHook(() => usePipelineAPI(params))
    await waitFor(() => expect(result.current.loading).toBe(false))
    const node = makeNode("n1")
    act(() => {
      result.current.fetchPreview(node)
    })
    // Should show loading state immediately
    expect(result.current.previewData?.status).toBe("loading")
  })
})
