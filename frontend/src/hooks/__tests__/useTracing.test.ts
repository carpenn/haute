import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { renderHook, cleanup, act, waitFor } from "@testing-library/react"
import type { Node, Edge } from "@xyflow/react"
import useTracing from "../useTracing"
import useToastStore from "../../stores/useToastStore"
import useSettingsStore from "../../stores/useSettingsStore"
import { makeNode, makeEdge } from "../../test-utils/factories"

vi.mock("@xyflow/react", async () => {
  const actual = await vi.importActual("@xyflow/react")
  return { ...actual, useStore: (selector: (s: { transform: [number, number, number] }) => unknown) => selector({ transform: [0, 0, 1] }) }
})

vi.mock("../../api/client", () => ({
  traceCell: vi.fn(),
}))

vi.mock("../../utils/buildGraph", () => ({
  resolveGraphFromRefs: vi.fn(() => ({ nodes: [], edges: [], preamble: "" })),
}))

import { traceCell } from "../../api/client"
const mockTraceCell = vi.mocked(traceCell)

function makeParams(overrides: Partial<Parameters<typeof useTracing>[0]> = {}) {
  return {
    nodes: [makeNode("n1"), makeNode("n2")] as Node[],
    edges: [makeEdge("n1", "n2")] as Edge[],
    selectedNode: makeNode("n2"),
    graphRef: { current: { nodes: [] as Node[], edges: [] as Edge[] } },
    parentGraphRef: { current: null },
    submodelsRef: { current: {} },
    preambleRef: { current: "" },
    nodeStatuses: {} as Record<string, "ok" | "error" | "running">,
    hoveredNodeId: null,
    ...overrides,
  }
}

describe("useTracing", () => {
  beforeEach(() => {
    useToastStore.setState({ toasts: [], _toastCounter: 0 })
    useSettingsStore.setState({ rowLimit: 1000, activeScenario: "live" })
    mockTraceCell.mockReset()
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
  })

  it("returns null traceResult initially", () => {
    const { result } = renderHook(() => useTracing(makeParams()))
    expect(result.current.traceResult).toBeNull()
    expect(result.current.tracedCell).toBeNull()
  })

  it("clearTrace resets traceResult and tracedCell", async () => {
    mockTraceCell.mockResolvedValue({
      status: "ok",
      trace: { steps: [], target_node_id: "n2", row_index: 0, column: "price", output_value: 1, total_nodes_in_pipeline: 2, nodes_in_trace: 1, execution_ms: 10, row_id_column: null, row_id_value: null },
    })
    const { result } = renderHook(() => useTracing(makeParams()))
    await act(async () => {
      result.current.handleCellClick(0, "price")
    })
    await waitFor(() => expect(result.current.traceResult).not.toBeNull())
    act(() => {
      result.current.clearTrace()
    })
    expect(result.current.traceResult).toBeNull()
    expect(result.current.tracedCell).toBeNull()
  })

  it("handleCellClick does nothing without selectedNode", () => {
    const params = makeParams({ selectedNode: null })
    const { result } = renderHook(() => useTracing(params))
    act(() => {
      result.current.handleCellClick(0, "price")
    })
    expect(mockTraceCell).not.toHaveBeenCalled()
  })

  it("handleCellClick calls traceCell and sets result on success", async () => {
    const trace = {
      steps: [{ node_id: "n1", node_name: "N1", node_type: "transform", schema_diff: { columns_added: [], columns_removed: [], columns_modified: [], columns_passed: [] }, input_values: {}, output_values: {}, column_relevant: true, execution_ms: 5 }],
      target_node_id: "n2",
      row_index: 0,
      column: "price",
      output_value: 42,
      total_nodes_in_pipeline: 2,
      nodes_in_trace: 1,
      execution_ms: 10,
      row_id_column: null,
      row_id_value: null,
    }
    mockTraceCell.mockResolvedValue({ status: "ok", trace })
    const { result } = renderHook(() => useTracing(makeParams()))
    await act(async () => {
      result.current.handleCellClick(0, "price")
    })
    await waitFor(() => expect(result.current.traceResult).not.toBeNull())
    expect(result.current.tracedCell).toEqual({ rowIndex: 0, column: "price" })
  })

  it("handleCellClick shows toast on trace failure", async () => {
    mockTraceCell.mockResolvedValue({ status: "error", error: "Something went wrong" })
    const { result } = renderHook(() => useTracing(makeParams()))
    await act(async () => {
      result.current.handleCellClick(0, "col")
    })
    await waitFor(() => {
      const toasts = useToastStore.getState().toasts
      expect(toasts.some((t) => t.type === "error")).toBe(true)
    })
    expect(result.current.traceResult).toBeNull()
  })

  it("handleCellClick shows toast on network error", async () => {
    mockTraceCell.mockRejectedValue(new Error("Network error"))
    const { result } = renderHook(() => useTracing(makeParams()))
    await act(async () => {
      result.current.handleCellClick(0, "col")
    })
    await waitFor(() => {
      const toasts = useToastStore.getState().toasts
      expect(toasts.some((t) => t.text.includes("Network error"))).toBe(true)
    })
  })

  it("nodesWithStatus applies status from nodeStatuses", () => {
    const params = makeParams({ nodeStatuses: { n1: "ok", n2: "error" } })
    const { result } = renderHook(() => useTracing(params))
    const statusMap = Object.fromEntries(
      result.current.nodesWithStatus.map((n) => [n.id, n.data._status]),
    )
    expect(statusMap.n1).toBe("ok")
    expect(statusMap.n2).toBe("error")
  })

  it("nodesWithStatus dims nodes not in trace via _traceDimmed data flag only", async () => {
    const trace = {
      steps: [{ node_id: "n1", node_name: "N1", node_type: "transform", schema_diff: { columns_added: [], columns_removed: [], columns_modified: [], columns_passed: [] }, input_values: {}, output_values: {}, column_relevant: true, execution_ms: 5 }],
      target_node_id: "n2",
      row_index: 0,
      column: "price",
      output_value: 1,
      total_nodes_in_pipeline: 2,
      nodes_in_trace: 1,
      execution_ms: 10,
      row_id_column: null,
      row_id_value: null,
    }
    mockTraceCell.mockResolvedValue({ status: "ok", trace })
    const { result } = renderHook(() => useTracing(makeParams()))
    await act(async () => {
      result.current.handleCellClick(0, "price")
    })
    await waitFor(() => expect(result.current.traceResult).not.toBeNull())
    // Dimmed node should have _traceDimmed in data, NOT style.opacity
    // (PipelineNode handles opacity via _traceDimmed to avoid double-opacity)
    const dimmedNode = result.current.nodesWithStatus.find((n) => n.id === "n2")!
    expect(dimmedNode.data._traceDimmed).toBe(true)
    expect(dimmedNode.style?.opacity).toBeUndefined()
  })

  it("nodesWithStatus does not set style.opacity on traced nodes either", async () => {
    const trace = {
      steps: [
        { node_id: "n1", node_name: "N1", node_type: "transform", schema_diff: { columns_added: [], columns_removed: [], columns_modified: [], columns_passed: [] }, input_values: {}, output_values: {}, column_relevant: true, execution_ms: 5 },
        { node_id: "n2", node_name: "N2", node_type: "transform", schema_diff: { columns_added: [], columns_removed: [], columns_modified: [], columns_passed: [] }, input_values: {}, output_values: {}, column_relevant: true, execution_ms: 5 },
      ],
      target_node_id: "n2",
      row_index: 0,
      column: "price",
      output_value: 1,
      total_nodes_in_pipeline: 2,
      nodes_in_trace: 2,
      execution_ms: 10,
      row_id_column: null,
      row_id_value: null,
    }
    mockTraceCell.mockResolvedValue({ status: "ok", trace })
    const { result } = renderHook(() => useTracing(makeParams()))
    await act(async () => {
      result.current.handleCellClick(0, "price")
    })
    await waitFor(() => expect(result.current.traceResult).not.toBeNull())
    // Traced (non-dimmed) nodes should also have no style.opacity
    const tracedNode = result.current.nodesWithStatus.find((n) => n.id === "n1")!
    expect(tracedNode.data._traceDimmed).toBe(false)
    expect(tracedNode.style?.opacity).toBeUndefined()
  })

  it("nodesWithStatus preserves transition on style", async () => {
    const trace = {
      steps: [{ node_id: "n1", node_name: "N1", node_type: "transform", schema_diff: { columns_added: [], columns_removed: [], columns_modified: [], columns_passed: [] }, input_values: {}, output_values: {}, column_relevant: true, execution_ms: 5 }],
      target_node_id: "n2",
      row_index: 0,
      column: "price",
      output_value: 1,
      total_nodes_in_pipeline: 2,
      nodes_in_trace: 1,
      execution_ms: 10,
      row_id_column: null,
      row_id_value: null,
    }
    mockTraceCell.mockResolvedValue({ status: "ok", trace })
    const { result } = renderHook(() => useTracing(makeParams()))
    await act(async () => {
      result.current.handleCellClick(0, "price")
    })
    await waitFor(() => expect(result.current.traceResult).not.toBeNull())
    // Both dimmed and non-dimmed nodes should have the transition style
    for (const n of result.current.nodesWithStatus) {
      expect(n.style?.transition).toBe("opacity 0.2s ease")
    }
  })

  it("edgesWithTrace highlights edges between traced nodes", async () => {
    const trace = {
      steps: [
        { node_id: "n1", node_name: "N1", node_type: "transform", schema_diff: { columns_added: [], columns_removed: [], columns_modified: [], columns_passed: [] }, input_values: {}, output_values: {}, column_relevant: true, execution_ms: 5 },
        { node_id: "n2", node_name: "N2", node_type: "transform", schema_diff: { columns_added: [], columns_removed: [], columns_modified: [], columns_passed: [] }, input_values: {}, output_values: {}, column_relevant: true, execution_ms: 5 },
      ],
      target_node_id: "n2",
      row_index: 0,
      column: "price",
      output_value: 1,
      total_nodes_in_pipeline: 2,
      nodes_in_trace: 2,
      execution_ms: 10,
      row_id_column: null,
      row_id_value: null,
    }
    mockTraceCell.mockResolvedValue({ status: "ok", trace })
    const { result } = renderHook(() => useTracing(makeParams()))
    await act(async () => {
      result.current.handleCellClick(0, "price")
    })
    await waitFor(() => expect(result.current.traceResult).not.toBeNull())
    const edge = result.current.edgesWithTrace[0]
    expect(edge.animated).toBe(true)
    expect(edge.style?.strokeWidth).toBe(2.5)
  })

  it("edgesWithTrace returns original edges when no trace", () => {
    const params = makeParams()
    const { result } = renderHook(() => useTracing(params))
    expect(result.current.edgesWithTrace).toBe(params.edges)
  })
})
