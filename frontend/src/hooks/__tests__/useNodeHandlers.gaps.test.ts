/**
 * Gap tests for useNodeHandlers — handleRenameNode is exported but has no tests.
 *
 * Tests cover:
 * 1. handleRenameNode opens the rename dialog with correct nodeId and currentLabel
 * 2. handleRenameNode does nothing when the node is not found
 * 3. handleRenameNode uses node.data.label (coerced to string)
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { renderHook, cleanup, act } from "@testing-library/react"
import type { Node, Edge } from "@xyflow/react"
import useNodeHandlers from "../useNodeHandlers"
import useToastStore from "../../stores/useToastStore"
import useNodeResultsStore from "../../stores/useNodeResultsStore"
import useUIStore from "../../stores/useUIStore"
import { makeNode } from "../../test-utils/factories"

vi.mock("../../utils/layout", () => ({
  getLayoutedElements: vi.fn(async (nodes: Node[]) => nodes),
}))

function makeParams() {
  return {
    graphRef: { current: { nodes: [] as Node[], edges: [] as Edge[] } },
    nodeIdCounter: { current: 10 },
    lastSelectedNodeRef: { current: null as Node | null },
    setNodes: vi.fn(),
    setEdges: vi.fn(),
    setSelectedNode: vi.fn(),
    setPreviewData: vi.fn(),
    fitView: vi.fn(),
  }
}

describe("useNodeHandlers — handleRenameNode", () => {
  beforeEach(() => {
    useToastStore.setState({ toasts: [], _toastCounter: 0 })
    useNodeResultsStore.setState({ previews: {}, columnCache: {} })
    useUIStore.setState({ renameDialog: null })
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
  })

  it("opens the rename dialog with correct nodeId and currentLabel", () => {
    // Catches: if handleRenameNode is accidentally removed or the
    // setRenameDialog call is broken, the user would click "Rename"
    // in the context menu and nothing would happen.
    const params = makeParams()
    const n1 = makeNode("n1", "polars", { data: { label: "My Transform", nodeType: "polars", config: {} } })
    params.graphRef.current = { nodes: [n1], edges: [] }

    const { result } = renderHook(() => useNodeHandlers(params))

    act(() => {
      result.current.handleRenameNode("n1")
    })

    const dialog = useUIStore.getState().renameDialog
    expect(dialog).toEqual({
      nodeId: "n1",
      currentLabel: "My Transform",
    })
  })

  it("does nothing when the node is not found in graphRef", () => {
    // Catches: if the early return guard is removed, setRenameDialog
    // would be called with undefined data, potentially crashing the
    // RenameDialog component.
    const params = makeParams()
    params.graphRef.current = { nodes: [], edges: [] }

    const { result } = renderHook(() => useNodeHandlers(params))

    act(() => {
      result.current.handleRenameNode("nonexistent")
    })

    const dialog = useUIStore.getState().renameDialog
    expect(dialog).toBeNull()
  })

  it("coerces non-string label to string via String()", () => {
    // Catches: if node.data.label is a number (e.g. from a legacy
    // pipeline), String() coercion prevents passing a raw number
    // to the rename dialog's text input.
    const params = makeParams()
    const n1 = makeNode("n1", "polars", { data: { label: 42 as unknown as string, nodeType: "polars", config: {} } })
    params.graphRef.current = { nodes: [n1], edges: [] }

    const { result } = renderHook(() => useNodeHandlers(params))

    act(() => {
      result.current.handleRenameNode("n1")
    })

    const dialog = useUIStore.getState().renameDialog
    expect(dialog).toEqual({
      nodeId: "n1",
      currentLabel: "42",
    })
  })

  it("handleDeleteNode clears lastSelectedNodeRef when deleting the selected node", () => {
    // Catches: stale lastSelectedNodeRef causes the next node-click to
    // skip selection because the ref still points to the deleted node.
    const params = makeParams()
    const n1 = makeNode("n1")
    params.graphRef.current = { nodes: [n1], edges: [] }
    params.lastSelectedNodeRef.current = n1

    const { result } = renderHook(() => useNodeHandlers(params))

    act(() => {
      result.current.handleDeleteNode("n1")
    })

    expect(params.lastSelectedNodeRef.current).toBeNull()
  })

  it("handleDeleteNode does NOT clear lastSelectedNodeRef when deleting a different node", () => {
    // Catches: if the id guard is missing, deleting any node would
    // clear the selection ref, confusing keyboard navigation.
    const params = makeParams()
    const n1 = makeNode("n1")
    const n2 = makeNode("n2")
    params.graphRef.current = { nodes: [n1, n2], edges: [] }
    params.lastSelectedNodeRef.current = n2

    const { result } = renderHook(() => useNodeHandlers(params))

    act(() => {
      result.current.handleDeleteNode("n1")
    })

    expect(params.lastSelectedNodeRef.current).toBe(n2)
  })
})
