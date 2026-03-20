import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { renderHook, cleanup } from "@testing-library/react"
import type { Node, Edge } from "@xyflow/react"
import useKeyboardShortcuts from "../useKeyboardShortcuts"
import useUIStore from "../../stores/useUIStore"
import useToastStore from "../../stores/useToastStore"

function makeParams(overrides: Partial<Parameters<typeof useKeyboardShortcuts>[0]> = {}) {
  return {
    handleSave: vi.fn(),
    setNodes: vi.fn(),
    setEdges: vi.fn(),
    undo: vi.fn(),
    redo: vi.fn(),
    fitView: vi.fn(),
    graphRef: { current: { nodes: [] as Node[], edges: [] as Edge[] } },
    clipboard: { current: { nodes: [] as Node[], edges: [] as Edge[] } },
    nodeIdCounter: { current: 0 },
    setSelectedNode: vi.fn(),
    setPreviewData: vi.fn(),
    clearTrace: vi.fn(),
    closePanel: vi.fn(),
    ...overrides,
  }
}

function fireKey(key: string, opts: Partial<KeyboardEventInit> = {}) {
  window.dispatchEvent(new KeyboardEvent("keydown", { key, bubbles: true, ...opts }))
}

describe("useKeyboardShortcuts", () => {
  let params: ReturnType<typeof makeParams>

  beforeEach(() => {
    // Reset store state between tests
    useUIStore.setState({
      shortcutsOpen: false, submodelDialog: null,
    })
    useToastStore.setState({
      toasts: [], _toastCounter: 0,
    })
    params = makeParams()
    renderHook(() => useKeyboardShortcuts(params))
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
  })

  it("Ctrl+S calls handleSave", () => {
    fireKey("s", { ctrlKey: true })
    expect(params.handleSave).toHaveBeenCalledOnce()
  })

  it("Ctrl+Z calls undo", () => {
    fireKey("z", { ctrlKey: true })
    expect(params.undo).toHaveBeenCalledOnce()
  })

  it("Ctrl+Shift+Z calls redo", () => {
    fireKey("z", { ctrlKey: true, shiftKey: true })
    expect(params.redo).toHaveBeenCalledOnce()
  })

  it("Ctrl+Y calls redo", () => {
    fireKey("y", { ctrlKey: true })
    expect(params.redo).toHaveBeenCalledOnce()
  })

  it("Ctrl+1 calls fitView", () => {
    fireKey("1", { ctrlKey: true })
    expect(params.fitView).toHaveBeenCalledWith({ padding: 0.8 })
  })

  it("Escape calls clearTrace", () => {
    fireKey("Escape")
    expect(params.clearTrace).toHaveBeenCalledOnce()
  })

  it("? toggles shortcuts panel", () => {
    fireKey("?")
    expect(useUIStore.getState().shortcutsOpen).toBe(true)
  })

  it("Ctrl+A selects all nodes", () => {
    fireKey("a", { ctrlKey: true })
    expect(params.setNodes).toHaveBeenCalledOnce()
  })

  it("Ctrl+C copies selected nodes and toasts", () => {
    const selected: Node[] = [
      { id: "n1", position: { x: 0, y: 0 }, data: { label: "A" }, selected: true } as Node,
    ]
    params.graphRef.current.nodes = selected
    fireKey("c", { ctrlKey: true })
    expect(params.clipboard.current.nodes).toHaveLength(1)
    const toasts = useToastStore.getState().toasts
    expect(toasts[toasts.length - 1]).toMatchObject({ type: "info", text: "Copied 1 node" })
  })

  it("Ctrl+V pastes copied nodes", () => {
    params.clipboard.current = {
      nodes: [{ id: "n1", position: { x: 0, y: 0 }, data: { label: "A" }, type: "pipelineNode" } as Node],
      edges: [],
    }
    fireKey("v", { ctrlKey: true })
    expect(params.setNodes).toHaveBeenCalledOnce()
    expect(params.setEdges).toHaveBeenCalledOnce()
    const toasts = useToastStore.getState().toasts
    expect(toasts[toasts.length - 1]).toMatchObject({ type: "info", text: "Pasted 1 node" })
  })

  it("Ctrl+V with empty clipboard does nothing", () => {
    fireKey("v", { ctrlKey: true })
    expect(params.setNodes).not.toHaveBeenCalled()
  })

  it("Delete removes selected nodes", () => {
    const nodes: Node[] = [
      { id: "n1", position: { x: 0, y: 0 }, data: { label: "A" }, selected: true } as Node,
      { id: "n2", position: { x: 0, y: 0 }, data: { label: "B" }, selected: false } as Node,
    ]
    params.graphRef.current.nodes = nodes
    params.graphRef.current.edges = [
      { id: "e1", source: "n1", target: "n2" } as Edge,
    ]
    fireKey("Delete")
    expect(params.setNodes).toHaveBeenCalled()
    expect(params.setEdges).toHaveBeenCalled()
    expect(params.setSelectedNode).toHaveBeenCalledWith(null)
    expect(params.setPreviewData).toHaveBeenCalledWith(null)
  })

  it("Delete with no selection does nothing", () => {
    params.graphRef.current.nodes = [
      { id: "n1", position: { x: 0, y: 0 }, data: { label: "A" }, selected: false } as Node,
    ]
    params.graphRef.current.edges = []
    fireKey("Delete")
    expect(params.setNodes).not.toHaveBeenCalled()
  })

  it("Ctrl+G with 2+ selected opens submodel dialog", () => {
    params.graphRef.current.nodes = [
      { id: "n1", position: { x: 0, y: 0 }, data: {}, selected: true } as Node,
      { id: "n2", position: { x: 0, y: 0 }, data: {}, selected: true } as Node,
    ]
    fireKey("g", { ctrlKey: true })
    expect(useUIStore.getState().submodelDialog).toEqual({ nodeIds: ["n1", "n2"] })
  })

  it("Ctrl+G with <2 selected toasts info", () => {
    params.graphRef.current.nodes = [
      { id: "n1", position: { x: 0, y: 0 }, data: {}, selected: true } as Node,
    ]
    fireKey("g", { ctrlKey: true })
    const toasts = useToastStore.getState().toasts
    expect(toasts[toasts.length - 1]).toMatchObject({ type: "info", text: expect.stringContaining("2 nodes") })
  })

  it("cleans up listener on unmount", () => {
    const removeSpy = vi.spyOn(window, "removeEventListener")
    const { unmount } = renderHook(() => useKeyboardShortcuts(params))
    unmount()
    expect(removeSpy).toHaveBeenCalledWith("keydown", expect.any(Function))
    removeSpy.mockRestore()
  })
})
