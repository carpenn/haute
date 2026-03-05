import { describe, it, expect, afterEach } from "vitest"
import { renderHook, cleanup, act } from "@testing-library/react"
import useUndoRedo from "../useUndoRedo"
import { makeNode, makeEdge } from "../../test-utils/factories"

describe("useUndoRedo", () => {
  afterEach(cleanup)

  it("initialises with provided nodes and edges", () => {
    const nodes = [makeNode("n1")]
    const edges = [makeEdge("n1", "n2", { id: "e1" })]
    const { result } = renderHook(() => useUndoRedo(nodes, edges))
    expect(result.current.nodes).toHaveLength(1)
    expect(result.current.edges).toHaveLength(1)
    expect(result.current.canUndo).toBe(false)
    expect(result.current.canRedo).toBe(false)
  })

  it("setNodes pushes snapshot and enables undo", () => {
    const { result } = renderHook(() => useUndoRedo([makeNode("n1")], []))
    act(() => {
      result.current.setNodes([makeNode("n1"), makeNode("n2")])
    })
    expect(result.current.canUndo).toBe(true)
    expect(result.current.canRedo).toBe(false)
  })

  it("setEdges pushes snapshot and enables undo", () => {
    const { result } = renderHook(() => useUndoRedo([], []))
    act(() => {
      result.current.setEdges([makeEdge("a", "b", { id: "e1" })])
    })
    expect(result.current.canUndo).toBe(true)
  })

  it("undo restores previous state", () => {
    const initial = [makeNode("n1")]
    const { result } = renderHook(() => useUndoRedo(initial, []))
    act(() => {
      result.current.setNodes([makeNode("n1"), makeNode("n2")])
    })
    expect(result.current.nodes).toHaveLength(2)
    act(() => {
      result.current.undo()
    })
    expect(result.current.nodes).toHaveLength(1)
    expect(result.current.canRedo).toBe(true)
    expect(result.current.canUndo).toBe(false)
  })

  it("redo restores undone state", () => {
    const { result } = renderHook(() => useUndoRedo([makeNode("n1")], []))
    act(() => {
      result.current.setNodes([makeNode("n1"), makeNode("n2")])
    })
    act(() => {
      result.current.undo()
    })
    expect(result.current.nodes).toHaveLength(1)
    act(() => {
      result.current.redo()
    })
    expect(result.current.nodes).toHaveLength(2)
    expect(result.current.canUndo).toBe(true)
    expect(result.current.canRedo).toBe(false)
  })

  it("new change after undo clears redo stack", () => {
    const { result } = renderHook(() => useUndoRedo([makeNode("n1")], []))
    act(() => {
      result.current.setNodes([makeNode("n1"), makeNode("n2")])
    })
    act(() => {
      result.current.undo()
    })
    expect(result.current.canRedo).toBe(true)
    act(() => {
      result.current.setNodes([makeNode("n3")])
    })
    expect(result.current.canRedo).toBe(false)
  })

  it("undo with empty history does nothing", () => {
    const { result } = renderHook(() => useUndoRedo([makeNode("n1")], []))
    act(() => {
      result.current.undo()
    })
    expect(result.current.nodes).toHaveLength(1)
  })

  it("redo with empty future does nothing", () => {
    const { result } = renderHook(() => useUndoRedo([makeNode("n1")], []))
    act(() => {
      result.current.redo()
    })
    expect(result.current.nodes).toHaveLength(1)
  })

  it("setNodesRaw bypasses history", () => {
    const { result } = renderHook(() => useUndoRedo([], []))
    act(() => {
      result.current.setNodesRaw([makeNode("n1"), makeNode("n2")])
    })
    expect(result.current.nodes).toHaveLength(2)
    // No snapshot was pushed — undo should remain unavailable
    expect(result.current.canUndo).toBe(false)
  })

  it("setEdgesRaw bypasses history", () => {
    const { result } = renderHook(() => useUndoRedo([], []))
    act(() => {
      result.current.setEdgesRaw([makeEdge("a", "b", { id: "e1" })])
    })
    expect(result.current.edges).toHaveLength(1)
    expect(result.current.canUndo).toBe(false)
  })

  it("onNodesChange with structural change pushes snapshot", () => {
    const { result } = renderHook(() => useUndoRedo([makeNode("n1")], []))
    act(() => {
      result.current.onNodesChange([{ type: "add", item: makeNode("n2") }])
    })
    expect(result.current.canUndo).toBe(true)
  })

  it("onEdgesChange with structural change pushes snapshot", () => {
    const { result } = renderHook(() => useUndoRedo([], [makeEdge("a", "b", { id: "e1" })]))
    act(() => {
      result.current.onEdgesChange([{ type: "remove", id: "e1" }])
    })
    expect(result.current.canUndo).toBe(true)
  })
})
