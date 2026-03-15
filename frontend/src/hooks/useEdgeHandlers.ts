/**
 * Edge/connection handlers and drag-drop/selection logic extracted from
 * App.tsx FlowEditor.
 *
 * Centralises connect, disconnect, selection-change, context-menu,
 * and drag-drop operations so the main component stays focused on
 * orchestration and rendering.
 */
import { useCallback, type MutableRefObject, type DragEvent } from "react"
import {
  addEdge,
  type Node,
  type Edge,
  type OnConnect,
  type OnSelectionChangeFunc,
} from "@xyflow/react"
import { nodeData } from "../types/node"
import { NODE_TYPES, NODE_TYPE_META, type NodeTypeValue } from "../utils/nodeTypes"

type ContextMenuData = {
  x: number
  y: number
  nodeId: string
  nodeLabel: string
  isSubmodel?: boolean
}

type UseEdgeHandlersParams = {
  graphRef: MutableRefObject<{ nodes: Node[]; edges: Edge[] }>
  nodeIdCounter: MutableRefObject<number>
  lastSelectedNodeRef: MutableRefObject<Node | null>
  setNodes: (updater: (nds: Node[]) => Node[]) => void
  setEdges: (updater: (eds: Edge[]) => Edge[]) => void
  setSelectedNode: (updater: React.SetStateAction<Node | null>) => void
  setContextMenu: (data: ContextMenuData | null) => void
  fetchPreview: (node: Node) => void
  clearTrace: () => void
  screenToFlowPosition: (pos: { x: number; y: number }) => { x: number; y: number }
}

export default function useEdgeHandlers({
  graphRef,
  nodeIdCounter,
  lastSelectedNodeRef,
  setNodes,
  setEdges,
  setSelectedNode,
  setContextMenu,
  fetchPreview,
  clearTrace,
  screenToFlowPosition,
}: UseEdgeHandlersParams) {
  const onConnect: OnConnect = useCallback(
    (params) => {
      if (params.source === params.target) return
      const { edges: currentEdges, nodes: currentNodes } = graphRef.current
      const exists = currentEdges.some(
        (e) => e.source === params.source && e.target === params.target
      )
      if (exists) return

      const targetNode = currentNodes.find((n) => n.id === params.target)
      if (targetNode && nodeData(targetNode).nodeType === NODE_TYPES.SUBMODEL && params.targetHandle) {
        setEdges((eds) => addEdge({ ...params, targetHandle: null }, eds))
        return
      }

      setEdges((eds) => addEdge(params, eds))
    },
    [graphRef, setEdges],
  )

  const onSelectionChange: OnSelectionChangeFunc = useCallback(({ nodes: selectedNodes }) => {
    if (selectedNodes.length !== 1) {
      // Canvas click or multi-select: deselect but keep panel showing last node
      setSelectedNode(null)
      clearTrace()
      // Don't clear previewData or lastSelectedNodeRef -- panel stays visible
    }
  }, [setSelectedNode, clearTrace])

  /** Opens panel on a full click (mousedown+mouseup) — skipped for drags. */
  const onNodeClick = useCallback((_event: React.MouseEvent, node: Node) => {
    setSelectedNode((prev) => {
      if (prev?.id !== node.id) {
        fetchPreview(node)
        clearTrace()
      }
      return node
    })
    lastSelectedNodeRef.current = node
  }, [setSelectedNode, fetchPreview, clearTrace, lastSelectedNodeRef])

  const handleDeleteEdge = useCallback((edgeId: string) => {
    setEdges((eds) => eds.filter((e) => e.id !== edgeId))
  }, [setEdges])

  const onNodeContextMenu = useCallback((event: React.MouseEvent, node: Node) => {
    event.preventDefault()
    setContextMenu({
      x: event.clientX,
      y: event.clientY,
      nodeId: node.id,
      nodeLabel: String(node.data.label),
      isSubmodel: nodeData(node).nodeType === NODE_TYPES.SUBMODEL,
    })
  }, [setContextMenu])

  const onDragOver = useCallback((event: DragEvent) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = "move"
  }, [])

  const onDrop = useCallback(
    (event: DragEvent) => {
      event.preventDefault()
      const type = event.dataTransfer.getData("application/reactflow-type")
      if (!type) return

      let config = {}
      try {
        config = JSON.parse(event.dataTransfer.getData("application/reactflow-config") || "{}")
      } catch { /* ignore */ }

      const position = screenToFlowPosition({ x: event.clientX, y: event.clientY })
      nodeIdCounter.current += 1
      const id = `${type}_${nodeIdCounter.current}`

      const newNode: Node = {
        id,
        type,
        position,
        data: {
          label: `${NODE_TYPE_META[type as NodeTypeValue]?.name || "Node"} ${nodeIdCounter.current}`,
          description: "",
          nodeType: type,
          config,
        },
      }

      setNodes((nds) => [
        ...nds.map((n) => ({ ...n, selected: false })),
        { ...newNode, selected: true },
      ])
      setSelectedNode(newNode)
    },
    [screenToFlowPosition, nodeIdCounter, setNodes, setSelectedNode],
  )

  return {
    onConnect,
    onSelectionChange,
    onNodeClick,
    handleDeleteEdge,
    onNodeContextMenu,
    onDragOver,
    onDrop,
  }
}
