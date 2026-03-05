/**
 * Node CRUD handlers extracted from App.tsx FlowEditor.
 *
 * Centralises add, delete, duplicate, rename, create-instance, and
 * auto-layout operations so the main component stays focused on
 * orchestration and rendering.
 */
import { useCallback, type MutableRefObject } from "react"
import type { Node, Edge } from "@xyflow/react"
import useToastStore from "../stores/useToastStore"
import useNodeResultsStore from "../stores/useNodeResultsStore"
import { nodeData } from "../types/node"
import { NODE_TYPES } from "../utils/nodeTypes"
import { getLayoutedElements } from "../utils/layout"
import type { PreviewData } from "../panels/DataPreview"

type UseNodeHandlersParams = {
  graphRef: MutableRefObject<{ nodes: Node[]; edges: Edge[] }>
  nodeIdCounter: MutableRefObject<number>
  lastSelectedNodeRef: MutableRefObject<Node | null>
  setNodes: (updater: (nds: Node[]) => Node[]) => void
  setEdges: (updater: (eds: Edge[]) => Edge[]) => void
  setSelectedNode: (updater: React.SetStateAction<Node | null>) => void
  setPreviewData: (updater: React.SetStateAction<PreviewData | null>) => void
  onUpdateNode: (id: string, data: Record<string, unknown>) => void
  fitView: (opts?: { padding?: number }) => void
}

export default function useNodeHandlers({
  graphRef,
  nodeIdCounter,
  lastSelectedNodeRef,
  setNodes,
  setEdges,
  setSelectedNode,
  setPreviewData,
  onUpdateNode,
  fitView,
}: UseNodeHandlersParams) {
  const addToast = useToastStore((s) => s.addToast)
  const clearNode = useNodeResultsStore((s) => s.clearNode)

  const handleDeleteNode = useCallback((id: string) => {
    const { nodes: n, edges: e } = graphRef.current
    setNodes(() => n.filter((node) => node.id !== id))
    setEdges(() => e.filter((edge) => edge.source !== id && edge.target !== id))
    setSelectedNode((prev) => (prev?.id === id ? null : prev))
    setPreviewData((prev) => (prev?.nodeId === id ? null : prev))
    clearNode(id)
    if (lastSelectedNodeRef.current?.id === id) lastSelectedNodeRef.current = null
  }, [graphRef, lastSelectedNodeRef, setNodes, setEdges, setSelectedNode, setPreviewData, clearNode])

  const handleDuplicateNode = useCallback((id: string) => {
    const { nodes: n } = graphRef.current
    const original = n.find((node) => node.id === id)
    if (!original) return
    nodeIdCounter.current += 1
    const newId = `${original.type}_${nodeIdCounter.current}`
    const newNode: Node = {
      ...original,
      id: newId,
      position: { x: original.position.x + 40, y: original.position.y + 40 },
      selected: true,
      data: { ...original.data, label: `${original.data.label} copy` },
    }
    setNodes((nds) => [...nds.map((nd) => ({ ...nd, selected: false })), newNode])
    setSelectedNode(newNode)
  }, [graphRef, nodeIdCounter, setNodes, setSelectedNode])

  const handleCreateInstance = useCallback((id: string) => {
    const { nodes: n } = graphRef.current
    const original = n.find((node) => node.id === id)
    if (!original) return
    nodeIdCounter.current += 1
    const origData = nodeData(original)
    const origNodeType = origData.nodeType || NODE_TYPES.TRANSFORM
    const newId = `${origNodeType}_${nodeIdCounter.current}`
    const newNode: Node = {
      id: newId,
      type: original.type,
      position: { x: original.position.x + 60, y: original.position.y + 80 },
      selected: true,
      data: {
        label: `${origData.label} instance`,
        description: `Instance of ${origData.label}`,
        nodeType: origNodeType,
        config: { instanceOf: id },
      },
    }
    setNodes((nds) => [...nds.map((nd) => ({ ...nd, selected: false })), newNode])
    setSelectedNode(newNode)
    addToast("info", `Created instance of "${origData.label}"`)
  }, [graphRef, nodeIdCounter, setNodes, setSelectedNode, addToast])

  const handleRenameNode = useCallback((id: string) => {
    const { nodes: n } = graphRef.current
    const node = n.find((nd) => nd.id === id)
    if (!node) return
    const newLabel = prompt("Rename node:", String(node.data.label))
    if (newLabel !== null && newLabel.trim()) {
      onUpdateNode(id, { ...node.data, label: newLabel.trim() })
    }
  }, [graphRef, onUpdateNode])

  const handleAutoLayout = useCallback(async () => {
    const { nodes: n, edges: e } = graphRef.current
    if (n.length === 0) return
    const layouted = await getLayoutedElements(n, e)
    setNodes(() => layouted)
    setTimeout(() => fitView({ padding: 0.8 }), 50)
    addToast("info", "Auto-layout applied")
  }, [graphRef, setNodes, fitView, addToast])

  return {
    handleDeleteNode,
    handleDuplicateNode,
    handleCreateInstance,
    handleRenameNode,
    handleAutoLayout,
  }
}
