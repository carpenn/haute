import { useEffect, useCallback, useState, useRef, type DragEvent } from "react"
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  useReactFlow,
  SelectionMode,
  type Node,
  type Edge,
  type OnConnect,
  type OnSelectionChangeFunc,
  addEdge,
  BackgroundVariant,
  MarkerType,
} from "@xyflow/react"
import "@xyflow/react/dist/style.css"

import PipelineNode from "./nodes/PipelineNode"
import SubmodelNode from "./nodes/SubmodelNode"
import SubmodelPortNode from "./nodes/SubmodelPortNode"
import NodePalette from "./panels/NodePalette"
import NodePanel, { type SimpleNode, type SimpleEdge } from "./panels/NodePanel"
import DataPreview from "./panels/DataPreview"
import OptimiserPreview from "./panels/OptimiserPreview"
import type { OptimiserPreviewData } from "./panels/OptimiserPreview"
import TracePanel from "./panels/TracePanel"
import ToastContainer from "./components/Toast"
import ContextMenu from "./components/ContextMenu"
import KeyboardShortcuts from "./components/KeyboardShortcuts"
import BreadcrumbBar from "./components/BreadcrumbBar"
import Toolbar from "./components/Toolbar"
import SettingsModal from "./components/SettingsModal"
import SubmodelDialog from "./components/SubmodelDialog"

import useUndoRedo from "./hooks/useUndoRedo"
import useWebSocketSync from "./hooks/useWebSocketSync"
import usePipelineAPI from "./hooks/usePipelineAPI"
import useTracing from "./hooks/useTracing"
import useSubmodelNavigation from "./hooks/useSubmodelNavigation"
import useKeyboardShortcuts from "./hooks/useKeyboardShortcuts"
import useUIStore from "./stores/useUIStore"

import { NODE_TYPES } from "./utils/nodeTypes"
import { getLayoutedElements } from "./utils/layout"
import { nodeData } from "./types/node"
import { PanelLeftOpen } from "lucide-react"

// ---------------------------------------------------------------------------
// ReactFlow node type → component registry
// ---------------------------------------------------------------------------

const nodeTypes = {
  [NODE_TYPES.API_INPUT]: PipelineNode,
  [NODE_TYPES.DATA_SOURCE]: PipelineNode,
  [NODE_TYPES.TRANSFORM]: PipelineNode,
  [NODE_TYPES.MODEL_SCORE]: PipelineNode,
  [NODE_TYPES.RATING_STEP]: PipelineNode,
  [NODE_TYPES.BANDING]: PipelineNode,
  [NODE_TYPES.OUTPUT]: PipelineNode,
  [NODE_TYPES.DATA_SINK]: PipelineNode,
  [NODE_TYPES.EXTERNAL_FILE]: PipelineNode,
  [NODE_TYPES.LIVE_SWITCH]: PipelineNode,
  [NODE_TYPES.MODELLING]: PipelineNode,
  [NODE_TYPES.OPTIMISER]: PipelineNode,
  [NODE_TYPES.OPTIMISER_APPLY]: PipelineNode,
  [NODE_TYPES.SCENARIO_EXPANDER]: PipelineNode,
  [NODE_TYPES.CONSTANT]: PipelineNode,
  [NODE_TYPES.SUBMODEL]: SubmodelNode,
  [NODE_TYPES.SUBMODEL_PORT]: SubmodelPortNode,
}

const labelMap: Record<string, string> = {
  [NODE_TYPES.API_INPUT]: "API Input",
  [NODE_TYPES.DATA_SOURCE]: "Data Source",
  [NODE_TYPES.TRANSFORM]: "Polars",
  [NODE_TYPES.MODEL_SCORE]: "Model Score",
  [NODE_TYPES.RATING_STEP]: "Rating Step",
  [NODE_TYPES.BANDING]: "Banding",
  [NODE_TYPES.OUTPUT]: "Output",
  [NODE_TYPES.DATA_SINK]: "Data Sink",
  [NODE_TYPES.EXTERNAL_FILE]: "External File",
  [NODE_TYPES.LIVE_SWITCH]: "Live Switch",
  [NODE_TYPES.MODELLING]: "Model Training",
  [NODE_TYPES.OPTIMISER]: "Optimiser",
  [NODE_TYPES.OPTIMISER_APPLY]: "Apply Optimisation",
  [NODE_TYPES.SCENARIO_EXPANDER]: "Expander",
  [NODE_TYPES.CONSTANT]: "Constant",
  [NODE_TYPES.SUBMODEL]: "Submodel",
  [NODE_TYPES.SUBMODEL_PORT]: "Port",
}

// ---------------------------------------------------------------------------
// FlowEditor — main orchestrator
// ---------------------------------------------------------------------------

function FlowEditor() {
  // Core ReactFlow state with undo/redo
  const {
    nodes, edges,
    setNodes, setEdges,
    setNodesRaw, setEdgesRaw,
    onNodesChange, onEdgesChange,
    undo, redo, canUndo, canRedo,
  } = useUndoRedo()
  const { screenToFlowPosition, fitView } = useReactFlow()

  // UI state from Zustand store (leaf-subscribed values live in their own components)
  const addToast = useUIStore((s) => s.addToast)
  const paletteOpen = useUIStore((s) => s.paletteOpen)
  const setPaletteOpen = useUIStore((s) => s.setPaletteOpen)
  const settingsOpen = useUIStore((s) => s.settingsOpen)
  const setSettingsOpen = useUIStore((s) => s.setSettingsOpen)
  const shortcutsOpen = useUIStore((s) => s.shortcutsOpen)
  const setShortcutsOpen = useUIStore((s) => s.setShortcutsOpen)
  const submodelDialog = useUIStore((s) => s.submodelDialog)
  const setSubmodelDialog = useUIStore((s) => s.setSubmodelDialog)
  const snapToGrid = useUIStore((s) => s.snapToGrid)
  const toggleSnapToGrid = useUIStore((s) => s.toggleSnapToGrid)
  const syncBanner = useUIStore((s) => s.syncBanner)
  const setSyncBanner = useUIStore((s) => s.setSyncBanner)
  const dirty = useUIStore((s) => s.dirty)
  const setDirty = useUIStore((s) => s.setDirty)

  // Local UI state (not worth globalizing)
  const [selectedNode, setSelectedNode] = useState<Node | null>(null)
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; nodeId: string; nodeLabel: string; isSubmodel?: boolean } | null>(null)
  const [preamble, setPreamble] = useState("")
  const [optimiserPreview, setOptimiserPreview] = useState<OptimiserPreviewData | null>(null)

  // Refs
  const submodelsRef = useRef<Record<string, unknown>>({})
  const clipboard = useRef<{ nodes: Node[]; edges: Edge[] }>({ nodes: [], edges: [] })
  const graphRef = useRef<{ nodes: Node[]; edges: Edge[] }>({ nodes: [], edges: [] })
  const parentGraphRef = useRef<{ nodes: Node[]; edges: Edge[]; submodels: Record<string, unknown> } | null>(null)
  const lastSavedRef = useRef<string>("")
  const preambleRef = useRef("")
  const pipelineNameRef = useRef("main")
  const sourceFileRef = useRef("")
  const nodeIdCounter = useRef(0)

  // Keep graphRef in sync so callbacks never see stale state
  useEffect(() => {
    graphRef.current = { nodes, edges }
  }, [nodes, edges])

  // Track dirty state via reference equality (avoids JSON.stringify overhead)
  const prevStateRef = useRef<{ nodes: Node[]; edges: Edge[]; preamble: string } | null>(null)
  useEffect(() => {
    if (lastSavedRef.current) {
      const prev = prevStateRef.current
      if (prev && (prev.nodes !== nodes || prev.edges !== edges || prev.preamble !== preamble)) {
        setDirty(true)
      }
    }
    prevStateRef.current = { nodes, edges, preamble }
  }, [nodes, edges, preamble, setDirty])

  // ---------------------------------------------------------------------------
  // Hooks
  // ---------------------------------------------------------------------------

  const wsStatus = useWebSocketSync({
    setNodesRaw, setEdgesRaw, setPreamble, preambleRef,
    nodeIdCounter, fitView,
  })

  const {
    loading, previewData, setPreviewData,
    nodeStatuses, runStatus,
    fetchPreview, handleRun, handleSave,
  } = usePipelineAPI({
    nodes, edges, selectedNode,
    graphRef, parentGraphRef, submodelsRef, setNodes,
    setNodesRaw, setEdgesRaw, setPreamble,
    preambleRef, pipelineNameRef, sourceFileRef, lastSavedRef,
    nodeIdCounter,
  })

  const {
    traceResult, tracedCell,
    handleCellClick, clearTrace,
    nodesWithStatus, edgesWithTrace,
  } = useTracing({
    nodes, edges, selectedNode,
    graphRef, parentGraphRef, submodelsRef,
    nodeStatuses,
  })

  const {
    viewStack,
    handleDrillIntoSubmodel, handleBreadcrumbNavigate,
    handleCreateSubmodel, handleDissolveSubmodel,
  } = useSubmodelNavigation({
    graphRef, parentGraphRef, submodelsRef,
    setNodesRaw, setEdgesRaw,
    setSelectedNode, setPreviewData: (d: null) => setPreviewData(d),
    preambleRef, sourceFileRef, pipelineNameRef,
    fitView,
  })

  useKeyboardShortcuts({
    handleSave, setNodes, setEdges, undo, redo, fitView,
    graphRef, clipboard, nodeIdCounter,
    setSelectedNode, setPreviewData: (d: null) => setPreviewData(d),
    clearTrace,
  })

  // ---------------------------------------------------------------------------
  // Node interaction handlers
  // ---------------------------------------------------------------------------

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
    [setEdges],
  )

  const onSelectionChange: OnSelectionChangeFunc = useCallback(({ nodes: selectedNodes }) => {
    if (selectedNodes.length === 1) {
      const node = selectedNodes[0]
      setSelectedNode((prev) => {
        if (prev?.id !== node.id) {
          fetchPreview(node)
          clearTrace()
          setOptimiserPreview(null)
        }
        return node
      })
    } else {
      setSelectedNode(null)
      setPreviewData(null)
      setOptimiserPreview(null)
      clearTrace()
    }
  }, [fetchPreview, clearTrace, setPreviewData])

  const onUpdateNode = useCallback(
    (id: string, data: Record<string, unknown>) => {
      setNodes((nds) => nds.map((n) => (n.id === id ? { ...n, data } : n)))
      setSelectedNode((prev) => (prev && prev.id === id ? { ...prev, data } : prev))
    },
    [setNodes],
  )

  const handleDeleteEdge = useCallback((edgeId: string) => {
    setEdges((eds) => eds.filter((e) => e.id !== edgeId))
  }, [setEdges])

  const handleDeleteNode = useCallback((id: string) => {
    const { nodes: n, edges: e } = graphRef.current
    setNodes(n.filter((node) => node.id !== id))
    setEdges(e.filter((edge) => edge.source !== id && edge.target !== id))
    setSelectedNode((prev) => (prev?.id === id ? null : prev))
    setPreviewData((prev) => (prev?.nodeId === id ? null : prev))
  }, [setNodes, setEdges, setPreviewData])

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
  }, [setNodes])

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
  }, [setNodes, addToast])

  const handleRenameNode = useCallback((id: string) => {
    const { nodes: n } = graphRef.current
    const node = n.find((nd) => nd.id === id)
    if (!node) return
    const newLabel = prompt("Rename node:", String(node.data.label))
    if (newLabel !== null && newLabel.trim()) {
      onUpdateNode(id, { ...node.data, label: newLabel.trim() })
    }
  }, [onUpdateNode])

  const handleAutoLayout = useCallback(async () => {
    const { nodes: n, edges: e } = graphRef.current
    if (n.length === 0) return
    const layouted = await getLayoutedElements(n, e)
    setNodes(layouted)
    setTimeout(() => fitView({ padding: 0.8 }), 50)
    addToast("info", "Auto-layout applied")
  }, [setNodes, fitView, addToast])

  const onNodeContextMenu = useCallback((event: React.MouseEvent, node: Node) => {
    event.preventDefault()
    setContextMenu({ x: event.clientX, y: event.clientY, nodeId: node.id, nodeLabel: String(node.data.label), isSubmodel: nodeData(node).nodeType === NODE_TYPES.SUBMODEL })
  }, [])

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
          label: `${labelMap[type] || "Node"} ${nodeIdCounter.current}`,
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
    [screenToFlowPosition, setNodes],
  )

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  if (loading) {
    return (
      <div className="h-full w-full flex items-center justify-center" style={{ background: 'var(--bg-base)' }}>
        <div className="text-sm" style={{ color: 'var(--text-muted)' }}>Loading pipeline...</div>
      </div>
    )
  }

  // eslint-disable-next-line react-hooks/refs -- ref is mutated by hooks; reading here is intentional
  const submodelsSnapshot = submodelsRef.current

  return (
    <div className="h-full w-full flex flex-col" style={{ background: 'var(--bg-base)' }}>
      <Toolbar
        nodeCount={nodes.length}
        edgeCount={edges.length}
        dirty={dirty}
        canUndo={canUndo}
        canRedo={canRedo}
        onUndo={undo}
        onRedo={redo}
        snapToGrid={snapToGrid}
        onToggleSnapToGrid={() => {
          toggleSnapToGrid()
          addToast("info", useUIStore.getState().snapToGrid ? "Snap to grid ON" : "Snap to grid OFF")
        }}
        onShowShortcuts={() => setShortcutsOpen(true)}
        onOpenSettings={() => setSettingsOpen(true)}
        onAutoLayout={handleAutoLayout}
        onRun={handleRun}
        runStatus={runStatus}
        onSave={handleSave}
        wsStatus={wsStatus}
      />

      <div className="flex-1 flex min-h-0">
        {paletteOpen ? (
          <NodePalette onCollapse={() => setPaletteOpen(false)} nodes={nodes} />
        ) : (
          <button
            onClick={() => setPaletteOpen(true)}
            aria-label="Show node palette"
            className="shrink-0 flex items-center justify-center w-10 transition-colors"
            style={{ background: 'var(--chrome)', borderRight: '1px solid var(--chrome-border)' }}
            onMouseEnter={(e) => e.currentTarget.style.background = 'var(--chrome-hover)'}
            onMouseLeave={(e) => e.currentTarget.style.background = 'var(--chrome)'}
            title="Show node palette"
          >
            <PanelLeftOpen size={16} style={{ color: 'var(--text-muted)' }} />
          </button>
        )}

        <div className="flex-1 flex flex-col min-w-0">
          {syncBanner && (
            <div className="flex items-center gap-2 px-3 py-1.5 text-[12px] font-medium"
              style={{ background: 'rgba(239, 68, 68, 0.15)', color: '#f87171', borderBottom: '1px solid rgba(239, 68, 68, 0.3)' }}>
              <span className="flex-1 truncate">{syncBanner}</span>
              <button onClick={() => setSyncBanner(null)} className="opacity-60 hover:opacity-100">✕</button>
            </div>
          )}
          <div className="flex-1 min-h-0 relative">
            <BreadcrumbBar viewStack={viewStack} onNavigate={handleBreadcrumbNavigate} />
            <ReactFlow
              nodes={nodesWithStatus}
              edges={edgesWithTrace}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              onSelectionChange={onSelectionChange}
              onNodeContextMenu={onNodeContextMenu}
              onNodeDoubleClick={(_event, node) => {
                if (nodeData(node).nodeType === NODE_TYPES.SUBMODEL) {
                  handleDrillIntoSubmodel(node.id)
                }
              }}
              onPaneClick={() => { setContextMenu(null); clearTrace() }}
              onDrop={onDrop}
              onDragOver={onDragOver}
              nodeTypes={nodeTypes}
              selectNodesOnDrag={false}
              selectionMode={SelectionMode.Partial}
              selectionKeyCode={"Shift"}
              snapToGrid={snapToGrid}
              snapGrid={[20, 20]}
              fitView
              fitViewOptions={{ padding: 0.8 }}
              proOptions={{ hideAttribution: true }}
              defaultEdgeOptions={{
                type: "default",
                animated: false,
                style: { stroke: 'rgba(255,255,255,.12)', strokeWidth: 1.5 },
                markerEnd: { type: MarkerType.ArrowClosed, width: 14, height: 14, color: 'rgba(255,255,255,.15)' },
              }}
            >
              <Background variant={BackgroundVariant.Dots} gap={snapToGrid ? 20 : 24} size={1} color={snapToGrid ? "rgba(255,255,255,.1)" : "rgba(255,255,255,.06)"} />
            </ReactFlow>
          </div>

          {optimiserPreview ? (
            <OptimiserPreview
              data={optimiserPreview}
              onClose={() => setOptimiserPreview(null)}
            />
          ) : (
            <DataPreview
              data={previewData}
              onClose={() => { setPreviewData(null); clearTrace() }}
              onCellClick={handleCellClick}
              tracedCell={tracedCell}
            />
          )}
        </div>

        {traceResult ? (
          <TracePanel trace={traceResult} onClose={clearTrace} />
        ) : (
          <NodePanel
            node={selectedNode as unknown as SimpleNode | null}
            edges={edges as unknown as SimpleEdge[]}
            allNodes={nodes as unknown as SimpleNode[]}
            submodels={submodelsSnapshot}
            onClose={() => setSelectedNode(null)}
            onUpdateNode={onUpdateNode}
            onDeleteEdge={handleDeleteEdge}
            onRefreshPreview={() => { if (selectedNode) fetchPreview(selectedNode) }}
            onOptimiserSolve={setOptimiserPreview}
          />
        )}
      </div>

      {contextMenu && (
        <ContextMenu
          x={contextMenu.x}
          y={contextMenu.y}
          nodeId={contextMenu.nodeId}
          nodeLabel={contextMenu.nodeLabel}
          onClose={() => setContextMenu(null)}
          onDelete={handleDeleteNode}
          onDuplicate={handleDuplicateNode}
          onRename={handleRenameNode}
          onCreateInstance={handleCreateInstance}
          isSubmodel={contextMenu.isSubmodel}
          onDissolveSubmodel={handleDissolveSubmodel}
        />
      )}

      {settingsOpen && (
        <SettingsModal
          preamble={preamble}
          onPreambleChange={(value) => {
            setPreamble(value)
            preambleRef.current = value
            setDirty(true)
          }}
          onClose={() => setSettingsOpen(false)}
        />
      )}

      {shortcutsOpen && <KeyboardShortcuts onClose={() => setShortcutsOpen(false)} />}

      {submodelDialog && (
        <SubmodelDialog
          nodeCount={submodelDialog.nodeIds.length}
          onClose={() => setSubmodelDialog(null)}
          onSubmit={(name) => {
            handleCreateSubmodel(name, submodelDialog.nodeIds)
            setSubmodelDialog(null)
          }}
        />
      )}

      <ToastContainer />
    </div>
  )
}

function App() {
  return (
    <ReactFlowProvider>
      <FlowEditor />
    </ReactFlowProvider>
  )
}

export default App
