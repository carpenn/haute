import { useEffect, useCallback, useState, useRef } from "react"
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  useReactFlow,
  SelectionMode,
  type Node,
  type Edge,
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
import { ModellingPreview } from "./panels/ModellingPreview"

import TracePanel from "./panels/TracePanel"
import ToastContainer from "./components/Toast"
import { ErrorBoundary } from "./components/ErrorBoundary"
import ContextMenu from "./components/ContextMenu"
import KeyboardShortcuts from "./components/KeyboardShortcuts"
import BreadcrumbBar from "./components/BreadcrumbBar"
import Toolbar from "./components/Toolbar"
import SubmodelDialog from "./components/SubmodelDialog"
import RenameDialog from "./components/RenameDialog"
import UtilityPanel from "./panels/UtilityPanel"
import ImportsPanel from "./panels/ImportsPanel"
import GitPanel from "./panels/GitPanel"

import useUndoRedo from "./hooks/useUndoRedo"
import useWebSocketSync from "./hooks/useWebSocketSync"
import usePipelineAPI from "./hooks/usePipelineAPI"
import useTracing from "./hooks/useTracing"
import useSubmodelNavigation from "./hooks/useSubmodelNavigation"
import useKeyboardShortcuts from "./hooks/useKeyboardShortcuts"
import useBackgroundJobs from "./hooks/useBackgroundJobs"
import useNodeHandlers from "./hooks/useNodeHandlers"
import useEdgeHandlers from "./hooks/useEdgeHandlers"
import useSettingsStore from "./stores/useSettingsStore"
import useUIStore from "./stores/useUIStore"
import useNodeResultsStore from "./stores/useNodeResultsStore"

import { NODE_TYPES } from "./utils/nodeTypes"
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
  // Settings store
  const fetchMlflow = useSettingsStore((s) => s.fetchMlflow)
  // UI store (chrome / layout)
  const paletteOpen = useUIStore((s) => s.paletteOpen)
  const setPaletteOpen = useUIStore((s) => s.setPaletteOpen)
  const utilityOpen = useUIStore((s) => s.utilityOpen)
  const setUtilityOpen = useUIStore((s) => s.setUtilityOpen)
  const importsOpen = useUIStore((s) => s.importsOpen)
  const setImportsOpen = useUIStore((s) => s.setImportsOpen)
  const gitOpen = useUIStore((s) => s.gitOpen)
  const setGitOpen = useUIStore((s) => s.setGitOpen)
  const shortcutsOpen = useUIStore((s) => s.shortcutsOpen)
  const setShortcutsOpen = useUIStore((s) => s.setShortcutsOpen)
  const submodelDialog = useUIStore((s) => s.submodelDialog)
  const setSubmodelDialog = useUIStore((s) => s.setSubmodelDialog)
  const renameDialog = useUIStore((s) => s.renameDialog)
  const setRenameDialog = useUIStore((s) => s.setRenameDialog)
  const syncBanner = useUIStore((s) => s.syncBanner)
  const setSyncBanner = useUIStore((s) => s.setSyncBanner)
  const dirty = useUIStore((s) => s.dirty)
  const setDirty = useUIStore((s) => s.setDirty)

  // Fetch MLflow status once on startup (shared by all panels)
  useEffect(() => { fetchMlflow() }, [fetchMlflow])

  // Local UI state (not worth globalizing)
  const [selectedNode, setSelectedNode] = useState<Node | null>(null)
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; nodeId: string; nodeLabel: string; isSubmodel?: boolean } | null>(null)
  const [preamble, setPreamble] = useState("")
  const lastSelectedNodeRef = useRef<Node | null>(null)

  // Node results store — background jobs + cached results
  const bumpGraphVersion = useNodeResultsStore((s) => s.bumpGraphVersion)
  const getOptimiserPreview = useNodeResultsStore((s) => s.getOptimiserPreview)
  const getModellingPreview = useNodeResultsStore((s) => s.getModellingPreview)


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
    bumpGraphVersion()
  }, [nodes, edges, bumpGraphVersion])

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
    nodeStatuses,
    fetchPreview, refreshPreview, handleSave,
  } = usePipelineAPI({
    selectedNode,
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
    preambleRef,
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

  // Background polling for optimiser/training jobs (survives panel unmount)
  useBackgroundJobs()

  // ---------------------------------------------------------------------------
  // Node + edge interaction handlers (extracted to custom hooks)
  // ---------------------------------------------------------------------------

  const onUpdateNode = useCallback(
    (id: string, data: Record<string, unknown>) => {
      setNodes((nds) => nds.map((n) => (n.id === id ? { ...n, data } : n)))
      setSelectedNode((prev) => (prev && prev.id === id ? { ...prev, data } : prev))
    },
    [setNodes],
  )

  const {
    handleDeleteNode, handleDuplicateNode,
    handleCreateInstance, handleRenameNode, handleAutoLayout,
  } = useNodeHandlers({
    graphRef, nodeIdCounter, lastSelectedNodeRef,
    setNodes, setEdges, setSelectedNode,
    setPreviewData, onUpdateNode, fitView,
  })

  const {
    onConnect, onSelectionChange, onNodeClick, handleDeleteEdge,
    onNodeContextMenu, onDragOver, onDrop,
  } = useEdgeHandlers({
    graphRef, nodeIdCounter, lastSelectedNodeRef,
    setNodes, setEdges, setSelectedNode, setContextMenu,
    fetchPreview, clearTrace, screenToFlowPosition,
  })

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
        dirty={dirty}
        canUndo={canUndo}
        canRedo={canRedo}
        onUndo={undo}
        onRedo={redo}
        onShowShortcuts={() => setShortcutsOpen(true)}
        onOpenUtility={() => { setUtilityOpen(true); setSelectedNode(null); lastSelectedNodeRef.current = null }}
        onOpenImports={() => { setImportsOpen(true); setSelectedNode(null); lastSelectedNodeRef.current = null }}
        onOpenGit={() => { setGitOpen(true); setSelectedNode(null); lastSelectedNodeRef.current = null }}
        onCentre={() => fitView({ padding: 0.8 })}
        onAutoLayout={handleAutoLayout}
        onSave={handleSave}
        wsStatus={wsStatus}
        timings={previewData?.timings}
        memory={previewData?.memory}
      />

      <div className="flex-1 flex min-h-0">
        <nav aria-label="Node palette">
          {paletteOpen ? (
            <ErrorBoundary name="NodePalette">
              <NodePalette onCollapse={() => setPaletteOpen(false)} nodes={nodes} />
            </ErrorBoundary>
          ) : (
            <button
              onClick={() => setPaletteOpen(true)}
              aria-label="Show node palette"
              className="shrink-0 flex items-center justify-center w-10 h-full transition-colors"
              style={{ background: 'var(--chrome)', borderRight: '1px solid var(--chrome-border)' }}
              onMouseEnter={(e) => e.currentTarget.style.background = 'var(--chrome-hover)'}
              onMouseLeave={(e) => e.currentTarget.style.background = 'var(--chrome)'}
              title="Show node palette"
            >
              <PanelLeftOpen size={16} style={{ color: 'var(--text-muted)' }} />
            </button>
          )}
        </nav>

        <main className="flex-1 flex flex-col min-w-0">
          {syncBanner && (
            <div className="flex items-center gap-2 px-3 py-1.5 text-[12px] font-medium"
              style={{ background: 'rgba(239, 68, 68, 0.15)', color: '#f87171', borderBottom: '1px solid rgba(239, 68, 68, 0.3)' }}>
              <span className="flex-1 truncate">{syncBanner}</span>
              <button onClick={() => setSyncBanner(null)} className="opacity-60 hover:opacity-100">✕</button>
            </div>
          )}
          <ErrorBoundary name="Canvas">
            <div className="flex-1 min-h-0 relative">
              <BreadcrumbBar viewStack={viewStack} onNavigate={handleBreadcrumbNavigate} />
              <ReactFlow
                nodes={nodesWithStatus}
                edges={edgesWithTrace}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                onSelectionChange={onSelectionChange}
                onNodeClick={(event, node) => { setUtilityOpen(false); setImportsOpen(false); setGitOpen(false); onNodeClick(event, node) }}
                onNodeContextMenu={onNodeContextMenu}
                onNodeDoubleClick={(_event, node) => {
                  if (nodeData(node).nodeType === NODE_TYPES.SUBMODEL) {
                    handleDrillIntoSubmodel(node.id)
                  }
                }}
                onPaneClick={() => { setContextMenu(null); clearTrace(); setSelectedNode(null); lastSelectedNodeRef.current = null; setUtilityOpen(false); setImportsOpen(false); setGitOpen(false) }}
                onDrop={onDrop}
                onDragOver={onDragOver}
                nodeTypes={nodeTypes}
                panOnDrag={[2]}
                selectionOnDrag
                selectNodesOnDrag
                selectionMode={SelectionMode.Partial}
                selectionKeyCode={null}
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
                <Background variant={BackgroundVariant.Dots} gap={24} size={1} color="rgba(255,255,255,.06)" />
              </ReactFlow>
            </div>
          </ErrorBoundary>

          <ErrorBoundary name="DataPreview">
            {(() => {
              const activeNodeId = selectedNode?.id ?? lastSelectedNodeRef.current?.id
              const modelPreview = activeNodeId ? getModellingPreview(activeNodeId) : null
              if (modelPreview) {
                return (
                  <ModellingPreview
                    data={modelPreview}
                    nodeId={activeNodeId!}
                  />
                )
              }
              const optPreview = activeNodeId ? getOptimiserPreview(activeNodeId) : null
              if (optPreview) {
                return (
                  <OptimiserPreview
                    data={optPreview}
                  />
                )
              }
              return (
                <DataPreview
                  data={previewData}
                  onCellClick={handleCellClick}
                  tracedCell={tracedCell}
                />
              )
            })()}
          </ErrorBoundary>
        </main>

        <aside aria-label="Node properties">
          <ErrorBoundary name="NodePanel">
            {gitOpen ? (
              <GitPanel onClose={() => setGitOpen(false)} />
            ) : utilityOpen ? (
              <UtilityPanel
                onClose={() => setUtilityOpen(false)}
                onImportAdded={(importLine) => {
                  const current = preambleRef.current
                  if (!current.includes(importLine)) {
                    const updated = current ? `${current}\n${importLine}` : importLine
                    setPreamble(updated)
                    preambleRef.current = updated
                    setDirty(true)
                  }
                }}
              />
            ) : importsOpen ? (
              <ImportsPanel
                preamble={preamble}
                onPreambleChange={(value) => {
                  setPreamble(value)
                  preambleRef.current = value
                  setDirty(true)
                }}
                onClose={() => setImportsOpen(false)}
              />
            ) : traceResult ? (
              <TracePanel trace={traceResult} onClose={clearTrace} />
            ) : (
              <NodePanel
                node={(() => {
                  const id = selectedNode?.id ?? lastSelectedNodeRef.current?.id
                  if (!id) return null
                  return (nodes.find((n) => n.id === id) ?? null) as unknown as SimpleNode | null
                })()}
                edges={edges as unknown as SimpleEdge[]}
                allNodes={nodes as unknown as SimpleNode[]}
                submodels={submodelsSnapshot}
                preamble={preamble}
                onClose={() => { setSelectedNode(null); lastSelectedNodeRef.current = null }}
                onUpdateNode={onUpdateNode}
                onDeleteEdge={handleDeleteEdge}
                onRefreshPreview={() => { if (selectedNode) refreshPreview(selectedNode) }}
                dimmed={!selectedNode && !!lastSelectedNodeRef.current}
                errorLine={
                  previewData?.nodeId === (selectedNode ?? lastSelectedNodeRef.current)?.id
                    ? previewData?.error_line ?? null
                    : null
                }
              />
            )}
          </ErrorBoundary>
        </aside>
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

      {renameDialog && (
        <RenameDialog
          defaultValue={renameDialog.currentLabel}
          onCancel={() => setRenameDialog(null)}
          onConfirm={(newName) => {
            const node = graphRef.current.nodes.find((n) => n.id === renameDialog.nodeId)
            if (node) onUpdateNode(renameDialog.nodeId, { ...node.data, label: newName })
            setRenameDialog(null)
          }}
        />
      )}

      <ErrorBoundary name="Toast">
        <ToastContainer />
      </ErrorBoundary>
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
