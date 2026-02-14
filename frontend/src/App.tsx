import { useEffect, useCallback, useState, useRef, useMemo, type DragEvent } from "react"
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  useNodesState,
  useEdgesState,
  useReactFlow,
  type Node,
  type Edge,
  type OnConnect,
  type OnSelectionChangeFunc,
  addEdge,
  BackgroundVariant,
  MarkerType,
} from "@xyflow/react"
import "@xyflow/react/dist/style.css"
import ELK from "elkjs/lib/elk.bundled.js"

import PipelineNode from "./nodes/PipelineNode"
import NodePalette from "./panels/NodePalette"
import NodePanel from "./panels/NodePanel"
import DataPreview, { type PreviewData } from "./panels/DataPreview"
import ToastContainer, { type ToastMessage } from "./components/Toast"
import ContextMenu from "./components/ContextMenu"
import { PanelLeftOpen, Settings } from "lucide-react"

const nodeTypes = {
  dataSource: PipelineNode,
  transform: PipelineNode,
  modelScore: PipelineNode,
  ratingStep: PipelineNode,
  output: PipelineNode,
  dataSink: PipelineNode,
  externalFile: PipelineNode,
}

const labelMap: Record<string, string> = {
  dataSource: "Data Source",
  transform: "Polars",
  modelScore: "Model Score",
  ratingStep: "Rating Step",
  output: "Output",
  dataSink: "Data Sink",
  externalFile: "External File",
}

function makePreviewData(
  nodeId: string,
  nodeLabel: string,
  overrides: Partial<PreviewData> = {},
): PreviewData {
  return {
    nodeId,
    nodeLabel,
    status: "ok",
    row_count: 0,
    column_count: 0,
    columns: [],
    preview: [],
    error: null,
    ...overrides,
  }
}

let nodeIdCounter = 0
let toastCounter = 0

const elk = new ELK()

async function getLayoutedElements(nodes: Node[], edges: Edge[]): Promise<Node[]> {
  const elkGraph = {
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "RIGHT",
      "elk.spacing.nodeNode": "60",
      "elk.layered.spacing.nodeNodeBetweenLayers": "120",
      "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
    },
    children: nodes.map((n) => ({
      id: n.id,
      width: 240,
      height: 70,
    })),
    edges: edges.map((e) => ({
      id: e.id,
      sources: [e.source],
      targets: [e.target],
    })),
  }

  const layout = await elk.layout(elkGraph)
  const posMap = new Map<string, { x: number; y: number }>()
  for (const child of layout.children || []) {
    posMap.set(child.id, { x: child.x ?? 0, y: child.y ?? 0 })
  }

  return nodes.map((n) => ({
    ...n,
    position: posMap.get(n.id) || n.position,
  }))
}

function FlowEditor() {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [loading, setLoading] = useState(true)
  const [selectedNode, setSelectedNode] = useState<Node | null>(null)
  const [previewData, setPreviewData] = useState<PreviewData | null>(null)
  const [nodeStatuses, setNodeStatuses] = useState<Record<string, "ok" | "error" | "running">>({})
  const [runStatus, setRunStatus] = useState<string | null>(null)
  const [dirty, setDirty] = useState(false)
  const [rowLimit, setRowLimit] = useState(1000)
  const [toasts, setToasts] = useState<ToastMessage[]>([])
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; nodeId: string; nodeLabel: string } | null>(null)
  const [syncBanner, setSyncBanner] = useState<string | null>(null)
  const [paletteOpen, setPaletteOpen] = useState(true)
  const [preamble, setPreamble] = useState("")
  const [settingsOpen, setSettingsOpen] = useState(false)
  const graphRef = useRef<{ nodes: Node[]; edges: Edge[] }>({ nodes: [], edges: [] })
  const lastSavedRef = useRef<string>("")
  const preambleRef = useRef("")
  const { screenToFlowPosition, fitView } = useReactFlow()

  const addToast = useCallback((type: ToastMessage["type"], text: string) => {
    toastCounter += 1
    setToasts((prev) => [...prev, { id: String(toastCounter), type, text }])
  }, [])

  const dismissToast = useCallback((id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id))
  }, [])

  // Keep graphRef in sync so callbacks never see stale state
  graphRef.current = { nodes, edges }

  // Track dirty state (includes preamble so preamble-only changes are tracked)
  useEffect(() => {
    const snapshot = JSON.stringify({ nodes, edges, preamble })
    if (lastSavedRef.current && snapshot !== lastSavedRef.current) {
      setDirty(true)
    }
  }, [nodes, edges, preamble])

  // WebSocket connection for live code ↔ GUI sync
  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:"
    const wsUrl = `${protocol}//${window.location.host}/ws/sync`
    let ws: WebSocket | null = null
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null

    function connect() {
      ws = new WebSocket(wsUrl)

      ws.onmessage = async (event) => {
        try {
          const msg = JSON.parse(event.data)

          if (msg.type === "graph_update" && msg.graph) {
            const g = msg.graph
            const newNodes = g.nodes || []
            const newEdges = (g.edges || []).map((e: Edge) => ({ ...e, type: "default", animated: false }))

            // If no saved positions, auto-layout with ELK
            const hasPositions = newNodes.some(
              (n: Node) => n.position && (n.position.x !== 0 || n.position.y !== 0)
            )

            if (hasPositions) {
              setNodes(newNodes)
            } else {
              const layouted = await getLayoutedElements(newNodes, newEdges)
              setNodes(layouted)
            }
            setEdges(newEdges)
            if (g.preamble !== undefined) {
              setPreamble(g.preamble || "")
              preambleRef.current = g.preamble || ""
            }
            nodeIdCounter = newNodes.length
            setSyncBanner(null)
            addToast("info", "Pipeline updated from file")
            setTimeout(() => fitView({ padding: 0.8 }), 100)
          }

          if (msg.type === "parse_error") {
            setSyncBanner(msg.error || "Parse error in pipeline file")
          }
        } catch (err) {
          console.error("WebSocket message error:", err)
        }
      }

      ws.onclose = () => {
        reconnectTimer = setTimeout(connect, 3000)
      }

      ws.onerror = () => {
        ws?.close()
      }
    }

    connect()

    return () => {
      if (reconnectTimer) clearTimeout(reconnectTimer)
      ws?.close()
    }
  }, [setNodes, setEdges, fitView, addToast])

  useEffect(() => {
    fetch("/api/pipeline")
      .then((res) => {
        if (!res.ok && res.status !== 404) throw new Error(`HTTP ${res.status}`)
        if (res.status === 404) return { nodes: [], edges: [] }
        return res.json()
      })
      .then((data) => {
        setNodes(data.nodes || [])
        setEdges((data.edges || []).map((e: Edge) => ({ ...e, type: "default", animated: false })))
        if (data.preamble !== undefined) {
          setPreamble(data.preamble || "")
          preambleRef.current = data.preamble || ""
        }
        nodeIdCounter = (data.nodes || []).length
        lastSavedRef.current = JSON.stringify({ nodes: data.nodes || [], edges: data.edges || [], preamble: data.preamble || "" })
        setLoading(false)
      })
      .catch((err) => {
        console.error("Failed to load pipeline:", err)
        // Start with blank canvas even on error
        setLoading(false)
      })
  }, [setNodes, setEdges])

  const onConnect: OnConnect = useCallback(
    (params) => {
      // Prevent self-loops
      if (params.source === params.target) return
      // Prevent duplicate edges
      const { edges: currentEdges } = graphRef.current
      const exists = currentEdges.some(
        (e) => e.source === params.source && e.target === params.target
      )
      if (exists) return
      setEdges((eds) => addEdge(params, eds))
    },
    [setEdges],
  )

  const fetchPreview = useCallback((node: Node) => {
    setPreviewData(makePreviewData(node.id, String(node.data.label || node.id), { status: "loading" }))
    const { nodes: n, edges: e } = graphRef.current
    fetch("/api/pipeline/preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ graph: { nodes: n, edges: e }, nodeId: node.id, rowLimit }),
    })
      .then((r) => r.json())
      .then((result) => {
        setPreviewData(makePreviewData(node.id, String(node.data.label || node.id), {
          status: result.status || "ok",
          row_count: result.row_count || 0,
          column_count: result.column_count || 0,
          columns: result.columns || [],
          preview: result.preview || [],
          error: result.error || null,
        }))
      })
      .catch((err) => {
        setPreviewData(makePreviewData(node.id, String(node.data.label || node.id), { status: "error", error: err.message }))
      })
  }, [rowLimit])

  const onSelectionChange: OnSelectionChangeFunc = useCallback(({ nodes: selectedNodes }) => {
    if (selectedNodes.length === 1) {
      const node = selectedNodes[0]
      setSelectedNode((prev) => {
        if (prev?.id !== node.id) {
          fetchPreview(node)
        }
        return node
      })
    } else if (selectedNodes.length === 0) {
      setSelectedNode(null)
    }
  }, [fetchPreview])

  const handleRun = useCallback(() => {
    if (nodes.length === 0) return
    setRunStatus("Running...")
    // Mark all nodes as running
    const running: Record<string, "running"> = {}
    nodes.forEach((n) => { running[n.id] = "running" })
    setNodeStatuses(running)

    fetch("/api/pipeline/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ graph: { nodes, edges } }),
    })
      .then((r) => r.json())
      .then((data) => {
        const statuses: Record<string, "ok" | "error"> = {}
        const results = data.results || {}
        for (const [nodeId, result] of Object.entries(results) as [string, any][]) {
          statuses[nodeId] = result.status === "ok" ? "ok" : "error"
        }
        setNodeStatuses(statuses)
        setRunStatus("Done")
        setTimeout(() => setRunStatus(null), 3000)

        // If a node is selected, update its preview with run results
        if (selectedNode && results[selectedNode.id]) {
          const r = results[selectedNode.id]
          setPreviewData(makePreviewData(selectedNode.id, String(selectedNode.data.label || selectedNode.id), {
            status: r.status || "ok",
            row_count: r.row_count || 0,
            column_count: r.column_count || 0,
            columns: r.columns || [],
            preview: r.preview || [],
            error: r.error || null,
          }))
        }
      })
      .catch((err) => {
        setRunStatus("Error")
        setNodeStatuses({})
        console.error("Run failed:", err)
        setTimeout(() => setRunStatus(null), 3000)
      })
  }, [nodes, edges, selectedNode])

  const handleSave = useCallback(() => {
    const { nodes: n, edges: e } = graphRef.current
    fetch("/api/pipeline/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: "my_pipeline",
        description: "",
        graph: { nodes: n, edges: e },
        preamble: preambleRef.current,
      }),
    })
      .then((r) => r.json())
      .then((data) => {
        lastSavedRef.current = JSON.stringify({ nodes: n, edges: e, preamble: preambleRef.current })
        setDirty(false)
        addToast("success", `Saved → ${data.file}`)
      })
      .catch(() => {
        addToast("error", "Failed to save pipeline")
      })
  }, [addToast])

  const onUpdateNode = useCallback(
    (id: string, data: Record<string, unknown>) => {
      setNodes((nds) =>
        nds.map((n) => (n.id === id ? { ...n, data } : n))
      )
      setSelectedNode((prev) => (prev && prev.id === id ? { ...prev, data } : prev))
    },
    [setNodes],
  )

  const handleDeleteEdge = useCallback((edgeId: string) => {
    setEdges((eds) => eds.filter((e) => e.id !== edgeId))
  }, [setEdges])

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName
      const isTyping = tag === "INPUT" || tag === "TEXTAREA"

      // Ctrl+S / Cmd+S → save
      if ((e.ctrlKey || e.metaKey) && e.key === "s") {
        e.preventDefault()
        handleSave()
        return
      }

      // Delete / Backspace → remove selected nodes and/or edges (unless typing)
      if ((e.key === "Delete" || e.key === "Backspace") && !isTyping) {
        const { nodes: currentNodes, edges: currentEdges } = graphRef.current
        const selectedNodeIds = new Set(currentNodes.filter((n) => n.selected).map((n) => n.id))
        const selectedEdgeIds = new Set(currentEdges.filter((ed) => ed.selected).map((ed) => ed.id))
        if (selectedNodeIds.size === 0 && selectedEdgeIds.size === 0) return
        if (selectedNodeIds.size > 0) {
          setNodes(currentNodes.filter((n) => !selectedNodeIds.has(n.id)))
          setEdges(currentEdges.filter((ed) => !selectedNodeIds.has(ed.source) && !selectedNodeIds.has(ed.target)))
          setSelectedNode(null)
          setPreviewData(null)
        } else {
          setEdges(currentEdges.filter((ed) => !selectedEdgeIds.has(ed.id)))
        }
      }
    }
    window.addEventListener("keydown", handler)
    return () => window.removeEventListener("keydown", handler)
  }, [handleSave, setNodes, setEdges])

  const handleDeleteNode = useCallback((id: string) => {
    const { nodes: n, edges: e } = graphRef.current
    setNodes(n.filter((node) => node.id !== id))
    setEdges(e.filter((edge) => edge.source !== id && edge.target !== id))
    setSelectedNode((prev) => (prev?.id === id ? null : prev))
    setPreviewData((prev) => (prev?.nodeId === id ? null : prev))
  }, [setNodes, setEdges])

  const handleDuplicateNode = useCallback((id: string) => {
    const { nodes: n } = graphRef.current
    const original = n.find((node) => node.id === id)
    if (!original) return
    nodeIdCounter += 1
    const newId = `${original.type}_${nodeIdCounter}`
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

  // Memoize nodes with status to avoid re-creating every node object on each render
  const nodesWithStatus = useMemo(() => {
    if (Object.keys(nodeStatuses).length === 0) return nodes
    return nodes.map((n) => {
      const status = nodeStatuses[n.id]
      if (status === n.data._status) return n
      return { ...n, data: { ...n.data, _status: status } }
    })
  }, [nodes, nodeStatuses])

  const onNodeContextMenu = useCallback((event: React.MouseEvent, node: Node) => {
    event.preventDefault()
    setContextMenu({ x: event.clientX, y: event.clientY, nodeId: node.id, nodeLabel: String(node.data.label) })
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

      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      })

      nodeIdCounter += 1
      const id = `${type}_${nodeIdCounter}`

      const newNode: Node = {
        id,
        type,
        position,
        data: {
          label: `${labelMap[type] || "Node"} ${nodeIdCounter}`,
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

  if (loading) {
    return (
      <div className="h-full w-full flex items-center justify-center" style={{ background: 'var(--bg-base)' }}>
        <div className="text-sm" style={{ color: 'var(--text-muted)' }}>Loading pipeline...</div>
      </div>
    )
  }

  return (
    <div className="h-full w-full flex flex-col" style={{ background: 'var(--bg-base)' }}>
      <header className="h-11 flex items-center px-4 shrink-0" style={{ background: 'var(--chrome)', borderBottom: '1px solid var(--chrome-border)' }}>
        <div className="flex items-center gap-2.5">
          <h1 className="text-sm font-bold tracking-tight" style={{ color: 'var(--text-primary)' }}>Runway</h1>
          <span className="text-[11px] font-mono" style={{ color: 'var(--text-muted)' }}>v0.1.0</span>
          {dirty && <span className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse-dot" title="Unsaved changes" />}
        </div>
        <div className="ml-auto flex items-center gap-1.5">
          <span className="text-[12px] mr-2" style={{ color: 'var(--text-muted)' }}>
            {nodes.length} nodes · {edges.length} edges
          </span>
          <div className="flex items-center gap-1 mr-1" title="Row limit for preview (0 = no limit)">
            <label className="text-[11px] font-medium" style={{ color: 'var(--text-muted)' }}>Rows</label>
            <input
              type="number"
              min={0}
              step={100}
              value={rowLimit}
              onChange={(e) => setRowLimit(Math.max(0, parseInt(e.target.value) || 0))}
              className="w-16 px-1.5 py-0.5 text-[12px] font-mono rounded text-center focus:outline-none"
              style={{ background: 'var(--chrome-hover)', border: '1px solid var(--chrome-border)', color: 'var(--text-primary)' }}
            />
          </div>
          <button
            onClick={() => setSettingsOpen(true)}
            className="px-2.5 py-1 text-[12px] font-medium rounded-md transition-colors flex items-center gap-1"
            style={{ color: 'var(--text-secondary)' }}
            onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-primary)' }}
            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)' }}
            title="Pipeline settings (imports, helpers)"
          >
            <Settings size={13} />
            Imports
          </button>
          <button
            onClick={handleAutoLayout}
            disabled={nodes.length === 0}
            className="px-2.5 py-1 text-[12px] font-medium rounded-md transition-colors disabled:opacity-30"
            style={{ color: 'var(--text-secondary)' }}
            onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-primary)' }}
            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)' }}
            title="Auto-arrange nodes"
          >
            Layout
          </button>
          <button
            onClick={handleRun}
            disabled={nodes.length === 0}
            className="px-3 py-1 text-[12px] font-semibold text-white rounded-md transition-colors disabled:opacity-30"
            style={{ background: '#22c55e' }}
            onMouseEnter={(e) => e.currentTarget.style.background = '#4ade80'}
            onMouseLeave={(e) => e.currentTarget.style.background = '#22c55e'}
          >
            {runStatus === "Running..." ? "Running..." : "Run"}
          </button>
          <button
            onClick={handleSave}
            className="px-3 py-1 text-[12px] font-semibold text-white rounded-md transition-colors"
            style={{ background: 'var(--accent)' }}
            onMouseEnter={(e) => e.currentTarget.style.background = '#60a5fa'}
            onMouseLeave={(e) => e.currentTarget.style.background = 'var(--accent)'}
            title="Ctrl+S"
          >
            Save
          </button>
        </div>
      </header>

      <div className="flex-1 flex min-h-0">
        {paletteOpen ? (
          <NodePalette onCollapse={() => setPaletteOpen(false)} />
        ) : (
          <button
            onClick={() => setPaletteOpen(true)}
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
          <div className="flex-1 min-h-0">
            <ReactFlow
              nodes={nodesWithStatus}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              onSelectionChange={onSelectionChange}
              onNodeContextMenu={onNodeContextMenu}
              onPaneClick={() => setContextMenu(null)}
              onDrop={onDrop}
              onDragOver={onDragOver}
              nodeTypes={nodeTypes}
              selectNodesOnDrag={false}
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

          <DataPreview data={previewData} onClose={() => setPreviewData(null)} />
        </div>

        <NodePanel
          node={selectedNode as any}
          edges={edges as any}
          allNodes={nodes as any}
          onClose={() => setSelectedNode(null)}
          onUpdateNode={onUpdateNode}
          onDeleteEdge={handleDeleteEdge}
        />
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
        />
      )}

      {settingsOpen && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center"
          style={{ background: 'rgba(0,0,0,.5)' }}
          onClick={(e) => { if (e.target === e.currentTarget) setSettingsOpen(false) }}
        >
          <div className="w-[560px] max-h-[80vh] flex flex-col rounded-xl overflow-hidden shadow-2xl" style={{ background: 'var(--bg-panel)', border: '1px solid var(--border)' }}>
            <div className="px-4 py-3 flex items-center justify-between shrink-0" style={{ borderBottom: '1px solid var(--border)' }}>
              <div>
                <h2 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>Pipeline Imports &amp; Helpers</h2>
                <p className="text-[11px] mt-0.5" style={{ color: 'var(--text-muted)' }}>
                  Extra imports, constants, and helper functions. Preserved across GUI saves.
                </p>
              </div>
              <button
                onClick={() => setSettingsOpen(false)}
                className="p-1 rounded transition-colors"
                style={{ color: 'var(--text-muted)' }}
                onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-hover)'}
                onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
              >
                ✕
              </button>
            </div>
            <div className="flex-1 min-h-0 p-4">
              <div className="text-[11px] font-mono mb-2 px-1" style={{ color: 'var(--text-muted)' }}>
                <span style={{ color: 'rgba(96,165,250,.5)' }}>import polars as pl</span> and <span style={{ color: 'rgba(96,165,250,.5)' }}>import runw</span> are always included
              </div>
              <textarea
                defaultValue={preamble}
                onChange={(e) => {
                  setPreamble(e.target.value)
                  preambleRef.current = e.target.value
                  setDirty(true)
                }}
                spellCheck={false}
                placeholder={"import numpy as np\nimport catboost\nfrom sklearn.preprocessing import StandardScaler\n\n# Helper functions\ndef my_helper(x):\n    return x * 2"}
                className="w-full h-[300px] px-3 py-2.5 text-[12px] font-mono rounded-lg focus:outline-none focus:ring-2 resize-none"
                style={{
                  background: 'var(--bg-input)',
                  border: '1px solid var(--border)',
                  color: '#a5f3fc',
                  caretColor: 'var(--accent)',
                  lineHeight: '1.625',
                }}
                onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(59,130,246,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
                onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
              />
            </div>
            <div className="px-4 py-3 flex justify-end shrink-0" style={{ borderTop: '1px solid var(--border)' }}>
              <button
                onClick={() => setSettingsOpen(false)}
                className="px-4 py-1.5 text-[12px] font-semibold text-white rounded-md transition-colors"
                style={{ background: 'var(--accent)' }}
                onMouseEnter={(e) => e.currentTarget.style.background = '#60a5fa'}
                onMouseLeave={(e) => e.currentTarget.style.background = 'var(--accent)'}
              >
                Done
              </button>
            </div>
          </div>
        </div>
      )}

      <ToastContainer toasts={toasts} onDismiss={dismissToast} />
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
