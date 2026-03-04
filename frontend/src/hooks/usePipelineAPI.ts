import { useEffect, useCallback, useRef, useState } from "react"
import type { Node, Edge } from "@xyflow/react"
import type { PreviewData } from "../panels/DataPreview"
import { makePreviewData } from "../utils/makePreviewData"
import { loadPipeline, previewNode, runPipeline, savePipeline, ApiError } from "../api/client"
import { resolveGraphFromRefs } from "../utils/buildGraph"
import type { NodeResult } from "../api/types"
import useToastStore from "../stores/useToastStore"
import useSettingsStore from "../stores/useSettingsStore"
import useUIStore from "../stores/useUIStore"
import useNodeResultsStore from "../stores/useNodeResultsStore"

interface PipelineAPIParams {
  selectedNode: Node | null
  graphRef: React.MutableRefObject<{ nodes: Node[]; edges: Edge[] }>
  parentGraphRef: React.MutableRefObject<{ nodes: Node[]; edges: Edge[]; submodels: Record<string, unknown> } | null>
  submodelsRef: React.MutableRefObject<Record<string, unknown>>
  setNodes: (updater: Node[] | ((nds: Node[]) => Node[])) => void
  setNodesRaw: (nodes: Node[]) => void
  setEdgesRaw: (edges: Edge[]) => void
  setPreamble: (p: string) => void
  preambleRef: React.MutableRefObject<string>
  pipelineNameRef: React.MutableRefObject<string>
  sourceFileRef: React.MutableRefObject<string>
  lastSavedRef: React.MutableRefObject<string>
  nodeIdCounter: React.MutableRefObject<number>
}

export interface PipelineAPIReturn {
  loading: boolean
  previewData: PreviewData | null
  setPreviewData: React.Dispatch<React.SetStateAction<PreviewData | null>>
  nodeStatuses: Record<string, "ok" | "error" | "running">
  runStatus: string | null
  fetchPreview: (node: Node) => void
  handleRun: () => void
  handleSave: () => void
}

function nodeLabel(node: Node): string {
  return String(node.data.label || node.id)
}

function resultToPreview(nodeId: string, label: string, r: NodeResult): PreviewData {
  const status = (r.status === "ok" || r.status === "error" || r.status === "loading") ? r.status : "ok"
  return makePreviewData(nodeId, label, {
    status,
    row_count: r.row_count ?? 0,
    column_count: r.column_count ?? 0,
    columns: r.columns ?? [],
    preview: r.preview ?? [],
    error: r.error ?? null,
    error_line: r.error_line ?? null,
    timing_ms: r.timing_ms ?? 0,
    memory_bytes: r.memory_bytes ?? 0,
    timings: r.timings ?? [],
    memory: r.memory ?? [],
    schema_warnings: r.schema_warnings ?? [],
  })
}

export default function usePipelineAPI({
  selectedNode,
  graphRef, parentGraphRef, submodelsRef, setNodes,
  setNodesRaw, setEdgesRaw, setPreamble,
  preambleRef, pipelineNameRef, sourceFileRef, lastSavedRef,
  nodeIdCounter,
}: PipelineAPIParams): PipelineAPIReturn {
  const rowLimit = useSettingsStore((s) => s.rowLimit)
  const activeScenario = useSettingsStore((s) => s.activeScenario)
  const setDirty = useUIStore((s) => s.setDirty)
  const addToast = useToastStore((s) => s.addToast)
  const [loading, setLoading] = useState(true)
  const [previewData, setPreviewData] = useState<PreviewData | null>(null)
  const [nodeStatuses, setNodeStatuses] = useState<Record<string, "ok" | "error" | "running">>({})
  const [runStatus, setRunStatus] = useState<string | null>(null)
  const previewAbort = useRef<AbortController | null>(null)
  const previewDebounce = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Stable refs for values that change across renders but shouldn't
  // trigger re-creation of callbacks. Read at call-time instead.
  const rowLimitRef = useRef(rowLimit)
  rowLimitRef.current = rowLimit
  const activeScenarioRef = useRef(activeScenario)
  activeScenarioRef.current = activeScenario
  const selectedNodeRef = useRef(selectedNode)
  selectedNodeRef.current = selectedNode

  // Initial pipeline load
  useEffect(() => {
    loadPipeline()
      .then((data) => {
        const pipelineNodes = data.nodes ?? []
        const pipelineEdges = data.edges ?? []
        setNodesRaw(pipelineNodes)
        setEdgesRaw(pipelineEdges.map((e: Edge) => ({ ...e, type: "default", animated: false })))
        if (data.preamble !== undefined) {
          setPreamble(data.preamble || "")
          preambleRef.current = data.preamble || ""
        }
        if (data.pipeline_name) pipelineNameRef.current = data.pipeline_name
        if (data.source_file) sourceFileRef.current = data.source_file
        if (data.submodels) submodelsRef.current = data.submodels
        // Populate scenario state from backend sidecar
        if (data.scenarios && Array.isArray(data.scenarios)) {
          useSettingsStore.getState().setScenarios(data.scenarios)
        }
        if (data.active_scenario) {
          useSettingsStore.getState().setActiveScenario(data.active_scenario)
        }
        nodeIdCounter.current = pipelineNodes.length
        lastSavedRef.current = JSON.stringify({ nodes: pipelineNodes, edges: pipelineEdges, preamble: data.preamble || "" })
        setLoading(false)
      })
      .catch((err) => {
        addToast("error", `Failed to load pipeline: ${err.message}`)
        setLoading(false)
      })
  }, [setNodesRaw, setEdgesRaw, setPreamble, preambleRef, pipelineNameRef, sourceFileRef, submodelsRef, nodeIdCounter, lastSavedRef, addToast])

  const fetchPreviewImmediate = useCallback((node: Node) => {
    // Abort any in-flight preview request
    previewAbort.current?.abort()
    const controller = new AbortController()
    previewAbort.current = controller

    const label = nodeLabel(node)
    const { getPreview, setPreview: storePreview, graphVersion } = useNodeResultsStore.getState()

    // Cache-first: show cached data immediately if available
    const cached = getPreview(node.id)
    if (cached) {
      setPreviewData(cached.data)
      // If cache is fresh (same graph version), skip the API call
      if (cached.graphVersion === graphVersion) return
      // Otherwise continue to fetch fresh data in background (cached data shown meanwhile)
    } else {
      setPreviewData(makePreviewData(node.id, label, { status: "loading" }))
    }

    const graph = resolveGraphFromRefs(graphRef, parentGraphRef, submodelsRef, preambleRef)

    previewNode(graph, node.id, rowLimitRef.current, activeScenarioRef.current, { signal: controller.signal })
      .then((result) => {
        const preview = resultToPreview(node.id, label, result)
        setPreviewData(preview)
        // Cache the result for next time
        storePreview(node.id, preview, useNodeResultsStore.getState().graphVersion)
        if (result.columns) {
          setNodes((nds) => nds.map((n) => n.id === node.id ? { ...n, data: { ...n.data, _columns: result.columns, _schemaWarnings: result.schema_warnings ?? [] } } : n))
        }
      })
      .catch((err) => {
        if (err instanceof ApiError || err.name !== "AbortError") {
          setPreviewData(makePreviewData(node.id, label, { status: "error", error: err.message }))
        }
      })
  }, [graphRef, parentGraphRef, submodelsRef, preambleRef, setNodes])

  const fetchPreview = useCallback((node: Node) => {
    if (previewDebounce.current) clearTimeout(previewDebounce.current)
    // Show cached data immediately if available (no loading flash)
    const cached = useNodeResultsStore.getState().getPreview(node.id)
    if (cached) {
      setPreviewData(cached.data)
    } else {
      setPreviewData(makePreviewData(node.id, nodeLabel(node), { status: "loading" }))
    }
    previewDebounce.current = setTimeout(() => fetchPreviewImmediate(node), 200)
  }, [fetchPreviewImmediate])

  const handleRun = useCallback(() => {
    const { nodes: currentNodes, edges: currentEdges } = graphRef.current
    if (currentNodes.length === 0) return
    // Cancel any pending preview since run results will replace it
    previewAbort.current?.abort()
    if (previewDebounce.current) clearTimeout(previewDebounce.current)
    setRunStatus("Running...")
    const running: Record<string, "running"> = {}
    currentNodes.forEach((n) => { running[n.id] = "running" })
    setNodeStatuses(running)

    runPipeline({ nodes: currentNodes, edges: currentEdges, preamble: preambleRef.current }, useSettingsStore.getState().activeScenario)
      .then((data) => {
        const statuses: Record<string, "ok" | "error"> = {}
        const results = data.results ?? {}
        for (const [nid, result] of Object.entries(results)) {
          statuses[nid] = result.status === "ok" ? "ok" : "error"
        }
        setNodeStatuses(statuses)
        setRunStatus("Done")
        setTimeout(() => setRunStatus(null), 3000)

        setNodes((nds) => nds.map((n) => {
          const r = results[n.id]
          return r?.columns ? { ...n, data: { ...n.data, _columns: r.columns } } : n
        }))

        const sel = selectedNodeRef.current
        // Read nodes from ref for label lookup (graph may have changed during async run)
        const latestNodes = graphRef.current.nodes
        if (sel && results[sel.id]) {
          // Build timings from all run results so the toolbar can show the full breakdown
          const allTimings = Object.entries(results).map(([nid, r]) => {
            const matchingNode = latestNodes.find((n) => n.id === nid)
            return { node_id: nid, label: matchingNode ? nodeLabel(matchingNode) : nid, timing_ms: r.timing_ms ?? 0 }
          })
          const allMemory = Object.entries(results).map(([nid, r]) => {
            const matchingNode = latestNodes.find((n) => n.id === nid)
            return { node_id: nid, label: matchingNode ? nodeLabel(matchingNode) : nid, memory_bytes: r.memory_bytes ?? 0 }
          })
          const totalMs = allTimings.reduce((s, t) => s + t.timing_ms, 0)
          const totalMemory = allMemory.reduce((s, m) => s + m.memory_bytes, 0)
          const preview = resultToPreview(sel.id, nodeLabel(sel), results[sel.id])
          preview.timings = allTimings
          preview.timing_ms = totalMs
          preview.memory = allMemory
          preview.memory_bytes = totalMemory
          setPreviewData(preview)
        }
      })
      .catch((err) => {
        setRunStatus("Error")
        setNodeStatuses({})
        addToast("error", `Run failed: ${err.message}`)
        setTimeout(() => setRunStatus(null), 3000)
      })
  }, [graphRef, preambleRef, setNodes, addToast])

  const handleSave = useCallback(() => {
    const { nodes: n, edges: e } = graphRef.current
    const { scenarios: sc, activeScenario: as_ } = useSettingsStore.getState()
    savePipeline({
      name: pipelineNameRef.current,
      description: "",
      graph: { nodes: n, edges: e, submodels: submodelsRef.current },
      preamble: preambleRef.current,
      source_file: sourceFileRef.current,
      scenarios: sc,
      active_scenario: as_,
    })
      .then((data) => {
        lastSavedRef.current = JSON.stringify({ nodes: n, edges: e, preamble: preambleRef.current })
        setDirty(false)
        addToast("success", `Saved → ${data.file}`)
      })
      .catch((err: unknown) => {
        console.warn("Pipeline save failed:", err)
        addToast("error", "Failed to save pipeline")
      })
  }, [graphRef, submodelsRef, preambleRef, sourceFileRef, pipelineNameRef, lastSavedRef, setDirty, addToast])

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      previewAbort.current?.abort()
      if (previewDebounce.current) clearTimeout(previewDebounce.current)
    }
  }, [])

  return {
    loading,
    previewData, setPreviewData,
    nodeStatuses,
    runStatus,
    fetchPreview, handleRun, handleSave,
  }
}
