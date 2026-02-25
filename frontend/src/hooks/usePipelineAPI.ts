import { useEffect, useCallback, useRef, useState } from "react"
import type { Node, Edge } from "@xyflow/react"
import type { PreviewData } from "../panels/DataPreview"
import { makePreviewData } from "../utils/makePreviewData"
import { loadPipeline, previewNode, runPipeline, savePipeline, ApiError } from "../api/client"
import type { NodeResult } from "../api/types"
import useUIStore from "../stores/useUIStore"
import useNodeResultsStore from "../stores/useNodeResultsStore"

interface PipelineAPIParams {
  nodes: Node[]
  edges: Edge[]
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
    timing_ms: r.timing_ms ?? 0,
    timings: r.timings ?? [],
    schema_warnings: r.schema_warnings ?? [],
  })
}

export default function usePipelineAPI({
  nodes, edges, selectedNode,
  graphRef, parentGraphRef, submodelsRef, setNodes,
  setNodesRaw, setEdgesRaw, setPreamble,
  preambleRef, pipelineNameRef, sourceFileRef, lastSavedRef,
  nodeIdCounter,
}: PipelineAPIParams): PipelineAPIReturn {
  const { rowLimit, setDirty, addToast } = useUIStore()
  const [loading, setLoading] = useState(true)
  const [previewData, setPreviewData] = useState<PreviewData | null>(null)
  const [nodeStatuses, setNodeStatuses] = useState<Record<string, "ok" | "error" | "running">>({})
  const [runStatus, setRunStatus] = useState<string | null>(null)
  const previewAbort = useRef<AbortController | null>(null)
  const previewDebounce = useRef<ReturnType<typeof setTimeout> | null>(null)

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

    const graph = parentGraphRef.current
      ? { nodes: parentGraphRef.current.nodes, edges: parentGraphRef.current.edges, submodels: parentGraphRef.current.submodels }
      : { nodes: graphRef.current.nodes, edges: graphRef.current.edges, submodels: submodelsRef.current }

    previewNode(graph, node.id, rowLimit, { signal: controller.signal })
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
  }, [rowLimit, graphRef, parentGraphRef, submodelsRef, setNodes])

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
    if (nodes.length === 0) return
    // Cancel any pending preview since run results will replace it
    previewAbort.current?.abort()
    if (previewDebounce.current) clearTimeout(previewDebounce.current)
    setRunStatus("Running...")
    const running: Record<string, "running"> = {}
    nodes.forEach((n) => { running[n.id] = "running" })
    setNodeStatuses(running)

    runPipeline({ nodes, edges })
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

        if (selectedNode && results[selectedNode.id]) {
          setPreviewData(resultToPreview(selectedNode.id, nodeLabel(selectedNode), results[selectedNode.id]))
        }
      })
      .catch((err) => {
        setRunStatus("Error")
        setNodeStatuses({})
        addToast("error", `Run failed: ${err.message}`)
        setTimeout(() => setRunStatus(null), 3000)
      })
  }, [nodes, edges, selectedNode, setNodes, addToast])

  const handleSave = useCallback(() => {
    const { nodes: n, edges: e } = graphRef.current
    savePipeline({
      name: pipelineNameRef.current,
      description: "",
      graph: { nodes: n, edges: e, submodels: submodelsRef.current },
      preamble: preambleRef.current,
      source_file: sourceFileRef.current,
    })
      .then((data) => {
        lastSavedRef.current = JSON.stringify({ nodes: n, edges: e, preamble: preambleRef.current })
        setDirty(false)
        addToast("success", `Saved → ${data.file}`)
      })
      .catch(() => {
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
