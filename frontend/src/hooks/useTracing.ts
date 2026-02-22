import { useCallback, useMemo, useState } from "react"
import type { Node, Edge } from "@xyflow/react"
import { MarkerType } from "@xyflow/react"
import type { TraceResult } from "../types/trace"
import { NODE_TYPES } from "../utils/nodeTypes"
import { nodeData } from "../types/node"
import { traceCell } from "../api/client"
import useUIStore from "../stores/useUIStore"

interface TracingParams {
  nodes: Node[]
  edges: Edge[]
  selectedNode: Node | null
  graphRef: React.MutableRefObject<{ nodes: Node[]; edges: Edge[] }>
  parentGraphRef: React.MutableRefObject<{ nodes: Node[]; edges: Edge[]; submodels: Record<string, unknown> } | null>
  submodelsRef: React.MutableRefObject<Record<string, unknown>>
  nodeStatuses: Record<string, "ok" | "error" | "running">
}

export interface TracingReturn {
  traceResult: TraceResult | null
  tracedCell: { rowIndex: number; column: string } | null
  handleCellClick: (rowIndex: number, column: string) => void
  clearTrace: () => void
  nodesWithStatus: Node[]
  edgesWithTrace: Edge[]
}

export default function useTracing({
  nodes, edges, selectedNode,
  graphRef, parentGraphRef, submodelsRef,
  nodeStatuses,
}: TracingParams): TracingReturn {
  const { rowLimit, addToast } = useUIStore()
  const [traceResult, setTraceResult] = useState<TraceResult | null>(null)
  const [tracedCell, setTracedCell] = useState<{ rowIndex: number; column: string } | null>(null)

  const clearTrace = useCallback(() => {
    setTraceResult(null)
    setTracedCell(null)
  }, [])

  const handleCellClick = useCallback((rowIndex: number, column: string) => {
    if (!selectedNode) return
    const graph = parentGraphRef.current
      ? { nodes: parentGraphRef.current.nodes, edges: parentGraphRef.current.edges, submodels: parentGraphRef.current.submodels }
      : { nodes: graphRef.current.nodes, edges: graphRef.current.edges, submodels: submodelsRef.current }
    setTracedCell({ rowIndex, column })
    traceCell({ graph, row_index: rowIndex, target_node_id: selectedNode.id, column, row_limit: rowLimit })
      .then((data) => {
        if (data.status === "ok" && data.trace) {
          setTraceResult(data.trace as unknown as TraceResult)
        } else {
          addToast("error", data.error || "Trace failed")
          clearTrace()
        }
      })
      .catch((err) => {
        addToast("error", `Trace error: ${err.message}`)
        clearTrace()
      })
  }, [selectedNode, graphRef, parentGraphRef, submodelsRef, rowLimit, addToast, clearTrace])

  // Map child node IDs → submodel placeholder node IDs
  const childToSubmodelId = useMemo(() => {
    const map = new Map<string, string>()
    for (const n of nodes) {
      const d = nodeData(n)
      if (d.nodeType === NODE_TYPES.SUBMODEL) {
        const cfg = d.config || {}
        const childIds: string[] = (cfg.childNodeIds as string[]) || []
        for (const cid of childIds) {
          map.set(cid, n.id)
        }
      }
    }
    return map
  }, [nodes])

  // Map external parent node IDs → port node IDs
  const parentToPortId = useMemo(() => {
    const map = new Map<string, string>()
    for (const n of nodes) {
      if (n.id.startsWith("port_in__")) {
        map.set(n.id.replace("port_in__", ""), n.id)
      } else if (n.id.startsWith("port_out__")) {
        map.set(n.id.replace("port_out__", ""), n.id)
      }
    }
    return map
  }, [nodes])

  const resolveTraceId = useCallback(
    (id: string) => childToSubmodelId.get(id) || parentToPortId.get(id) || id,
    [childToSubmodelId, parentToPortId],
  )

  const allTraceNodeIds = useMemo(() => {
    if (!traceResult) return new Set<string>()
    const ids = new Set<string>()
    for (const s of traceResult.steps) {
      ids.add(resolveTraceId(s.node_id))
    }
    return ids
  }, [traceResult, resolveTraceId])

  const { traceValueMap, relevantNodeIds } = useMemo(() => {
    if (!traceResult) return { traceValueMap: new Map<string, unknown>(), relevantNodeIds: new Set<string>() }
    const valMap = new Map<string, unknown>()
    const relIds = new Set<string>()
    for (const s of traceResult.steps) {
      if (!s.column_relevant) continue
      const visibleId = resolveTraceId(s.node_id)
      relIds.add(visibleId)
      if (traceResult.column && s.output_values[traceResult.column] !== undefined) {
        valMap.set(visibleId, s.output_values[traceResult.column])
      } else {
        const k = s.schema_diff.columns_added[0] || s.schema_diff.columns_modified[0]
        if (k) valMap.set(visibleId, s.output_values[k])
      }
    }
    return { traceValueMap: valMap, relevantNodeIds: relIds }
  }, [traceResult, resolveTraceId])

  const nodesWithStatus = useMemo(() => {
    const hasTrace = traceResult !== null
    return nodes.map((n) => {
      const status = nodeStatuses[n.id]
      const inTrace = allTraceNodeIds.has(n.id)
      const dimmed = hasTrace && !inTrace
      return {
        ...n,
        data: {
          ...n.data,
          _status: status,
          _traceActive: hasTrace && relevantNodeIds.has(n.id),
          _traceDimmed: dimmed,
          _traceValue: traceValueMap.get(n.id),
        },
        style: {
          ...(n.style || {}),
          opacity: dimmed ? 0.4 : 1,
          transition: 'opacity 0.2s ease',
          ...(dimmed ? { outline: '1px solid rgba(255,255,255,.08)' } : {}),
        },
      }
    })
  }, [nodes, nodeStatuses, traceResult, allTraceNodeIds, relevantNodeIds, traceValueMap])

  const edgesWithTrace = useMemo(() => {
    if (!traceResult) return edges
    return edges.map((e) => {
      const srcInTrace = allTraceNodeIds.has(e.source)
      const tgtInTrace = allTraceNodeIds.has(e.target)
      if (srcInTrace && tgtInTrace) {
        return {
          ...e,
          style: { stroke: 'var(--accent)', strokeWidth: 2.5, filter: 'drop-shadow(0 0 4px var(--accent))' },
          markerEnd: { type: MarkerType.ArrowClosed as const, width: 14, height: 14, color: 'var(--accent)' },
          animated: true,
        }
      }
      return {
        ...e,
        style: { stroke: 'rgba(255,255,255,.05)', strokeWidth: 1 },
        markerEnd: { type: MarkerType.ArrowClosed as const, width: 14, height: 14, color: 'rgba(255,255,255,.05)' },
      }
    })
  }, [edges, traceResult, allTraceNodeIds])

  return {
    traceResult, tracedCell,
    handleCellClick, clearTrace,
    nodesWithStatus, edgesWithTrace,
  }
}
