import { useState, useEffect } from "react"
import { previewNode } from "../api/client"
import useNodeResultsStore from "../stores/useNodeResultsStore"
import { buildGraph } from "../utils/buildGraph"
import type { SimpleNode, SimpleEdge } from "../panels/editors/_shared"

/**
 * Fetches and caches columns from a data input node.
 * Shows cached columns immediately (no loading flash), re-fetches if stale.
 */
export function useDataInputColumns(
  dataInput: string,
  allNodes: SimpleNode[],
  edges: SimpleEdge[],
  submodels?: Record<string, unknown>,
): { name: string; dtype: string }[] {
  const setColumnsCache = useNodeResultsStore((s) => s.setColumns)
  // Split into two leaf selectors so Zustand's Object.is check works
  // (avoids creating a new object on every store update)
  const cachedColumns = useNodeResultsStore((s) =>
    dataInput ? s.columnCache[dataInput]?.columns ?? null : null,
  )
  const isCacheFresh = useNodeResultsStore((s) => {
    if (!dataInput) return false
    const entry = s.columnCache[dataInput]
    return entry ? entry.graphVersion === s.graphVersion : false
  })

  const [dataInputColumns, setDataInputColumns] = useState<{ name: string; dtype: string }[]>(
    cachedColumns ?? [],
  )

  useEffect(() => {
    if (!dataInput) {
      setDataInputColumns([])
      return
    }
    // Show cached columns immediately (no loading flash)
    if (cachedColumns) {
      setDataInputColumns(cachedColumns)
      if (isCacheFresh) return // cache is current, skip API call
    }
    // Fetch fresh columns (cached value shown meanwhile)
    const graph = buildGraph(allNodes, edges, submodels)
    previewNode(graph, dataInput, 1)
      .then((result) => {
        if (result.columns) {
          setDataInputColumns(result.columns)
          // getState() in .then() callback: reads graphVersion at completion time,
          // not at effect setup time, so the cached version stays accurate.
          setColumnsCache(dataInput, result.columns, useNodeResultsStore.getState().graphVersion)
        }
      })
      .catch((e) => {
        console.warn("Column fetch failed", e)
        if (!cachedColumns) setDataInputColumns([])
      })
  }, [dataInput, allNodes, edges, submodels, setColumnsCache, cachedColumns, isCacheFresh]) // re-fetch when input, graph, or cache updates

  return dataInputColumns
}
