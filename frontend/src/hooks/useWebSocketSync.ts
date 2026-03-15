import { useEffect, useRef, useState } from "react"
import type { Node, Edge } from "@xyflow/react"
import { getLayoutedElements } from "../utils/layout"
import { computeNextNodeId, normalizeEdges } from "../utils/graphHelpers"
import useToastStore from "../stores/useToastStore"
import useUIStore from "../stores/useUIStore"

export type WsStatus = "connected" | "reconnecting" | "disconnected"

interface WebSocketSyncParams {
  setNodesRaw: (nodes: Node[]) => void
  setEdgesRaw: (edges: Edge[]) => void
  setPreamble: (p: string) => void
  preambleRef: React.MutableRefObject<string>
  nodeIdCounter: React.MutableRefObject<number>
  fitView: (options?: { padding?: number }) => void
}

const MAX_RETRIES = 50
const INITIAL_BACKOFF_MS = 1_000
const MAX_BACKOFF_MS = 30_000

export default function useWebSocketSync({
  setNodesRaw, setEdgesRaw, setPreamble, preambleRef,
  nodeIdCounter, fitView,
}: WebSocketSyncParams): WsStatus {
  const { setSyncBanner } = useUIStore()
  const { addToast } = useToastStore()
  const [status, setStatus] = useState<WsStatus>("reconnecting")
  const retriesRef = useRef(0)

  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:"
    const wsUrl = `${protocol}//${window.location.host}/ws/sync`
    let ws: WebSocket | null = null
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null
    let mounted = true

    function connect() {
      if (!mounted) return
      ws = new WebSocket(wsUrl)

      ws.onopen = () => {
        if (!mounted) return
        retriesRef.current = 0
        setStatus("connected")
      }

      ws.onmessage = async (event) => {
        try {
          const msg = JSON.parse(event.data)

          if (msg.type === "graph_update" && msg.graph) {
            // Skip file-watcher updates when the user has unsaved edits.
            // Their next save will write the .py file, and the subsequent
            // broadcast (after save) will be accepted since dirty is false.
            if (useUIStore.getState().dirty) return

            const g = msg.graph
            const newNodes = g.nodes || []
            const newEdges = normalizeEdges(g.edges || [])

            const hasPositions = newNodes.some(
              (n: Node) => n.position && (n.position.x !== 0 || n.position.y !== 0)
            )

            if (hasPositions) {
              setNodesRaw(newNodes)
            } else {
              const layouted = await getLayoutedElements(newNodes, newEdges)
              setNodesRaw(layouted)
            }
            setEdgesRaw(newEdges)
            if (g.preamble !== undefined) {
              setPreamble(g.preamble || "")
              preambleRef.current = g.preamble || ""
            }
            nodeIdCounter.current = computeNextNodeId(newNodes)
            setSyncBanner(null)
            addToast("info", "Pipeline updated from file")
            setTimeout(() => fitView({ padding: 0.8 }), 100)
          }

          if (msg.type === "parse_error") {
            setSyncBanner(msg.error || "Parse error in pipeline file")
          }
        } catch (err) {
          addToast("error", `WebSocket sync error: ${err instanceof Error ? err.message : String(err)}`)
        }
      }

      ws.onclose = () => {
        if (!mounted) return
        retriesRef.current += 1

        if (retriesRef.current > MAX_RETRIES) {
          setStatus("disconnected")
          return
        }

        setStatus("reconnecting")
        const backoff = Math.min(INITIAL_BACKOFF_MS * 2 ** (retriesRef.current - 1), MAX_BACKOFF_MS)
        reconnectTimer = setTimeout(connect, backoff)
      }

      ws.onerror = () => {
        ws?.close()
      }
    }

    connect()

    return () => {
      mounted = false
      if (reconnectTimer) clearTimeout(reconnectTimer)
      ws?.close()
    }
  }, [setNodesRaw, setEdgesRaw, setPreamble, preambleRef, nodeIdCounter, fitView, setSyncBanner, addToast])

  return status
}
