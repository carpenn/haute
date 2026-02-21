import { useEffect } from "react"
import type { Node, Edge } from "@xyflow/react"
import { getLayoutedElements } from "../utils/layout"
import type { ToastMessage } from "../components/Toast"

interface WebSocketSyncParams {
  setNodesRaw: (nodes: Node[]) => void
  setEdgesRaw: (edges: Edge[]) => void
  setPreamble: (p: string) => void
  preambleRef: React.MutableRefObject<string>
  nodeIdCounter: React.MutableRefObject<number>
  setSyncBanner: (banner: string | null) => void
  addToast: (type: ToastMessage["type"], text: string) => void
  fitView: (options?: { padding?: number }) => void
}

export default function useWebSocketSync({
  setNodesRaw, setEdgesRaw, setPreamble, preambleRef,
  nodeIdCounter, setSyncBanner, addToast, fitView,
}: WebSocketSyncParams) {
  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:"
    const wsUrl = `${protocol}//${window.location.host}/ws/sync`
    let ws: WebSocket | null = null
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null
    let mounted = true

    function connect() {
      if (!mounted) return
      ws = new WebSocket(wsUrl)

      ws.onmessage = async (event) => {
        try {
          const msg = JSON.parse(event.data)

          if (msg.type === "graph_update" && msg.graph) {
            const g = msg.graph
            const newNodes = g.nodes || []
            const newEdges = (g.edges || []).map((e: Edge) => ({ ...e, type: "default", animated: false }))

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
            nodeIdCounter.current = newNodes.length
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
        if (mounted) reconnectTimer = setTimeout(connect, 3000)
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
  }, [setNodesRaw, setEdgesRaw, setPreamble, preambleRef, nodeIdCounter, setSyncBanner, addToast, fitView])
}
