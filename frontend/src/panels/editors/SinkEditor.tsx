import { useState } from "react"
import { HardDriveDownload } from "lucide-react"
import type { SimpleNode, SimpleEdge, OnUpdateConfig } from "./_shared"
import { executeSink } from "../../api/client"
import { configField } from "../../utils/configField"
import { buildGraph } from "../../utils/buildGraph"
import useUIStore from "../../stores/useUIStore"

export default function SinkEditor({
  config,
  onUpdate,
  nodeId,
  allNodes,
  edges,
  submodels,
  preamble,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  nodeId: string
  allNodes: SimpleNode[]
  edges: SimpleEdge[]
  submodels?: Record<string, unknown>
  preamble?: string
}) {
  const format = configField(config, "format", "parquet")
  const [writing, setWriting] = useState(false)
  const [writeResult, setWriteResult] = useState<{ status: string; message: string } | null>(null)

  const hasPath = Boolean(config.path)

  const handleWrite = () => {
    if (!hasPath || writing) return
    setWriting(true)
    setWriteResult(null)

    const graph = buildGraph(allNodes, edges, submodels, preamble)

    executeSink(graph, nodeId, useUIStore.getState().activeScenario)
      .then((data) => {
        setWriteResult({ status: data.status || "ok", message: data.message || "Written successfully" })
        setWriting(false)
      })
      .catch((err: Error) => {
        setWriteResult({ status: "error", message: err.message })
        setWriting(false)
      })
  }

  return (
    <div className="px-4 py-3 space-y-3">
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Format</label>
        <div className="mt-1 flex gap-1.5">
          {["parquet", "csv"].map((fmt) => (
            <button
              key={fmt}
              onClick={() => onUpdate("format", fmt)}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: format === fmt ? 'rgba(245,158,11,.1)' : 'var(--bg-input)',
                border: format === fmt ? '1px solid #f59e0b' : '1px solid var(--border)',
                color: format === fmt ? '#f59e0b' : 'var(--text-secondary)',
              }}
            >
              {fmt.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
          Output Path
        </label>
        <input
          type="text"
          placeholder={format === "csv" ? "output/results.csv" : "output/results.parquet"}
          defaultValue={configField(config, "path", "")}
          onChange={(e) => onUpdate("path", e.target.value)}
          className="w-full px-2.5 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
          style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
          onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(245,158,11,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px rgba(245,158,11,.1)' }}
          onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
        />
      </div>

      <button
        onClick={handleWrite}
        disabled={!hasPath || writing}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 text-[12px] font-semibold rounded-lg transition-colors disabled:opacity-40"
        style={{ background: '#f59e0b', color: '#000' }}
        onMouseEnter={(e) => { if (hasPath && !writing) e.currentTarget.style.background = '#fbbf24' }}
        onMouseLeave={(e) => { e.currentTarget.style.background = '#f59e0b' }}
      >
        <HardDriveDownload size={14} />
        {writing ? "Writing..." : "Write"}
      </button>

      {writeResult && (
        <div
          className="px-2.5 py-2 rounded-lg text-xs"
          style={{
            background: writeResult.status === "ok" ? 'rgba(34,197,94,.1)' : 'rgba(239,68,68,.1)',
            border: writeResult.status === "ok" ? '1px solid rgba(34,197,94,.2)' : '1px solid rgba(239,68,68,.2)',
            color: writeResult.status === "ok" ? '#4ade80' : '#f87171',
          }}
        >
          {writeResult.message}
        </div>
      )}
    </div>
  )
}
