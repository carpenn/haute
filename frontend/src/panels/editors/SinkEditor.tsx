import { useState } from "react"
import { HardDriveDownload } from "lucide-react"
import type { SimpleNode, SimpleEdge, OnUpdateConfig } from "./_shared"
import { executeSink } from "../../api/client"
import { configField } from "../../utils/configField"
import { withAlpha } from "../../utils/color"
import ToggleButtonGroup from "../../components/ToggleButtonGroup"
import { buildGraph } from "../../utils/buildGraph"
import useSettingsStore from "../../stores/useSettingsStore"
import { EditorLabel } from "../../components/form"

export default function SinkEditor({
  config,
  onUpdate,
  nodeId,
  allNodes,
  edges,
  submodels,
  preamble,
  accentColor,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  nodeId: string
  allNodes: SimpleNode[]
  edges: SimpleEdge[]
  submodels?: Record<string, unknown>
  preamble?: string
  accentColor: string
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

    executeSink(graph, nodeId, useSettingsStore.getState().activeScenario)
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
        <EditorLabel>Format</EditorLabel>
        <div className="mt-1">
          <ToggleButtonGroup
            value={format}
            onChange={(fmt) => onUpdate("format", fmt)}
            options={[
              { key: "parquet", label: "PARQUET" },
              { key: "csv", label: "CSV" },
            ]}
            accentColor={accentColor}
          />
        </div>
      </div>

      <div>
        <EditorLabel className="mb-1.5 block">Output Path</EditorLabel>
        <input
          type="text"
          placeholder={format === "csv" ? "output/results.csv" : "output/results.parquet"}
          defaultValue={configField(config, "path", "")}
          onChange={(e) => onUpdate("path", e.target.value)}
          className="w-full px-2.5 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
          style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
          onFocus={(e) => { e.currentTarget.style.borderColor = withAlpha(accentColor, 0.3); e.currentTarget.style.boxShadow = `0 0 0 2px ${withAlpha(accentColor, 0.1)}` }}
          onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
        />
      </div>

      <button
        onClick={handleWrite}
        disabled={!hasPath || writing}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 text-[12px] font-semibold rounded-lg transition-colors disabled:opacity-40"
        style={{ background: accentColor, color: '#000' }}
        onMouseEnter={(e) => { if (hasPath && !writing) e.currentTarget.style.opacity = '0.85' }}
        onMouseLeave={(e) => { e.currentTarget.style.opacity = '1' }}
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
