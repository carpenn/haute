import { memo } from "react"
import { Handle, Position, type NodeProps } from "@xyflow/react"
import { Radio, Link2 } from "lucide-react"
import PolarsIcon from "../components/PolarsIcon"
import { nodeTypeIcons, nodeTypeColors, nodeTypeLabels } from "../utils/nodeTypes"
import { formatValueCompact } from "../utils/formatValue"

const statusColors: Record<string, string> = {
  ok: "#22c55e",
  error: "#ef4444",
  running: "#6366f1",
}

export type PipelineNodeData = {
  label: string
  description: string
  nodeType: string
  config?: Record<string, unknown>
  _status?: "ok" | "error" | "running"
  _traceActive?: boolean
  _traceDimmed?: boolean
  _traceValue?: unknown
}

function PipelineNode({ data, selected }: NodeProps) {
  const nodeData = data as unknown as PipelineNodeData
  const nodeType = nodeData.nodeType || "transform"
  const Icon = nodeTypeIcons[nodeType] || PolarsIcon
  const accent = nodeTypeColors[nodeType] || nodeTypeColors.transform
  const typeLabel = nodeTypeLabels[nodeType] || "NODE"
  const isDeployInput = nodeType === "apiInput"
  const missingRowId = isDeployInput && !nodeData.config?.row_id_column
  const isInstance = !!(nodeData.config?.instanceOf)
  const traceActive = !!nodeData._traceActive
  const traceDimmed = !!nodeData._traceDimmed
  const traceValue = nodeData._traceValue

  return (
    <div
      className="relative rounded-xl min-w-[180px] max-w-[260px] cursor-pointer"
      style={{
        background: "var(--bg-elevated)",
        border: traceActive
          ? `1.5px solid ${accent}`
          : selected
            ? `1.5px solid ${accent}`
            : isInstance
              ? `1.5px dashed ${accent}60`
              : "1px solid var(--border-bright)",
        boxShadow: traceActive
          ? `0 0 12px ${accent}40, var(--node-shadow)`
          : "var(--node-shadow)",
        opacity: traceDimmed ? 0.3 : 1,
        transition: "border-color 0.15s ease, opacity 0.2s ease, box-shadow 0.2s ease",
      }}
    >
      {/* Left accent stripe */}
      <div
        className="absolute left-0 top-3 bottom-3 w-[3px] rounded-full"
        style={{ backgroundColor: accent, opacity: selected ? 1 : 0.6, transition: "opacity 0.2s ease" }}
      />

      <Handle type="target" position={Position.Left} />

      <div className="pl-4 pr-3 py-2.5">
        <div className="flex items-center gap-2 mb-1">
          <Icon size={12} style={{ color: accent }} className="shrink-0 opacity-80" />
          <span
            className="text-[10px] font-bold uppercase tracking-[0.1em] shrink-0"
            style={{ color: accent, opacity: 0.8 }}
          >
            {typeLabel}
          </span>
          {isInstance && (
            <span
              className="ml-auto inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded-full text-[9px] font-bold uppercase tracking-[0.08em] shrink-0"
              style={{ background: `${accent}15`, color: accent, border: `1px solid ${accent}25` }}
              title={`Instance of ${nodeData.config?.instanceOf}`}
            >
              <Link2 size={8} />
              Instance
            </span>
          )}
          {isDeployInput && (
            <span
              className={`ml-auto inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded-full text-[9px] font-bold uppercase tracking-[0.08em] shrink-0`}
              style={{ background: "rgba(34,197,94,.12)", color: "#22c55e", border: "1px solid rgba(34,197,94,.2)" }}
            >
              <Radio size={8} />
              API
              {missingRowId && (
                <span
                  className="w-[6px] h-[6px] rounded-full shrink-0 ml-0.5"
                  style={{ backgroundColor: "#f59e0b" }}
                  title="Row ID column not set — required for tracing"
                />
              )}
            </span>
          )}
          {nodeData._status && (
            <span
              className={`${isDeployInput ? "" : "ml-auto "} w-[7px] h-[7px] rounded-full shrink-0 ${nodeData._status === "running" ? "animate-pulse-dot" : ""}`}
              style={{ backgroundColor: statusColors[nodeData._status] }}
            />
          )}
        </div>
        <div className="font-semibold text-[13px] leading-tight truncate" style={{ color: "var(--text-primary)" }}>
          {nodeData.label}
        </div>
        {traceActive && traceValue !== undefined && (
          <div
            className="mt-1 px-1.5 py-0.5 rounded text-[11px] font-mono truncate"
            style={{
              background: `${accent}18`,
              color: accent,
              border: `1px solid ${accent}30`,
              maxWidth: "100%",
            }}
          >
            {formatValueCompact(traceValue)}
          </div>
        )}
      </div>

      <Handle type="source" position={Position.Right} />
    </div>
  )
}

export default memo(PipelineNode)
