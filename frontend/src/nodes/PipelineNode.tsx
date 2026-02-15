import { memo } from "react"
import { Handle, Position, type NodeProps } from "@xyflow/react"
import { Database, Brain, TableProperties, CircleDot, HardDriveDownload, FileArchive, Radio } from "lucide-react"
import PolarsIcon from "../components/PolarsIcon"

const iconMap: Record<string, React.ElementType> = {
  dataSource: Database,
  transform: PolarsIcon,
  modelScore: Brain,
  ratingStep: TableProperties,
  output: CircleDot,
  dataSink: HardDriveDownload,
  externalFile: FileArchive,
}

const accentMap: Record<string, string> = {
  dataSource: "#3b82f6",
  transform: "#06b6d4",
  modelScore: "#8b5cf6",
  ratingStep: "#10b981",
  output: "#f43f5e",
  dataSink: "#f59e0b",
  externalFile: "#ec4899",
}

const labelMap: Record<string, string> = {
  dataSource: "SOURCE",
  transform: "POLARS",
  modelScore: "MODEL",
  ratingStep: "RATING",
  output: "OUTPUT",
  dataSink: "SINK",
  externalFile: "EXTERNAL",
}

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
}

function PipelineNode({ data, selected }: NodeProps) {
  const nodeData = data as unknown as PipelineNodeData
  const nodeType = nodeData.nodeType || "transform"
  const Icon = iconMap[nodeType] || PolarsIcon
  const accent = accentMap[nodeType] || accentMap.transform
  const typeLabel = labelMap[nodeType] || "NODE"
  const isDeployInput = !!(nodeData.config?.deploy_input)

  return (
    <div
      className="relative rounded-xl min-w-[180px] max-w-[260px] cursor-pointer"
      style={{
        background: "var(--bg-elevated)",
        border: selected ? `1.5px solid ${accent}` : "1px solid var(--border-bright)",
        boxShadow: "var(--node-shadow)",
        transition: "border-color 0.15s ease",
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
          {isDeployInput && (
            <span
              className="ml-auto inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded-full text-[9px] font-bold uppercase tracking-[0.08em] shrink-0"
              style={{ background: "rgba(34,197,94,.12)", color: "#22c55e", border: "1px solid rgba(34,197,94,.2)" }}
            >
              <Radio size={8} />
              API
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
      </div>

      <Handle type="source" position={Position.Right} />
    </div>
  )
}

export default memo(PipelineNode)
