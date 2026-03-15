import { memo } from "react"
import { Handle, Position, useStore, type NodeProps } from "@xyflow/react"
import { Radio, Link2 } from "lucide-react"
import PolarsIcon from "../components/PolarsIcon"
import { NODE_TYPES, NODE_TYPE_META, SOURCE_ONLY_TYPES, SINK_ONLY_TYPES, PILL_TYPES, nodeTypeIcons, nodeTypeColors, nodeTypeLabels, type NodeTypeValue } from "../utils/nodeTypes"
import { formatValueCompact } from "../utils/formatValue"
import useSettingsStore from "../stores/useSettingsStore"
import type { HauteNodeData } from "../types/node"

const statusColors: Record<string, string> = {
  ok: "#22c55e",
  error: "#ef4444",
  running: "#6366f1",
}

export type PipelineNodeData = HauteNodeData

/** Isolated component so only LiveSwitch nodes subscribe to the settings store. */
function LiveSwitchBadge({ accent }: { accent: string }) {
  const activeScenario = useSettingsStore((s) => s.activeScenario)
  if (activeScenario !== "live") return null
  return (
    <span
      className="ml-auto inline-flex items-center px-1.5 py-0.5 rounded-full text-[9px] font-bold uppercase tracking-[0.08em] shrink-0"
      style={{ background: `${accent}1f`, color: accent, border: `1px solid ${accent}33` }}
    >
      LIVE
    </span>
  )
}

/** Zoom-level selector — only re-renders when crossing a threshold, not on every pixel. */
const zoomSelector = (s: { transform: [number, number, number] }) => {
  const z = s.transform[2]
  if (z > 0.55) return "full"
  if (z > 0.3) return "medium"
  return "compact"
}

function PipelineNode({ data, selected }: NodeProps) {
  const nodeData = data as unknown as PipelineNodeData
  const nodeType = nodeData.nodeType || NODE_TYPES.TRANSFORM
  const Icon = nodeTypeIcons[nodeType] || PolarsIcon
  const accent = nodeTypeColors[nodeType] || nodeTypeColors[NODE_TYPES.TRANSFORM]
  const typeLabel = nodeTypeLabels[nodeType] || "NODE"
  const isDeployInput = nodeType === NODE_TYPES.API_INPUT
  const isLiveSwitch = nodeType === NODE_TYPES.LIVE_SWITCH
  const isInstance = !!(nodeData.config?.instanceOf)
  const isSourceOnly = SOURCE_ONLY_TYPES.has(nodeType)
  const isSinkOnly = SINK_ONLY_TYPES.has(nodeType)
  const isPill = PILL_TYPES.has(nodeType)
  const traceActive = !!nodeData._traceActive
  const traceDimmed = !!nodeData._traceDimmed
  const hoverDimmed = !!nodeData._hoverDimmed
  const traceValue = nodeData._traceValue
  const zoomLevel = useStore(zoomSelector)

  const dimmed = traceDimmed || hoverDimmed

  // Accessible label: "{Type} node: {label}" + status
  const typeName = NODE_TYPE_META[nodeType as NodeTypeValue]?.name || typeLabel
  const statusText = nodeData._status ? `, status: ${nodeData._status}` : ""
  const ariaLabel = `${typeName} node: ${nodeData.label}${statusText}${isInstance ? ", instance" : ""}${traceActive ? ", trace active" : ""}`

  // Compact mode: just a colored pill with icon — readable at far zoom
  if (zoomLevel === "compact") {
    return (
      <div
        aria-label={ariaLabel}
        role="button"
        className={`relative min-w-[120px] max-w-[160px] cursor-pointer ${isPill ? "rounded-full" : "rounded-lg"}`}
        style={{
          background: `linear-gradient(${accent}18, ${accent}10), var(--bg-elevated)`,
          border: selected ? `2px solid ${accent}` : `1.5px solid ${accent}30`,
          boxShadow: "var(--node-shadow)",
          opacity: dimmed ? 0.25 : 1,
          transition: "opacity 0.2s ease",
        }}
      >
        <div
          className="absolute left-0 top-1.5 bottom-1.5 w-[4px] rounded-full"
          style={{ backgroundColor: accent }}
        />
        {!isSourceOnly && <Handle type="target" position={Position.Left} />}
        <div className="flex items-center gap-2 pl-3.5 pr-2.5 py-2">
          <Icon size={16} style={{ color: accent }} className="shrink-0" />
          <div className="font-bold text-[12px] leading-tight truncate" style={{ color: "var(--text-primary)" }}>
            {nodeData.label}
          </div>
        </div>
        {!isSinkOnly && <Handle type="source" position={Position.Right} />}
      </div>
    )
  }

  // Medium mode: icon + label + type badge, no extra badges/status
  if (zoomLevel === "medium") {
    return (
      <div
        aria-label={ariaLabel}
        role="button"
        className={`relative min-w-[180px] max-w-[260px] cursor-pointer ${isPill ? "rounded-2xl" : "rounded-xl"}`}
        style={{
          background: `linear-gradient(${accent}10, ${accent}08), var(--bg-elevated)`,
          border: traceActive || selected
            ? `1.5px solid ${accent}`
            : isInstance
              ? `1.5px dashed ${accent}60`
              : `1px solid ${accent}20`,
          boxShadow: traceActive
            ? `0 0 12px ${accent}40, var(--node-shadow)`
            : "var(--node-shadow)",
          opacity: dimmed ? 0.25 : 1,
          transition: "border-color 0.15s ease, opacity 0.2s ease, box-shadow 0.2s ease",
        }}
      >
        <div
          className="absolute left-0 top-2 bottom-2 w-[4px] rounded-full"
          style={{ backgroundColor: accent, opacity: selected || traceActive ? 1 : 0.7 }}
        />
        {!isSourceOnly && <Handle type="target" position={Position.Left} />}
        <div className="pl-4 pr-3 py-2">
          <div className="flex items-center gap-2 mb-0.5">
            <Icon size={14} style={{ color: accent }} className="shrink-0" />
            <span className="text-[10px] font-bold uppercase tracking-[0.1em] shrink-0" style={{ color: accent }}>
              {typeLabel}
            </span>
          </div>
          <div className="font-semibold text-[13px] leading-tight truncate" style={{ color: "var(--text-primary)" }}>
            {nodeData.label}
          </div>
        </div>
        {!isSinkOnly && <Handle type="source" position={Position.Right} />}
      </div>
    )
  }

  // Full mode: all details visible
  return (
    <div
      aria-label={ariaLabel}
      role="button"
      className={`relative min-w-[180px] max-w-[260px] cursor-pointer ${isPill ? "rounded-2xl" : "rounded-xl"}`}
      style={{
        background: `linear-gradient(${accent}10, ${accent}08), var(--bg-elevated)`,
        border: traceActive
          ? `1.5px solid ${accent}`
          : selected
            ? `1.5px solid ${accent}`
            : isInstance
              ? `1.5px dashed ${accent}60`
              : `1px solid ${accent}20`,
        boxShadow: traceActive
          ? `0 0 12px ${accent}40, var(--node-shadow)`
          : "var(--node-shadow)",
        opacity: dimmed ? 0.25 : 1,
        transition: "border-color 0.15s ease, opacity 0.2s ease, box-shadow 0.2s ease",
      }}
    >
      {/* Left accent stripe */}
      <div
        className="absolute left-0 top-2 bottom-2 w-[4px] rounded-full"
        style={{ backgroundColor: accent, opacity: selected || traceActive ? 1 : 0.7, transition: "opacity 0.2s ease" }}
      />

      {!isSourceOnly && <Handle type="target" position={Position.Left} />}

      <div className="pl-4 pr-3 py-2.5">
        <div className="flex items-center gap-2 mb-1">
          <Icon size={14} style={{ color: accent }} className="shrink-0" />
          <span
            className="text-[10px] font-bold uppercase tracking-[0.1em] shrink-0"
            style={{ color: accent }}
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
              className="ml-auto inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded-full text-[9px] font-bold uppercase tracking-[0.08em] shrink-0"
              style={{ background: `${accent}1f`, color: accent, border: `1px solid ${accent}33` }}
            >
              <Radio size={8} />
              API
            </span>
          )}
          {isLiveSwitch && <LiveSwitchBadge accent={accent} />}
          {nodeData._status && (
            <span
              className={`${isDeployInput ? "" : "ml-auto "} w-[7px] h-[7px] rounded-full shrink-0 ${nodeData._status === "running" ? "animate-pulse-dot" : ""}`}
              style={{ backgroundColor: statusColors[nodeData._status] }}
              role="status"
              aria-label={`Node ${nodeData._status}`}
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

      {!isSinkOnly && <Handle type="source" position={Position.Right} />}
    </div>
  )
}

export default memo(PipelineNode)
