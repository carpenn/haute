import { memo } from "react"
import { Handle, Position, type NodeProps } from "@xyflow/react"
import { ArrowRight, ArrowLeft } from "lucide-react"

export type SubmodelPortData = {
  label: string
  portDirection: "input" | "output"
  portName: string
}

const portColor = "#94a3b8"

function SubmodelPortNode({ data }: NodeProps) {
  const nodeData = data as unknown as SubmodelPortData
  const isInput = nodeData.portDirection === "input"
  const Icon = isInput ? ArrowRight : ArrowLeft

  return (
    <div
      className="relative rounded-full px-4 py-2 flex items-center gap-2 min-w-[120px]"
      style={{
        background: "var(--bg-elevated)",
        border: `1.5px dashed ${portColor}40`,
        opacity: 0.85,
      }}
    >
      {!isInput && <Handle type="target" position={Position.Left} />}

      <Icon size={12} style={{ color: portColor }} className="shrink-0" />
      <span
        className="text-[11px] font-mono font-medium truncate"
        style={{ color: portColor }}
      >
        {nodeData.portName || nodeData.label}
      </span>

      {isInput && <Handle type="source" position={Position.Right} />}
    </div>
  )
}

export default memo(SubmodelPortNode)
