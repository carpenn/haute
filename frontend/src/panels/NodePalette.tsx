import { Database, Brain, TableProperties, CircleDot, PanelLeftClose } from "lucide-react"
import PolarsIcon from "../components/PolarsIcon"
import type { DragEvent } from "react"

const nodeTemplates = [
  {
    type: "dataSource",
    label: "Data Source",
    description: "Read from parquet, CSV, or Databricks table",
    icon: Database,
    accent: "#3b82f6",
    defaultConfig: { path: "" },
  },
  {
    type: "transform",
    label: "Polars",
    description: "Polars transform / feature engineering",
    icon: PolarsIcon,
    accent: "#06b6d4",
    defaultConfig: {},
  },
  {
    type: "modelScore",
    label: "Model Score",
    description: "Score using an MLflow model",
    icon: Brain,
    accent: "#8b5cf6",
    defaultConfig: { model_uri: "" },
  },
  {
    type: "ratingStep",
    label: "Rating Step",
    description: "Lookup, factor, cap/floor",
    icon: TableProperties,
    accent: "#10b981",
    defaultConfig: { table: "", key: "" },
  },
  {
    type: "output",
    label: "Output",
    description: "Final price / prediction",
    icon: CircleDot,
    accent: "#f43f5e",
    defaultConfig: { columns: [] },
  },
]

function onDragStart(event: DragEvent, template: typeof nodeTemplates[number]) {
  event.dataTransfer.setData("application/reactflow-type", template.type)
  event.dataTransfer.setData("application/reactflow-config", JSON.stringify(template.defaultConfig))
  event.dataTransfer.effectAllowed = "move"
}

export default function NodePalette({ onCollapse }: { onCollapse?: () => void }) {
  return (
    <div className="w-[180px] h-full overflow-y-auto shrink-0 flex flex-col" style={{ background: "var(--chrome)", borderRight: "1px solid var(--chrome-border)" }}>
      <div className="px-4 py-3 flex items-center justify-between">
        <h2 className="text-[11px] font-bold uppercase tracking-[0.1em]" style={{ color: "var(--text-muted)" }}>Nodes</h2>
        {onCollapse && (
          <button
            onClick={onCollapse}
            className="p-0.5 rounded transition-colors"
            style={{ color: 'var(--text-muted)' }}
            onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-secondary)' }}
            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-muted)' }}
            title="Collapse palette"
          >
            <PanelLeftClose size={14} />
          </button>
        )}
      </div>

      <div className="px-2 space-y-0.5 flex-1">
        {nodeTemplates.map((template) => {
          const Icon = template.icon
          return (
            <div
              key={template.type}
              draggable
              onDragStart={(e) => onDragStart(e, template)}
              className="flex items-center gap-2.5 px-2.5 py-2 rounded-lg cursor-grab active:cursor-grabbing transition-colors"
              style={{ ["--hover-bg" as string]: "var(--chrome-hover)" }}
              onMouseEnter={(e) => (e.currentTarget.style.background = "var(--chrome-hover)")}
              onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
              title={template.description}
            >
              <div className="w-6 h-6 rounded-md flex items-center justify-center shrink-0" style={{ background: `${template.accent}18` }}>
                <Icon size={13} style={{ color: template.accent }} />
              </div>
              <span className="text-[13px] font-medium" style={{ color: "var(--text-primary)" }}>{template.label}</span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
