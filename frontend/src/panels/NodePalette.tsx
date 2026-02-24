import { Database, Brain, TableProperties, CircleDot, PanelLeftClose, HardDriveDownload, FileArchive, Radio, ToggleLeft, SlidersHorizontal, FlaskConical, Target, Rows3 } from "lucide-react"
import PolarsIcon from "../components/PolarsIcon"
import type { DragEvent } from "react"
import type { Node } from "@xyflow/react"
import { NODE_TYPES, SINGLETON_TYPES } from "../utils/nodeTypes"

const nodeTemplates = [
  {
    type: NODE_TYPES.API_INPUT,
    label: "API Input",
    description: "Live API input for deployment (max 1)",
    icon: Radio,
    accent: "#22c55e",
    defaultConfig: { path: "" },
  },
  {
    type: NODE_TYPES.DATA_SOURCE,
    label: "Data Source",
    description: "Read from parquet, CSV, or Databricks table",
    icon: Database,
    accent: "#3b82f6",
    defaultConfig: { path: "" },
  },
  {
    type: NODE_TYPES.TRANSFORM,
    label: "Polars",
    description: "Polars transform / feature engineering",
    icon: PolarsIcon,
    accent: "#06b6d4",
    defaultConfig: {},
  },
  {
    type: NODE_TYPES.MODEL_SCORE,
    label: "Model Score",
    description: "Score using an MLflow model",
    icon: Brain,
    accent: "#8b5cf6",
    defaultConfig: { sourceType: "registered", registered_model: "", version: "latest", task: "regression", output_column: "prediction", code: "" },
  },
  {
    type: NODE_TYPES.BANDING,
    label: "Banding",
    description: "Group numerical or categorical values into bands",
    icon: SlidersHorizontal,
    accent: "#14b8a6",
    defaultConfig: { factors: [{ banding: "continuous", column: "", outputColumn: "", rules: [], default: null }] },
  },
  {
    type: NODE_TYPES.SCENARIO_EXPANDER,
    label: "Expander",
    description: "Cross-join rows with scenario values (price, tier, etc.)",
    icon: Rows3,
    accent: "#0ea5e9",
    defaultConfig: { quote_id: "quote_id", column_name: "scenario_value", min_value: 0.8, max_value: 1.2, steps: 21, step_column: "scenario_index" },
  },
  {
    type: NODE_TYPES.RATING_STEP,
    label: "Rating Step",
    description: "Lookup, factor, cap/floor",
    icon: TableProperties,
    accent: "#10b981",
    defaultConfig: { tables: [{ name: "Table 1", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }], operation: "multiply", combinedColumn: "" },
  },
  {
    type: NODE_TYPES.OUTPUT,
    label: "Output",
    description: "Final price / prediction",
    icon: CircleDot,
    accent: "#f43f5e",
    defaultConfig: { fields: [] },
  },
  {
    type: NODE_TYPES.LIVE_SWITCH,
    label: "Live Switch",
    description: "Switch between live API and batch data (max 1)",
    icon: ToggleLeft,
    accent: "#f59e0b",
    defaultConfig: { mode: "live" },
  },
  {
    type: NODE_TYPES.DATA_SINK,
    label: "Data Sink",
    description: "Write to parquet, CSV, or directory",
    icon: HardDriveDownload,
    accent: "#f59e0b",
    defaultConfig: { path: "", format: "parquet" },
  },
  {
    type: NODE_TYPES.EXTERNAL_FILE,
    label: "External File",
    description: "Load a pickle, JSON, or joblib file and use in code",
    icon: FileArchive,
    accent: "#ec4899",
    defaultConfig: { path: "", fileType: "pickle", code: "" },
  },
  {
    type: NODE_TYPES.MODELLING,
    label: "Model Training",
    description: "Train and evaluate a CatBoost model",
    icon: FlaskConical,
    accent: "#a855f7",
    defaultConfig: { algorithm: "catboost", task: "regression", target: "", weight: "", exclude: [], params: { iterations: 1000, learning_rate: 0.05, depth: 6 }, split: { strategy: "random", test_size: 0.2, seed: 42 }, metrics: ["gini", "rmse"] },
  },
  {
    type: NODE_TYPES.OPTIMISER,
    label: "Optimiser",
    description: "Price optimisation via Lagrangian solver",
    icon: Target,
    accent: "#f97316",
    defaultConfig: { mode: "online", objective: "", constraints: {}, quote_id: "quote_id", scenario_index: "scenario_index", scenario_value: "scenario_value", max_iter: 50, tolerance: 1e-6 },
  },
]

function onDragStart(event: DragEvent, template: typeof nodeTemplates[number]) {
  event.dataTransfer.setData("application/reactflow-type", template.type)
  event.dataTransfer.setData("application/reactflow-config", JSON.stringify(template.defaultConfig))
  event.dataTransfer.effectAllowed = "move"
}

export default function NodePalette({ onCollapse, nodes }: { onCollapse?: () => void; nodes?: Node[] }) {
  // Build set of singleton types already present in the graph
  const existingSingletons = new Set<string>()
  if (nodes) {
    for (const n of nodes) {
      const nt = n.data.nodeType as string
      if (nt && SINGLETON_TYPES.has(nt as typeof NODE_TYPES[keyof typeof NODE_TYPES])) existingSingletons.add(nt)
    }
  }

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
          const disabled = SINGLETON_TYPES.has(template.type) && existingSingletons.has(template.type)
          return (
            <div
              key={template.type}
              draggable={!disabled}
              onDragStart={(e) => { if (!disabled) onDragStart(e, template) }}
              className={`flex items-center gap-2.5 px-2.5 py-2 rounded-lg transition-colors ${disabled ? "opacity-35 cursor-not-allowed" : "cursor-grab active:cursor-grabbing"}`}
              style={{ ["--hover-bg" as string]: "var(--chrome-hover)" }}
              onMouseEnter={(e) => { if (!disabled) e.currentTarget.style.background = "var(--chrome-hover)" }}
              onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
              title={disabled ? `Only one ${template.label} allowed per pipeline` : template.description}
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
