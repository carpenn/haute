import { Database, Brain, TableProperties, CircleDot, HardDriveDownload, FileArchive, Package, ArrowRight, Radio, ToggleLeft, SlidersHorizontal, FlaskConical, Target, Crosshair, Rows3, Hash } from "lucide-react"
import PolarsIcon from "../components/PolarsIcon"

export const NODE_TYPES = {
  API_INPUT: "apiInput",
  DATA_SOURCE: "dataSource",
  TRANSFORM: "transform",
  MODEL_SCORE: "modelScore",
  BANDING: "banding",
  RATING_STEP: "ratingStep",
  OUTPUT: "output",
  DATA_SINK: "dataSink",
  EXTERNAL_FILE: "externalFile",
  LIVE_SWITCH: "liveSwitch",
  MODELLING: "modelling",
  OPTIMISER: "optimiser",
  OPTIMISER_APPLY: "optimiserApply",
  SCENARIO_EXPANDER: "scenarioExpander",
  CONSTANT: "constant",
  SUBMODEL: "submodel",
  SUBMODEL_PORT: "submodelPort",
} as const

export type NodeTypeValue = typeof NODE_TYPES[keyof typeof NODE_TYPES]

/**
 * Single source of truth for all node type metadata.
 *
 * - icon:          Lucide icon component (canvas node + palette)
 * - color:         Hex accent color (canvas node + palette + editors)
 * - label:         Short UPPER CASE badge text shown on canvas nodes
 * - name:          Full Title Case display name (palette, tooltips, dialogs)
 * - description:   One-line tooltip for the palette and help UI
 * - defaultConfig: Initial config object when a node of this type is created
 */
export const NODE_TYPE_META: Record<NodeTypeValue, {
  icon: React.ElementType
  color: string
  label: string
  name: string
  description: string
  defaultConfig: Record<string, unknown>
  maxInputs?: number
}> = {
  // Entry group (emerald) — pipeline starts here
  [NODE_TYPES.API_INPUT]:          { icon: Radio,              color: "#10b981", label: "QUOTE IN",       name: "Quote Input",          description: "Live API input for deployment (max 1)",                       defaultConfig: { path: "" } },
  [NODE_TYPES.LIVE_SWITCH]:        { icon: ToggleLeft,         color: "#34d399", label: "SWITCH",         name: "Source Switch",        description: "Switch between live API and batch data",                      defaultConfig: { mode: "live" } },
  // Exit (rose) — pipeline destination
  [NODE_TYPES.OUTPUT]:             { icon: CircleDot,          color: "#f43f5e", label: "QUOTE OUT",      name: "Quote Response",       description: "Final price / prediction",                                    defaultConfig: { fields: [] } },
  // Data group (blue) — read/write external data
  [NODE_TYPES.DATA_SOURCE]:        { icon: Database,           color: "#3b82f6", label: "SOURCE",         name: "Data Source",          description: "Read from parquet, CSV, or Databricks table",                 defaultConfig: { path: "" } },
  [NODE_TYPES.DATA_SINK]:          { icon: HardDriveDownload,  color: "#60a5fa", label: "SINK",           name: "Data Sink",            description: "Write to parquet, CSV, or directory",                         defaultConfig: { path: "", format: "parquet" }, maxInputs: 1 },
  [NODE_TYPES.EXTERNAL_FILE]:      { icon: FileArchive,        color: "#93c5fd", label: "LOAD FILE",      name: "Load File",            description: "Load a pickle, JSON, or joblib file and use in code",         defaultConfig: { path: "", fileType: "pickle", code: "" } },
  [NODE_TYPES.CONSTANT]:           { icon: Hash,               color: "#94a3b8", label: "CONSTANT",       name: "Constant",             description: "Named constant values (1-row DataFrame)",                     defaultConfig: { values: [{ name: "constant_1", value: "1.0" }] } },
  // Transform group (cyan-teal) — process/reshape data
  [NODE_TYPES.TRANSFORM]:          { icon: PolarsIcon,         color: "#06b6d4", label: "POLARS",         name: "Polars",               description: "Polars transform / feature engineering",                      defaultConfig: {} },
  [NODE_TYPES.BANDING]:            { icon: SlidersHorizontal,  color: "#22d3ee", label: "BANDING",        name: "Banding",              description: "Group numerical or categorical values into bands",             defaultConfig: { factors: [{ banding: "continuous", column: "", outputColumn: "", rules: [], default: null }] }, maxInputs: 1 },
  [NODE_TYPES.SCENARIO_EXPANDER]:  { icon: Rows3,              color: "#2dd4bf", label: "EXPANDER",       name: "Expander",             description: "Cross-join rows with scenario values (price, tier, etc.)",    defaultConfig: { quote_id: "quote_id", column_name: "scenario_value", min_value: 0.8, max_value: 1.2, steps: 21, step_column: "scenario_index" }, maxInputs: 1 },
  [NODE_TYPES.RATING_STEP]:        { icon: TableProperties,    color: "#14b8a6", label: "RATING",         name: "Rating Step",          description: "Lookup, factor, cap/floor",                                   defaultConfig: { tables: [{ name: "Table 1", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }], operation: "multiply", combinedColumn: "" }, maxInputs: 1 },
  // Model group (violet) — ML training & scoring
  [NODE_TYPES.MODELLING]:          { icon: FlaskConical,       color: "#8b5cf6", label: "TRAINING",       name: "Model Training",       description: "Train a CatBoost or GLM model",                         defaultConfig: {}, maxInputs: 1 },
  [NODE_TYPES.MODEL_SCORE]:        { icon: Brain,              color: "#8b5cf6", label: "SCORING",        name: "Model Scoring",        description: "Score using an MLflow model",                                 defaultConfig: { sourceType: "registered", registered_model: "", version: "latest", task: "regression", output_column: "prediction", code: "" }, maxInputs: 1 },
  // Optimisation group (amber) — price optimisation
  [NODE_TYPES.OPTIMISER]:          { icon: Target,             color: "#f59e0b", label: "OPTIMISATION",   name: "Optimisation",         description: "Price optimisation via Lagrangian solver",                     defaultConfig: { mode: "online", objective: "", constraints: {}, quote_id: "quote_id", scenario_index: "scenario_index", scenario_value: "scenario_value", max_iter: 50, tolerance: 1e-6 } },
  [NODE_TYPES.OPTIMISER_APPLY]:    { icon: Crosshair,          color: "#f59e0b", label: "APPLY OPT",     name: "Apply Optimisation",   description: "Apply saved optimisation results (lambdas or factor tables)",  defaultConfig: { sourceType: "file", artifact_path: "", version_column: "__optimiser_version__" } },
  // Structure (slate) — composition & utility
  [NODE_TYPES.SUBMODEL]:           { icon: Package,            color: "#64748b", label: "SUBMODEL",       name: "Submodel",             description: "Reusable sub-pipeline",                                       defaultConfig: {} },
  [NODE_TYPES.SUBMODEL_PORT]:      { icon: ArrowRight,         color: "#94a3b8", label: "PORT",           name: "Port",                 description: "Submodel input/output port",                                  defaultConfig: {} },
}

export const SINGLETON_TYPES = new Set<NodeTypeValue>([
  NODE_TYPES.API_INPUT, NODE_TYPES.OUTPUT,
])

/** Nodes that only produce data — no input handle. */
export const SOURCE_ONLY_TYPES = new Set<string>([
  NODE_TYPES.DATA_SOURCE, NODE_TYPES.API_INPUT, NODE_TYPES.CONSTANT,
])

/** Nodes that only consume data — no output handle. */
export const SINK_ONLY_TYPES = new Set<string>([
  NODE_TYPES.OUTPUT, NODE_TYPES.DATA_SINK, NODE_TYPES.MODELLING, NODE_TYPES.OPTIMISER,
])

/** Node types shown in the palette, in display order. Submodel/port are excluded (created via dialog). */
export const PALETTE_TYPES: NodeTypeValue[] = [
  NODE_TYPES.API_INPUT, NODE_TYPES.LIVE_SWITCH, NODE_TYPES.OUTPUT,
  NODE_TYPES.DATA_SOURCE, NODE_TYPES.DATA_SINK, NODE_TYPES.EXTERNAL_FILE, NODE_TYPES.CONSTANT,
  NODE_TYPES.TRANSFORM, NODE_TYPES.SCENARIO_EXPANDER, NODE_TYPES.BANDING, NODE_TYPES.RATING_STEP,
  NODE_TYPES.MODELLING, NODE_TYPES.MODEL_SCORE,
  NODE_TYPES.OPTIMISER, NODE_TYPES.OPTIMISER_APPLY,
]

/** Derived lookups — backward compatible, prefer NODE_TYPE_META for new code. */
export const nodeTypeIcons: Record<string, React.ElementType> =
  Object.fromEntries(Object.entries(NODE_TYPE_META).map(([k, v]) => [k, v.icon]))

export const nodeTypeColors: Record<string, string> =
  Object.fromEntries(Object.entries(NODE_TYPE_META).map(([k, v]) => [k, v.color]))

export const nodeTypeLabels: Record<string, string> =
  Object.fromEntries(Object.entries(NODE_TYPE_META).map(([k, v]) => [k, v.label]))
