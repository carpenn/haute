import { Database, Brain, TableProperties, CircleDot, HardDriveDownload, FileArchive, Package, ArrowRight, Radio, ToggleLeft, SlidersHorizontal, FlaskConical } from "lucide-react"
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
  SUBMODEL: "submodel",
  SUBMODEL_PORT: "submodelPort",
} as const

export type NodeTypeValue = typeof NODE_TYPES[keyof typeof NODE_TYPES]

/** Single source of truth for all node type metadata. */
export const NODE_TYPE_META: Record<NodeTypeValue, {
  icon: React.ElementType
  color: string
  label: string
}> = {
  [NODE_TYPES.API_INPUT]:     { icon: Radio,              color: "#22c55e", label: "API INPUT" },
  [NODE_TYPES.DATA_SOURCE]:   { icon: Database,           color: "#3b82f6", label: "SOURCE" },
  [NODE_TYPES.TRANSFORM]:     { icon: PolarsIcon,         color: "#06b6d4", label: "POLARS" },
  [NODE_TYPES.MODEL_SCORE]:   { icon: Brain,              color: "#8b5cf6", label: "MODEL" },
  [NODE_TYPES.BANDING]:       { icon: SlidersHorizontal,  color: "#14b8a6", label: "BANDING" },
  [NODE_TYPES.RATING_STEP]:   { icon: TableProperties,    color: "#10b981", label: "RATING" },
  [NODE_TYPES.OUTPUT]:        { icon: CircleDot,          color: "#f43f5e", label: "OUTPUT" },
  [NODE_TYPES.DATA_SINK]:     { icon: HardDriveDownload,  color: "#f59e0b", label: "SINK" },
  [NODE_TYPES.EXTERNAL_FILE]: { icon: FileArchive,        color: "#ec4899", label: "EXTERNAL" },
  [NODE_TYPES.LIVE_SWITCH]:   { icon: ToggleLeft,         color: "#f59e0b", label: "SWITCH" },
  [NODE_TYPES.MODELLING]:     { icon: FlaskConical,       color: "#a855f7", label: "TRAINING" },
  [NODE_TYPES.SUBMODEL]:      { icon: Package,            color: "#f97316", label: "SUBMODEL" },
  [NODE_TYPES.SUBMODEL_PORT]: { icon: ArrowRight,         color: "#94a3b8", label: "PORT" },
}

export const SINGLETON_TYPES = new Set<NodeTypeValue>([
  NODE_TYPES.API_INPUT, NODE_TYPES.OUTPUT, NODE_TYPES.LIVE_SWITCH,
])

/** Nodes that only produce data — no input handle. */
export const SOURCE_ONLY_TYPES = new Set<string>([
  NODE_TYPES.DATA_SOURCE, NODE_TYPES.API_INPUT,
])

/** Nodes that only consume data — no output handle. */
export const SINK_ONLY_TYPES = new Set<string>([
  NODE_TYPES.OUTPUT, NODE_TYPES.DATA_SINK, NODE_TYPES.MODELLING,
])

/** Derived lookups — backward compatible, prefer NODE_TYPE_META for new code. */
export const nodeTypeIcons: Record<string, React.ElementType> =
  Object.fromEntries(Object.entries(NODE_TYPE_META).map(([k, v]) => [k, v.icon]))

export const nodeTypeColors: Record<string, string> =
  Object.fromEntries(Object.entries(NODE_TYPE_META).map(([k, v]) => [k, v.color]))

export const nodeTypeLabels: Record<string, string> =
  Object.fromEntries(Object.entries(NODE_TYPE_META).map(([k, v]) => [k, v.label]))
