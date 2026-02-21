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

export const nodeTypeIcons: Record<string, React.ElementType> = {
  [NODE_TYPES.API_INPUT]: Radio,
  [NODE_TYPES.DATA_SOURCE]: Database,
  [NODE_TYPES.TRANSFORM]: PolarsIcon,
  [NODE_TYPES.MODEL_SCORE]: Brain,
  [NODE_TYPES.RATING_STEP]: TableProperties,
  [NODE_TYPES.BANDING]: SlidersHorizontal,
  [NODE_TYPES.OUTPUT]: CircleDot,
  [NODE_TYPES.DATA_SINK]: HardDriveDownload,
  [NODE_TYPES.EXTERNAL_FILE]: FileArchive,
  [NODE_TYPES.LIVE_SWITCH]: ToggleLeft,
  [NODE_TYPES.MODELLING]: FlaskConical,
  [NODE_TYPES.SUBMODEL]: Package,
  [NODE_TYPES.SUBMODEL_PORT]: ArrowRight,
}

export const nodeTypeColors: Record<string, string> = {
  [NODE_TYPES.API_INPUT]: "#22c55e",
  [NODE_TYPES.DATA_SOURCE]: "#3b82f6",
  [NODE_TYPES.TRANSFORM]: "#06b6d4",
  [NODE_TYPES.MODEL_SCORE]: "#8b5cf6",
  [NODE_TYPES.RATING_STEP]: "#10b981",
  [NODE_TYPES.BANDING]: "#14b8a6",
  [NODE_TYPES.OUTPUT]: "#f43f5e",
  [NODE_TYPES.DATA_SINK]: "#f59e0b",
  [NODE_TYPES.EXTERNAL_FILE]: "#ec4899",
  [NODE_TYPES.LIVE_SWITCH]: "#f59e0b",
  [NODE_TYPES.MODELLING]: "#a855f7",
  [NODE_TYPES.SUBMODEL]: "#f97316",
  [NODE_TYPES.SUBMODEL_PORT]: "#94a3b8",
}

export const nodeTypeLabels: Record<string, string> = {
  [NODE_TYPES.API_INPUT]: "API INPUT",
  [NODE_TYPES.DATA_SOURCE]: "SOURCE",
  [NODE_TYPES.TRANSFORM]: "POLARS",
  [NODE_TYPES.MODEL_SCORE]: "MODEL",
  [NODE_TYPES.RATING_STEP]: "RATING",
  [NODE_TYPES.BANDING]: "BANDING",
  [NODE_TYPES.OUTPUT]: "OUTPUT",
  [NODE_TYPES.DATA_SINK]: "SINK",
  [NODE_TYPES.EXTERNAL_FILE]: "EXTERNAL",
  [NODE_TYPES.LIVE_SWITCH]: "SWITCH",
  [NODE_TYPES.MODELLING]: "TRAINING",
  [NODE_TYPES.SUBMODEL]: "SUBMODEL",
  [NODE_TYPES.SUBMODEL_PORT]: "PORT",
}
