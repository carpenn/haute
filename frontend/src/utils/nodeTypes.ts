import { Database, Brain, TableProperties, CircleDot, HardDriveDownload, FileArchive, Package, ArrowRight, Radio, ToggleLeft, SlidersHorizontal } from "lucide-react"
import PolarsIcon from "../components/PolarsIcon"

export const nodeTypeIcons: Record<string, React.ElementType> = {
  apiInput: Radio,
  dataSource: Database,
  transform: PolarsIcon,
  modelScore: Brain,
  ratingStep: TableProperties,
  banding: SlidersHorizontal,
  output: CircleDot,
  dataSink: HardDriveDownload,
  externalFile: FileArchive,
  liveSwitch: ToggleLeft,
  submodel: Package,
  submodelPort: ArrowRight,
}

export const nodeTypeColors: Record<string, string> = {
  apiInput: "#22c55e",
  dataSource: "#3b82f6",
  transform: "#06b6d4",
  modelScore: "#8b5cf6",
  ratingStep: "#10b981",
  banding: "#14b8a6",
  output: "#f43f5e",
  dataSink: "#f59e0b",
  externalFile: "#ec4899",
  liveSwitch: "#f59e0b",
  submodel: "#f97316",
  submodelPort: "#94a3b8",
}

export const nodeTypeLabels: Record<string, string> = {
  apiInput: "API INPUT",
  dataSource: "SOURCE",
  transform: "POLARS",
  modelScore: "MODEL",
  ratingStep: "RATING",
  banding: "BANDING",
  output: "OUTPUT",
  dataSink: "SINK",
  externalFile: "EXTERNAL",
  liveSwitch: "SWITCH",
  submodel: "SUBMODEL",
  submodelPort: "PORT",
}
