import { Database, Brain, TableProperties, CircleDot, HardDriveDownload, FileArchive, Package, ArrowRight } from "lucide-react"
import PolarsIcon from "../components/PolarsIcon"

export const nodeTypeIcons: Record<string, React.ElementType> = {
  dataSource: Database,
  transform: PolarsIcon,
  modelScore: Brain,
  ratingStep: TableProperties,
  output: CircleDot,
  dataSink: HardDriveDownload,
  externalFile: FileArchive,
  submodel: Package,
  submodelPort: ArrowRight,
}

export const nodeTypeColors: Record<string, string> = {
  dataSource: "#3b82f6",
  transform: "#06b6d4",
  modelScore: "#8b5cf6",
  ratingStep: "#10b981",
  output: "#f43f5e",
  dataSink: "#f59e0b",
  externalFile: "#ec4899",
  submodel: "#f97316",
  submodelPort: "#94a3b8",
}

export const nodeTypeLabels: Record<string, string> = {
  dataSource: "SOURCE",
  transform: "POLARS",
  modelScore: "MODEL",
  ratingStep: "RATING",
  output: "OUTPUT",
  dataSink: "SINK",
  externalFile: "EXTERNAL",
  submodel: "SUBMODEL",
  submodelPort: "PORT",
}
