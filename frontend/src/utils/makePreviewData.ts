import type { PreviewData } from "../panels/DataPreview"

export function makePreviewData(
  nodeId: string,
  nodeLabel: string,
  overrides: Partial<PreviewData> = {},
): PreviewData {
  return {
    nodeId,
    nodeLabel,
    status: "ok",
    row_count: 0,
    column_count: 0,
    columns: [],
    preview: [],
    error: null,
    ...overrides,
  }
}
