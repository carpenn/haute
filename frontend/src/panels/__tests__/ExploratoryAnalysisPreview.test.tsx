import { afterEach, describe, expect, it, vi } from "vitest"
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import ExploratoryAnalysisPreview from "../ExploratoryAnalysisPreview"

vi.mock("../../hooks/useDragResize", () => ({
  useDragResize: () => ({
    height: 360,
    containerRef: { current: null },
    onDragStart: vi.fn(),
  }),
}))

const fetchExploratoryAnalysis = vi.fn()
const fetchExploratoryOneWayChart = vi.fn()

vi.mock("../../api/client", () => ({
  fetchExploratoryAnalysis: (...args: unknown[]) => fetchExploratoryAnalysis(...args),
  fetchExploratoryOneWayChart: (...args: unknown[]) => fetchExploratoryOneWayChart(...args),
}))

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

describe("ExploratoryAnalysisPreview", () => {
  it("renders tab labels and fetches analysis", async () => {
    fetchExploratoryAnalysis.mockResolvedValue({
      status: "ok",
      row_count: 6,
      field_roles: { target_value: "target", accident_date: "accident_date" },
      descriptive_statistics: [{
        field: "target_value",
        role: "target",
        dtype: "Float64",
        non_missing_count: 6,
        missing_count: 0,
        missing_proportion: 0,
        distinct_count: 6,
        distinct_proportion: 1,
        mean: 12.3,
        std: 1.2,
        min: 10,
        median: 12,
        max: 15,
        top_values: ["10 (1)"],
        distribution: { kind: "histogram", values: [1, 2, 3] },
      }],
      outliers_inliers: [{
        field: "target_value",
        role: "target",
        dtype: "Float64",
        outlier: ["99"],
        outlier_proportion: 0.1,
        inlier: ["10"],
        inlier_proportion: 0.9,
      }],
      disguised_missings: [{
        field: "feature",
        role: "covariate",
        dtype: "String",
        missing_values: ["?"],
        missing_proportion: 0.1,
      }],
      correlations: {
        fields: ["target_value", "feature"],
        types: ["auto"],
        cells: [[{}, { auto: 0.5 }], [{ auto: 0.5 }, {}]],
      },
      one_way_options: [{ field: "accident_date", role: "accident_date", dtype: "Date" }],
      default_x_field: "accident_date",
      chart: {
        x_field: "accident_date",
        bar_label: "Unique claim_id",
        line_label: "Sum of target_value",
        points: [{ x: "2024-01", bar_value: 2, line_value: 100 }],
      },
    })

    render(
      <ExploratoryAnalysisPreview
        data={{ nodeId: "eda", nodeLabel: "EDA", status: "ok", row_count: 0, column_count: 0, columns: [], preview: [], error: null }}
        config={{ fieldRoles: { target_value: "target" } }}
        nodeId="eda"
        getGraph={() => ({ nodes: [], edges: [] })}
      />,
    )

    await waitFor(() => expect(fetchExploratoryAnalysis).toHaveBeenCalled())
    expect(screen.getByText("Descriptive Statistics")).toBeInTheDocument()
    expect(screen.getByText("Correlations")).toBeInTheDocument()
    expect(screen.getByText("target_value")).toBeInTheDocument()
  })

  it("re-renders one-way chart when x-axis changes", async () => {
    fetchExploratoryAnalysis.mockResolvedValue({
      status: "ok",
      row_count: 6,
      field_roles: { target_value: "target", accident_date: "accident_date", feature: "covariate" },
      descriptive_statistics: [],
      outliers_inliers: [],
      disguised_missings: [],
      correlations: { fields: [], types: [], cells: [] },
      one_way_options: [
        { field: "accident_date", role: "accident_date", dtype: "Date" },
        { field: "feature", role: "covariate", dtype: "String" },
      ],
      default_x_field: "accident_date",
      chart: {
        x_field: "accident_date",
        bar_label: "Unique claim_id",
        line_label: "Sum of target_value",
        points: [{ x: "2024-01", bar_value: 2, line_value: 100 }],
      },
    })
    fetchExploratoryOneWayChart.mockResolvedValue({
      status: "ok",
      chart: {
        x_field: "feature",
        bar_label: "Unique claim_id",
        line_label: "Sum of target_value",
        points: [{ x: "A", bar_value: 3, line_value: 50 }],
      },
    })

    render(
      <ExploratoryAnalysisPreview
        data={{ nodeId: "eda", nodeLabel: "EDA", status: "ok", row_count: 0, column_count: 0, columns: [], preview: [], error: null }}
        config={{ fieldRoles: { target_value: "target" } }}
        nodeId="eda"
        getGraph={() => ({ nodes: [], edges: [] })}
      />,
    )

    await waitFor(() => expect(fetchExploratoryAnalysis).toHaveBeenCalled())
    fireEvent.click(screen.getByText("One-way charts"))
    fireEvent.change(screen.getByLabelText("One-way chart x-axis"), { target: { value: "feature" } })

    await waitFor(() => expect(fetchExploratoryOneWayChart).toHaveBeenCalled())
  })
})
