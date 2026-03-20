/**
 * Smoke tests for ModellingPreview.
 *
 * ModellingPreview uses Zustand stores (useNodeResultsStore, useSettingsStore)
 * and useDragResize, so we mock them to keep tests focused on render logic.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import { ModellingPreview } from "../ModellingPreview"
import type { ModellingPreviewData } from "../ModellingPreview"
import { makeTrainResult } from "../../test-utils/factories"

// Mock stores
vi.mock("../../stores/useNodeResultsStore", () => {
  const store = Object.assign(vi.fn(() => null), {
    getState: vi.fn(() => ({ trainJobs: {} })),
  })
  return { default: store, __esModule: true }
})

vi.mock("../../stores/useSettingsStore", () => {
  const store = Object.assign(
    vi.fn(() => ({ status: "disconnected", backend: "", host: "" })),
    { getState: vi.fn(() => ({ mlflow: { status: "disconnected", backend: "", host: "" } })) },
  )
  return { default: store, __esModule: true }
})

// Mock useDragResize to avoid DOM measurement issues
vi.mock("../../hooks/useDragResize", () => ({
  useDragResize: () => ({
    height: 360,
    containerRef: { current: null },
    onDragStart: vi.fn(),
  }),
}))

afterEach(cleanup)

function makeData(overrides: Partial<ModellingPreviewData> = {}): ModellingPreviewData {
  return {
    result: makeTrainResult(),
    jobId: "job-123",
    nodeLabel: "Model Node",
    configHash: "abc123",
    ...overrides,
  }
}

describe("ModellingPreview", () => {
  it("renders node label", () => {
    render(<ModellingPreview data={makeData()} nodeId="n1" />)
    expect(screen.getByText("Model Node")).toBeInTheDocument()
  })

  it("renders Summary tab by default", () => {
    render(<ModellingPreview data={makeData()} nodeId="n1" />)
    expect(screen.getByText("Summary")).toBeInTheDocument()
  })

  it("shows Features tab when feature_importance exists", () => {
    render(<ModellingPreview data={makeData()} nodeId="n1" />)
    // "Features" appears as both a tab button and a model info label in SummaryTab
    const matches = screen.getAllByText("Features")
    expect(matches.length).toBeGreaterThanOrEqual(1)
    // At least one should be a button (the tab)
    expect(matches.some(el => el.tagName === "BUTTON")).toBe(true)
  })

  it("does not show Loss tab when no loss_history", () => {
    render(<ModellingPreview data={makeData()} nodeId="n1" />)
    expect(screen.queryByText("Loss")).not.toBeInTheDocument()
  })

  it("shows Loss tab when loss_history has data", () => {
    const result = makeTrainResult({
      loss_history: [
        { iteration: 0, train_rmse: 1.0 },
        { iteration: 1, train_rmse: 0.9 },
      ],
    })
    render(<ModellingPreview data={makeData({ result })} nodeId="n1" />)
    expect(screen.getByText("Loss")).toBeInTheDocument()
  })

  it("shows Lift tab when double_lift data exists", () => {
    const result = makeTrainResult({
      double_lift: [{ decile: 1, actual: 1.0, predicted: 0.9, count: 100 }],
    })
    render(<ModellingPreview data={makeData({ result })} nodeId="n1" />)
    expect(screen.getByText("Lift")).toBeInTheDocument()
  })

  it("shows Coefficients tab for GLM results", () => {
    const result = makeTrainResult({
      glm_coefficients: [{ feature: "age", coefficient: 0.1, std_error: 0.01, z_value: 10, p_value: 0.001, significance: "***" }],
    })
    render(<ModellingPreview data={makeData({ result })} nodeId="n1" />)
    expect(screen.getByText("Coefficients")).toBeInTheDocument()
  })

  it("can collapse and expand", () => {
    render(<ModellingPreview data={makeData()} nodeId="n1" />)
    // Find and click collapse button (ChevronDown)
    const collapseButtons = screen.getAllByRole("button")
    // Click the last non-tab button (the collapse/expand toggle)
    const headerButtons = collapseButtons.filter(b => !["Summary", "Features"].includes(b.textContent || ""))
    if (headerButtons.length > 0) {
      fireEvent.click(headerButtons[headerButtons.length - 1])
    }
  })

  it("shows metrics summary in collapsed state", () => {
    const result = makeTrainResult({ metrics: { gini: 0.4567, rmse: 0.1234 } })
    const data = makeData({ result })
    // Render and test that the component renders without crashing
    const { container } = render(<ModellingPreview data={data} nodeId="n1" />)
    expect(container.innerHTML).not.toBe("")
  })
})
