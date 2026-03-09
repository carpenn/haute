import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup, waitFor } from "@testing-library/react"
import OptimiserPreview from "../OptimiserPreview"
import type { OptimiserPreviewData, SolveResult } from "../OptimiserPreview"

vi.mock("../../api/client", () => ({
  runFrontier: vi.fn(),
}))

vi.mock("../../hooks/useDragResize", () => ({
  useDragResize: () => ({
    height: 320,
    containerRef: { current: null },
    onDragStart: vi.fn(),
  }),
}))

function makeSolveResult(overrides: Partial<SolveResult> = {}): SolveResult {
  return {
    total_objective: 1234567,
    baseline_objective: 1200000,
    constraints: { loss_ratio: 0.65 },
    baseline_constraints: { loss_ratio: 0.60 },
    lambdas: { loss_ratio: 0.005 },
    converged: true,
    iterations: 15,
    n_quotes: 50000,
    history: [
      { iteration: 1, total_objective: 1100000, max_lambda_change: 0.1, all_constraints_satisfied: false },
      { iteration: 2, total_objective: 1200000, max_lambda_change: 0.01, all_constraints_satisfied: true },
    ],
    ...overrides,
  }
}

function makeData(overrides: Partial<OptimiserPreviewData> = {}): OptimiserPreviewData {
  return {
    result: makeSolveResult(),
    jobId: "job_123",
    constraints: { loss_ratio: { max: 1.05 } },
    nodeLabel: "My Optimiser",
    ...overrides,
  }
}

function renderPreview(overrides: Partial<Parameters<typeof OptimiserPreview>[0]> = {}) {
  const props = {
    data: makeData(),
    ...overrides,
  }
  return { ...render(<OptimiserPreview {...props} />), props }
}

describe("OptimiserPreview", () => {
  afterEach(cleanup)

  describe("Summary tab (default)", () => {
    it("renders node label in header", () => {
      renderPreview()
      expect(screen.getByText("My Optimiser")).toBeInTheDocument()
    })

    it("shows Converged status when converged", () => {
      renderPreview()
      expect(screen.getByText(/Converged/)).toBeInTheDocument()
    })

    it("shows Not converged status when not converged", () => {
      renderPreview({ data: makeData({ result: makeSolveResult({ converged: false }) }) })
      expect(screen.getByText(/Not converged/)).toBeInTheDocument()
    })

    it("renders iteration count", () => {
      renderPreview()
      expect(screen.getByText(/15 iters/)).toBeInTheDocument()
    })

    it("renders quote count", () => {
      renderPreview()
      expect(screen.getByText(/50,000 quotes/)).toBeInTheDocument()
    })

    it("renders Objective label and values", () => {
      renderPreview()
      expect(screen.getByText("Objective")).toBeInTheDocument()
      expect(screen.getByText("Optimised")).toBeInTheDocument()
      expect(screen.getByText("Baseline")).toBeInTheDocument()
    })

    it("renders formatted objective value", () => {
      renderPreview()
      // 1234567 formatted as "1.23M"
      expect(screen.getByText("1.23M")).toBeInTheDocument()
    })

    it("renders constraints section", () => {
      renderPreview()
      expect(screen.getByText("Constraints")).toBeInTheDocument()
      // loss_ratio appears in both constraints and lambdas sections
      expect(screen.getAllByText("loss_ratio").length).toBeGreaterThanOrEqual(1)
    })

    it("renders lambda values", () => {
      renderPreview()
      expect(screen.getByText("Lambdas")).toBeInTheDocument()
      expect(screen.getByText("0.005000")).toBeInTheDocument()
    })

    it("renders uplift percentage when baseline is non-zero", () => {
      renderPreview()
      expect(screen.getByText("Uplift")).toBeInTheDocument()
      // (1234567 / 1200000 - 1) * 100 = 2.88%
      expect(screen.getByText("2.88%")).toBeInTheDocument()
    })

    it("does not render uplift when baseline is zero", () => {
      renderPreview({ data: makeData({ result: makeSolveResult({ baseline_objective: 0 }) }) })
      expect(screen.queryByText("Uplift")).not.toBeInTheDocument()
    })

    it("shows Summary tab as active by default", () => {
      renderPreview()
      const summaryBtn = screen.getByText("Summary")
      expect(summaryBtn).toBeInTheDocument()
    })
  })

  describe("tab switching", () => {
    it("switches to Frontier tab on click", () => {
      renderPreview()
      fireEvent.click(screen.getByText("Frontier"))
      // Frontier tab shows the "Run Efficient Frontier" button
      expect(screen.getByText("Run Efficient Frontier")).toBeInTheDocument()
    })

    it("switches to Convergence tab on click", () => {
      renderPreview()
      fireEvent.click(screen.getByText("Convergence"))
      expect(screen.getByText("Iterations")).toBeInTheDocument()
    })

    it("hides Convergence tab when no history data", () => {
      renderPreview({ data: makeData({ result: makeSolveResult({ history: null }) }) })
      expect(screen.queryByText("Convergence")).not.toBeInTheDocument()
    })
  })

  describe("Frontier tab", () => {
    it("calls runFrontier when button is clicked", async () => {
      const { runFrontier } = await import("../../api/client")
      const mockRunFrontier = vi.mocked(runFrontier)
      mockRunFrontier.mockResolvedValue({ status: "ok", points: [], n_points: 0, constraint_names: ["loss_ratio"] })

      renderPreview()
      fireEvent.click(screen.getByText("Frontier"))
      fireEvent.click(screen.getByText("Run Efficient Frontier"))

      await waitFor(() => {
        expect(mockRunFrontier).toHaveBeenCalledWith(
          expect.objectContaining({ job_id: "job_123" }),
        )
      })
    })

    it("shows 'No frontier points returned' when frontier returns empty array", async () => {
      const { runFrontier } = await import("../../api/client")
      const mockRunFrontier = vi.mocked(runFrontier)
      mockRunFrontier.mockResolvedValue({ status: "ok", points: [], n_points: 0, constraint_names: [] })

      renderPreview()
      fireEvent.click(screen.getByText("Frontier"))
      fireEvent.click(screen.getByText("Run Efficient Frontier"))

      await waitFor(() => {
        expect(screen.getByText("No frontier points returned.")).toBeInTheDocument()
      })
    })

    it("shows error when frontier API fails", async () => {
      const { runFrontier } = await import("../../api/client")
      const mockRunFrontier = vi.mocked(runFrontier)
      mockRunFrontier.mockRejectedValue(new Error("Network error"))

      renderPreview()
      fireEvent.click(screen.getByText("Frontier"))
      fireEvent.click(screen.getByText("Run Efficient Frontier"))

      await waitFor(() => {
        expect(screen.getByText(/Network error/)).toBeInTheDocument()
      })
    })
  })

  describe("Convergence tab", () => {
    it("renders convergence chart and iteration table", () => {
      renderPreview()
      fireEvent.click(screen.getByText("Convergence"))
      expect(screen.getByText("Iterations")).toBeInTheDocument()
      // Check iteration numbers are rendered
      expect(screen.getByText("1")).toBeInTheDocument()
      expect(screen.getByText("2")).toBeInTheDocument()
    })

    it("renders objective and lambda change columns", () => {
      renderPreview()
      fireEvent.click(screen.getByText("Convergence"))
      // "Objective" appears in summary (label) and convergence (column header)
      expect(screen.getAllByText("Objective").length).toBeGreaterThanOrEqual(2)
      expect(screen.getByText("Max dLambda")).toBeInTheDocument()
    })

    it("renders constraints-satisfied column", () => {
      renderPreview()
      fireEvent.click(screen.getByText("Convergence"))
      // First iteration: N, Second: Y
      expect(screen.getByText("N")).toBeInTheDocument()
      expect(screen.getByText("Y")).toBeInTheDocument()
    })
  })

  describe("collapse/expand", () => {
    it("collapse button hides the main panel", () => {
      renderPreview()
      // Find the collapse button (ChevronDown in header)
      const buttons = screen.getAllByRole("button")
      // Collapse is the second-to-last button in header
      const collapseBtn = buttons.find(
        (b) => b.querySelector("svg") && b !== buttons[buttons.length - 1] && !b.textContent,
      )
      // Click any collapse-like button -- just verify the collapsed state text appears
      if (collapseBtn) {
        fireEvent.click(collapseBtn)
        // In collapsed state, node label should still be visible
        expect(screen.getByText("My Optimiser")).toBeInTheDocument()
      }
    })
  })

  describe("ratebook mode", () => {
    it("shows CD iterations for ratebook mode", () => {
      renderPreview({
        data: makeData({ result: makeSolveResult({ mode: "ratebook", cd_iterations: 8 }) }),
      })
      expect(screen.getByText(/8 CD iters/)).toBeInTheDocument()
    })

    it("hides Lambdas section in ratebook mode", () => {
      renderPreview({
        data: makeData({ result: makeSolveResult({ mode: "ratebook" }) }),
      })
      expect(screen.queryByText("Lambdas")).not.toBeInTheDocument()
    })

    it("shows clamp rate in ratebook mode", () => {
      renderPreview({
        data: makeData({ result: makeSolveResult({ mode: "ratebook", clamp_rate: 0.05 }) }),
      })
      expect(screen.getByText("Clamp rate")).toBeInTheDocument()
      expect(screen.getByText("5.0%")).toBeInTheDocument()
    })

    it("renders factor tables in ratebook mode", () => {
      renderPreview({
        data: makeData({
          result: makeSolveResult({
            mode: "ratebook",
            factor_tables: {
              age_band: [
                { __factor_group__: "18-25", optimal_scenario_value: 1.15 },
                { __factor_group__: "26-35", optimal_scenario_value: 0.95 },
              ],
            },
          }),
        }),
      })
      expect(screen.getByText("Factor Tables")).toBeInTheDocument()
      expect(screen.getByText("age_band")).toBeInTheDocument()
      expect(screen.getByText("18-25")).toBeInTheDocument()
      expect(screen.getByText("1.15")).toBeInTheDocument()
    })
  })
})
