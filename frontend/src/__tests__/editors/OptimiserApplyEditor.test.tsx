/**
 * Render tests for OptimiserApplyEditor.
 *
 * Tests: source type toggle, file/registered/run modes, version column,
 * artifact metadata display, and config updates.
 */
import { describe, it, expect, vi, afterEach, beforeEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import OptimiserApplyEditor from "../../panels/editors/OptimiserApplyEditor"

// Mock useMlflowBrowser — returns empty defaults
const mockMlflow = {
  experiments: [] as { experiment_id: string; name: string }[],
  runs: [] as { run_id: string; run_name: string; metrics: Record<string, number>; artifacts: string[] }[],
  models: [] as { name: string; latest_versions: { version: string; status: string; run_id: string }[] }[],
  modelVersions: [] as { version: string; run_id: string; status: string; description: string }[],
  loadingExperiments: false,
  loadingRuns: false,
  loadingModels: false,
  loadingVersions: false,
  errorExperiments: "",
  errorRuns: "",
  errorModels: "",
  errorVersions: "",
  browseExpId: "",
  setBrowseExpId: vi.fn(),
  setRuns: vi.fn(),
  refreshExperiments: vi.fn(),
  refreshRuns: vi.fn(),
  refreshModels: vi.fn(),
  refreshVersions: vi.fn(),
  resetRunsGuard: vi.fn(),
}

vi.mock("../../hooks/useMlflowBrowser", () => ({
  useMlflowBrowser: () => mockMlflow,
}))

// Mock fetch for artifact metadata loading
const originalFetch = globalThis.fetch
beforeEach(() => {
  globalThis.fetch = vi.fn(() =>
    Promise.resolve(new Response(JSON.stringify({}), { status: 404, statusText: "Not Found" })),
  ) as unknown as typeof fetch
})

afterEach(() => {
  cleanup()
  globalThis.fetch = originalFetch
})

const DEFAULT_PROPS = {
  config: {},
  onUpdate: vi.fn(),
  inputSources: [],
}

describe("OptimiserApplyEditor", () => {
  it("renders source type toggle with all three modes", () => {
    render(<OptimiserApplyEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("File Path")).toBeTruthy()
    expect(screen.getByText("Registered")).toBeTruthy()
    expect(screen.getByText("Experiment Run")).toBeTruthy()
  })

  it("defaults to file source type", () => {
    render(<OptimiserApplyEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Artifact Path")).toBeTruthy()
    expect(screen.getByPlaceholderText("artifacts/optimiser_v1.json")).toBeTruthy()
  })

  it("calls onUpdate when switching source type", () => {
    const onUpdate = vi.fn()
    render(<OptimiserApplyEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    fireEvent.click(screen.getByText("Registered"))
    expect(onUpdate).toHaveBeenCalledWith("sourceType", "registered")
  })

  it("renders artifact path input in file mode", () => {
    render(
      <OptimiserApplyEditor
        {...DEFAULT_PROPS}
        config={{ artifact_path: "results/opt.json" }}
      />,
    )
    const input = screen.getByDisplayValue("results/opt.json") as HTMLInputElement
    expect(input).toBeTruthy()
  })

  it("calls onUpdate when changing artifact path", () => {
    const onUpdate = vi.fn()
    render(<OptimiserApplyEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    const input = screen.getByPlaceholderText("artifacts/optimiser_v1.json")
    fireEvent.change(input, { target: { value: "new/path.json" } })
    expect(onUpdate).toHaveBeenCalledWith("artifact_path", "new/path.json")
  })

  it("renders version column input with default value", () => {
    render(<OptimiserApplyEditor {...DEFAULT_PROPS} />)
    const input = screen.getByPlaceholderText("__optimiser_version__") as HTMLInputElement
    expect(input.value).toBe("__optimiser_version__")
  })

  it("calls onUpdate when changing version column", () => {
    const onUpdate = vi.fn()
    render(<OptimiserApplyEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    const input = screen.getByPlaceholderText("__optimiser_version__")
    fireEvent.change(input, { target: { value: "opt_ver" } })
    expect(onUpdate).toHaveBeenCalledWith("version_column", "opt_ver")
  })

  it("renders model name select in registered mode", () => {
    render(
      <OptimiserApplyEditor
        {...DEFAULT_PROPS}
        config={{ sourceType: "registered" }}
      />,
    )
    expect(screen.getByText("Model Name")).toBeTruthy()
    expect(screen.getByText("Select a model...")).toBeTruthy()
  })

  it("renders experiment select in run mode", () => {
    render(
      <OptimiserApplyEditor
        {...DEFAULT_PROPS}
        config={{ sourceType: "run" }}
      />,
    )
    expect(screen.getByText("Experiment")).toBeTruthy()
    expect(screen.getByText("Select an experiment...")).toBeTruthy()
  })

  it("renders run ID input in run mode", () => {
    render(
      <OptimiserApplyEditor
        {...DEFAULT_PROPS}
        config={{ sourceType: "run" }}
      />,
    )
    expect(screen.getByText("Run ID")).toBeTruthy()
    expect(screen.getByPlaceholderText("e.g. a1b2c3d4e5f6...")).toBeTruthy()
  })

  it("renders artifact source label", () => {
    render(<OptimiserApplyEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Artifact Source")).toBeTruthy()
  })
})
