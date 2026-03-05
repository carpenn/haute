/**
 * Render tests for ModelScoreEditor.
 *
 * Tests: source type toggle, registered/run modes, task selector,
 * output column, classification note, and post-processing toggle.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import ModelScoreEditor from "../../panels/editors/ModelScoreEditor"

// Mock useMlflowBrowser — returns empty defaults
vi.mock("../../hooks/useMlflowBrowser", () => ({
  useMlflowBrowser: () => ({
    experiments: [],
    runs: [],
    models: [],
    modelVersions: [],
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
  }),
}))

afterEach(cleanup)

const DEFAULT_PROPS = {
  config: {},
  onUpdate: vi.fn(),
  inputSources: [],
  accentColor: "#a855f7",
}

describe("ModelScoreEditor", () => {
  it("renders source type toggle with two modes", () => {
    render(<ModelScoreEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Registered Model")).toBeTruthy()
    expect(screen.getByText("Experiment Run")).toBeTruthy()
  })

  it("defaults to registered source type", () => {
    render(<ModelScoreEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Model Name")).toBeTruthy()
    expect(screen.getByText("Select a model...")).toBeTruthy()
  })

  it("calls onUpdate when switching source type", () => {
    const onUpdate = vi.fn()
    render(<ModelScoreEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    fireEvent.click(screen.getByText("Experiment Run"))
    expect(onUpdate).toHaveBeenCalledWith("sourceType", "run")
  })

  it("renders task select with regression and classification", () => {
    render(<ModelScoreEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Task")).toBeTruthy()
    expect(screen.getByText("Regression")).toBeTruthy()
    expect(screen.getByText("Classification")).toBeTruthy()
  })

  it("defaults task to regression", () => {
    render(<ModelScoreEditor {...DEFAULT_PROPS} />)
    const select = screen.getByDisplayValue("Regression") as HTMLSelectElement
    expect(select.value).toBe("regression")
  })

  it("calls onUpdate when changing task", () => {
    const onUpdate = vi.fn()
    render(<ModelScoreEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    const select = screen.getByDisplayValue("Regression")
    fireEvent.change(select, { target: { value: "classification" } })
    expect(onUpdate).toHaveBeenCalledWith("task", "classification")
  })

  it("renders output column input with default value", () => {
    render(<ModelScoreEditor {...DEFAULT_PROPS} />)
    const input = screen.getByDisplayValue("prediction") as HTMLInputElement
    expect(input).toBeTruthy()
  })

  it("calls onUpdate when changing output column", () => {
    const onUpdate = vi.fn()
    render(<ModelScoreEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    const input = screen.getByDisplayValue("prediction")
    fireEvent.change(input, { target: { value: "score" } })
    expect(onUpdate).toHaveBeenCalledWith("output_column", "score")
  })

  it("shows classification proba note when task is classification", () => {
    render(
      <ModelScoreEditor
        {...DEFAULT_PROPS}
        config={{ task: "classification" }}
        accentColor="#a855f7"
      />,
    )
    expect(screen.getByText(/prediction_proba/)).toBeTruthy()
  })

  it("does not show proba note when task is regression", () => {
    render(<ModelScoreEditor {...DEFAULT_PROPS} />)
    expect(screen.queryByText(/prediction_proba/)).toBeNull()
  })

  it("renders post-processing code toggle", () => {
    render(<ModelScoreEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Post-processing Code (optional)")).toBeTruthy()
  })

  it("renders experiment select in run mode", () => {
    render(
      <ModelScoreEditor
        {...DEFAULT_PROPS}
        config={{ sourceType: "run" }}
        accentColor="#a855f7"
      />,
    )
    expect(screen.getByText("Experiment")).toBeTruthy()
    expect(screen.getByText("Select an experiment...")).toBeTruthy()
  })
})
