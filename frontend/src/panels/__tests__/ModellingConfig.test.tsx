import { describe, it, expect, vi, afterEach, beforeEach } from "vitest"
import { render, screen, fireEvent, cleanup, waitFor, within } from "@testing-library/react"
import ModellingConfig from "../ModellingConfig"
import useNodeResultsStore, { hashConfig } from "../../stores/useNodeResultsStore"
import useSettingsStore from "../../stores/useSettingsStore"
import type { TrainResult } from "../../stores/useNodeResultsStore"

// ── Mocks ────────────────────────────────────────────────────────

const mockTrainModel = vi.fn()
const mockExportTraining = vi.fn()
const mockEstimateTrainingRam = vi.fn()

vi.mock("../../api/client", () => ({
  trainModel: (...args: unknown[]) => mockTrainModel(...args),
  exportTraining: (...args: unknown[]) => mockExportTraining(...args),
  estimateTrainingRam: (...args: unknown[]) => mockEstimateTrainingRam(...args),
}))

vi.mock("../../utils/buildGraph", () => ({
  buildGraph: vi.fn(() => ({ nodes: [], edges: [], preamble: "" })),
}))

// Mock child components that are already well-tested
vi.mock("../modelling/LossChart", () => ({
  LossChart: ({ lossHistory }: { lossHistory: unknown[] }) => (
    <div data-testid="loss-chart">{lossHistory ? "chart" : "no data"}</div>
  ),
}))
vi.mock("../modelling/FeatureImportance", () => ({
  FeatureImportance: () => <div data-testid="feature-importance" />,
}))
vi.mock("../modelling/MlflowExportSection", () => ({
  MlflowExportSection: () => <div data-testid="mlflow-export" />,
}))
vi.mock("../modelling/TrainingProgress", () => ({
  TrainingProgress: () => <div data-testid="training-progress" />,
}))

// ── Helpers ──────────────────────────────────────────────────────

const defaultColumns = [
  { name: "loss_ratio", dtype: "Float64" },
  { name: "age", dtype: "Int64" },
  { name: "region", dtype: "String" },
  { name: "exposure", dtype: "Float64" },
]

function defaultProps(overrides: Partial<Parameters<typeof ModellingConfig>[0]> = {}) {
  return {
    config: { _nodeId: "node_1", target: "loss_ratio", task: "regression" },
    onUpdate: vi.fn(),
    upstreamColumns: defaultColumns,
    allNodes: [],
    edges: [],
    ...overrides,
  }
}

function renderConfig(overrides: Partial<Parameters<typeof ModellingConfig>[0]> = {}) {
  const props = defaultProps(overrides)
  const result = render(<ModellingConfig {...props} />)
  return { ...result, props }
}

function makeTrainResult(overrides: Partial<TrainResult> = {}): TrainResult {
  return {
    status: "ok",
    metrics: { gini: 0.45, rmse: 0.12 },
    feature_importance: [
      { feature: "age", importance: 0.6 },
      { feature: "region", importance: 0.4 },
    ],
    model_path: "/models/catboost_model.cbm",
    train_rows: 8000,
    test_rows: 2000,
    ...overrides,
  }
}

// ── Setup / teardown ─────────────────────────────────────────────

beforeEach(() => {
  useNodeResultsStore.setState({
    trainJobs: {},
    trainResults: {},
  })
  useSettingsStore.setState({
    mlflow: { status: "pending", backend: "", host: "" },
    collapsedSections: {},
  })
  mockTrainModel.mockReset()
  mockExportTraining.mockReset()
  // Return a never-resolving promise by default so the useEffect doesn't cause
  // act() warnings from resolved promises after unmount.
  mockEstimateTrainingRam.mockReset().mockReturnValue(new Promise(() => {}))
})

afterEach(cleanup)

// ═════════════════════════════════════════════════════════════════
// Config rendering
// ═════════════════════════════════════════════════════════════════

describe("ModellingConfig", () => {
  describe("Config rendering", () => {
    it("renders target column dropdown with upstream columns", () => {
      renderConfig()
      // Target select: the label says "Target column", find the select within that section
      const targetLabel = screen.getByText("Target column")
      const targetSection = targetLabel.closest("div")!
      const targetSelect = targetSection.querySelector("select")!
      expect(targetSelect.value).toBe("loss_ratio")
      const options = within(targetSelect).getAllByRole("option")
      // 1 placeholder + 4 columns
      expect(options).toHaveLength(5)
      expect(options.map((o) => o.textContent)).toContain("loss_ratio (Float64)")
      expect(options.map((o) => o.textContent)).toContain("age (Int64)")
    })

    it("renders weight column dropdown with 'None' default", () => {
      renderConfig()
      // Weight defaults to "" which is the "None" option
      const weightSelect = screen.getAllByDisplayValue("None")[0]
      expect(weightSelect).toBeTruthy()
    })

    it("renders task toggle with regression active by default", () => {
      renderConfig()
      const regressionBtn = screen.getByRole("button", { name: "regression" })
      const classificationBtn = screen.getByRole("button", { name: "classification" })
      expect(regressionBtn).toBeTruthy()
      expect(classificationBtn).toBeTruthy()
    })

    it("switching task to classification calls onUpdate with new task and metrics", () => {
      const { props } = renderConfig()
      fireEvent.click(screen.getByRole("button", { name: "classification" }))
      // Should call onUpdate twice: once for task, once for metrics
      expect(props.onUpdate).toHaveBeenCalledWith("task", "classification")
      expect(props.onUpdate).toHaveBeenCalledWith("metrics", ["auc", "logloss"])
    })

    it("feature count shows correct number (excludes target and weight)", () => {
      renderConfig()
      // 4 columns total. Target=loss_ratio excluded, weight="" so not excluded.
      // Feature columns: age, region, exposure = 3 of 4
      expect(screen.getByText(/3 of 4/)).toBeTruthy()
    })

    it("feature count adjusts when weight is set", () => {
      renderConfig({
        config: { _nodeId: "node_1", target: "loss_ratio", task: "regression", weight: "exposure" },
      })
      // Target=loss_ratio, weight=exposure both excluded. Features: age, region = 2 of 4
      expect(screen.getByText(/2 of 4/)).toBeTruthy()
    })

    it("exclude column toggles work", () => {
      const { props } = renderConfig()
      // Click "age" to exclude it
      const ageBtn = screen.getByRole("button", { name: "age" })
      fireEvent.click(ageBtn)
      expect(props.onUpdate).toHaveBeenCalledWith("exclude", ["age"])
    })

    it("excluded column re-includes on second click", () => {
      const { props } = renderConfig({
        config: { _nodeId: "node_1", target: "loss_ratio", task: "regression", exclude: ["age"] },
      })
      const ageBtn = screen.getByRole("button", { name: "age" })
      fireEvent.click(ageBtn)
      // Should remove "age" from exclusion list
      expect(props.onUpdate).toHaveBeenCalledWith("exclude", [])
    })

    it("algorithm dropdown shows CatBoost", () => {
      renderConfig()
      const algoSelect = screen.getByDisplayValue("CatBoost")
      expect(algoSelect).toBeTruthy()
    })

    it("loss function dropdown shows regression losses for regression task", () => {
      renderConfig()
      // Find the loss function select (value is "" = Default)
      const selects = screen.getAllByRole("combobox")
      const lossSelect = selects.find((s) => {
        const opts = within(s).queryAllByRole("option")
        return opts.some((o) => o.textContent === "RMSE")
      })
      expect(lossSelect).toBeTruthy()
      const options = within(lossSelect!).getAllByRole("option")
      const optionTexts = options.map((o) => o.textContent)
      expect(optionTexts).toContain("RMSE")
      expect(optionTexts).toContain("MAE")
      expect(optionTexts).toContain("Poisson")
      expect(optionTexts).toContain("Tweedie")
      // Should NOT contain classification losses
      expect(optionTexts).not.toContain("Logloss")
    })

    it("loss function dropdown changes to classification losses when task=classification", () => {
      renderConfig({
        config: { _nodeId: "node_1", target: "loss_ratio", task: "classification" },
      })
      const selects = screen.getAllByRole("combobox")
      const lossSelect = selects.find((s) => {
        const opts = within(s).queryAllByRole("option")
        return opts.some((o) => o.textContent === "Logloss")
      })
      expect(lossSelect).toBeTruthy()
      const options = within(lossSelect!).getAllByRole("option")
      const optionTexts = options.map((o) => o.textContent)
      expect(optionTexts).toContain("Logloss")
      expect(optionTexts).toContain("CrossEntropy")
      // Should NOT contain regression losses
      expect(optionTexts).not.toContain("RMSE")
    })

    it("Tweedie variance power slider only visible when loss_function=Tweedie", () => {
      // Without Tweedie: no slider
      const { unmount } = render(<ModellingConfig {...defaultProps()} />)
      expect(screen.queryByText(/Variance power/)).toBeNull()
      unmount()

      // With Tweedie: slider visible
      render(
        <ModellingConfig
          {...defaultProps({
            config: { _nodeId: "node_1", target: "loss_ratio", task: "regression", loss_function: "Tweedie" },
          })}
        />,
      )
      expect(screen.getByText(/Variance power/)).toBeTruthy()
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // Hyperparameter inputs
  // ═════════════════════════════════════════════════════════════════

  describe("Hyperparameter inputs", () => {
    it("core params render with defaults", () => {
      renderConfig()
      expect(screen.getByText("Iterations")).toBeTruthy()
      expect(screen.getByText("Learning Rate")).toBeTruthy()
      expect(screen.getByText("Depth")).toBeTruthy()
      expect(screen.getByText("L2 Reg")).toBeTruthy()

      // Check default values are present
      expect(screen.getByDisplayValue("1000")).toBeTruthy()
      expect(screen.getByDisplayValue("0.05")).toBeTruthy()
      expect(screen.getByDisplayValue("6")).toBeTruthy()
      expect(screen.getByDisplayValue("3")).toBeTruthy()
    })

    it("changing a param calls onUpdate with merged params object", () => {
      const { props } = renderConfig()
      const iterInput = screen.getByDisplayValue("1000")
      fireEvent.change(iterInput, { target: { value: "500" } })
      expect(props.onUpdate).toHaveBeenCalledWith("params", expect.objectContaining({ iterations: 500 }))
    })

    it("regularisation params render with defaults", () => {
      renderConfig()
      expect(screen.getByText("Random Strength")).toBeTruthy()
      expect(screen.getByText("Bagging Temp")).toBeTruthy()
      expect(screen.getByText("Min Data in Leaf")).toBeTruthy()
      expect(screen.getByText("Border Count")).toBeTruthy()
    })

    it("grow policy defaults to SymmetricTree", () => {
      renderConfig()
      expect(screen.getByDisplayValue("SymmetricTree")).toBeTruthy()
    })

    it("early stopping rounds defaults to 50", () => {
      renderConfig()
      expect(screen.getByDisplayValue("50")).toBeTruthy()
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // Split/Eval section
  // ═════════════════════════════════════════════════════════════════

  describe("Split/Eval section", () => {
    it("renders split strategy buttons (random, temporal, group)", () => {
      renderConfig()
      expect(screen.getByRole("button", { name: "random" })).toBeTruthy()
      expect(screen.getByRole("button", { name: "temporal" })).toBeTruthy()
      expect(screen.getByRole("button", { name: "group" })).toBeTruthy()
    })

    it("random split shows test size and seed inputs", () => {
      renderConfig()
      expect(screen.getByText("Test size")).toBeTruthy()
      expect(screen.getByText("Seed")).toBeTruthy()
      expect(screen.getByDisplayValue("0.2")).toBeTruthy()
      expect(screen.getByDisplayValue("42")).toBeTruthy()
    })

    it("changing split strategy to temporal calls handleSplitUpdate", () => {
      const { props } = renderConfig()
      fireEvent.click(screen.getByRole("button", { name: "temporal" }))
      expect(props.onUpdate).toHaveBeenCalledWith("split", expect.objectContaining({ strategy: "temporal" }))
    })

    it("temporal split shows date column and cutoff date", () => {
      renderConfig({
        config: {
          _nodeId: "node_1",
          target: "loss_ratio",
          task: "regression",
          split: { strategy: "temporal", test_size: 0.2, seed: 42 },
        },
      })
      expect(screen.getByText("Date column")).toBeTruthy()
      expect(screen.getByText("Cutoff date")).toBeTruthy()
    })

    it("group split shows group column and test size", () => {
      renderConfig({
        config: {
          _nodeId: "node_1",
          target: "loss_ratio",
          task: "regression",
          split: { strategy: "group", test_size: 0.2, seed: 42 },
        },
      })
      expect(screen.getByText("Group column")).toBeTruthy()
    })

    it("metrics checkboxes for regression render correctly", () => {
      renderConfig()
      // Regression metrics
      expect(screen.getByRole("button", { name: "gini" })).toBeTruthy()
      expect(screen.getByRole("button", { name: "rmse" })).toBeTruthy()
      expect(screen.getByRole("button", { name: "mae" })).toBeTruthy()
      expect(screen.getByRole("button", { name: "r2" })).toBeTruthy()
    })

    it("clicking a metric button toggles it", () => {
      const { props } = renderConfig({
        config: { _nodeId: "node_1", target: "loss_ratio", task: "regression", metrics: ["gini", "rmse"] },
      })
      // Click "mae" to add it
      fireEvent.click(screen.getByRole("button", { name: "mae" }))
      expect(props.onUpdate).toHaveBeenCalledWith("metrics", ["gini", "rmse", "mae"])
    })

    it("clicking a selected metric removes it", () => {
      const { props } = renderConfig({
        config: { _nodeId: "node_1", target: "loss_ratio", task: "regression", metrics: ["gini", "rmse"] },
      })
      // Click "gini" to remove it
      fireEvent.click(screen.getByRole("button", { name: "gini" }))
      expect(props.onUpdate).toHaveBeenCalledWith("metrics", ["rmse"])
    })

    it("classification task shows classification metrics", () => {
      renderConfig({
        config: { _nodeId: "node_1", target: "loss_ratio", task: "classification" },
      })
      expect(screen.getByRole("button", { name: "auc" })).toBeTruthy()
      expect(screen.getByRole("button", { name: "logloss" })).toBeTruthy()
      // Regression metrics should NOT be visible
      expect(screen.queryByRole("button", { name: "rmse" })).toBeNull()
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // Training actions
  // ═════════════════════════════════════════════════════════════════

  describe("Training actions", () => {
    it("train button calls trainModel API with graph and node_id", async () => {
      mockTrainModel.mockResolvedValue({ status: "started", job_id: "job_1" })
      renderConfig()
      fireEvent.click(screen.getByRole("button", { name: /Train Model/ }))
      await waitFor(() => expect(mockTrainModel).toHaveBeenCalledTimes(1))
      const callArgs = mockTrainModel.mock.calls[0][0]
      expect(callArgs).toEqual(
        expect.objectContaining({
          graph: expect.any(Object),
          node_id: "node_1",
        }),
      )
    })

    it("train button is disabled when no target is set", () => {
      renderConfig({
        config: { _nodeId: "node_1", target: "", task: "regression" },
      })
      const trainBtn = screen.getByRole("button", { name: /Train Model/ })
      expect(trainBtn).toHaveProperty("disabled", true)
    })

    it("train button shows 'Training...' when job is active", () => {
      useNodeResultsStore.setState({
        trainJobs: {
          node_1: {
            jobId: "job_1",
            nodeId: "node_1",
            nodeLabel: "Model",
            progress: null,
            error: null,
            configHash: "abc",
          },
        },
      })
      renderConfig()
      expect(screen.getByRole("button", { name: /Training\.\.\./ })).toBeTruthy()
      expect(screen.getByRole("button", { name: /Training\.\.\./ })).toHaveProperty("disabled", true)
    })

    it("stores error result when trainModel throws", async () => {
      mockTrainModel.mockRejectedValue(new Error("Network fail"))
      renderConfig()
      fireEvent.click(screen.getByRole("button", { name: /Train Model/ }))
      await waitFor(() => {
        const store = useNodeResultsStore.getState()
        const cached = store.trainResults.node_1
        expect(cached).toBeTruthy()
        expect(cached.result.status).toBe("error")
        expect(cached.result.error).toBe("Error: Network fail")
      })
    })

    it("stores synchronous result when trainModel returns non-started status", async () => {
      const result = makeTrainResult()
      mockTrainModel.mockResolvedValue(result)
      renderConfig()
      fireEvent.click(screen.getByRole("button", { name: /Train Model/ }))
      await waitFor(() => {
        const store = useNodeResultsStore.getState()
        const cached = store.trainResults.node_1
        expect(cached).toBeTruthy()
        expect(cached.result.status).toBe("ok")
      })
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // Staleness indicator
  // ═════════════════════════════════════════════════════════════════

  describe("Staleness indicator", () => {
    it("shows staleness warning when config hash changed after training", () => {
      // Put a cached result with a different config hash
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult(),
            jobId: "job_1",
            configHash: "old_hash_that_wont_match",
          },
        },
      })
      renderConfig()
      expect(screen.getByText("Config changed since last training")).toBeTruthy()
      expect(screen.getByRole("button", { name: "Re-train" })).toBeTruthy()
    })

    it("does not show staleness warning when config hash matches", () => {
      const config = { _nodeId: "node_1", target: "loss_ratio", task: "regression" }
      const hash = hashConfig(config)

      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult(),
            jobId: "job_1",
            configHash: hash,
          },
        },
      })
      renderConfig({ config })
      expect(screen.queryByText("Config changed since last training")).toBeNull()
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // Training results
  // ═════════════════════════════════════════════════════════════════

  describe("Training results", () => {
    it("shows training progress panel when trainJob has progress", () => {
      useNodeResultsStore.setState({
        trainJobs: {
          node_1: {
            jobId: "job_1",
            nodeId: "node_1",
            nodeLabel: "Model",
            progress: {
              status: "running",
              progress: 0.5,
              message: "Training...",
              iteration: 50,
              total_iterations: 100,
              train_loss: { rmse: 0.1 },
              elapsed_seconds: 10,
            },
            error: null,
            configHash: "abc",
          },
        },
      })
      renderConfig()
      expect(screen.getByTestId("training-progress")).toBeTruthy()
    })

    it("shows error message when trainResult.status === 'error'", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult({ status: "error", error: "OOM: out of memory" }),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByText("Training failed")).toBeTruthy()
      expect(screen.getByText("OOM: out of memory")).toBeTruthy()
    })

    it("shows model path and row counts on successful result", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult(),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByText(/\/models\/catboost_model\.cbm/)).toBeTruthy()
      expect(screen.getByText(/8,000 train/)).toBeTruthy()
      expect(screen.getByText(/2,000 test/)).toBeTruthy()
    })

    it("shows loss chart when trainResult has loss_history", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult({
              loss_history: [
                { iteration: 1, train_rmse: 0.5 },
                { iteration: 2, train_rmse: 0.3 },
              ],
            }),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByTestId("loss-chart")).toBeTruthy()
    })

    it("does not show loss chart when loss_history has fewer than 2 entries", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult({
              loss_history: [{ iteration: 1, train_rmse: 0.5 }],
            }),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.queryByTestId("loss-chart")).toBeNull()
    })

    it("shows feature importance when trainResult has non-empty feature_importance", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult(),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByTestId("feature-importance")).toBeTruthy()
    })

    it("does not show feature importance when feature_importance is empty", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult({ feature_importance: [] }),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.queryByTestId("feature-importance")).toBeNull()
    })

    it("shows MlflowExportSection when result exists and mlflow is connected", () => {
      useSettingsStore.setState({
        mlflow: { status: "connected", backend: "databricks", host: "https://example.com" },
      })
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult(),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByTestId("mlflow-export")).toBeTruthy()
    })

    it("does not show MlflowExportSection when mlflow is not connected", () => {
      useSettingsStore.setState({
        mlflow: { status: "pending", backend: "", host: "" },
      })
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult(),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.queryByTestId("mlflow-export")).toBeNull()
    })

    it("shows metrics from successful result", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult({ metrics: { gini: 0.4567, rmse: 0.1234 } }),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByText("0.4567")).toBeTruthy()
      expect(screen.getByText("0.1234")).toBeTruthy()
    })

    it("shows early stopping info when best_iteration is set", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult({ best_iteration: 300 }),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByText(/Stopped early at iteration 300/)).toBeTruthy()
    })

    it("shows warning when trainResult.warning is set", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult({ warning: "Dataset was downsampled" }),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByText("Dataset was downsampled")).toBeTruthy()
    })

    it("shows double lift table when present", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult({
              double_lift: [
                { decile: 1, actual: 0.05, predicted: 0.06, count: 500 },
                { decile: 2, actual: 0.10, predicted: 0.11, count: 500 },
              ],
            }),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByText("Double Lift (Actual vs Predicted by Decile)")).toBeTruthy()
      expect(screen.getByText("0.0500")).toBeTruthy()
      expect(screen.getByText("0.0600")).toBeTruthy()
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // Export
  // ═════════════════════════════════════════════════════════════════

  describe("Export", () => {
    it("export button calls exportTraining API", async () => {
      mockExportTraining.mockResolvedValue({ script: "print('hello')" })
      renderConfig()
      fireEvent.click(screen.getByRole("button", { name: /Export Training Script/ }))
      await waitFor(() => expect(mockExportTraining).toHaveBeenCalledTimes(1))
      const callArgs = mockExportTraining.mock.calls[0][0]
      expect(callArgs).toEqual(
        expect.objectContaining({
          graph: expect.any(Object),
          node_id: "node_1",
        }),
      )
    })

    it("shows exported script text after export", async () => {
      mockExportTraining.mockResolvedValue({ script: "import catboost\nmodel.fit()" })
      renderConfig()
      fireEvent.click(screen.getByRole("button", { name: /Export Training Script/ }))
      await waitFor(() => {
        expect(screen.getByText(/import catboost/)).toBeTruthy()
      })
    })

    it("shows Generated Script heading and Copy button after export", async () => {
      mockExportTraining.mockResolvedValue({ script: "print('hello')" })
      renderConfig()
      fireEvent.click(screen.getByRole("button", { name: /Export Training Script/ }))
      await waitFor(() => {
        expect(screen.getByText("Generated Script")).toBeTruthy()
        expect(screen.getByRole("button", { name: "Copy" })).toBeTruthy()
      })
    })

    it("export button is disabled when no target", () => {
      renderConfig({
        config: { _nodeId: "node_1", target: "", task: "regression" },
      })
      const exportBtn = screen.getByRole("button", { name: /Export Training Script/ })
      expect(exportBtn).toHaveProperty("disabled", true)
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // RAM estimate
  // ═════════════════════════════════════════════════════════════════

  describe("RAM estimate", () => {
    it("calls estimateTrainingRam on mount", () => {
      renderConfig()
      expect(mockEstimateTrainingRam).toHaveBeenCalledTimes(1)
      const callArgs = mockEstimateTrainingRam.mock.calls[0][0]
      expect(callArgs).toEqual(
        expect.objectContaining({
          graph: expect.any(Object),
          node_id: "node_1",
        }),
      )
    })

    it("shows loading state while estimating", () => {
      // The mock returns a never-resolving promise, so loading persists
      renderConfig()
      expect(screen.getByText("Estimating dataset size...")).toBeTruthy()
    })

    it("shows RAM estimate data when resolved", async () => {
      mockEstimateTrainingRam.mockResolvedValue({
        total_rows: 100000,
        safe_row_limit: null,
        estimated_mb: 50,
        training_mb: 200,
        available_mb: 8192,
        bytes_per_row: 500,
        was_downsampled: false,
      })
      renderConfig()
      await waitFor(() => {
        expect(screen.getByText("Dataset fits in memory")).toBeTruthy()
        expect(screen.getByText("100,000")).toBeTruthy()
        expect(screen.getByText("200 MB")).toBeTruthy()
      })
    })

    it("shows downsample warning when was_downsampled is true", async () => {
      mockEstimateTrainingRam.mockResolvedValue({
        total_rows: 5000000,
        safe_row_limit: 1000000,
        estimated_mb: 2500,
        training_mb: 10000,
        available_mb: 8192,
        bytes_per_row: 500,
        was_downsampled: true,
      })
      renderConfig()
      await waitFor(() => {
        expect(screen.getByText("Will downsample")).toBeTruthy()
        expect(screen.getByText("1,000,000")).toBeTruthy()
      })
    })

    it("shows GPU VRAM info when gpu fields present", async () => {
      mockEstimateTrainingRam.mockResolvedValue({
        total_rows: 100000,
        safe_row_limit: null,
        estimated_mb: 50,
        training_mb: 200,
        available_mb: 8192,
        bytes_per_row: 500,
        was_downsampled: false,
        gpu_vram_estimated_mb: 512,
        gpu_vram_available_mb: 8192,
      })
      renderConfig()
      await waitFor(() => {
        expect(screen.getByText("Est. GPU VRAM")).toBeTruthy()
        expect(screen.getByText("512 MB")).toBeTruthy()
      })
    })

    it("shows GPU warning when gpu_warning is set", async () => {
      mockEstimateTrainingRam.mockResolvedValue({
        total_rows: 100000,
        safe_row_limit: null,
        estimated_mb: 50,
        training_mb: 200,
        available_mb: 8192,
        bytes_per_row: 500,
        was_downsampled: false,
        gpu_vram_estimated_mb: 12000,
        gpu_warning: "VRAM may be insufficient",
      })
      renderConfig()
      await waitFor(() => {
        expect(screen.getByText("VRAM may be insufficient")).toBeTruthy()
      })
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // Collapsible sections
  // ═════════════════════════════════════════════════════════════════

  describe("Collapsible sections", () => {
    it("advanced params section is collapsed by default", () => {
      renderConfig()
      const advBtn = screen.getByRole("button", { name: /Advanced params/ })
      expect(advBtn).toBeTruthy()
      // The textarea should not be visible when collapsed
      expect(screen.queryByRole("textbox", { name: /advanced/i })).toBeNull()
    })

    it("clicking advanced params toggle opens JSON editor", () => {
      renderConfig()
      fireEvent.click(screen.getByRole("button", { name: /Advanced params/ }))
      // After clicking, the store should be toggled and a textarea should appear
      // Since we're using the real store, the section should now be open
      const textareas = document.querySelectorAll("textarea")
      expect(textareas.length).toBeGreaterThan(0)
    })

    it("MLflow Logging section is collapsed by default", () => {
      renderConfig()
      const mlflowBtn = screen.getByRole("button", { name: /MLflow Logging/ })
      expect(mlflowBtn).toBeTruthy()
      // Experiment path input should not be visible
      expect(screen.queryByPlaceholderText("/Shared/haute/experiment")).toBeNull()
    })

    it("clicking MLflow Logging toggle shows experiment inputs", () => {
      renderConfig()
      fireEvent.click(screen.getByRole("button", { name: /MLflow Logging/ }))
      expect(screen.getByPlaceholderText("/Shared/haute/experiment")).toBeTruthy()
    })

    it("Monotonic Constraints section is collapsed by default (when columns exist)", () => {
      renderConfig()
      const monoBtn = screen.getByRole("button", { name: /Monotonic Constraints/ })
      expect(monoBtn).toBeTruthy()
    })

    it("clicking Monotonic Constraints shows numeric feature constraints", () => {
      renderConfig()
      fireEvent.click(screen.getByRole("button", { name: /Monotonic Constraints/ }))
      // Should show constraint controls for numeric features (age, exposure)
      // but not for string features (region) or target (loss_ratio)
      // After toggle, we should see "age" and "exposure" but not "region" or "loss_ratio"
      expect(screen.getByText("Set per-feature constraints (numeric features only)")).toBeTruthy()
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // Cross-validation
  // ═════════════════════════════════════════════════════════════════

  describe("Cross-validation", () => {
    it("CV toggle defaults to Off", () => {
      renderConfig()
      const cvBtn = screen.getByRole("button", { name: "Off" })
      expect(cvBtn).toBeTruthy()
    })

    it("clicking CV toggle calls onUpdate to enable cv_folds", () => {
      const { props } = renderConfig()
      fireEvent.click(screen.getByRole("button", { name: "Off" }))
      expect(props.onUpdate).toHaveBeenCalledWith("cv_folds", 5)
    })

    it("clicking CV toggle when on calls onUpdate to disable cv_folds", () => {
      const { props } = renderConfig({
        config: { _nodeId: "node_1", target: "loss_ratio", task: "regression", cv_folds: 5 },
      })
      fireEvent.click(screen.getByRole("button", { name: "On" }))
      expect(props.onUpdate).toHaveBeenCalledWith("cv_folds", null)
    })

    it("shows CV results in train output when present", () => {
      useNodeResultsStore.setState({
        trainResults: {
          node_1: {
            result: makeTrainResult({
              cv_results: {
                mean_metrics: { gini: 0.44 },
                std_metrics: { gini: 0.02 },
                n_folds: 5,
              },
            }),
            jobId: "job_1",
            configHash: "irrelevant",
          },
        },
      })
      renderConfig()
      expect(screen.getByText(/Cross-Validation \(5-fold\)/)).toBeTruthy()
      expect(screen.getByText("0.4400")).toBeTruthy()
      expect(screen.getByText(/0\.0200/)).toBeTruthy()
    })
  })

  // ═════════════════════════════════════════════════════════════════
  // Edge cases
  // ═════════════════════════════════════════════════════════════════

  describe("Edge cases", () => {
    it("renders without upstream columns", () => {
      renderConfig({ upstreamColumns: undefined })
      // Should not crash, feature count section still renders
      expect(screen.getByText(/Features/)).toBeTruthy()
    })

    it("renders with empty columns array", () => {
      renderConfig({ upstreamColumns: [] })
      expect(screen.getByText(/Features/)).toBeTruthy()
    })

    it("GPU toggle calls handleParamUpdate", () => {
      const { props } = renderConfig()
      const gpuCheckbox = screen.getByRole("checkbox")
      fireEvent.click(gpuCheckbox)
      expect(props.onUpdate).toHaveBeenCalledWith("params", expect.objectContaining({ task_type: "GPU" }))
    })

    it("GPU unchecked sets CPU", () => {
      const { props } = renderConfig({
        config: { _nodeId: "node_1", target: "loss_ratio", task: "regression", params: { task_type: "GPU" } },
      })
      const gpuCheckbox = screen.getByRole("checkbox")
      fireEvent.click(gpuCheckbox)
      expect(props.onUpdate).toHaveBeenCalledWith("params", expect.objectContaining({ task_type: "CPU" }))
    })
  })
})
