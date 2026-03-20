import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import { FeatureAndAlgorithmConfig } from "../FeatureAndAlgorithmConfig"
import type { FeatureAndAlgorithmConfigProps } from "../FeatureAndAlgorithmConfig"

afterEach(cleanup)

const COLUMNS = [
  { name: "target", dtype: "Float64" },
  { name: "weight_col", dtype: "Float64" },
  { name: "age", dtype: "Int64" },
  { name: "income", dtype: "Float64" },
  { name: "region", dtype: "Utf8" },
]

function makeProps(overrides: Partial<FeatureAndAlgorithmConfigProps> = {}): FeatureAndAlgorithmConfigProps {
  return {
    onUpdate: vi.fn(),
    columns: COLUMNS,
    target: "target",
    weight: "weight_col",
    exclude: [],
    params: { iterations: 1000, learning_rate: 0.05, depth: 6 },
    featureCount: 3,
    featuresOpen: false,
    toggleSection: vi.fn(),
    ...overrides,
  }
}

describe("FeatureAndAlgorithmConfig", () => {
  it("renders feature count summary in collapsed state", () => {
    render(<FeatureAndAlgorithmConfig {...makeProps()} />)
    expect(screen.getByText(/3 of 5/)).toBeInTheDocument()
  })

  it("clicking features section header toggles it", () => {
    const toggleSection = vi.fn()
    render(<FeatureAndAlgorithmConfig {...makeProps({ toggleSection })} />)
    fireEvent.click(screen.getByText("Features", { exact: false }))
    expect(toggleSection).toHaveBeenCalledWith("modelling.features")
  })

  it("shows feature list when featuresOpen is true, excluding target and weight", () => {
    render(<FeatureAndAlgorithmConfig {...makeProps({ featuresOpen: true })} />)
    expect(screen.getByText("age")).toBeInTheDocument()
    expect(screen.getByText("income")).toBeInTheDocument()
    expect(screen.getByText("region")).toBeInTheDocument()
    // target and weight should be filtered out
    expect(screen.queryByText("target")).not.toBeInTheDocument()
    expect(screen.queryByText("weight_col")).not.toBeInTheDocument()
  })

  it("Select all button clears exclude list", () => {
    const onUpdate = vi.fn()
    render(<FeatureAndAlgorithmConfig {...makeProps({ featuresOpen: true, onUpdate })} />)
    fireEvent.click(screen.getByText("Select all"))
    expect(onUpdate).toHaveBeenCalledWith("exclude", [])
  })

  it("Deselect all button excludes all non-target/weight features", () => {
    const onUpdate = vi.fn()
    render(<FeatureAndAlgorithmConfig {...makeProps({ featuresOpen: true, onUpdate })} />)
    fireEvent.click(screen.getByText("Deselect all"))
    expect(onUpdate).toHaveBeenCalledWith("exclude", ["age", "income", "region"])
  })

  it("excluding a feature calls onUpdate with updated exclude list", () => {
    const onUpdate = vi.fn()
    render(<FeatureAndAlgorithmConfig {...makeProps({ featuresOpen: true, onUpdate })} />)
    // Each feature has Include and Exclude buttons — click Exclude for "age"
    const excludeButtons = screen.getAllByText("Exclude")
    fireEvent.click(excludeButtons[0]) // first feature alphabetically is "age"
    expect(onUpdate).toHaveBeenCalledWith("exclude", ["age"])
  })

  it("renders hyperparameters textarea with JSON content", () => {
    render(<FeatureAndAlgorithmConfig {...makeProps()} />)
    const textarea = screen.getByRole("textbox")
    expect(textarea).toBeInTheDocument()
    expect((textarea as HTMLTextAreaElement).value).toContain("iterations")
    expect((textarea as HTMLTextAreaElement).value).toContain("1000")
  })

  it("shows parse error on invalid JSON blur", () => {
    render(<FeatureAndAlgorithmConfig {...makeProps()} />)
    const textarea = screen.getByRole("textbox")
    fireEvent.change(textarea, { target: { value: "not json" } })
    fireEvent.blur(textarea)
    // Should display an error
    expect(screen.getByText(/Unexpected token/i, { exact: false })).toBeInTheDocument()
  })

  it("commits valid JSON on blur", () => {
    const onUpdate = vi.fn()
    render(<FeatureAndAlgorithmConfig {...makeProps({ onUpdate })} />)
    const textarea = screen.getByRole("textbox")
    fireEvent.change(textarea, { target: { value: '{"iterations": 500}' } })
    fireEvent.blur(textarea)
    expect(onUpdate).toHaveBeenCalledWith("params", { iterations: 500 })
  })

  it("GPU checkbox toggles task_type", () => {
    const onUpdate = vi.fn()
    render(<FeatureAndAlgorithmConfig {...makeProps({ onUpdate })} />)
    const gpuCheckbox = screen.getByRole("checkbox")
    fireEvent.click(gpuCheckbox)
    expect(onUpdate).toHaveBeenCalledWith("params", expect.objectContaining({ task_type: "GPU" }))
  })

  it("GPU checkbox is checked when params include task_type GPU", () => {
    render(<FeatureAndAlgorithmConfig {...makeProps({ params: { iterations: 1000, task_type: "GPU" } })} />)
    const gpuCheckbox = screen.getByRole("checkbox") as HTMLInputElement
    expect(gpuCheckbox.checked).toBe(true)
  })

  it("shows excluded count when features are excluded", () => {
    render(<FeatureAndAlgorithmConfig {...makeProps({ exclude: ["age", "income"], featureCount: 1 })} />)
    expect(screen.getByText(/2 excluded/)).toBeInTheDocument()
  })
})
