/**
 * Render tests for ScenarioExpanderEditor.
 *
 * Tests: quote id label, select vs text input, value column, range section,
 * default values, editing min, step column, preview line, selecting a column.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import ScenarioExpanderEditor from "../../panels/editors/ScenarioExpanderEditor"

vi.mock("../../panels/editors/_shared", async () => {
  const actual = await vi.importActual("../../panels/editors/_shared")
  return {
    ...actual,
    InputSourcesBar: ({ inputSources }: { inputSources: { varName: string; edgeId: string; sourceLabel: string }[] }) => (
      <div data-testid="input-sources">{inputSources?.length ?? 0} inputs</div>
    ),
    INPUT_STYLE: {},
  }
})

afterEach(cleanup)

const DEFAULT_PROPS = {
  config: {},
  onUpdate: vi.fn(),
  inputSources: [] as { varName: string; edgeId: string; sourceLabel: string }[],
  upstreamColumns: [] as { name: string; dtype: string }[],
}

describe("ScenarioExpanderEditor", () => {
  it("renders Quote ID Column label", () => {
    render(<ScenarioExpanderEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Quote ID Column")).toBeTruthy()
  })

  it("shows select dropdown when upstreamColumns provided", () => {
    const columns = [
      { name: "quote_id", dtype: "Utf8" },
      { name: "product", dtype: "Utf8" },
    ]
    const { container } = render(
      <ScenarioExpanderEditor {...DEFAULT_PROPS} upstreamColumns={columns} />,
    )
    const select = container.querySelector("select")
    expect(select).toBeTruthy()
    expect(screen.getByText("quote_id")).toBeTruthy()
    expect(screen.getByText("product")).toBeTruthy()
  })

  it("shows text input when upstreamColumns is empty", () => {
    const { container } = render(
      <ScenarioExpanderEditor {...DEFAULT_PROPS} upstreamColumns={[]} />,
    )
    const select = container.querySelector("select")
    expect(select).toBeNull()
    expect(screen.getByPlaceholderText("quote_id")).toBeTruthy()
  })

  it("renders Value Column input with default scenario_value", () => {
    render(<ScenarioExpanderEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Value Column")).toBeTruthy()
    expect(screen.getByDisplayValue("scenario_value")).toBeTruthy()
  })

  it("renders Range section with Min, Max, Steps inputs", () => {
    render(<ScenarioExpanderEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Range")).toBeTruthy()
    expect(screen.getByText("Min")).toBeTruthy()
    expect(screen.getByText("Max")).toBeTruthy()
    expect(screen.getByText("Steps")).toBeTruthy()
  })

  it("uses default values: min=0.8, max=1.2, steps=21", () => {
    render(<ScenarioExpanderEditor {...DEFAULT_PROPS} />)
    expect(screen.getByDisplayValue("0.8")).toBeTruthy()
    expect(screen.getByDisplayValue("1.2")).toBeTruthy()
    expect(screen.getByDisplayValue("21")).toBeTruthy()
  })

  it("editing min calls onUpdate with parsed float", () => {
    const onUpdate = vi.fn()
    render(<ScenarioExpanderEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    const minInput = screen.getByDisplayValue("0.8")
    fireEvent.change(minInput, { target: { value: "0.5" } })
    expect(onUpdate).toHaveBeenCalledWith("min_value", 0.5)
  })

  it("renders Step Column input with default scenario_index", () => {
    render(<ScenarioExpanderEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Step Column")).toBeTruthy()
    expect(screen.getByDisplayValue("scenario_index")).toBeTruthy()
  })

  it("shows preview line with step count", () => {
    const { container } = render(<ScenarioExpanderEditor {...DEFAULT_PROPS} />)
    const previewText = container.textContent || ""
    expect(previewText).toContain("21")
    expect(previewText).toContain("output rows")
  })

  it("selecting a quote_id column calls onUpdate", () => {
    const onUpdate = vi.fn()
    const columns = [
      { name: "quote_id", dtype: "Utf8" },
      { name: "product", dtype: "Utf8" },
    ]
    const { container } = render(
      <ScenarioExpanderEditor
        {...DEFAULT_PROPS}
        onUpdate={onUpdate}
        upstreamColumns={columns}
      />,
    )
    const select = container.querySelector("select")!
    fireEvent.change(select, { target: { value: "product" } })
    expect(onUpdate).toHaveBeenCalledWith("quote_id", "product")
  })
})
