/**
 * Render tests for BandingEditor.
 *
 * Tests: renders with default config, factor tabs, type toggle,
 * rules grid, column selection, and config updates.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import BandingEditor from "../../panels/editors/BandingEditor"

afterEach(cleanup)

describe("BandingEditor", () => {
  it("renders with default empty config", () => {
    render(
      <BandingEditor
        config={{}}
        onUpdate={vi.fn()}
        inputSources={[]}
      />,
    )
    expect(screen.getByText(/Group values into bands/)).toBeTruthy()
    expect(screen.getByText(/1 factor\b/)).toBeTruthy()
  })

  it("renders factor count for multiple factors", () => {
    const config = {
      factors: [
        { banding: "continuous", column: "age", outputColumn: "age_band", rules: [] },
        { banding: "categorical", column: "region", outputColumn: "region_group", rules: [] },
      ],
    }
    render(
      <BandingEditor config={config} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText(/2 factors/)).toBeTruthy()
  })

  it("renders factor tab labels from outputColumn", () => {
    const config = {
      factors: [
        { banding: "continuous", column: "age", outputColumn: "age_band", rules: [] },
        { banding: "categorical", column: "region", outputColumn: "region_group", rules: [] },
      ],
    }
    render(
      <BandingEditor config={config} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("age_band")).toBeTruthy()
    expect(screen.getByText("region_group")).toBeTruthy()
  })

  it("renders factor tab labels from column when outputColumn is empty", () => {
    const config = {
      factors: [
        { banding: "continuous", column: "age", outputColumn: "", rules: [] },
      ],
    }
    render(
      <BandingEditor config={config} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("age")).toBeTruthy()
  })

  it("shows type toggle buttons", () => {
    render(
      <BandingEditor config={{}} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("Continuous")).toBeTruthy()
    expect(screen.getByText("Categorical")).toBeTruthy()
  })

  it("calls onUpdate when switching banding type", () => {
    const onUpdate = vi.fn()
    render(
      <BandingEditor config={{}} onUpdate={onUpdate} inputSources={[]} />,
    )
    fireEvent.click(screen.getByText("Categorical"))
    expect(onUpdate).toHaveBeenCalledWith("factors", expect.arrayContaining([
      expect.objectContaining({ banding: "categorical", rules: [] }),
    ]))
  })

  it("renders continuous rules grid headers", () => {
    const config = {
      factors: [{
        banding: "continuous",
        column: "age",
        outputColumn: "age_band",
        rules: [{ op1: ">", val1: "25", op2: "<=", val2: "35", assignment: "young" }],
      }],
    }
    render(
      <BandingEditor config={config} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("Band")).toBeTruthy()
    // Check rule count
    expect(screen.getByText("Rules (1)")).toBeTruthy()
  })

  it("renders categorical rules grid headers", () => {
    const config = {
      factors: [{
        banding: "categorical",
        column: "region",
        outputColumn: "region_group",
        rules: [{ value: "London", assignment: "South" }],
      }],
    }
    render(
      <BandingEditor config={config} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("Group")).toBeTruthy()
  })

  it("shows 'No rules yet' when rules are empty", () => {
    render(
      <BandingEditor config={{}} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("No rules yet")).toBeTruthy()
  })

  it("renders column dropdown when upstreamColumns provided", () => {
    const columns = [
      { name: "age", dtype: "int64" },
      { name: "region", dtype: "Utf8" },
    ]
    render(
      <BandingEditor
        config={{}}
        onUpdate={vi.fn()}
        inputSources={[]}
        upstreamColumns={columns}
      />,
    )
    // Should render a select with column options
    const selects = screen.getAllByRole("combobox")
    const colSelect = selects.find(s => {
      const opts = Array.from((s as HTMLSelectElement).options)
      return opts.some(o => o.textContent?.includes("age"))
    })
    expect(colSelect).toBeTruthy()
  })

  it("renders text input for column when no upstreamColumns", () => {
    render(
      <BandingEditor config={{}} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByPlaceholderText("driver_age")).toBeTruthy()
  })

  it("renders default value input", () => {
    render(
      <BandingEditor config={{}} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByPlaceholderText("null")).toBeTruthy()
  })

  it("renders summary when factors have complete config", () => {
    const config = {
      factors: [{
        banding: "continuous",
        column: "age",
        outputColumn: "age_band",
        rules: [{ op1: ">", val1: "25", op2: "", val2: "", assignment: "young" }],
      }],
    }
    render(
      <BandingEditor config={config} onUpdate={vi.fn()} inputSources={[]} />,
    )
    // Summary shows column → outputColumn · rules · type
    expect(screen.getAllByText("age").length).toBeGreaterThanOrEqual(1)
    expect(screen.getAllByText("age_band").length).toBeGreaterThanOrEqual(1)
  })
})
