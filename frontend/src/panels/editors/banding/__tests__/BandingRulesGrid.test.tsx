import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import { BandingRulesGrid } from "../BandingRulesGrid"
import type { BandingFactor, ContinuousRule, CategoricalRule } from "../../../../types/banding"

function makeFactor(overrides: Partial<BandingFactor> = {}): BandingFactor {
  return {
    banding: "continuous",
    column: "age",
    outputColumn: "age_band",
    rules: [],
    default: null,
    ...overrides,
  }
}

describe("BandingRulesGrid", () => {
  afterEach(cleanup)

  it("renders empty state for continuous banding with no rules", () => {
    render(<BandingRulesGrid factor={makeFactor()} onUpdateFactor={vi.fn()} />)
    expect(screen.getByText("No rules yet")).toBeInTheDocument()
  })

  it("renders empty state for categorical banding with no rules", () => {
    render(<BandingRulesGrid factor={makeFactor({ banding: "categorical" })} onUpdateFactor={vi.fn()} />)
    expect(screen.getByText("No rules yet")).toBeInTheDocument()
  })

  it("renders continuous rule rows with correct headers", () => {
    const rules: ContinuousRule[] = [
      { op1: "<", val1: "25", op2: "", val2: "", assignment: "young" },
      { op1: ">=", val1: "25", op2: "<", val2: "60", assignment: "mid" },
    ]
    render(<BandingRulesGrid factor={makeFactor({ rules })} onUpdateFactor={vi.fn()} />)
    // Two "Op" columns (primary and secondary)
    expect(screen.getAllByText("Op", { selector: "th" })).toHaveLength(2)
    expect(screen.getByText("Band", { selector: "th" })).toBeInTheDocument()
  })

  it("renders categorical rule rows", () => {
    const rules: CategoricalRule[] = [
      { value: "Semi-detached", assignment: "House" },
      { value: "Terraced", assignment: "House" },
    ]
    render(<BandingRulesGrid factor={makeFactor({ banding: "categorical", rules })} onUpdateFactor={vi.fn()} />)
    expect(screen.getByDisplayValue("Semi-detached")).toBeInTheDocument()
    expect(screen.getByDisplayValue("Terraced")).toBeInTheDocument()
    // Both map to "House" assignment
    expect(screen.getAllByDisplayValue("House")).toHaveLength(2)
  })

  it("delete button removes a continuous rule", () => {
    const onUpdate = vi.fn()
    const rules: ContinuousRule[] = [
      { op1: "<", val1: "25", op2: "", val2: "", assignment: "young" },
      { op1: ">=", val1: "60", op2: "", val2: "", assignment: "old" },
    ]
    render(<BandingRulesGrid factor={makeFactor({ rules })} onUpdateFactor={onUpdate} />)
    // Click the first delete button
    const deleteButtons = screen.getAllByRole("button")
    fireEvent.click(deleteButtons[0])
    expect(onUpdate).toHaveBeenCalledWith({ rules: [rules[1]] })
  })

  it("updating a categorical rule field calls onUpdateFactor", () => {
    const onUpdate = vi.fn()
    const rules: CategoricalRule[] = [
      { value: "Car", assignment: "Vehicle" },
    ]
    render(<BandingRulesGrid factor={makeFactor({ banding: "categorical", rules })} onUpdateFactor={onUpdate} />)
    const inputs = screen.getAllByRole("textbox")
    fireEvent.change(inputs[0], { target: { value: "Truck" } })
    expect(onUpdate).toHaveBeenCalledWith({
      rules: [{ value: "Truck", assignment: "Vehicle" }],
    })
  })
})
