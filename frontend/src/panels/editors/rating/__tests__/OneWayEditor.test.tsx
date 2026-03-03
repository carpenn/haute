import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import { OneWayEditor } from "../OneWayEditor"
import type { RatingTable } from "../ratingTableUtils"

function makeTable(overrides: Partial<RatingTable> = {}): RatingTable {
  return {
    name: "Table 1",
    factors: ["age_band"],
    outputColumn: "age_factor",
    defaultValue: "1.0",
    entries: [
      { age_band: "young", value: 1.2 },
      { age_band: "mid", value: 1.0 },
      { age_band: "old", value: 0.8 },
    ],
    ...overrides,
  }
}

describe("OneWayEditor", () => {
  afterEach(cleanup)

  it("renders factor column header", () => {
    render(
      <OneWayEditor
        table={makeTable()}
        bandingLevels={{ age_band: ["young", "mid", "old"] }}
        onUpdateEntries={vi.fn()}
      />,
    )
    expect(screen.getByText("age_band")).toBeInTheDocument()
  })

  it("renders all banding levels as rows", () => {
    render(
      <OneWayEditor
        table={makeTable()}
        bandingLevels={{ age_band: ["young", "mid", "old"] }}
        onUpdateEntries={vi.fn()}
      />,
    )
    expect(screen.getByText("young")).toBeInTheDocument()
    expect(screen.getByText("mid")).toBeInTheDocument()
    expect(screen.getByText("old")).toBeInTheDocument()
  })

  it("renders empty message when no banding levels", () => {
    render(
      <OneWayEditor
        table={makeTable()}
        bandingLevels={{}}
        onUpdateEntries={vi.fn()}
      />,
    )
    expect(screen.getByText("No banding levels found")).toBeInTheDocument()
  })

  it("returns null when factor is missing", () => {
    const { container } = render(
      <OneWayEditor
        table={makeTable({ factors: [] })}
        bandingLevels={{}}
        onUpdateEntries={vi.fn()}
      />,
    )
    expect(container.innerHTML).toBe("")
  })

  it("calls onUpdateEntries when a cell value changes", () => {
    const onUpdate = vi.fn()
    render(
      <OneWayEditor
        table={makeTable()}
        bandingLevels={{ age_band: ["young", "mid", "old"] }}
        onUpdateEntries={onUpdate}
      />,
    )
    const inputs = screen.getAllByRole("spinbutton")
    // Change the first input (young = 1.2)
    fireEvent.blur(inputs[0], { target: { value: "1.5" } })
    expect(onUpdate).toHaveBeenCalledOnce()
  })
})
