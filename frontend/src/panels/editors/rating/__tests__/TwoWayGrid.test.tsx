import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import { TwoWayGrid } from "../TwoWayGrid"
import type { RatingTable } from "../ratingTableUtils"

function makeTable(overrides: Partial<RatingTable> = {}): RatingTable {
  return {
    name: "Table 1",
    factors: ["age_band", "region"],
    outputColumn: "factor",
    defaultValue: "1.0",
    entries: [
      { age_band: "young", region: "north", value: 1.1 },
      { age_band: "young", region: "south", value: 0.9 },
      { age_band: "old", region: "north", value: 1.3 },
      { age_band: "old", region: "south", value: 0.7 },
    ],
    ...overrides,
  }
}

const bandingLevels = {
  age_band: ["young", "old"],
  region: ["north", "south"],
}

describe("TwoWayGrid", () => {
  afterEach(cleanup)

  it("renders row and column factor headers", () => {
    render(
      <TwoWayGrid
        table={makeTable()}
        bandingLevels={bandingLevels}
        onUpdateEntries={vi.fn()}
      />,
    )
    expect(screen.getByText(/age_band/)).toBeInTheDocument()
    expect(screen.getByText(/region/)).toBeInTheDocument()
  })

  it("renders row labels from bandingLevels", () => {
    render(
      <TwoWayGrid
        table={makeTable()}
        bandingLevels={bandingLevels}
        onUpdateEntries={vi.fn()}
      />,
    )
    expect(screen.getByText("young")).toBeInTheDocument()
    expect(screen.getByText("old")).toBeInTheDocument()
  })

  it("renders column labels in table header", () => {
    render(
      <TwoWayGrid
        table={makeTable()}
        bandingLevels={bandingLevels}
        onUpdateEntries={vi.fn()}
      />,
    )
    expect(screen.getByText("north")).toBeInTheDocument()
    expect(screen.getByText("south")).toBeInTheDocument()
  })

  it("returns null when less than two factors", () => {
    const { container } = render(
      <TwoWayGrid
        table={makeTable({ factors: ["age_band"] })}
        bandingLevels={bandingLevels}
        onUpdateEntries={vi.fn()}
      />,
    )
    expect(container.innerHTML).toBe("")
  })

  it("shows empty message when no banding levels for factors", () => {
    render(
      <TwoWayGrid
        table={makeTable()}
        bandingLevels={{ age_band: [], region: [] }}
        onUpdateEntries={vi.fn()}
      />,
    )
    expect(screen.getByText(/No banding levels found/)).toBeInTheDocument()
  })

  it("calls onUpdateEntries when a cell value changes", () => {
    const onUpdate = vi.fn()
    render(
      <TwoWayGrid
        table={makeTable()}
        bandingLevels={bandingLevels}
        onUpdateEntries={onUpdate}
      />,
    )
    const inputs = screen.getAllByRole("spinbutton")
    // Blur the first input with a new value
    fireEvent.blur(inputs[0], { target: { value: "2.0" } })
    expect(onUpdate).toHaveBeenCalledOnce()
  })
})
