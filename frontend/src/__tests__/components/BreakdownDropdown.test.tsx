/**
 * Tests for BreakdownDropdown component.
 *
 * Tests: null rendering for empty items, total display, toggle open/close,
 * sorted item display, and total text in the dropdown panel.
 */
import { describe, it, expect, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import BreakdownDropdown, { type BreakdownItem } from "../../components/BreakdownDropdown"

afterEach(cleanup)

// ── Helpers ──────────────────────────────────────────────────────

const MockIcon = ({ size }: { size: number }) => (
  <span data-testid="icon">{size}</span>
)

const formatValue = (value: number) => `${value.toFixed(1)}ms`

const sampleItems: BreakdownItem[] = [
  { node_id: "a", label: "Alpha", value: 10 },
  { node_id: "b", label: "Beta", value: 30 },
  { node_id: "c", label: "Gamma", value: 20 },
]

function renderDropdown(items: BreakdownItem[] = sampleItems) {
  return render(
    <BreakdownDropdown
      icon={MockIcon}
      title="Latency"
      items={items}
      formatValue={formatValue}
    />,
  )
}

// ── Tests ────────────────────────────────────────────────────────

describe("BreakdownDropdown", () => {
  it("returns null for empty items", () => {
    const { container } = renderDropdown([])
    expect(container.innerHTML).toBe("")
  })

  it("renders the total value using formatValue", () => {
    renderDropdown()

    // Total = 10 + 30 + 20 = 60
    expect(screen.getByText("60.0ms")).toBeTruthy()
  })

  it("renders the icon with the correct size", () => {
    renderDropdown()

    const icon = screen.getByTestId("icon")
    expect(icon.textContent).toBe("12")
  })

  it("opens the dropdown when the button is clicked", () => {
    renderDropdown()

    expect(screen.queryByText("Latency")).toBeNull()

    fireEvent.click(screen.getByText("60.0ms"))

    expect(screen.getByText("Latency")).toBeTruthy()
  })

  it("shows items sorted by value descending when open", () => {
    const { container } = renderDropdown()

    fireEvent.click(screen.getByText("60.0ms"))

    const labels = container.querySelectorAll<HTMLSpanElement>(
      "span[title]",
    )
    const labelTexts = Array.from(labels).map((el) => el.textContent)

    // Should be sorted: Beta (30), Gamma (20), Alpha (10)
    expect(labelTexts).toEqual(["Beta", "Gamma", "Alpha"])
  })

  it("shows 'total' text with formatted value when open", () => {
    renderDropdown()

    fireEvent.click(screen.getByText("60.0ms"))

    expect(screen.getByText("60.0ms total")).toBeTruthy()
  })

  it("closes the dropdown when the button is clicked again", () => {
    renderDropdown()

    const toggleButton = screen.getByText("60.0ms")

    fireEvent.click(toggleButton)
    expect(screen.getByText("Latency")).toBeTruthy()

    fireEvent.click(toggleButton)
    expect(screen.queryByText("Latency")).toBeNull()
  })

  it("displays individual formatted values for each item", () => {
    renderDropdown()

    fireEvent.click(screen.getByText("60.0ms"))

    expect(screen.getByText("30.0ms")).toBeTruthy()
    expect(screen.getByText("20.0ms")).toBeTruthy()
    expect(screen.getByText("10.0ms")).toBeTruthy()
  })
})
