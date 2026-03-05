import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import ToggleButtonGroup from "../ToggleButtonGroup"

const OPTIONS = [
  { key: "a" as const, label: "Alpha" },
  { key: "b" as const, label: "Beta" },
  { key: "c" as const, label: "Gamma" },
]

const ACCENT = "#3b82f6"
const ACCENT_RGB = "rgb(59, 130, 246)"

function renderToggle(overrides: Partial<Parameters<typeof ToggleButtonGroup>[0]> = {}) {
  const props = {
    value: "a" as string,
    onChange: vi.fn(),
    options: OPTIONS,
    accentColor: ACCENT,
    ...overrides,
  }
  return { ...render(<ToggleButtonGroup {...props} />), props }
}

describe("ToggleButtonGroup", () => {
  afterEach(cleanup)

  it("renders all option labels", () => {
    renderToggle()
    expect(screen.getByText("Alpha")).toBeInTheDocument()
    expect(screen.getByText("Beta")).toBeInTheDocument()
    expect(screen.getByText("Gamma")).toBeInTheDocument()
  })

  it("calls onChange with clicked option key", () => {
    const { props } = renderToggle()
    fireEvent.click(screen.getByText("Beta"))
    expect(props.onChange).toHaveBeenCalledWith("b")
  })

  it("active button has accent-colored border", () => {
    renderToggle({ value: "a" })
    const activeBtn = screen.getByText("Alpha").closest("button")!
    expect(activeBtn.style.border).toContain(ACCENT_RGB)
  })

  it("inactive button has default border", () => {
    renderToggle({ value: "a" })
    const inactiveBtn = screen.getByText("Beta").closest("button")!
    expect(inactiveBtn.style.border).toContain("var(--border)")
  })

  it("active button text color matches accent", () => {
    renderToggle({ value: "b" })
    const activeBtn = screen.getByText("Beta").closest("button")!
    expect(activeBtn.style.color).toBe(ACCENT_RGB)
  })

  it("inactive button text color is secondary", () => {
    renderToggle({ value: "b" })
    const inactiveBtn = screen.getByText("Alpha").closest("button")!
    expect(inactiveBtn.style.color).toBe("var(--text-secondary)")
  })

  it("renders icons when provided", () => {
    const options = [
      { key: "x" as const, label: "With Icon", icon: <span data-testid="test-icon">I</span> },
    ]
    render(
      <ToggleButtonGroup
        value="x"
        onChange={vi.fn()}
        options={options}
        accentColor={ACCENT}
      />,
    )
    expect(screen.getByTestId("test-icon")).toBeInTheDocument()
  })

  it("does not call onChange for already-active option", () => {
    const { props } = renderToggle({ value: "a" })
    fireEvent.click(screen.getByText("Alpha"))
    expect(props.onChange).toHaveBeenCalledWith("a")
  })
})
