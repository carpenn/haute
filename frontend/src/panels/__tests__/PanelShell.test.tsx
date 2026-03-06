import { describe, it, expect, beforeEach, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import PanelShell from "../PanelShell"
import useUIStore from "../../stores/useUIStore"

beforeEach(() => {
  useUIStore.setState({ nodePanelWidth: 600 })
})

afterEach(cleanup)

describe("PanelShell", () => {
  it("renders children", () => {
    render(<PanelShell><span data-testid="child">Hello</span></PanelShell>)
    expect(screen.getByTestId("child")).toBeTruthy()
  })

  it("uses panel width from store", () => {
    const { container } = render(<PanelShell><span>content</span></PanelShell>)
    const shell = container.firstElementChild as HTMLElement
    expect(shell.style.width).toBe("600px")
  })

  it("applies background from CSS variable", () => {
    const { container } = render(<PanelShell><span>content</span></PanelShell>)
    const shell = container.firstElementChild as HTMLElement
    expect(shell.style.background).toBe("var(--bg-panel)")
  })

  it("merges additional style prop", () => {
    const { container } = render(
      <PanelShell style={{ opacity: 0.5 }}><span>content</span></PanelShell>,
    )
    const shell = container.firstElementChild as HTMLElement
    expect(shell.style.opacity).toBe("0.5")
  })

  it("has a visible drag handle with col-resize cursor", () => {
    const { container } = render(<PanelShell><span>content</span></PanelShell>)
    const handle = container.querySelector(".cursor-col-resize") as HTMLElement
    expect(handle).toBeTruthy()
  })

  it("drag handle updates panel width via mouse events", () => {
    useUIStore.setState({ nodePanelWidth: 500 })
    const { container } = render(<PanelShell><span>content</span></PanelShell>)
    const handle = container.querySelector(".cursor-col-resize") as HTMLElement

    fireEvent.mouseDown(handle, { clientX: 400 })
    fireEvent.mouseMove(window, { clientX: 300 }) // delta = 100 → 500 + 100 = 600
    fireEvent.mouseUp(window)

    expect(useUIStore.getState().nodePanelWidth).toBe(600)
  })

  it("clamps width to minimum of 320", () => {
    useUIStore.setState({ nodePanelWidth: 400 })
    const { container } = render(<PanelShell><span>content</span></PanelShell>)
    const handle = container.querySelector(".cursor-col-resize") as HTMLElement

    fireEvent.mouseDown(handle, { clientX: 400 })
    fireEvent.mouseMove(window, { clientX: 700 }) // delta = -300 → 400 - 300 = 100 → clamped to 320
    fireEvent.mouseUp(window)

    expect(useUIStore.getState().nodePanelWidth).toBe(320)
  })

  it("clamps width to maximum of 900", () => {
    useUIStore.setState({ nodePanelWidth: 800 })
    const { container } = render(<PanelShell><span>content</span></PanelShell>)
    const handle = container.querySelector(".cursor-col-resize") as HTMLElement

    fireEvent.mouseDown(handle, { clientX: 400 })
    fireEvent.mouseMove(window, { clientX: -100 }) // delta = 500 → 800 + 500 = 1300 → clamped to 900
    fireEvent.mouseUp(window)

    expect(useUIStore.getState().nodePanelWidth).toBe(900)
  })

  it("has slide-in animation class", () => {
    const { container } = render(<PanelShell><span>content</span></PanelShell>)
    const shell = container.firstElementChild as HTMLElement
    expect(shell.classList.contains("animate-slide-in")).toBe(true)
  })
})
