import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import Toolbar from "../Toolbar"
import useSettingsStore from "../../stores/useSettingsStore"

function makeProps(overrides: Partial<Parameters<typeof Toolbar>[0]> = {}) {
  return {
    nodeCount: 5,
    edgeCount: 3,
    dirty: false,
    canUndo: true,
    canRedo: false,
    onUndo: vi.fn(),
    onRedo: vi.fn(),
    snapToGrid: false,
    onToggleSnapToGrid: vi.fn(),
    onShowShortcuts: vi.fn(),
    onOpenSettings: vi.fn(),
    onAutoLayout: vi.fn(),
    onRun: vi.fn(),
    runStatus: null as string | null,
    onSave: vi.fn(),
    wsStatus: "connected" as const,
    ...overrides,
  }
}

describe("Toolbar", () => {
  beforeEach(() => {
    useSettingsStore.setState({
      rowLimit: 1000,
      scenarios: ["live"],
      activeScenario: "live",
    })
  })

  afterEach(cleanup)

  it("renders node and edge counts", () => {
    render(<Toolbar {...makeProps()} />)
    expect(screen.getByText("5 nodes · 3 edges")).toBeInTheDocument()
  })

  it("renders Haute brand name", () => {
    render(<Toolbar {...makeProps()} />)
    expect(screen.getByText("Haute")).toBeInTheDocument()
  })

  it("clicking Run calls onRun", () => {
    const props = makeProps()
    render(<Toolbar {...props} />)
    fireEvent.click(screen.getByText("Run"))
    expect(props.onRun).toHaveBeenCalledOnce()
  })

  it("clicking Save calls onSave", () => {
    const props = makeProps()
    render(<Toolbar {...props} />)
    fireEvent.click(screen.getByText("Save"))
    expect(props.onSave).toHaveBeenCalledOnce()
  })

  it("shows Running... when runStatus is Running...", () => {
    render(<Toolbar {...makeProps({ runStatus: "Running..." })} />)
    expect(screen.getByText("Running...")).toBeInTheDocument()
  })

  it("Run button is disabled when nodeCount is 0", () => {
    render(<Toolbar {...makeProps({ nodeCount: 0 })} />)
    const runBtn = screen.getByText("Run")
    expect(runBtn).toBeDisabled()
  })

  it("Layout button is disabled when nodeCount is 0", () => {
    render(<Toolbar {...makeProps({ nodeCount: 0 })} />)
    const layoutBtn = screen.getByText("Layout")
    expect(layoutBtn).toBeDisabled()
  })

  it("clicking Layout calls onAutoLayout", () => {
    const props = makeProps()
    render(<Toolbar {...props} />)
    fireEvent.click(screen.getByText("Layout"))
    expect(props.onAutoLayout).toHaveBeenCalledOnce()
  })

  it("clicking Imports calls onOpenSettings", () => {
    const props = makeProps()
    render(<Toolbar {...props} />)
    fireEvent.click(screen.getByText("Imports"))
    expect(props.onOpenSettings).toHaveBeenCalledOnce()
  })

  it("undo button calls onUndo", () => {
    const props = makeProps()
    render(<Toolbar {...props} />)
    // Find by title
    const undoBtn = screen.getByTitle("Undo (Ctrl+Z)")
    fireEvent.click(undoBtn)
    expect(props.onUndo).toHaveBeenCalledOnce()
  })

  it("redo button is disabled when canRedo is false", () => {
    render(<Toolbar {...makeProps({ canRedo: false })} />)
    const redoBtn = screen.getByLabelText("Redo")
    expect(redoBtn).toBeDisabled()
  })

  it("shows unsaved indicator when dirty", () => {
    render(<Toolbar {...makeProps({ dirty: true })} />)
    expect(screen.getByTitle("Unsaved changes")).toBeInTheDocument()
  })

  it("row limit input changes the store value", () => {
    render(<Toolbar {...makeProps()} />)
    const input = screen.getByRole("spinbutton")
    fireEvent.change(input, { target: { value: "500" } })
    expect(useSettingsStore.getState().rowLimit).toBe(500)
  })

  it("row limit clamps negative values to 0", () => {
    render(<Toolbar {...makeProps()} />)
    const input = screen.getByRole("spinbutton")
    fireEvent.change(input, { target: { value: "-50" } })
    expect(useSettingsStore.getState().rowLimit).toBe(0)
  })

  it("row limit treats NaN input as 0", () => {
    render(<Toolbar {...makeProps()} />)
    const input = screen.getByRole("spinbutton")
    fireEvent.change(input, { target: { value: "abc" } })
    expect(useSettingsStore.getState().rowLimit).toBe(0)
  })

  it("row limit input shows current store value", () => {
    useSettingsStore.setState({ rowLimit: 2000 })
    render(<Toolbar {...makeProps()} />)
    const input = screen.getByRole("spinbutton") as HTMLInputElement
    expect(input.value).toBe("2000")
  })

  it("snap-to-grid button calls onToggleSnapToGrid", () => {
    const props = makeProps()
    render(<Toolbar {...props} />)
    const snapBtn = screen.getByTitle("Toggle snap-to-grid (G)")
    fireEvent.click(snapBtn)
    expect(props.onToggleSnapToGrid).toHaveBeenCalledOnce()
  })

  it("snap-to-grid button shows accent color when active", () => {
    render(<Toolbar {...makeProps({ snapToGrid: true })} />)
    const snapBtn = screen.getByTitle("Toggle snap-to-grid (G)")
    expect(snapBtn.style.color).toBe("var(--accent)")
  })

  it("snap-to-grid button shows secondary color when inactive", () => {
    render(<Toolbar {...makeProps({ snapToGrid: false })} />)
    const snapBtn = screen.getByTitle("Toggle snap-to-grid (G)")
    expect(snapBtn.style.color).toBe("var(--text-secondary)")
  })

  it("keyboard shortcuts button calls onShowShortcuts", () => {
    const props = makeProps()
    render(<Toolbar {...props} />)
    const kbBtn = screen.getByLabelText("Keyboard shortcuts")
    fireEvent.click(kbBtn)
    expect(props.onShowShortcuts).toHaveBeenCalledOnce()
  })

  it("undo button is disabled when canUndo is false", () => {
    render(<Toolbar {...makeProps({ canUndo: false })} />)
    const undoBtn = screen.getByTitle("Undo (Ctrl+Z)")
    expect(undoBtn).toBeDisabled()
  })

  it("redo button is enabled when canRedo is true", () => {
    render(<Toolbar {...makeProps({ canRedo: true })} />)
    const redoBtn = screen.getByLabelText("Redo")
    expect(redoBtn).not.toBeDisabled()
  })

  it("redo button calls onRedo when clicked", () => {
    const props = makeProps({ canRedo: true })
    render(<Toolbar {...props} />)
    const redoBtn = screen.getByLabelText("Redo")
    fireEvent.click(redoBtn)
    expect(props.onRedo).toHaveBeenCalledOnce()
  })

  it("shows websocket connected status dot", () => {
    render(<Toolbar {...makeProps({ wsStatus: "connected" })} />)
    const dot = screen.getByTitle("Live sync connected")
    expect(dot).toBeInTheDocument()
  })

  it("shows websocket reconnecting status dot", () => {
    render(<Toolbar {...makeProps({ wsStatus: "reconnecting" })} />)
    const dot = screen.getByTitle("Reconnecting to server\u2026")
    expect(dot).toBeInTheDocument()
  })

  it("shows websocket disconnected status dot", () => {
    render(<Toolbar {...makeProps({ wsStatus: "disconnected" })} />)
    const dot = screen.getByTitle("Server unreachable \u2014 restart haute serve")
    expect(dot).toBeInTheDocument()
  })

  it("does not show unsaved indicator when not dirty", () => {
    render(<Toolbar {...makeProps({ dirty: false })} />)
    expect(screen.queryByTitle("Unsaved changes")).not.toBeInTheDocument()
  })

  it("scenario selector renders with default 'live' option", () => {
    render(<Toolbar {...makeProps()} />)
    const select = screen.getByRole("combobox") as HTMLSelectElement
    expect(select.value).toBe("live")
  })

  it("scenario selector shows all scenarios", () => {
    useSettingsStore.setState({ scenarios: ["live", "test_scenario"], activeScenario: "live" })
    render(<Toolbar {...makeProps()} />)
    const options = screen.getAllByRole("option")
    const optionTexts = options.map((o) => o.textContent)
    expect(optionTexts).toContain("● live")
    expect(optionTexts).toContain("test_scenario")
  })

  it("switching scenario updates store", () => {
    useSettingsStore.setState({ scenarios: ["live", "test_scenario"], activeScenario: "live" })
    render(<Toolbar {...makeProps()} />)
    const select = screen.getByRole("combobox")
    fireEvent.change(select, { target: { value: "test_scenario" } })
    expect(useSettingsStore.getState().activeScenario).toBe("test_scenario")
  })

  it("shows remove option for non-live scenarios", () => {
    useSettingsStore.setState({ scenarios: ["live", "test_scenario"], activeScenario: "test_scenario" })
    render(<Toolbar {...makeProps()} />)
    const options = screen.getAllByRole("option")
    const removeOption = options.find((o) => o.textContent?.includes("Remove"))
    expect(removeOption).toBeTruthy()
  })

  it("does not show remove option when live is active", () => {
    useSettingsStore.setState({ scenarios: ["live"], activeScenario: "live" })
    render(<Toolbar {...makeProps()} />)
    const options = screen.getAllByRole("option")
    const removeOption = options.find((o) => o.textContent?.includes("Remove"))
    expect(removeOption).toBeUndefined()
  })
})
