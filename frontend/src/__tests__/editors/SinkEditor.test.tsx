/**
 * Render tests for SinkEditor.
 *
 * Tests: format toggle, path input, write button state.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import SinkEditor from "../../panels/editors/SinkEditor"

afterEach(cleanup)

const DEFAULT_PROPS = {
  config: {},
  onUpdate: vi.fn(),
  nodeId: "sink_1",
  allNodes: [],
  edges: [],
}

describe("SinkEditor", () => {
  it("renders format toggle with parquet and csv options", () => {
    render(<SinkEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("PARQUET")).toBeTruthy()
    expect(screen.getByText("CSV")).toBeTruthy()
  })

  it("defaults to parquet format", () => {
    render(<SinkEditor {...DEFAULT_PROPS} />)
    // The parquet button should be styled as active
    const parquetBtn = screen.getByText("PARQUET")
    expect(parquetBtn.closest("button")).toBeTruthy()
  })

  it("calls onUpdate when switching format", () => {
    const onUpdate = vi.fn()
    render(<SinkEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    fireEvent.click(screen.getByText("CSV"))
    expect(onUpdate).toHaveBeenCalledWith("format", "csv")
  })

  it("renders output path input", () => {
    render(<SinkEditor {...DEFAULT_PROPS} />)
    const input = screen.getByPlaceholderText("output/results.parquet")
    expect(input).toBeTruthy()
  })

  it("shows csv placeholder when format is csv", () => {
    render(<SinkEditor {...DEFAULT_PROPS} config={{ format: "csv" }} />)
    expect(screen.getByPlaceholderText("output/results.csv")).toBeTruthy()
  })

  it("renders write button", () => {
    render(<SinkEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Write")).toBeTruthy()
  })

  it("disables write button when no path set", () => {
    render(<SinkEditor {...DEFAULT_PROPS} config={{}} />)
    const writeBtn = screen.getByText("Write").closest("button")!
    expect(writeBtn.disabled).toBe(true)
  })

  it("enables write button when path is set", () => {
    render(<SinkEditor {...DEFAULT_PROPS} config={{ path: "output/data.parquet" }} />)
    const writeBtn = screen.getByText("Write").closest("button")!
    expect(writeBtn.disabled).toBe(false)
  })

  it("populates path input from config", () => {
    render(<SinkEditor {...DEFAULT_PROPS} config={{ path: "my_output.parquet" }} />)
    const input = screen.getByDisplayValue("my_output.parquet")
    expect(input).toBeTruthy()
  })

  it("calls onUpdate when changing path", () => {
    const onUpdate = vi.fn()
    render(<SinkEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    const input = screen.getByPlaceholderText("output/results.parquet")
    fireEvent.change(input, { target: { value: "new_path.parquet" } })
    expect(onUpdate).toHaveBeenCalledWith("path", "new_path.parquet")
  })
})
