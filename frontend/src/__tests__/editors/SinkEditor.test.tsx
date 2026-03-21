/**
 * Render tests for SinkEditor.
 *
 * Tests: format toggle, path input, write button state,
 * successful write shows success message, failed write shows error message,
 * placeholder changes based on format selection.
 */
import { describe, it, expect, vi, afterEach, beforeEach } from "vitest"
import { render, screen, fireEvent, cleanup, waitFor, act } from "@testing-library/react"
import SinkEditor from "../../panels/editors/SinkEditor"

// Mock API client
const mockExecuteSink = vi.fn()
vi.mock("../../api/client", () => ({
  executeSink: (...args: unknown[]) => mockExecuteSink(...args),
}))

// Mock settings store
vi.mock("../../stores/useSettingsStore", () => ({
  default: {
    getState: () => ({ activeSource: "live" }),
  },
}))

afterEach(cleanup)

beforeEach(() => {
  mockExecuteSink.mockReset()
})

const DEFAULT_PROPS = {
  config: {} as Record<string, unknown>,
  onUpdate: vi.fn(),
  nodeId: "sink_1",
  allNodes: [] as { id: string; type?: string; data: { label: string; description: string; nodeType: string; config?: Record<string, unknown> } }[],
  edges: [] as { id: string; source: string; target: string }[],
  accentColor: "#60a5fa",
}

describe("SinkEditor", () => {
  it("renders format toggle with parquet and csv options", () => {
    render(<SinkEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("PARQUET")).toBeTruthy()
    expect(screen.getByText("CSV")).toBeTruthy()
  })

  it("format toggle switches between parquet and csv, calls onUpdate", () => {
    const onUpdate = vi.fn()
    render(<SinkEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)

    // Click CSV
    fireEvent.click(screen.getByText("CSV"))
    expect(onUpdate).toHaveBeenCalledWith("format", "csv")

    // Click PARQUET
    fireEvent.click(screen.getByText("PARQUET"))
    expect(onUpdate).toHaveBeenCalledWith("format", "parquet")
  })

  it("path input calls onUpdate when changed", () => {
    const onUpdate = vi.fn()
    render(<SinkEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    const input = screen.getByRole("textbox")
    fireEvent.change(input, { target: { value: "new_path.parquet" } })
    expect(onUpdate).toHaveBeenCalledWith("path", "new_path.parquet")
  })

  it("Write button disabled when no path set", () => {
    render(<SinkEditor {...DEFAULT_PROPS} config={{}} />)
    const writeBtn = screen.getByText("Write").closest("button")!
    expect(writeBtn.disabled).toBe(true)
  })

  it("Write button enabled when path is set", () => {
    render(<SinkEditor {...DEFAULT_PROPS} config={{ path: "output/data.parquet" }} />)
    const writeBtn = screen.getByText("Write").closest("button")!
    expect(writeBtn.disabled).toBe(false)
  })

  it("Write button disabled while writing and shows 'Writing...'", async () => {
    // Keep the promise pending to simulate in-flight write
    let resolveWrite!: (value: unknown) => void
    mockExecuteSink.mockReturnValue(new Promise((resolve) => { resolveWrite = resolve }))

    render(<SinkEditor {...DEFAULT_PROPS} config={{ path: "output/data.parquet" }} />)
    const writeBtn = screen.getByText("Write").closest("button")!

    await act(async () => {
      fireEvent.click(writeBtn)
    })

    // Button should now say "Writing..." and be disabled
    expect(screen.getByText("Writing...")).toBeTruthy()
    const writingBtn = screen.getByText("Writing...").closest("button")!
    expect(writingBtn.disabled).toBe(true)

    // Resolve to clean up
    await act(async () => {
      resolveWrite({ status: "ok", message: "Done" })
    })
  })

  it("successful write shows success message", async () => {
    mockExecuteSink.mockResolvedValue({ status: "ok", message: "Written successfully" })

    render(<SinkEditor {...DEFAULT_PROPS} config={{ path: "output/data.parquet" }} />)

    await act(async () => {
      fireEvent.click(screen.getByText("Write").closest("button")!)
    })

    await waitFor(() => {
      expect(screen.getByText("Written successfully")).toBeTruthy()
    })

    // The success message container should have green-ish color (ok status)
    const resultDiv = screen.getByText("Written successfully").closest("div")!
    expect(resultDiv.style.background).toContain("34, 197, 94")
  })

  it("failed write shows error message", async () => {
    mockExecuteSink.mockRejectedValue(new Error("Network error"))

    render(<SinkEditor {...DEFAULT_PROPS} config={{ path: "output/data.parquet" }} />)

    await act(async () => {
      fireEvent.click(screen.getByText("Write").closest("button")!)
    })

    await waitFor(() => {
      expect(screen.getByText("Network error")).toBeTruthy()
    })

    // The error message container should have red-ish background (error status)
    const resultDiv = screen.getByText("Network error").closest("div")!
    expect(resultDiv.style.background).toContain("239, 68, 68")
  })

  it("path input has no placeholder text", () => {
    render(<SinkEditor {...DEFAULT_PROPS} config={{}} />)
    const input = screen.getByRole("textbox")
    expect(input.getAttribute("placeholder")).toBe("")
  })

  it("populates path input from config", () => {
    render(<SinkEditor {...DEFAULT_PROPS} config={{ path: "my_output.parquet" }} />)
    const input = screen.getByDisplayValue("my_output.parquet")
    expect(input).toBeTruthy()
  })

  it("does not trigger write when button is clicked with no path", () => {
    render(<SinkEditor {...DEFAULT_PROPS} config={{}} />)
    const writeBtn = screen.getByText("Write").closest("button")!
    fireEvent.click(writeBtn)
    expect(mockExecuteSink).not.toHaveBeenCalled()
  })

  it("passes correct arguments to executeSink", async () => {
    mockExecuteSink.mockResolvedValue({ status: "ok", message: "Done" })

    render(<SinkEditor {...DEFAULT_PROPS} config={{ path: "output/data.parquet" }} />)

    await act(async () => {
      fireEvent.click(screen.getByText("Write").closest("button")!)
    })

    expect(mockExecuteSink).toHaveBeenCalledTimes(1)
    // First arg: graph object, second: nodeId, third: activeSource
    const [graph, nodeId, source] = mockExecuteSink.mock.calls[0]
    expect(nodeId).toBe("sink_1")
    expect(source).toBe("live")
    expect(graph).toHaveProperty("nodes")
    expect(graph).toHaveProperty("edges")
  })
})
