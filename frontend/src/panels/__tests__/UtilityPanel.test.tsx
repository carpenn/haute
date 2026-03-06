import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup, waitFor, act } from "@testing-library/react"
import UtilityPanel from "../UtilityPanel"

// Mock the CodeEditor (heavy CodeMirror dependency)
vi.mock("../editors", () => ({
  CodeEditor: ({ defaultValue, onChange, placeholder, errorLine }: any) => (
    <textarea
      data-testid="code-editor"
      defaultValue={defaultValue}
      onChange={(e) => onChange?.(e.target.value)}
      placeholder={placeholder}
      data-error-line={errorLine}
    />
  ),
}))

// Mock API client
const mockListFiles = vi.fn()
const mockReadFile = vi.fn()
const mockCreateFile = vi.fn()
const mockUpdateFile = vi.fn()
const mockDeleteFile = vi.fn()

vi.mock("../../api/client", () => ({
  listUtilityFiles: (...args: any[]) => mockListFiles(...args),
  readUtilityFile: (...args: any[]) => mockReadFile(...args),
  createUtilityFile: (...args: any[]) => mockCreateFile(...args),
  updateUtilityFile: (...args: any[]) => mockUpdateFile(...args),
  deleteUtilityFile: (...args: any[]) => mockDeleteFile(...args),
}))

describe("UtilityPanel", () => {
  const defaultProps = {
    onClose: vi.fn(),
    onImportAdded: vi.fn(),
  }

  beforeEach(() => {
    vi.clearAllMocks()
    mockListFiles.mockResolvedValue({ files: [] })
  })

  afterEach(cleanup)

  it("renders header and close button", () => {
    render(<UtilityPanel {...defaultProps} />)
    expect(screen.getByText("Utility Scripts")).toBeInTheDocument()
    expect(screen.getByTitle("Close")).toBeInTheDocument()
  })

  it("close button calls onClose", () => {
    render(<UtilityPanel {...defaultProps} />)
    fireEvent.click(screen.getByTitle("Close"))
    expect(defaultProps.onClose).toHaveBeenCalledOnce()
  })

  it("shows empty state when no files", async () => {
    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByText("No utility files yet.")).toBeInTheDocument())
  })

  it("shows create button in empty state", async () => {
    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByText("Create one")).toBeInTheDocument())
  })

  it("loads files on mount", async () => {
    mockListFiles.mockResolvedValue({
      files: [{ name: "features.py", module: "features" }],
    })
    mockReadFile.mockResolvedValue({
      name: "features.py", module: "features", content: "x = 1\n",
    })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(mockListFiles).toHaveBeenCalledOnce())
  })

  it("auto-selects and loads first file", async () => {
    mockListFiles.mockResolvedValue({
      files: [{ name: "features.py", module: "features" }],
    })
    mockReadFile.mockResolvedValue({
      name: "features.py", module: "features", content: "def foo(): pass\n",
    })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(mockReadFile).toHaveBeenCalledWith("features"))
    expect(screen.getByTestId("code-editor")).toHaveValue("def foo(): pass\n")
  })

  it("shows module name in dropdown (no utility/ prefix or .py suffix)", async () => {
    mockListFiles.mockResolvedValue({
      files: [{ name: "features.py", module: "features" }],
    })
    mockReadFile.mockResolvedValue({
      name: "features.py", module: "features", content: "x = 1\n",
    })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => {
      expect(screen.getByText("features")).toBeInTheDocument()
      expect(screen.queryByText("utility/features.py")).toBeNull()
      expect(screen.queryByText("features.py")).toBeNull()
    })
  })

  it("does not show a Save button (auto-save mode)", async () => {
    mockListFiles.mockResolvedValue({
      files: [{ name: "features.py", module: "features" }],
    })
    mockReadFile.mockResolvedValue({
      name: "features.py", module: "features", content: "x = 1\n",
    })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByTestId("code-editor")).toBeInTheDocument())
    expect(screen.queryByText("Save")).toBeNull()
    expect(screen.queryByText("Unsaved changes")).toBeNull()
    expect(screen.queryByText("Saved")).toBeNull()
  })

  it("calls onImportAdded on create", async () => {
    mockListFiles.mockResolvedValue({ files: [] })
    mockCreateFile.mockResolvedValue({
      status: "ok", name: "helpers.py", module: "helpers",
      import_line: "from utility.helpers import *",
    })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByText("No utility files yet.")).toBeInTheDocument())

    fireEvent.click(screen.getByText("Create one"))

    const input = screen.getByPlaceholderText("module_name")
    fireEvent.change(input, { target: { value: "helpers" } })
    fireEvent.submit(input.closest("form")!)

    await waitFor(() => {
      expect(mockCreateFile).toHaveBeenCalledWith({ name: "helpers" })
      expect(defaultProps.onImportAdded).toHaveBeenCalledWith("from utility.helpers import *")
    })
  })
})

describe("UtilityPanel auto-save", () => {
  const defaultProps = {
    onClose: vi.fn(),
    onImportAdded: vi.fn(),
  }

  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers({ shouldAdvanceTime: true })
    mockListFiles.mockResolvedValue({
      files: [{ name: "features.py", module: "features" }],
    })
    mockReadFile.mockResolvedValue({
      name: "features.py", module: "features", content: "x = 1\n",
    })
  })

  afterEach(() => {
    // Advance timers to flush any pending debounced saves before cleanup
    vi.advanceTimersByTime(1000)
    // Unmount first (triggers clearTimeout in the component) while fake timers are still active
    cleanup()
    vi.useRealTimers()
  })

  it("auto-saves after debounce when code changes", async () => {
    mockUpdateFile.mockResolvedValue({ status: "ok", name: "features.py", module: "features", import_line: "" })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByTestId("code-editor")).toBeInTheDocument())

    fireEvent.change(screen.getByTestId("code-editor"), { target: { value: "x = 2\n" } })

    // Should not have saved yet (within debounce window)
    expect(mockUpdateFile).not.toHaveBeenCalled()

    // Advance past the 500ms debounce
    await act(async () => { vi.advanceTimersByTime(600) })

    await waitFor(() => expect(mockUpdateFile).toHaveBeenCalledWith("features", "x = 2\n"))
  })

  it("debounces rapid edits (only saves last value)", async () => {
    mockUpdateFile.mockResolvedValue({ status: "ok", name: "features.py", module: "features", import_line: "" })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByTestId("code-editor")).toBeInTheDocument())

    // Rapid edits
    fireEvent.change(screen.getByTestId("code-editor"), { target: { value: "x = 2\n" } })
    await act(async () => { vi.advanceTimersByTime(200) })
    fireEvent.change(screen.getByTestId("code-editor"), { target: { value: "x = 3\n" } })
    await act(async () => { vi.advanceTimersByTime(200) })
    fireEvent.change(screen.getByTestId("code-editor"), { target: { value: "x = 4\n" } })

    // Advance past debounce for the last edit
    await act(async () => { vi.advanceTimersByTime(600) })

    await waitFor(() => {
      expect(mockUpdateFile).toHaveBeenCalledTimes(1)
      expect(mockUpdateFile).toHaveBeenCalledWith("features", "x = 4\n")
    })
  })

  it("shows syntax error from auto-save", async () => {
    mockUpdateFile.mockResolvedValue({
      status: "error", name: "features.py", module: "features",
      error: "unexpected EOF", error_line: 3,
    })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByTestId("code-editor")).toBeInTheDocument())

    fireEvent.change(screen.getByTestId("code-editor"), { target: { value: "def foo(\n" } })
    await act(async () => { vi.advanceTimersByTime(600) })

    await waitFor(() => expect(screen.getByText("unexpected EOF")).toBeInTheDocument())
  })

  it("clears error when user edits again", async () => {
    mockUpdateFile
      .mockResolvedValueOnce({
        status: "error", name: "features.py", module: "features",
        error: "unexpected EOF", error_line: 3,
      })
      .mockResolvedValueOnce({
        status: "ok", name: "features.py", module: "features", import_line: "",
      })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByTestId("code-editor")).toBeInTheDocument())

    // Trigger error
    fireEvent.change(screen.getByTestId("code-editor"), { target: { value: "def foo(\n" } })
    await act(async () => { vi.advanceTimersByTime(600) })
    await waitFor(() => expect(screen.getByText("unexpected EOF")).toBeInTheDocument())

    // Edit again — error should clear immediately
    fireEvent.change(screen.getByTestId("code-editor"), { target: { value: "def foo(): pass\n" } })
    expect(screen.queryByText("unexpected EOF")).toBeNull()
  })

  it("shows error from failed auto-save", async () => {
    mockUpdateFile.mockResolvedValue({
      status: "error", name: "features.py", module: "features",
      error: "Save failed", error_line: 1,
    })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByTestId("code-editor")).toBeInTheDocument())

    fireEvent.change(screen.getByTestId("code-editor"), { target: { value: "x = 2\n" } })
    await act(async () => { vi.advanceTimersByTime(600) })

    await waitFor(() => expect(screen.getByText("Save failed")).toBeInTheDocument())
  })

  it("sets error-line attribute on editor when save returns error_line", async () => {
    mockUpdateFile.mockResolvedValue({
      status: "error", name: "features.py", module: "features",
      error: "syntax error", error_line: 5,
    })

    render(<UtilityPanel {...defaultProps} />)
    await waitFor(() => expect(screen.getByTestId("code-editor")).toBeInTheDocument())

    fireEvent.change(screen.getByTestId("code-editor"), { target: { value: "bad code\n" } })
    await act(async () => { vi.advanceTimersByTime(600) })

    await waitFor(() => {
      expect(screen.getByTestId("code-editor")).toHaveAttribute("data-error-line", "5")
    })
  })
})
