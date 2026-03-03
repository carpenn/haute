import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup, waitFor, act } from "@testing-library/react"
import { CodeEditor, FileBrowser, SchemaPreview } from "../_shared"
import type { SchemaInfo } from "../_shared"

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

vi.mock("../../../api/client", () => ({
  listFiles: vi.fn(),
}))

// Provide a minimal settings store with file-list cache helpers
const mockGetFileListCache = vi.fn<(key: string) => unknown[] | null>().mockReturnValue(null)
const mockSetFileListCache = vi.fn()

vi.mock("../../../stores/useSettingsStore", () => {
  const actual = { create: vi.fn() }
  const store = (selector: (s: Record<string, unknown>) => unknown) =>
    selector({
      getFileListCache: mockGetFileListCache,
      setFileListCache: mockSetFileListCache,
    })
  store.getState = () => ({
    getFileListCache: mockGetFileListCache,
    setFileListCache: mockSetFileListCache,
  })
  store.setState = vi.fn()
  store.subscribe = vi.fn()
  return {
    __esModule: true,
    default: store,
    useMlflowStatus: () => ({ mlflowStatus: "connected", mlflowBackend: "local" }),
  }
})

import { listFiles } from "../../../api/client"
const mockListFiles = listFiles as ReturnType<typeof vi.fn>

// jsdom does not implement document.execCommand; stub it so CodeEditor's
// insertText / replaceRange helpers work.
beforeEach(() => {
  document.execCommand = vi.fn((command: string, _showUI?: boolean, value?: string) => {
    // Simulate "insertText": replace current selection with `value`
    const ta = document.activeElement as HTMLTextAreaElement | null
    if (command === "insertText" && ta && "selectionStart" in ta) {
      const start = ta.selectionStart
      const end = ta.selectionEnd
      const before = ta.value.slice(0, start)
      const after = ta.value.slice(end)
      ta.value = before + (value ?? "") + after
      const newCursor = start + (value?.length ?? 0)
      ta.selectionStart = newCursor
      ta.selectionEnd = newCursor
      // Fire a synthetic input event so React sees the change
      ta.dispatchEvent(new Event("input", { bubbles: true }))
    }
    return true
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// CodeEditor
// ═══════════════════════════════════════════════════════════════════════════

describe("CodeEditor", () => {
  afterEach(cleanup)

  function renderEditor(defaultValue = "", onChange = vi.fn()) {
    const result = render(
      <CodeEditor defaultValue={defaultValue} onChange={onChange} />,
    )
    const textarea = result.container.querySelector("textarea") as HTMLTextAreaElement
    return { ...result, textarea, onChange }
  }

  // Helper to set cursor position in the textarea
  function setCursor(ta: HTMLTextAreaElement, pos: number) {
    ta.setSelectionRange(pos, pos)
  }

  function setSelection(ta: HTMLTextAreaElement, start: number, end: number) {
    ta.setSelectionRange(start, end)
  }

  // ── Tab ──────────────────────────────────────────────────────────────

  it("Tab inserts 4 spaces at cursor", () => {
    const { textarea } = renderEditor("hello")
    textarea.focus()
    setCursor(textarea, 5)
    fireEvent.keyDown(textarea, { key: "Tab" })
    expect(textarea.value).toBe("hello    ")
  })

  it("Tab indents selected lines", () => {
    const code = "line1\nline2\nline3"
    const { textarea } = renderEditor(code)
    textarea.focus()
    // Select "line1\nline2"
    setSelection(textarea, 0, 11)
    fireEvent.keyDown(textarea, { key: "Tab" })
    expect(textarea.value).toBe("    line1\n    line2\nline3")
  })

  it("Shift+Tab dedents selected lines", () => {
    const code = "    line1\n    line2\nline3"
    const { textarea } = renderEditor(code)
    textarea.focus()
    setSelection(textarea, 0, 19)
    fireEvent.keyDown(textarea, { key: "Tab", shiftKey: true })
    expect(textarea.value).toBe("line1\nline2\nline3")
  })

  it("Shift+Tab on single line removes 4 leading spaces when cursor is after indent", () => {
    const code = "    hello"
    const { textarea } = renderEditor(code)
    textarea.focus()
    // Cursor must be right after the 4 spaces (pos 4) so lineText ends with "    "
    setCursor(textarea, 4)
    fireEvent.keyDown(textarea, { key: "Tab", shiftKey: true })
    expect(textarea.value).toBe("hello")
  })

  // ── Bracket pair insertion ──────────────────────────────────────────

  it("typing ( inserts () and places cursor between", () => {
    const { textarea } = renderEditor("")
    textarea.focus()
    setCursor(textarea, 0)
    fireEvent.keyDown(textarea, { key: "(" })
    expect(textarea.value).toBe("()")
    expect(textarea.selectionStart).toBe(1)
  })

  it("typing [ inserts [] and places cursor between", () => {
    const { textarea } = renderEditor("")
    textarea.focus()
    setCursor(textarea, 0)
    fireEvent.keyDown(textarea, { key: "[" })
    expect(textarea.value).toBe("[]")
    expect(textarea.selectionStart).toBe(1)
  })

  it("typing { inserts {} and places cursor between", () => {
    const { textarea } = renderEditor("")
    textarea.focus()
    setCursor(textarea, 0)
    fireEvent.keyDown(textarea, { key: "{" })
    expect(textarea.value).toBe("{}")
    expect(textarea.selectionStart).toBe(1)
  })

  // ── Bracket skip ───────────────────────────────────────────────────

  it("typing ) when cursor is before ) skips instead of inserting", () => {
    const { textarea } = renderEditor("()")
    textarea.focus()
    setCursor(textarea, 1) // between ( and )
    fireEvent.keyDown(textarea, { key: ")" })
    expect(textarea.value).toBe("()") // no extra character
    expect(textarea.selectionStart).toBe(2)
  })

  it("typing ] when cursor is before ] skips", () => {
    const { textarea } = renderEditor("[]")
    textarea.focus()
    setCursor(textarea, 1)
    fireEvent.keyDown(textarea, { key: "]" })
    expect(textarea.value).toBe("[]")
    expect(textarea.selectionStart).toBe(2)
  })

  // ── Bracket delete ─────────────────────────────────────────────────

  it("Backspace removes matching pair when cursor is between empty brackets", () => {
    const { textarea } = renderEditor("()")
    textarea.focus()
    setCursor(textarea, 1) // between ( and )
    fireEvent.keyDown(textarea, { key: "Backspace" })
    expect(textarea.value).toBe("")
  })

  it("Backspace removes matching pair for {}", () => {
    const { textarea } = renderEditor("{}")
    textarea.focus()
    setCursor(textarea, 1)
    fireEvent.keyDown(textarea, { key: "Backspace" })
    expect(textarea.value).toBe("")
  })

  // ── Enter / auto-indent ────────────────────────────────────────────

  it("Enter auto-indents matching previous line indent", () => {
    const code = "    hello"
    const { textarea } = renderEditor(code)
    textarea.focus()
    setCursor(textarea, 9) // end of "    hello"
    fireEvent.keyDown(textarea, { key: "Enter" })
    expect(textarea.value).toBe("    hello\n    ")
  })

  it("Enter adds extra indent after colon", () => {
    const code = "def foo():"
    const { textarea } = renderEditor(code)
    textarea.focus()
    setCursor(textarea, 10) // end of "def foo():"
    fireEvent.keyDown(textarea, { key: "Enter" })
    expect(textarea.value).toBe("def foo():\n    ")
  })

  // ── Home key ────────────────────────────────────────────────────────

  it("Home goes to first non-whitespace character", () => {
    const code = "    hello"
    const { textarea } = renderEditor(code)
    textarea.focus()
    setCursor(textarea, 9) // end of "    hello"
    fireEvent.keyDown(textarea, { key: "Home" })
    expect(textarea.selectionStart).toBe(4)
  })

  // ── Ctrl+D (duplicate) ─────────────────────────────────────────────

  it("Ctrl+D duplicates current line", () => {
    const code = "line1\nline2"
    const { textarea } = renderEditor(code)
    textarea.focus()
    setCursor(textarea, 2) // somewhere in "line1"
    fireEvent.keyDown(textarea, { key: "d", ctrlKey: true })
    expect(textarea.value).toBe("line1\nline1\nline2")
  })

  // ── Ctrl+/ (toggle comment) ────────────────────────────────────────

  it("Ctrl+/ comments out a line", () => {
    const code = "hello"
    const { textarea } = renderEditor(code)
    textarea.focus()
    setCursor(textarea, 2)
    fireEvent.keyDown(textarea, { key: "/", ctrlKey: true })
    expect(textarea.value).toBe("# hello")
  })

  it("Ctrl+/ uncomments a commented line", () => {
    const code = "# hello"
    const { textarea } = renderEditor(code)
    textarea.focus()
    setCursor(textarea, 3)
    fireEvent.keyDown(textarea, { key: "/", ctrlKey: true })
    expect(textarea.value).toBe("hello")
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// FileBrowser
// ═══════════════════════════════════════════════════════════════════════════

describe("FileBrowser", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockGetFileListCache.mockReturnValue(null)
  })

  afterEach(cleanup)

  it("shows loading spinner initially then renders file list", async () => {
    mockListFiles.mockResolvedValue({
      items: [
        { name: "data.csv", path: "data.csv", type: "file", size: 2048 },
        { name: "subdir", path: "subdir", type: "directory" },
      ],
    })
    render(<FileBrowser onSelect={vi.fn()} />)
    expect(screen.getByText("Loading...")).toBeInTheDocument()
    await waitFor(() => {
      expect(screen.getByText("data.csv")).toBeInTheDocument()
    })
    expect(screen.getByText("subdir")).toBeInTheDocument()
  })

  it("uses cache on second load", async () => {
    const cached = [
      { name: "cached.csv", path: "cached.csv", type: "file" as const },
    ]
    mockGetFileListCache.mockReturnValue(cached)
    render(<FileBrowser onSelect={vi.fn()} />)
    // Should use cache, not call listFiles
    expect(mockListFiles).not.toHaveBeenCalled()
    expect(screen.getByText("cached.csv")).toBeInTheDocument()
  })

  it("clicking a directory navigates into it", async () => {
    mockListFiles.mockResolvedValueOnce({
      items: [
        { name: "subdir", path: "subdir", type: "directory" },
      ],
    })
    mockListFiles.mockResolvedValueOnce({
      items: [
        { name: "nested.csv", path: "subdir/nested.csv", type: "file" },
      ],
    })
    render(<FileBrowser onSelect={vi.fn()} />)
    await waitFor(() => {
      expect(screen.getByText("subdir")).toBeInTheDocument()
    })
    fireEvent.click(screen.getByText("subdir"))
    await waitFor(() => {
      expect(screen.getByText("nested.csv")).toBeInTheDocument()
    })
  })

  it("shows error state when API call fails", async () => {
    mockListFiles.mockRejectedValue(new Error("Network error"))
    render(<FileBrowser onSelect={vi.fn()} />)
    await waitFor(() => {
      expect(screen.getByText("Network error")).toBeInTheDocument()
    })
  })

  it("shows empty directory message", async () => {
    mockListFiles.mockResolvedValue({ items: [] })
    render(<FileBrowser onSelect={vi.fn()} />)
    await waitFor(() => {
      expect(screen.getByText("No matching files")).toBeInTheDocument()
    })
  })

  it("clicking a file calls onSelect with its path", async () => {
    const onSelect = vi.fn()
    mockListFiles.mockResolvedValue({
      items: [
        { name: "data.csv", path: "data/data.csv", type: "file" },
      ],
    })
    render(<FileBrowser onSelect={onSelect} />)
    await waitFor(() => {
      expect(screen.getByText("data.csv")).toBeInTheDocument()
    })
    fireEvent.click(screen.getByText("data.csv"))
    expect(onSelect).toHaveBeenCalledWith("data/data.csv")
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// SchemaPreview
// ═══════════════════════════════════════════════════════════════════════════

describe("SchemaPreview", () => {
  afterEach(cleanup)

  const sampleSchema: SchemaInfo = {
    path: "data.csv",
    columns: [
      { name: "id", dtype: "Int64" },
      { name: "name", dtype: "Utf8" },
    ],
    row_count: 100,
    column_count: 2,
    preview: [
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ],
  }

  it("renders column names and types from schema", () => {
    render(<SchemaPreview schema={sampleSchema} />)
    expect(screen.getByText("id")).toBeInTheDocument()
    expect(screen.getByText("name")).toBeInTheDocument()
    expect(screen.getByText("Int64")).toBeInTheDocument()
    expect(screen.getByText("Utf8")).toBeInTheDocument()
  })

  it("shows row and column counts", () => {
    render(<SchemaPreview schema={sampleSchema} />)
    expect(screen.getByText("2 cols / 100 rows")).toBeInTheDocument()
  })

  it("toggles preview table on button click", () => {
    render(<SchemaPreview schema={sampleSchema} />)
    expect(screen.getByText("Show preview")).toBeInTheDocument()
    expect(screen.queryByText("Alice")).not.toBeInTheDocument()
    fireEvent.click(screen.getByText("Show preview"))
    expect(screen.getByText("Hide preview")).toBeInTheDocument()
    expect(screen.getByText("Alice")).toBeInTheDocument()
    expect(screen.getByText("Bob")).toBeInTheDocument()
  })

  it("renders nothing when schema is null", () => {
    const { container } = render(<SchemaPreview schema={null} />)
    expect(container.innerHTML).toBe("")
  })
})
