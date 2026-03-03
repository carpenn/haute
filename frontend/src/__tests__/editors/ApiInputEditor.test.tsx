/**
 * Render tests for ApiInputEditor.
 *
 * Tests: API banner, preview data label, FileBrowser, row ID column
 * handling, SchemaPreview, and JSON cache button visibility.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import ApiInputEditor from "../../panels/editors/ApiInputEditor"

afterEach(cleanup)

// Mock the shared components that make API calls
vi.mock("../../panels/editors/_shared", async () => {
  const actual = await vi.importActual("../../panels/editors/_shared")
  return {
    ...actual,
    FileBrowser: ({ currentPath, onSelect }: { currentPath?: string; onSelect: (path: string) => void }) => (
      <div data-testid="file-browser">
        <span data-testid="current-path">{currentPath || ""}</span>
        <button data-testid="select-file" onClick={() => onSelect("test.parquet")}>Select</button>
      </div>
    ),
    SchemaPreview: ({ schema }: { schema: unknown }) => (
      <div data-testid="schema-preview">{schema ? "Schema loaded" : "No schema"}</div>
    ),
  }
})

vi.mock("../../hooks/useSchemaFetch", () => ({
  useSchemaFetch: (initialPath?: string) => ({
    schema: initialPath ? { columns: [{ name: "col1", dtype: "Int64" }, { name: "col2", dtype: "String" }], preview: [], row_count: 10 } : null,
    setSchema: vi.fn(),
    loading: false,
    fetchForPath: vi.fn(),
  }),
}))

vi.mock("../../api/client", () => ({
  fetchDatabricksSchema: vi.fn(),
  buildJsonCache: vi.fn().mockResolvedValue({}),
  getJsonCacheProgress: vi.fn().mockResolvedValue({ active: false }),
  getJsonCacheStatus: vi.fn().mockResolvedValue({ cached: false }),
  deleteJsonCache: vi.fn().mockResolvedValue({ cached: false }),
  ApiError: class ApiError extends Error {
    status: number;
    detail?: string;
    constructor(message: string, status: number, detail?: string) {
      super(message); this.status = status; this.detail = detail;
    }
  },
}))

const DEFAULT_PROPS = {
  config: {},
  onUpdate: vi.fn(),
}

describe("ApiInputEditor", () => {
  it("renders live API requests banner text", () => {
    render(<ApiInputEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("This node receives live API requests at deploy time")).toBeTruthy()
  })

  it("renders Preview Data label", () => {
    render(<ApiInputEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Preview Data")).toBeTruthy()
  })

  it("renders FileBrowser for data selection", () => {
    render(<ApiInputEditor {...DEFAULT_PROPS} />)
    expect(screen.getByTestId("file-browser")).toBeTruthy()
  })

  it("shows Row ID Column label", () => {
    render(<ApiInputEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Row ID Column")).toBeTruthy()
  })

  it("shows Required for tracing warning when no row_id_column set", () => {
    render(<ApiInputEditor {...DEFAULT_PROPS} config={{}} />)
    expect(screen.getByText("Required for tracing")).toBeTruthy()
  })

  it("does NOT show Required for tracing warning when row_id_column is set", () => {
    render(<ApiInputEditor {...DEFAULT_PROPS} config={{ row_id_column: "id" }} />)
    expect(screen.queryByText("Required for tracing")).toBeNull()
  })

  it("renders column options from schema when path is set", () => {
    render(<ApiInputEditor {...DEFAULT_PROPS} config={{ path: "data/input.json" }} />)
    const select = screen.getByRole("combobox") as HTMLSelectElement
    const optionTexts = Array.from(select.options).map(o => o.text)
    expect(optionTexts).toContain("col1 (Int64)")
    expect(optionTexts).toContain("col2 (String)")
  })

  it("calls onUpdate when selecting a row_id_column", () => {
    const onUpdate = vi.fn()
    render(<ApiInputEditor {...DEFAULT_PROPS} config={{ path: "data/input.json" }} onUpdate={onUpdate} />)
    const select = screen.getByRole("combobox")
    fireEvent.change(select, { target: { value: "col1" } })
    expect(onUpdate).toHaveBeenCalledWith("row_id_column", "col1")
  })

  it("shows SchemaPreview component", () => {
    render(<ApiInputEditor {...DEFAULT_PROPS} />)
    expect(screen.getByTestId("schema-preview")).toBeTruthy()
  })

  it("shows Cache as Parquet button when path ends with .json", () => {
    render(<ApiInputEditor {...DEFAULT_PROPS} config={{ path: "data/input.json" }} />)
    expect(screen.getByText("Cache as Parquet")).toBeTruthy()
  })
})
