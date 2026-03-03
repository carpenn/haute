/**
 * Render tests for DataSourceEditor.
 *
 * Tests: source type toggle, flat file / Databricks mode rendering,
 * FileBrowser display, SchemaPreview display, loading schema text,
 * source type switching calls onUpdate.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import DataSourceEditor from "../../panels/editors/DataSourceEditor"

afterEach(cleanup)

// Mock the shared components that make API calls
vi.mock("../../panels/editors/_shared", async () => {
  const actual = await vi.importActual("../../panels/editors/_shared")
  return {
    ...actual,
    FileBrowser: ({ currentPath, onSelect, extensions }: { currentPath?: string; onSelect: (path: string) => void; extensions?: string }) => (
      <div data-testid="file-browser">
        <span data-testid="current-path">{currentPath || ""}</span>
        <span data-testid="extensions">{extensions || ""}</span>
        <button data-testid="select-file" onClick={() => onSelect("test.parquet")}>Select</button>
      </div>
    ),
    SchemaPreview: ({ schema }: { schema: unknown }) => (
      <div data-testid="schema-preview">{schema ? "Schema loaded" : "No schema"}</div>
    ),
  }
})

const mockFetchForPath = vi.fn()
const mockSetSchema = vi.fn()
const mockGetLoading = vi.fn(() => false)

vi.mock("../../hooks/useSchemaFetch", () => ({
  useSchemaFetch: (initialPath?: string) => ({
    schema: initialPath ? { columns: [{ name: "col1", dtype: "Int64" }, { name: "col2", dtype: "String" }], preview: [], row_count: 10 } : null,
    setSchema: mockSetSchema,
    loading: mockGetLoading(),
    fetchForPath: mockFetchForPath,
  }),
}))

vi.mock("../../panels/editors/_DatabricksSelector", () => ({
  WarehousePicker: ({ httpPath }: { httpPath: string; onSelect: (v: string) => void }) => (
    <div data-testid="warehouse-picker">{httpPath}</div>
  ),
  CatalogTablePicker: ({ table }: { table: string; onSelect: (v: string) => void }) => (
    <div data-testid="catalog-picker">{table}</div>
  ),
  DatabricksFetchButton: ({ onFetched }: { onFetched: () => void }) => (
    <button data-testid="fetch-btn" onClick={onFetched}>Fetch</button>
  ),
}))

vi.mock("../../api/client", () => ({
  fetchDatabricksSchema: vi.fn().mockResolvedValue({ columns: [], preview: [], row_count: 0 }),
}))

const DEFAULT_PROPS = {
  config: {} as Record<string, unknown>,
  onUpdate: vi.fn(),
}

describe("DataSourceEditor", () => {
  it("renders flat_file tab selected by default", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    const flatFileBtn = screen.getByText("Flat File").closest("button")!
    // Active button has accent border
    expect(flatFileBtn.style.border).toContain("var(--accent)")
    // Databricks button should not have accent border
    const dbBtn = screen.getByText("Databricks").closest("button")!
    expect(dbBtn.style.border).not.toContain("var(--accent)")
  })

  it("shows FileBrowser in flat_file mode", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    expect(screen.getByTestId("file-browser")).toBeTruthy()
    // Should NOT show Databricks controls
    expect(screen.queryByTestId("warehouse-picker")).toBeNull()
    expect(screen.queryByTestId("catalog-picker")).toBeNull()
  })

  it("switching to databricks shows Databricks controls", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    fireEvent.click(screen.getByText("Databricks"))
    expect(screen.getByTestId("warehouse-picker")).toBeTruthy()
    expect(screen.getByTestId("catalog-picker")).toBeTruthy()
    expect(screen.getByText("SQL Query")).toBeTruthy()
    expect(screen.getByTestId("fetch-btn")).toBeTruthy()
    // FileBrowser should be hidden
    expect(screen.queryByTestId("file-browser")).toBeNull()
  })

  it("switching source type calls onUpdate with sourceType", () => {
    const onUpdate = vi.fn()
    render(<DataSourceEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)

    // Switch to Databricks
    fireEvent.click(screen.getByText("Databricks"))
    expect(onUpdate).toHaveBeenCalledWith("sourceType", "databricks")

    // Switch back to Flat File
    fireEvent.click(screen.getByText("Flat File"))
    expect(onUpdate).toHaveBeenCalledWith("sourceType", "flat_file")
  })

  it("shows Databricks controls when config has sourceType databricks", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} config={{ sourceType: "databricks" }} />)
    expect(screen.getByTestId("warehouse-picker")).toBeTruthy()
    expect(screen.getByTestId("catalog-picker")).toBeTruthy()
    expect(screen.getByText("SQL Query")).toBeTruthy()
  })

  it("loading schema shows 'Loading schema...' text", () => {
    mockGetLoading.mockReturnValueOnce(true)
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Loading schema...")).toBeTruthy()
  })

  it("shows SchemaPreview component", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    expect(screen.getByTestId("schema-preview")).toBeTruthy()
  })

  it("selecting a file calls onUpdate and fetchForPath", () => {
    const onUpdate = vi.fn()
    mockFetchForPath.mockClear()
    render(<DataSourceEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)

    // Click the select button in mocked FileBrowser
    fireEvent.click(screen.getByTestId("select-file"))
    expect(onUpdate).toHaveBeenCalledWith("path", "test.parquet")
    expect(mockFetchForPath).toHaveBeenCalledWith("test.parquet")
  })

  it("renders both source type toggle buttons", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Source Type")).toBeTruthy()
    expect(screen.getByText("Flat File")).toBeTruthy()
    expect(screen.getByText("Databricks")).toBeTruthy()
  })

  it("passes current path to FileBrowser from config", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} config={{ path: "data/input.parquet" }} />)
    expect(screen.getByTestId("current-path").textContent).toBe("data/input.parquet")
  })

  it("renders SQL query textarea in databricks mode", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} config={{ sourceType: "databricks" }} />)
    const textarea = screen.getByPlaceholderText(/SELECT \*/)
    expect(textarea).toBeTruthy()
    expect(textarea.tagName).toBe("TEXTAREA")
  })

  it("shows query helper text in databricks mode", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} config={{ sourceType: "databricks" }} />)
    expect(screen.getByText("Combined with table above as: query FROM table")).toBeTruthy()
  })
})
