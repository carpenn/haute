/**
 * Render tests for DataSourceEditor.
 *
 * Tests: source type toggle, flat file / Databricks mode rendering,
 * FileBrowser display, SchemaPreview display.
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
  config: {},
  onUpdate: vi.fn(),
}

describe("DataSourceEditor", () => {
  it("renders Source Type label", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Source Type")).toBeTruthy()
  })

  it("shows both source type toggle buttons", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    expect(screen.getByText("Flat File")).toBeTruthy()
    expect(screen.getByText("Databricks")).toBeTruthy()
  })

  it("defaults to flat_file mode (flat file button styled as active)", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    const flatFileBtn = screen.getByText("Flat File").closest("button")!
    // Active button has accent border
    expect(flatFileBtn.style.border).toContain("var(--accent)")
  })

  it("shows FileBrowser in flat_file mode", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    expect(screen.getByTestId("file-browser")).toBeTruthy()
  })

  it("switching to Databricks mode calls onUpdate with sourceType databricks", () => {
    const onUpdate = vi.fn()
    render(<DataSourceEditor {...DEFAULT_PROPS} onUpdate={onUpdate} />)
    fireEvent.click(screen.getByText("Databricks"))
    expect(onUpdate).toHaveBeenCalledWith("sourceType", "databricks")
  })

  it("shows Databricks controls when sourceType is databricks", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} config={{ sourceType: "databricks" }} />)
    expect(screen.getByTestId("warehouse-picker")).toBeTruthy()
    expect(screen.getByTestId("catalog-picker")).toBeTruthy()
    expect(screen.getByText("SQL Query")).toBeTruthy()
  })

  it("switching back to flat_file mode calls onUpdate with sourceType flat_file", () => {
    const onUpdate = vi.fn()
    render(<DataSourceEditor {...DEFAULT_PROPS} config={{ sourceType: "databricks" }} onUpdate={onUpdate} />)
    fireEvent.click(screen.getByText("Flat File"))
    expect(onUpdate).toHaveBeenCalledWith("sourceType", "flat_file")
  })

  it("shows SchemaPreview component", () => {
    render(<DataSourceEditor {...DEFAULT_PROPS} />)
    expect(screen.getByTestId("schema-preview")).toBeTruthy()
  })
})
