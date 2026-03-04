/**
 * Tests for App.tsx — the main orchestrator component.
 *
 * Strategy: mock every hook and sub-component to lightweight stubs so we can
 * test orchestration logic, conditional rendering, and UI-store-driven
 * visibility without pulling in the full dependency tree.
 */
import { describe, it, expect, vi, afterEach, beforeEach } from "vitest"
import { render, screen, cleanup, fireEvent, act } from "@testing-library/react"

// ---------------------------------------------------------------------------
// Mock ReactFlow entirely
// ---------------------------------------------------------------------------

vi.mock("@xyflow/react", () => ({
  ReactFlow: ({ children, ...props }: any) => (
    <div data-testid="react-flow" {...(props.onPaneClick ? { onClick: props.onPaneClick } : {})}>
      {children}
    </div>
  ),
  ReactFlowProvider: ({ children }: any) => <div>{children}</div>,
  Background: () => null,
  useReactFlow: () => ({
    screenToFlowPosition: vi.fn(() => ({ x: 0, y: 0 })),
    fitView: vi.fn(),
  }),
  SelectionMode: { Partial: 0 },
  BackgroundVariant: { Dots: "dots" },
  MarkerType: { ArrowClosed: "arrowclosed" },
}))

// ---------------------------------------------------------------------------
// Mock all hooks to return stable defaults
// ---------------------------------------------------------------------------

const mockSetNodes = vi.fn()
const mockSetEdges = vi.fn()
let mockLoading = false

vi.mock("../hooks/useUndoRedo", () => ({
  default: () => ({
    nodes: [],
    edges: [],
    setNodes: mockSetNodes,
    setEdges: mockSetEdges,
    setNodesRaw: vi.fn(),
    setEdgesRaw: vi.fn(),
    onNodesChange: vi.fn(),
    onEdgesChange: vi.fn(),
    undo: vi.fn(),
    redo: vi.fn(),
    canUndo: false,
    canRedo: false,
  }),
}))

vi.mock("../hooks/useWebSocketSync", () => ({
  default: () => "connected",
}))

vi.mock("../hooks/usePipelineAPI", () => ({
  default: () => ({
    loading: mockLoading,
    previewData: null,
    setPreviewData: vi.fn(),
    nodeStatuses: {},
    fetchPreview: vi.fn(),
    handleSave: vi.fn(),
  }),
}))

vi.mock("../hooks/useTracing", () => ({
  default: () => ({
    traceResult: null,
    tracedCell: null,
    handleCellClick: vi.fn(),
    clearTrace: vi.fn(),
    nodesWithStatus: [],
    edgesWithTrace: [],
  }),
}))

vi.mock("../hooks/useSubmodelNavigation", () => ({
  default: () => ({
    viewStack: [{ name: "main" }],
    handleDrillIntoSubmodel: vi.fn(),
    handleBreadcrumbNavigate: vi.fn(),
    handleCreateSubmodel: vi.fn(),
    handleDissolveSubmodel: vi.fn(),
  }),
}))

vi.mock("../hooks/useKeyboardShortcuts", () => ({
  default: vi.fn(),
}))

vi.mock("../hooks/useBackgroundJobs", () => ({
  default: vi.fn(),
}))

vi.mock("../hooks/useNodeHandlers", () => ({
  default: () => ({
    handleDeleteNode: vi.fn(),
    handleDuplicateNode: vi.fn(),
    handleCreateInstance: vi.fn(),
    handleRenameNode: vi.fn(),
    handleAutoLayout: vi.fn(),
  }),
}))

vi.mock("../hooks/useEdgeHandlers", () => ({
  default: () => ({
    onConnect: vi.fn(),
    onSelectionChange: vi.fn(),
    handleDeleteEdge: vi.fn(),
    onNodeContextMenu: vi.fn(),
    onDragOver: vi.fn(),
    onDrop: vi.fn(),
  }),
}))

// ---------------------------------------------------------------------------
// Mock all sub-components to lightweight stubs
// ---------------------------------------------------------------------------

vi.mock("../nodes/PipelineNode", () => ({ default: () => null }))
vi.mock("../nodes/SubmodelNode", () => ({ default: () => null }))
vi.mock("../nodes/SubmodelPortNode", () => ({ default: () => null }))

vi.mock("../panels/NodePalette", () => ({
  default: ({ onCollapse }: { onCollapse: () => void }) => (
    <div data-testid="node-palette">
      <button onClick={onCollapse}>Collapse</button>
    </div>
  ),
}))

vi.mock("../panels/NodePanel", () => ({
  default: () => <div data-testid="node-panel" />,
}))

vi.mock("../panels/DataPreview", () => ({
  default: () => <div data-testid="data-preview" />,
}))

vi.mock("../panels/OptimiserPreview", () => ({
  default: () => <div data-testid="optimiser-preview" />,
}))

vi.mock("../panels/TracePanel", () => ({
  default: () => <div data-testid="trace-panel" />,
}))

vi.mock("../components/Toast", () => ({
  default: () => <div data-testid="toast" />,
}))

vi.mock("../components/ContextMenu", () => ({
  default: () => <div data-testid="context-menu" />,
}))

vi.mock("../components/KeyboardShortcuts", () => ({
  default: ({ onClose }: { onClose: () => void }) => (
    <div data-testid="shortcuts">
      <button onClick={onClose}>Close</button>
    </div>
  ),
}))

vi.mock("../components/BreadcrumbBar", () => ({
  default: () => <div data-testid="breadcrumb-bar" />,
}))

vi.mock("../components/Toolbar", () => ({
  default: (props: any) => (
    <div data-testid="toolbar">
      <button data-testid="settings-btn" onClick={props.onOpenSettings}>
        Settings
      </button>
      <button data-testid="shortcuts-btn" onClick={props.onShowShortcuts}>
        Shortcuts
      </button>
    </div>
  ),
}))

vi.mock("../components/SettingsModal", () => ({
  default: ({ onClose }: { onClose: () => void }) => (
    <div data-testid="settings-modal">
      <button onClick={onClose}>Close</button>
    </div>
  ),
}))

vi.mock("../components/SubmodelDialog", () => ({
  default: () => <div data-testid="submodel-dialog" />,
}))

vi.mock("../components/ErrorBoundary", () => ({
  ErrorBoundary: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

// Mock the API client so the settings store fetchMlflow doesn't make real requests
vi.mock("../api/client", () => ({
  checkMlflow: vi.fn(() => Promise.resolve({ mlflow_installed: false })),
}))

// ---------------------------------------------------------------------------
// Imports (AFTER mocks are declared)
// ---------------------------------------------------------------------------

import App from "../App"
import useUIStore from "../stores/useUIStore"
import useSettingsStore from "../stores/useSettingsStore"

// ---------------------------------------------------------------------------
// Setup / teardown
// ---------------------------------------------------------------------------

afterEach(cleanup)

beforeEach(() => {
  mockLoading = false

  // Reset UI store to known defaults
  useUIStore.setState({
    paletteOpen: true,
    settingsOpen: false,
    shortcutsOpen: false,
    submodelDialog: null,
    snapToGrid: false,
    syncBanner: null,
    dirty: false,
  })

  // Reset settings store MLflow state so fetchMlflow runs cleanly
  useSettingsStore.setState({
    mlflow: { status: "pending", backend: "", host: "" },
    _mlflowFetching: false,
    _mlflowLastAttempt: 0,
  })
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("App", () => {
  // ── Basic rendering ─────────────────────────────────────────────

  it("renders without crashing", () => {
    const { container } = render(<App />)
    expect(container.innerHTML.length).toBeGreaterThan(0)
  })

  it("shows loading state when loading is true", () => {
    mockLoading = true
    render(<App />)
    expect(screen.getByText("Loading pipeline...")).toBeTruthy()
  })

  it("does not show Toolbar in loading state", () => {
    mockLoading = true
    render(<App />)
    expect(screen.queryByTestId("toolbar")).toBeNull()
  })

  it("shows Toolbar when not loading", () => {
    render(<App />)
    expect(screen.getByTestId("toolbar")).toBeTruthy()
  })

  it("shows ReactFlow canvas when not loading", () => {
    render(<App />)
    expect(screen.getByTestId("react-flow")).toBeTruthy()
  })

  it("shows BreadcrumbBar", () => {
    render(<App />)
    expect(screen.getByTestId("breadcrumb-bar")).toBeTruthy()
  })

  it("shows DataPreview by default (no trace or optimiser preview)", () => {
    render(<App />)
    expect(screen.getByTestId("data-preview")).toBeTruthy()
  })

  it("shows NodePanel when no trace result", () => {
    render(<App />)
    expect(screen.getByTestId("node-panel")).toBeTruthy()
  })

  it("shows Toast container", () => {
    render(<App />)
    expect(screen.getByTestId("toast")).toBeTruthy()
  })

  // ── Node palette ────────────────────────────────────────────────

  it("shows NodePalette when paletteOpen is true", () => {
    render(<App />)
    expect(screen.getByTestId("node-palette")).toBeTruthy()
  })

  it("shows palette toggle button when paletteOpen is false", () => {
    useUIStore.setState({ paletteOpen: false })
    render(<App />)
    expect(screen.queryByTestId("node-palette")).toBeNull()
    expect(screen.getByRole("button", { name: "Show node palette" })).toBeTruthy()
  })

  it("clicking palette toggle opens palette", () => {
    useUIStore.setState({ paletteOpen: false })
    render(<App />)
    fireEvent.click(screen.getByRole("button", { name: "Show node palette" }))
    expect(useUIStore.getState().paletteOpen).toBe(true)
  })

  // ── Modals ──────────────────────────────────────────────────────

  it("SettingsModal not shown by default", () => {
    render(<App />)
    expect(screen.queryByTestId("settings-modal")).toBeNull()
  })

  it("clicking settings button in Toolbar opens SettingsModal", () => {
    render(<App />)
    fireEvent.click(screen.getByTestId("settings-btn"))
    expect(screen.getByTestId("settings-modal")).toBeTruthy()
  })

  it("KeyboardShortcuts not shown by default", () => {
    render(<App />)
    expect(screen.queryByTestId("shortcuts")).toBeNull()
  })

  it("clicking shortcuts button opens shortcuts panel", () => {
    render(<App />)
    fireEvent.click(screen.getByTestId("shortcuts-btn"))
    expect(screen.getByTestId("shortcuts")).toBeTruthy()
  })

  // ── Sync banner ─────────────────────────────────────────────────

  it("no sync banner by default", () => {
    render(<App />)
    expect(screen.queryByText("File changed on disk")).toBeNull()
  })

  it("sync banner shown when syncBanner has text", () => {
    useUIStore.setState({ syncBanner: "File changed on disk" })
    render(<App />)
    expect(screen.getByText("File changed on disk")).toBeTruthy()
  })

  it("dismiss button clears sync banner", () => {
    useUIStore.setState({ syncBanner: "Outdated pipeline" })
    render(<App />)

    // The dismiss button is the one with the X character
    const dismissBtn = screen.getByText("\u2715")
    fireEvent.click(dismissBtn)
    expect(useUIStore.getState().syncBanner).toBeNull()
  })

  // ── SubmodelDialog ──────────────────────────────────────────────

  it("SubmodelDialog not shown when submodelDialog is null", () => {
    render(<App />)
    expect(screen.queryByTestId("submodel-dialog")).toBeNull()
  })

  it("SubmodelDialog shown when submodelDialog is set in store", () => {
    useUIStore.setState({ submodelDialog: { nodeIds: ["n1", "n2"] } })
    render(<App />)
    expect(screen.getByTestId("submodel-dialog")).toBeTruthy()
  })
})
