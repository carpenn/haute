/**
 * Tests for useUIStore — toasts, MLflow dedup, file list cache,
 * collapsible sections, and node panel width.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"

// Mock the API module BEFORE importing the store
vi.mock("../../api/client.ts", () => ({
  checkMlflow: vi.fn(),
}))

import useUIStore from "../../stores/useUIStore.ts"
import { checkMlflow } from "../../api/client.ts"

// ── Helpers ──────────────────────────────────────────────────────

function resetStore() {
  useUIStore.setState({
    toasts: [],
    _toastCounter: 0,
    paletteOpen: true,
    settingsOpen: false,
    shortcutsOpen: false,
    submodelDialog: null,
    snapToGrid: false,
    rowLimit: 1000,
    syncBanner: null,
    dirty: false,
    nodePanelWidth: 400,
    collapsedSections: {},
    mlflow: { status: "pending", backend: "", host: "" },
    _mlflowFetching: false,
    _mlflowLastAttempt: 0,
    fileListCache: {},
  })
}

// ── Test suites ──────────────────────────────────────────────────

describe("useUIStore", () => {
  beforeEach(() => {
    resetStore()
    vi.clearAllMocks()
  })

  // ────────────────────────────────────────────────────────────────
  // Toast lifecycle
  // ────────────────────────────────────────────────────────────────

  describe("toast lifecycle", () => {
    it("addToast creates a toast with incrementing IDs", () => {
      const s = useUIStore.getState()
      s.addToast("success", "First toast")
      s.addToast("error", "Second toast")

      const { toasts } = useUIStore.getState()
      expect(toasts).toHaveLength(2)
      expect(toasts[0].id).toBe("1")
      expect(toasts[0].type).toBe("success")
      expect(toasts[0].text).toBe("First toast")
      expect(toasts[1].id).toBe("2")
      expect(toasts[1].type).toBe("error")
      expect(toasts[1].text).toBe("Second toast")
    })

    it("dismissToast removes the specified toast", () => {
      const s = useUIStore.getState()
      s.addToast("success", "Keep me")
      s.addToast("error", "Remove me")
      s.addToast("info", "Also keep")

      s.dismissToast("2")

      const { toasts } = useUIStore.getState()
      expect(toasts).toHaveLength(2)
      expect(toasts.map((t) => t.id)).toEqual(["1", "3"])
    })

    it("dismissToast is safe when ID does not exist", () => {
      const s = useUIStore.getState()
      s.addToast("success", "One toast")
      s.dismissToast("999")

      expect(useUIStore.getState().toasts).toHaveLength(1)
    })

    it("counter increments monotonically even after dismissals", () => {
      const s = useUIStore.getState()
      s.addToast("info", "A")
      s.addToast("info", "B")
      s.dismissToast("1")
      s.addToast("info", "C")

      const { toasts, _toastCounter } = useUIStore.getState()
      expect(_toastCounter).toBe(3)
      expect(toasts[toasts.length - 1].id).toBe("3")
    })
  })

  // ────────────────────────────────────────────────────────────────
  // MLflow dedup
  // ────────────────────────────────────────────────────────────────

  describe("MLflow fetch dedup", () => {
    it("calling fetchMlflow twice rapidly only makes one API request", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      let resolvePromise: (value: { mlflow_installed: boolean }) => void
      const promise = new Promise<{ mlflow_installed: boolean }>((resolve) => {
        resolvePromise = resolve
      })
      mockCheckMlflow.mockReturnValue(promise)

      const s = useUIStore.getState()
      s.fetchMlflow()
      s.fetchMlflow() // second call should be deduped

      expect(mockCheckMlflow).toHaveBeenCalledTimes(1)

      // Resolve the promise to clean up
      resolvePromise!({ mlflow_installed: true })
      // Wait for microtask to flush the .then()
      await vi.waitFor(() => {
        expect(useUIStore.getState()._mlflowFetching).toBe(false)
      })
    })

    it("successful MLflow check sets connected status", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      mockCheckMlflow.mockResolvedValue({
        mlflow_installed: true,
        backend: "databricks",
        databricks_host: "https://db.example.com",
      })

      useUIStore.getState().fetchMlflow()

      await vi.waitFor(() => {
        expect(useUIStore.getState().mlflow.status).toBe("connected")
      })

      const { mlflow } = useUIStore.getState()
      expect(mlflow.backend).toBe("databricks")
      expect(mlflow.host).toBe("https://db.example.com")
    })

    it("failed MLflow check sets error status", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      mockCheckMlflow.mockRejectedValue(new Error("Network error"))

      useUIStore.getState().fetchMlflow()

      await vi.waitFor(() => {
        expect(useUIStore.getState().mlflow.status).toBe("error")
      })
    })

    it("does not re-fetch after successful connection", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      mockCheckMlflow.mockResolvedValue({ mlflow_installed: true, backend: "local" })

      useUIStore.getState().fetchMlflow()
      await vi.waitFor(() => {
        expect(useUIStore.getState().mlflow.status).toBe("connected")
      })

      // Second call should be a no-op since status is "connected"
      mockCheckMlflow.mockClear()
      useUIStore.getState().fetchMlflow()
      expect(mockCheckMlflow).not.toHaveBeenCalled()
    })

    it("mlflow_installed: false sets error status", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      mockCheckMlflow.mockResolvedValue({ mlflow_installed: false })

      useUIStore.getState().fetchMlflow()

      await vi.waitFor(() => {
        expect(useUIStore.getState().mlflow.status).toBe("error")
      })
    })
  })

  // ────────────────────────────────────────────────────────────────
  // File list cache
  // ────────────────────────────────────────────────────────────────

  describe("file list cache", () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    it("setFileListCache then getFileListCache returns items within TTL", () => {
      const items = [
        { name: "data.csv", path: "/data/data.csv", type: "file" as const },
        { name: "models", path: "/data/models", type: "directory" as const },
      ]

      useUIStore.getState().setFileListCache("dir|csv", items)

      const result = useUIStore.getState().getFileListCache("dir|csv")
      expect(result).toEqual(items)
    })

    it("getFileListCache returns null for unknown key", () => {
      expect(useUIStore.getState().getFileListCache("nope")).toBeNull()
    })

    it("cache expires after 30 seconds", () => {
      const items = [{ name: "test.csv", path: "/test.csv", type: "file" as const }]
      useUIStore.getState().setFileListCache("key1", items)

      // Still fresh at 29 seconds
      vi.advanceTimersByTime(29_000)
      expect(useUIStore.getState().getFileListCache("key1")).toEqual(items)

      // Expired at 31 seconds
      vi.advanceTimersByTime(2_000)
      expect(useUIStore.getState().getFileListCache("key1")).toBeNull()
    })

    it("cache is exactly expired at 30001ms", () => {
      const items = [{ name: "a.csv", path: "/a.csv", type: "file" as const }]
      useUIStore.getState().setFileListCache("k", items)

      vi.advanceTimersByTime(30_001)
      expect(useUIStore.getState().getFileListCache("k")).toBeNull()
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Collapsible sections
  // ────────────────────────────────────────────────────────────────

  describe("collapsible sections", () => {
    it("toggleSection toggles a section on and off", () => {
      const s = useUIStore.getState()

      // Initially undefined (uses default)
      s.toggleSection("advanced")
      expect(useUIStore.getState().collapsedSections["advanced"]).toBe(true)

      useUIStore.getState().toggleSection("advanced")
      expect(useUIStore.getState().collapsedSections["advanced"]).toBe(false)
    })

    it("isSectionOpen returns defaultOpen when section has no stored value", () => {
      const s = useUIStore.getState()
      // Default is false when not specified
      expect(s.isSectionOpen("unknown")).toBe(false)
      // Default is true when specified
      expect(s.isSectionOpen("unknown", true)).toBe(true)
    })

    it("isSectionOpen returns stored value regardless of default", () => {
      const s = useUIStore.getState()
      s.toggleSection("sec1") // sets to true (toggling from undefined/false)

      expect(useUIStore.getState().isSectionOpen("sec1")).toBe(true)
      expect(useUIStore.getState().isSectionOpen("sec1", false)).toBe(true)
    })

    it("multiple sections are independent", () => {
      const s = useUIStore.getState()
      s.toggleSection("a")
      s.toggleSection("b")

      expect(useUIStore.getState().collapsedSections["a"]).toBe(true)
      expect(useUIStore.getState().collapsedSections["b"]).toBe(true)

      useUIStore.getState().toggleSection("a")
      expect(useUIStore.getState().collapsedSections["a"]).toBe(false)
      expect(useUIStore.getState().collapsedSections["b"]).toBe(true)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Node panel width
  // ────────────────────────────────────────────────────────────────

  describe("nodePanelWidth", () => {
    it("has a default value of 400", () => {
      expect(useUIStore.getState().nodePanelWidth).toBe(400)
    })

    it("setNodePanelWidth updates the width", () => {
      useUIStore.getState().setNodePanelWidth(600)
      expect(useUIStore.getState().nodePanelWidth).toBe(600)
    })

    it("persists width across multiple get calls", () => {
      useUIStore.getState().setNodePanelWidth(550)
      expect(useUIStore.getState().nodePanelWidth).toBe(550)
      expect(useUIStore.getState().nodePanelWidth).toBe(550)
    })
  })
})
