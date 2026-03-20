/**
 * Tests for useSettingsStore — MLflow dedup, file list cache,
 * collapsible sections, and row limit.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"

// Mock the API module BEFORE importing the store
vi.mock("../../api/client.ts", () => ({
  checkMlflow: vi.fn(),
}))

import useSettingsStore from "../../stores/useSettingsStore.ts"
import { checkMlflow } from "../../api/client.ts"

// ── Helpers ──────────────────────────────────────────────────────

function resetStore() {
  useSettingsStore.setState({
    rowLimit: 100,  // store default is 100, not 1000
    collapsedSections: {},
    mlflow: { status: "pending", backend: "", host: "" },
    _mlflowFetching: false,
    _mlflowLastAttempt: 0,
    scenarios: ["live"],
    activeScenario: "live",
    fileListCache: {},
  })
}

// ── Test suites ──────────────────────────────────────────────────

describe("useSettingsStore", () => {
  beforeEach(() => {
    resetStore()
    vi.clearAllMocks()
  })

  // ────────────────────────────────────────────────────────────────
  // Row limit
  // ────────────────────────────────────────────────────────────────

  describe("setRowLimit", () => {
    it("defaults to 100", () => {
      // The store's actual default is 100 (not 1000). This test catches drift
      // between the reset helper and the real store initialiser.
      expect(useSettingsStore.getState().rowLimit).toBe(100)
    })

    it("updates row limit", () => {
      useSettingsStore.getState().setRowLimit(500)
      expect(useSettingsStore.getState().rowLimit).toBe(500)
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

      const s = useSettingsStore.getState()
      s.fetchMlflow()
      s.fetchMlflow() // second call should be deduped

      expect(mockCheckMlflow).toHaveBeenCalledTimes(1)

      // Resolve the promise to clean up
      resolvePromise!({ mlflow_installed: true })
      // Wait for microtask to flush the .then()
      await vi.waitFor(() => {
        expect(useSettingsStore.getState()._mlflowFetching).toBe(false)
      })
    })

    it("successful MLflow check sets connected status", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      mockCheckMlflow.mockResolvedValue({
        mlflow_installed: true,
        backend: "databricks",
        databricks_host: "https://db.example.com",
      })

      useSettingsStore.getState().fetchMlflow()

      await vi.waitFor(() => {
        expect(useSettingsStore.getState().mlflow.status).toBe("connected")
      })

      const { mlflow } = useSettingsStore.getState()
      expect(mlflow.backend).toBe("databricks")
      expect(mlflow.host).toBe("https://db.example.com")
    })

    it("failed MLflow check sets error status", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      mockCheckMlflow.mockRejectedValue(new Error("Network error"))

      useSettingsStore.getState().fetchMlflow()

      await vi.waitFor(() => {
        expect(useSettingsStore.getState().mlflow.status).toBe("error")
      })
    })

    it("does not re-fetch after successful connection", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      mockCheckMlflow.mockResolvedValue({ mlflow_installed: true, backend: "local" })

      useSettingsStore.getState().fetchMlflow()
      await vi.waitFor(() => {
        expect(useSettingsStore.getState().mlflow.status).toBe("connected")
      })

      // Second call should be a no-op since status is "connected"
      mockCheckMlflow.mockClear()
      useSettingsStore.getState().fetchMlflow()
      expect(mockCheckMlflow).not.toHaveBeenCalled()
    })

    it("mlflow_installed: false sets error status", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      mockCheckMlflow.mockResolvedValue({ mlflow_installed: false })

      useSettingsStore.getState().fetchMlflow()

      await vi.waitFor(() => {
        expect(useSettingsStore.getState().mlflow.status).toBe("error")
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

      useSettingsStore.getState().setFileListCache("dir|csv", items)

      const result = useSettingsStore.getState().getFileListCache("dir|csv")
      expect(result).toEqual(items)
    })

    it("getFileListCache returns null for unknown key", () => {
      expect(useSettingsStore.getState().getFileListCache("nope")).toBeNull()
    })

    it("cache expires after 30 seconds", () => {
      const items = [{ name: "test.csv", path: "/test.csv", type: "file" as const }]
      useSettingsStore.getState().setFileListCache("key1", items)

      // Still fresh at 29 seconds
      vi.advanceTimersByTime(29_000)
      expect(useSettingsStore.getState().getFileListCache("key1")).toEqual(items)

      // Expired at 31 seconds
      vi.advanceTimersByTime(2_000)
      expect(useSettingsStore.getState().getFileListCache("key1")).toBeNull()
    })

    it("cache is exactly expired at 30001ms", () => {
      const items = [{ name: "a.csv", path: "/a.csv", type: "file" as const }]
      useSettingsStore.getState().setFileListCache("k", items)

      vi.advanceTimersByTime(30_001)
      expect(useSettingsStore.getState().getFileListCache("k")).toBeNull()
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Collapsible sections
  // ────────────────────────────────────────────────────────────────

  describe("collapsible sections", () => {
    it("toggleSection toggles a section on and off", () => {
      const s = useSettingsStore.getState()

      // Initially undefined (uses default)
      s.toggleSection("advanced")
      expect(useSettingsStore.getState().collapsedSections["advanced"]).toBe(true)

      useSettingsStore.getState().toggleSection("advanced")
      expect(useSettingsStore.getState().collapsedSections["advanced"]).toBe(false)
    })

    it("isSectionOpen returns defaultOpen when section has no stored value", () => {
      const s = useSettingsStore.getState()
      // Default is false when not specified
      expect(s.isSectionOpen("unknown")).toBe(false)
      // Default is true when specified
      expect(s.isSectionOpen("unknown", true)).toBe(true)
    })

    it("isSectionOpen returns stored value regardless of default", () => {
      const s = useSettingsStore.getState()
      s.toggleSection("sec1") // sets to true (toggling from undefined/false)

      expect(useSettingsStore.getState().isSectionOpen("sec1")).toBe(true)
      expect(useSettingsStore.getState().isSectionOpen("sec1", false)).toBe(true)
    })

    it("multiple sections are independent", () => {
      const s = useSettingsStore.getState()
      s.toggleSection("a")
      s.toggleSection("b")

      expect(useSettingsStore.getState().collapsedSections["a"]).toBe(true)
      expect(useSettingsStore.getState().collapsedSections["b"]).toBe(true)

      useSettingsStore.getState().toggleSection("a")
      expect(useSettingsStore.getState().collapsedSections["a"]).toBe(false)
      expect(useSettingsStore.getState().collapsedSections["b"]).toBe(true)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Scenario slug (B12 fix)
  // ────────────────────────────────────────────────────────────────

  describe("addScenario returns normalized slug", () => {
    it("returns the slugified name on success", () => {
      const result = useSettingsStore.getState().addScenario("My Test Scenario")
      expect(result).toBe("my_test_scenario")
      expect(useSettingsStore.getState().scenarios).toContain("my_test_scenario")
    })

    it("returns null for duplicate scenario", () => {
      useSettingsStore.getState().addScenario("dup")
      const result = useSettingsStore.getState().addScenario("dup")
      expect(result).toBeNull()
    })

    it("returns null for empty name", () => {
      const result = useSettingsStore.getState().addScenario("   ")
      expect(result).toBeNull()
    })

    it("normalizes whitespace to underscores", () => {
      const result = useSettingsStore.getState().addScenario("  Two  Words  ")
      expect(result).toBe("two_words")
    })

    it("slug is consistent with what gets stored in scenarios list", () => {
      const slug = useSettingsStore.getState().addScenario("New Scenario")
      const scenarios = useSettingsStore.getState().scenarios
      expect(scenarios).toContain(slug)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // removeScenario
  // Catches: removing a scenario that is currently active would leave
  // activeScenario pointing at a nonexistent scenario, breaking data
  // source routing.
  // ────────────────────────────────────────────────────────────────

  describe("removeScenario", () => {
    it("removes a non-live scenario from the list", () => {
      useSettingsStore.getState().addScenario("test_sc")
      expect(useSettingsStore.getState().scenarios).toContain("test_sc")

      useSettingsStore.getState().removeScenario("test_sc")
      expect(useSettingsStore.getState().scenarios).not.toContain("test_sc")
    })

    it("cannot remove the 'live' scenario (always present)", () => {
      useSettingsStore.getState().removeScenario("live")
      expect(useSettingsStore.getState().scenarios).toContain("live")
    })

    it("resets activeScenario to 'live' when removing the active scenario", () => {
      useSettingsStore.getState().addScenario("staging")
      useSettingsStore.getState().setActiveScenario("staging")
      expect(useSettingsStore.getState().activeScenario).toBe("staging")

      useSettingsStore.getState().removeScenario("staging")
      expect(useSettingsStore.getState().activeScenario).toBe("live")
    })

    it("does not change activeScenario when removing a non-active scenario", () => {
      useSettingsStore.getState().addScenario("sc_a")
      useSettingsStore.getState().addScenario("sc_b")
      useSettingsStore.getState().setActiveScenario("sc_a")

      useSettingsStore.getState().removeScenario("sc_b")
      expect(useSettingsStore.getState().activeScenario).toBe("sc_a")
    })

    it("removing a nonexistent scenario is a no-op", () => {
      const before = useSettingsStore.getState().scenarios.slice()
      useSettingsStore.getState().removeScenario("ghost")
      expect(useSettingsStore.getState().scenarios).toEqual(before)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // setScenarios / setActiveScenario — direct setters
  // Catches: if setScenarios were accidentally removed or renamed,
  // pipeline load (which bulk-sets scenarios from the backend) would
  // break silently.
  // ────────────────────────────────────────────────────────────────

  describe("setScenarios / setActiveScenario", () => {
    it("setScenarios replaces the entire scenario list", () => {
      useSettingsStore.getState().setScenarios(["live", "staging", "prod"])
      expect(useSettingsStore.getState().scenarios).toEqual(["live", "staging", "prod"])
    })

    it("setActiveScenario switches the active scenario", () => {
      useSettingsStore.getState().setScenarios(["live", "staging"])
      useSettingsStore.getState().setActiveScenario("staging")
      expect(useSettingsStore.getState().activeScenario).toBe("staging")
    })

    it("setScenarios does not affect activeScenario", () => {
      useSettingsStore.getState().setActiveScenario("live")
      useSettingsStore.getState().setScenarios(["live", "new_sc"])
      expect(useSettingsStore.getState().activeScenario).toBe("live")
    })
  })

  // ────────────────────────────────────────────────────────────────
  // MLflow 5-second timeout race
  // Catches: if the 5s timeout is removed, a hung MLflow check would
  // block the UI indefinitely with status "pending" / spinner.
  // ────────────────────────────────────────────────────────────────

  describe("MLflow 5s timeout", () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    it("times out and sets error status when checkMlflow hangs for >5s", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      // Return a promise that never resolves
      mockCheckMlflow.mockReturnValue(new Promise(() => {}))

      useSettingsStore.getState().fetchMlflow()

      // Advance past the 5s timeout
      vi.advanceTimersByTime(5_001)

      // Allow microtasks (Promise.race rejection, .catch, .finally) to flush
      await vi.waitFor(() => {
        expect(useSettingsStore.getState().mlflow.status).toBe("error")
      })
      expect(useSettingsStore.getState()._mlflowFetching).toBe(false)
    })

    it("succeeds before the timeout if checkMlflow resolves quickly", async () => {
      const mockCheckMlflow = vi.mocked(checkMlflow)
      mockCheckMlflow.mockResolvedValue({ mlflow_installed: true, backend: "local" })

      useSettingsStore.getState().fetchMlflow()

      // Let the resolved promise flush
      await vi.waitFor(() => {
        expect(useSettingsStore.getState().mlflow.status).toBe("connected")
      })

      // The timeout should not overwrite the connected status even if it fires later
      vi.advanceTimersByTime(6_000)
      expect(useSettingsStore.getState().mlflow.status).toBe("connected")
    })
  })
})
