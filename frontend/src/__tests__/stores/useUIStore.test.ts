/**
 * Tests for useUIStore — panel toggles, grid, sync banner, dirty flag,
 * and node panel width.
 *
 * Toast tests are in useToastStore.test.ts.
 * MLflow, file cache, collapsible sections, and row limit tests are in
 * useSettingsStore.test.ts.
 */
import { describe, it, expect, beforeEach } from "vitest"
import useUIStore from "../../stores/useUIStore.ts"

// ── Helpers ──────────────────────────────────────────────────────

function resetStore() {
  useUIStore.setState({
    paletteOpen: true,
    settingsOpen: false,
    shortcutsOpen: false,
    submodelDialog: null,
    snapToGrid: false,
    syncBanner: null,
    dirty: false,
    nodePanelWidth: 400,
  })
}

// ── Test suites ──────────────────────────────────────────────────

describe("useUIStore", () => {
  beforeEach(() => {
    resetStore()
  })

  // ────────────────────────────────────────────────────────────────
  // Modals / panels
  // ────────────────────────────────────────────────────────────────

  describe("setPaletteOpen", () => {
    it("toggles palette state", () => {
      expect(useUIStore.getState().paletteOpen).toBe(true)
      useUIStore.getState().setPaletteOpen(false)
      expect(useUIStore.getState().paletteOpen).toBe(false)
    })
  })

  describe("setSettingsOpen", () => {
    it("toggles settings state", () => {
      useUIStore.getState().setSettingsOpen(true)
      expect(useUIStore.getState().settingsOpen).toBe(true)
    })
  })

  describe("setShortcutsOpen", () => {
    it("accepts a boolean", () => {
      useUIStore.getState().setShortcutsOpen(true)
      expect(useUIStore.getState().shortcutsOpen).toBe(true)
    })

    it("accepts a function updater", () => {
      useUIStore.getState().setShortcutsOpen(false)
      useUIStore.getState().setShortcutsOpen((prev) => !prev)
      expect(useUIStore.getState().shortcutsOpen).toBe(true)
    })
  })

  describe("setSubmodelDialog", () => {
    it("sets and clears dialog", () => {
      useUIStore.getState().setSubmodelDialog({ nodeIds: ["a", "b"] })
      expect(useUIStore.getState().submodelDialog).toEqual({ nodeIds: ["a", "b"] })
      useUIStore.getState().setSubmodelDialog(null)
      expect(useUIStore.getState().submodelDialog).toBeNull()
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Grid
  // ────────────────────────────────────────────────────────────────

  describe("toggleSnapToGrid", () => {
    it("toggles from false to true", () => {
      useUIStore.getState().toggleSnapToGrid()
      expect(useUIStore.getState().snapToGrid).toBe(true)
    })

    it("toggles back to false", () => {
      useUIStore.getState().toggleSnapToGrid()
      useUIStore.getState().toggleSnapToGrid()
      expect(useUIStore.getState().snapToGrid).toBe(false)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Sync banner
  // ────────────────────────────────────────────────────────────────

  describe("setSyncBanner", () => {
    it("sets and clears banner", () => {
      useUIStore.getState().setSyncBanner("Parse error")
      expect(useUIStore.getState().syncBanner).toBe("Parse error")
      useUIStore.getState().setSyncBanner(null)
      expect(useUIStore.getState().syncBanner).toBeNull()
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Dirty flag
  // ────────────────────────────────────────────────────────────────

  describe("setDirty", () => {
    it("defaults to false", () => {
      expect(useUIStore.getState().dirty).toBe(false)
    })

    it("sets dirty flag", () => {
      useUIStore.getState().setDirty(true)
      expect(useUIStore.getState().dirty).toBe(true)
    })

    it("clears dirty flag", () => {
      useUIStore.getState().setDirty(true)
      useUIStore.getState().setDirty(false)
      expect(useUIStore.getState().dirty).toBe(false)
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
