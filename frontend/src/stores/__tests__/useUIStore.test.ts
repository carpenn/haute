import { describe, it, expect, beforeEach } from "vitest"
import useUIStore from "../useUIStore"

function reset() {
  useUIStore.setState({
    paletteOpen: true,
    shortcutsOpen: false,
    submodelDialog: null,
    syncBanner: null,
    dirty: false,
    nodePanelWidth: 0,
  })
}

describe("useUIStore", () => {
  beforeEach(reset)

  // -----------------------------------------------------------------------
  // Modals / panels
  // -----------------------------------------------------------------------

  describe("setPaletteOpen", () => {
    it("toggles palette state", () => {
      expect(useUIStore.getState().paletteOpen).toBe(true)
      useUIStore.getState().setPaletteOpen(false)
      expect(useUIStore.getState().paletteOpen).toBe(false)
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

  // -----------------------------------------------------------------------
  // Sync banner
  // -----------------------------------------------------------------------

  describe("setSyncBanner", () => {
    it("sets and clears banner", () => {
      useUIStore.getState().setSyncBanner("Parse error")
      expect(useUIStore.getState().syncBanner).toBe("Parse error")
      useUIStore.getState().setSyncBanner(null)
      expect(useUIStore.getState().syncBanner).toBeNull()
    })
  })

  // -----------------------------------------------------------------------
  // Dirty flag
  // -----------------------------------------------------------------------

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

  // -----------------------------------------------------------------------
  // Git panel mutual exclusion
  // -----------------------------------------------------------------------

  describe("setGitOpen", () => {
    it("opens git panel", () => {
      useUIStore.getState().setGitOpen(true)
      expect(useUIStore.getState().gitOpen).toBe(true)
    })

    it("closes utility and imports when opening git", () => {
      useUIStore.getState().setUtilityOpen(true)
      useUIStore.getState().setImportsOpen(true)
      useUIStore.getState().setGitOpen(true)
      expect(useUIStore.getState().gitOpen).toBe(true)
      expect(useUIStore.getState().utilityOpen).toBe(false)
      expect(useUIStore.getState().importsOpen).toBe(false)
    })

    it("setting utility closes git", () => {
      useUIStore.getState().setGitOpen(true)
      useUIStore.getState().setUtilityOpen(true)
      expect(useUIStore.getState().gitOpen).toBe(false)
    })

    it("setting imports closes git", () => {
      useUIStore.getState().setGitOpen(true)
      useUIStore.getState().setImportsOpen(true)
      expect(useUIStore.getState().gitOpen).toBe(false)
    })
  })

  // -----------------------------------------------------------------------
  // Node panel width
  // -----------------------------------------------------------------------

  describe("nodePanelWidth", () => {
    it("defaults to 0 (sentinel for dynamic sizing)", () => {
      expect(useUIStore.getState().nodePanelWidth).toBe(0)
    })

    it("setNodePanelWidth updates the width", () => {
      useUIStore.getState().setNodePanelWidth(600)
      expect(useUIStore.getState().nodePanelWidth).toBe(600)
    })
  })
})
