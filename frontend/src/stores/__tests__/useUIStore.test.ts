import { describe, it, expect, beforeEach } from "vitest"
import useUIStore from "../useUIStore"

function reset() {
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
  })
}

describe("useUIStore", () => {
  beforeEach(reset)

  // -----------------------------------------------------------------------
  // Toast
  // -----------------------------------------------------------------------

  describe("addToast / dismissToast", () => {
    it("adds a toast with incrementing id", () => {
      useUIStore.getState().addToast("info", "Hello")
      const { toasts, _toastCounter } = useUIStore.getState()
      expect(toasts).toHaveLength(1)
      expect(toasts[0]).toEqual({ id: "1", type: "info", text: "Hello" })
      expect(_toastCounter).toBe(1)
    })

    it("accumulates multiple toasts", () => {
      const { addToast } = useUIStore.getState()
      addToast("info", "First")
      addToast("error", "Second")
      addToast("success", "Third")
      const { toasts } = useUIStore.getState()
      expect(toasts).toHaveLength(3)
      expect(toasts.map((t) => t.type)).toEqual(["info", "error", "success"])
      expect(toasts.map((t) => t.id)).toEqual(["1", "2", "3"])
    })

    it("dismisses a toast by id", () => {
      const { addToast } = useUIStore.getState()
      addToast("info", "Keep")
      addToast("error", "Remove")
      useUIStore.getState().dismissToast("2")
      const { toasts } = useUIStore.getState()
      expect(toasts).toHaveLength(1)
      expect(toasts[0].text).toBe("Keep")
    })

    it("dismissing non-existent id is a no-op", () => {
      useUIStore.getState().addToast("info", "Only")
      useUIStore.getState().dismissToast("999")
      expect(useUIStore.getState().toasts).toHaveLength(1)
    })

    it("counter keeps incrementing after dismiss", () => {
      const { addToast } = useUIStore.getState()
      addToast("info", "A")
      useUIStore.getState().dismissToast("1")
      addToast("info", "B")
      expect(useUIStore.getState().toasts[0].id).toBe("2")
    })
  })

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

  // -----------------------------------------------------------------------
  // Grid
  // -----------------------------------------------------------------------

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

  // -----------------------------------------------------------------------
  // Row limit
  // -----------------------------------------------------------------------

  describe("setRowLimit", () => {
    it("defaults to 1000", () => {
      expect(useUIStore.getState().rowLimit).toBe(1000)
    })

    it("updates row limit", () => {
      useUIStore.getState().setRowLimit(500)
      expect(useUIStore.getState().rowLimit).toBe(500)
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
})
