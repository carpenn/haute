/**
 * Zustand store for chrome / layout UI state — panel toggles, modals,
 * sync banner, dirty flag, and node panel width.
 *
 * Toast notifications live in useToastStore.
 * Application settings (MLflow, scenarios, caches) live in useSettingsStore.
 */
import { create } from "zustand"

interface UIState {
  // Modals / panels
  paletteOpen: boolean
  setPaletteOpen: (open: boolean) => void
  utilityOpen: boolean
  setUtilityOpen: (open: boolean) => void
  importsOpen: boolean
  setImportsOpen: (open: boolean) => void
  gitOpen: boolean
  setGitOpen: (open: boolean) => void
  shortcutsOpen: boolean
  setShortcutsOpen: (open: boolean | ((prev: boolean) => boolean)) => void
  submodelDialog: { nodeIds: string[] } | null
  setSubmodelDialog: (dialog: { nodeIds: string[] } | null) => void

  // Sync banner
  syncBanner: string | null
  setSyncBanner: (banner: string | null) => void

  // Dirty flag
  dirty: boolean
  setDirty: (dirty: boolean) => void

  // Node panel width (persisted across selection changes)
  nodePanelWidth: number
  setNodePanelWidth: (width: number) => void
}

const useUIStore = create<UIState>()((set) => ({
  // Modals / panels
  paletteOpen: true,
  setPaletteOpen: (open) => set({ paletteOpen: open }),
  utilityOpen: false,
  setUtilityOpen: (open) => set({ utilityOpen: open, importsOpen: false, gitOpen: false }),
  importsOpen: false,
  setImportsOpen: (open) => set({ importsOpen: open, utilityOpen: false, gitOpen: false }),
  gitOpen: false,
  setGitOpen: (open) => set({ gitOpen: open, utilityOpen: false, importsOpen: false }),
  shortcutsOpen: false,
  setShortcutsOpen: (open) => {
    if (typeof open === "function") {
      set((s) => ({ shortcutsOpen: open(s.shortcutsOpen) }))
    } else {
      set({ shortcutsOpen: open })
    }
  },
  submodelDialog: null,
  setSubmodelDialog: (dialog) => set({ submodelDialog: dialog }),

  // Sync banner
  syncBanner: null,
  setSyncBanner: (banner) => set({ syncBanner: banner }),

  // Dirty flag
  dirty: false,
  setDirty: (dirty) => set({ dirty }),

  // Node panel width
  nodePanelWidth: 900,
  setNodePanelWidth: (width) => set({ nodePanelWidth: width }),
}))

export default useUIStore
