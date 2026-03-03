/**
 * Zustand store for chrome / layout UI state — panel toggles, modals,
 * grid settings, sync banner, dirty flag, and node panel width.
 *
 * Toast notifications live in useToastStore.
 * Application settings (MLflow, scenarios, caches) live in useSettingsStore.
 */
import { create } from "zustand"

interface UIState {
  // Modals / panels
  paletteOpen: boolean
  setPaletteOpen: (open: boolean) => void
  settingsOpen: boolean
  setSettingsOpen: (open: boolean) => void
  shortcutsOpen: boolean
  setShortcutsOpen: (open: boolean | ((prev: boolean) => boolean)) => void
  submodelDialog: { nodeIds: string[] } | null
  setSubmodelDialog: (dialog: { nodeIds: string[] } | null) => void

  // Grid / layout
  snapToGrid: boolean
  toggleSnapToGrid: () => void

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
  settingsOpen: false,
  setSettingsOpen: (open) => set({ settingsOpen: open }),
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

  // Grid
  snapToGrid: false,
  toggleSnapToGrid: () => set((s) => ({ snapToGrid: !s.snapToGrid })),

  // Sync banner
  syncBanner: null,
  setSyncBanner: (banner) => set({ syncBanner: banner }),

  // Dirty flag
  dirty: false,
  setDirty: (dirty) => set({ dirty }),

  // Node panel width
  nodePanelWidth: 400,
  setNodePanelWidth: (width) => set({ nodePanelWidth: width }),
}))

export default useUIStore
