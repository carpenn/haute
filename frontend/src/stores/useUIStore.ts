/**
 * Zustand store for UI state — toasts, modals, panels, settings.
 *
 * Eliminates prop-drilling of addToast, setSettingsOpen, etc. through
 * every hook and component in the tree.
 */
import { create } from "zustand"
import type { ToastMessage } from "../components/Toast"

interface UIState {
  // Toast
  toasts: ToastMessage[]
  _toastCounter: number
  addToast: (type: ToastMessage["type"], text: string) => void
  dismissToast: (id: string) => void

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

  // Row limit
  rowLimit: number
  setRowLimit: (limit: number) => void

  // Sync banner
  syncBanner: string | null
  setSyncBanner: (banner: string | null) => void

  // Dirty flag
  dirty: boolean
  setDirty: (dirty: boolean) => void
}

const useUIStore = create<UIState>()((set, get) => ({
  // Toast
  toasts: [],
  _toastCounter: 0,
  addToast: (type, text) => {
    const id = String(get()._toastCounter + 1)
    set((s) => ({
      _toastCounter: s._toastCounter + 1,
      toasts: [...s.toasts, { id, type, text }],
    }))
  },
  dismissToast: (id) => set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) })),

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

  // Row limit
  rowLimit: 1000,
  setRowLimit: (limit) => set({ rowLimit: limit }),

  // Sync banner
  syncBanner: null,
  setSyncBanner: (banner) => set({ syncBanner: banner }),

  // Dirty flag
  dirty: false,
  setDirty: (dirty) => set({ dirty }),
}))

export default useUIStore
