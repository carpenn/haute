/**
 * Zustand store for UI state — toasts, modals, panels, settings, global caches.
 *
 * Eliminates prop-drilling of addToast, setSettingsOpen, etc. through
 * every hook and component in the tree.
 */
import { create } from "zustand"
import type { ToastMessage } from "../components/Toast"
import { checkMlflow } from "../api/client"

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

  // Node panel width (persisted across selection changes)
  nodePanelWidth: number
  setNodePanelWidth: (width: number) => void

  // Collapsible section states (keyed by section ID, e.g. "optimiser.advanced")
  collapsedSections: Record<string, boolean>
  toggleSection: (key: string) => void
  isSectionOpen: (key: string, defaultOpen?: boolean) => boolean

  // MLflow status cache (fetched once, shared by all panels)
  mlflow: { status: "pending" | "connected" | "error"; backend: string; host: string }
  _mlflowFetching: boolean
  fetchMlflow: () => void

  // File listing cache (keyed by "dir|extensions")
  fileListCache: Record<string, { items: { name: string; path: string; type: "file" | "directory"; size?: number }[]; fetchedAt: number }>
  setFileListCache: (key: string, items: { name: string; path: string; type: "file" | "directory"; size?: number }[]) => void
  getFileListCache: (key: string) => { name: string; path: string; type: "file" | "directory"; size?: number }[] | null
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

  // Node panel width
  nodePanelWidth: 400,
  setNodePanelWidth: (width) => set({ nodePanelWidth: width }),

  // Collapsible sections
  collapsedSections: {},
  toggleSection: (key) => set((s) => ({
    collapsedSections: { ...s.collapsedSections, [key]: !s.collapsedSections[key] },
  })),
  isSectionOpen: (key, defaultOpen = false) => {
    const val = get().collapsedSections[key]
    // undefined means use default; stored value is "isOpen"
    return val === undefined ? defaultOpen : val
  },

  // MLflow status cache — fetched once on first call, shared by all panels
  mlflow: { status: "pending", backend: "", host: "" },
  _mlflowFetching: false,
  fetchMlflow: () => {
    const state = get()
    if (state.mlflow.status !== "pending" || state._mlflowFetching) return
    set({ _mlflowFetching: true })
    checkMlflow()
      .then((data) => {
        if (data.mlflow_installed) {
          set({ mlflow: { status: "connected", backend: data.backend || "local", host: data.databricks_host || "" } })
        } else {
          set({ mlflow: { status: "error", backend: "", host: "" } })
        }
      })
      .catch(() => {
        set({ mlflow: { status: "error", backend: "", host: "" } })
      })
  },

  // File listing cache
  fileListCache: {},
  setFileListCache: (key, items) => set((s) => ({
    fileListCache: { ...s.fileListCache, [key]: { items, fetchedAt: Date.now() } },
  })),
  getFileListCache: (key) => {
    const entry = get().fileListCache[key]
    if (!entry) return null
    // Expire after 30s — file system can change
    if (Date.now() - entry.fetchedAt > 30_000) return null
    return entry.items
  },
}))

export default useUIStore
