/**
 * Zustand store for the toast notification system.
 *
 * Separated from useUIStore because toasts are a cross-cutting concern used
 * by nearly every hook and component. Having a dedicated store makes the
 * dependency explicit and keeps the toast counter/lifecycle self-contained.
 */
import { create } from "zustand"
import type { ToastMessage } from "../components/Toast"

interface ToastState {
  toasts: ToastMessage[]
  _toastCounter: number
  addToast: (type: ToastMessage["type"], text: string) => void
  dismissToast: (id: string) => void
}

const useToastStore = create<ToastState>()((set, get) => ({
  toasts: [],
  _toastCounter: 0,
  addToast: (type, text) => {
    const id = String(get()._toastCounter + 1)
    set((s) => ({
      _toastCounter: s._toastCounter + 1,
      toasts: [...s.toasts.slice(-9), { id, type, text }],
    }))
  },
  dismissToast: (id) => set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) })),
}))

export default useToastStore
