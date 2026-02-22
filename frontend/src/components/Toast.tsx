import { useEffect } from "react"
import { CheckCircle2, AlertCircle, Info, X } from "lucide-react"
import useUIStore from "../stores/useUIStore"

export interface ToastMessage {
  id: string
  type: "success" | "error" | "info"
  text: string
}

const icons = {
  success: CheckCircle2,
  error: AlertCircle,
  info: Info,
}

const accentColors = {
  success: "#22c55e",
  error: "#ef4444",
  info: "#6366f1",
}

function ToastItem({ toast, onDismiss }: { toast: ToastMessage; onDismiss: (id: string) => void }) {
  const Icon = icons[toast.type]
  const accent = accentColors[toast.type]

  useEffect(() => {
    const timer = setTimeout(() => onDismiss(toast.id), 3000)
    return () => clearTimeout(timer)
  }, [toast.id, onDismiss])

  return (
    <div
      className="px-4 py-2.5 rounded-lg shadow-xl flex items-center gap-2.5 text-[12px] font-medium animate-slide-in min-w-[240px] max-w-[380px]"
      style={{
        background: "var(--bg-panel)",
        color: "var(--text-primary)",
        border: "1px solid var(--border-bright)",
      }}
    >
      <Icon size={14} style={{ color: accent }} className="shrink-0" />
      <span className="flex-1" style={{ color: "var(--text-primary)" }}>{toast.text}</span>
      <button
        onClick={() => onDismiss(toast.id)}
        className="p-0.5 rounded shrink-0 transition-colors"
        style={{ color: "var(--text-muted)" }}
        onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg-hover)" }}
        onMouseLeave={(e) => { e.currentTarget.style.background = "transparent" }}
      >
        <X size={12} />
      </button>
    </div>
  )
}

export default function ToastContainer() {
  const toasts = useUIStore((s) => s.toasts)
  const onDismiss = useUIStore((s) => s.dismissToast)
  if (toasts.length === 0) return null

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2 items-end">
      {toasts.map((t) => (
        <ToastItem key={t.id} toast={t} onDismiss={onDismiss} />
      ))}
    </div>
  )
}
