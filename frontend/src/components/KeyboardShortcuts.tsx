import { useEffect } from "react"

const isMac = typeof navigator !== "undefined" && /Mac/.test(navigator.userAgent)
const mod = isMac ? "⌘" : "Ctrl"

const shortcuts = [
  { keys: `${mod}+Z`, label: "Undo" },
  { keys: `${mod}+Shift+Z`, label: "Redo" },
  { keys: `${mod}+C`, label: "Copy selected nodes" },
  { keys: `${mod}+V`, label: "Paste nodes" },
  { keys: `${mod}+A`, label: "Select all nodes" },
  { keys: `${mod}+S`, label: "Save pipeline" },
  { keys: `${mod}+1`, label: "Fit view" },
  { keys: "Delete / Backspace", label: "Delete selected" },
  { keys: "G", label: "Toggle snap-to-grid" },
  { keys: "?", label: "Show this help" },
]

export default function KeyboardShortcuts({ onClose }: { onClose: () => void }) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape" || e.key === "?") {
        e.preventDefault()
        onClose()
      }
    }
    window.addEventListener("keydown", handler)
    return () => window.removeEventListener("keydown", handler)
  }, [onClose])

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      role="dialog"
      aria-modal="true"
      aria-label="Keyboard shortcuts"
      style={{ background: "rgba(0,0,0,.5)" }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose() }}
    >
      <div
        className="w-[360px] rounded-xl overflow-hidden shadow-2xl"
        style={{ background: "var(--bg-panel)", border: "1px solid var(--border)" }}
      >
        <div className="px-4 py-3 flex items-center justify-between" style={{ borderBottom: "1px solid var(--border)" }}>
          <h2 className="text-sm font-semibold" style={{ color: "var(--text-primary)" }}>Keyboard Shortcuts</h2>
          <button
            onClick={onClose}
            aria-label="Close keyboard shortcuts"
            className="p-1 rounded transition-colors"
            style={{ color: "var(--text-muted)" }}
            onMouseEnter={(e) => e.currentTarget.style.background = "var(--bg-hover)"}
            onMouseLeave={(e) => e.currentTarget.style.background = "transparent"}
          >
            ✕
          </button>
        </div>
        <div className="px-4 py-3 space-y-1.5">
          {shortcuts.map((s) => (
            <div key={s.keys} className="flex items-center justify-between py-1">
              <span className="text-[12px]" style={{ color: "var(--text-secondary)" }}>{s.label}</span>
              <kbd
                className="text-[11px] font-mono px-2 py-0.5 rounded"
                style={{
                  background: "var(--bg-input)",
                  border: "1px solid var(--border)",
                  color: "var(--text-muted)",
                }}
              >
                {s.keys}
              </kbd>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
