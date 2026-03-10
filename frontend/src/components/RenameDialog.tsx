import { useEffect, useRef } from "react"

interface RenameDialogProps {
  defaultValue: string
  onConfirm: (newName: string) => void
  onCancel: () => void
}

export default function RenameDialog({ defaultValue, onConfirm, onCancel }: RenameDialogProps) {
  const inputRef = useRef<HTMLInputElement>(null)

  // Auto-focus and select all text on mount
  useEffect(() => {
    const el = inputRef.current
    if (el) {
      el.focus()
      el.select()
    }
  }, [])

  // Close on Escape (when focus is outside the input, e.g. on the backdrop)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onCancel()
    }
    document.addEventListener("keydown", handler)
    return () => document.removeEventListener("keydown", handler)
  }, [onCancel])

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      role="dialog"
      aria-modal="true"
      aria-label="Rename node"
      style={{ background: "rgba(0,0,0,.5)" }}
      onClick={(e) => { if (e.target === e.currentTarget) onCancel() }}
    >
      <div
        className="w-[360px] flex flex-col rounded-xl overflow-hidden shadow-2xl"
        style={{ background: "var(--bg-panel)", border: "1px solid var(--border)" }}
      >
        <div className="px-4 py-3" style={{ borderBottom: "1px solid var(--border)" }}>
          <h2 className="text-sm font-semibold" style={{ color: "var(--text-primary)" }}>
            Rename Node
          </h2>
        </div>
        <form
          className="p-4 flex flex-col gap-3"
          onSubmit={(e) => {
            e.preventDefault()
            const value = inputRef.current?.value.trim()
            if (value) onConfirm(value)
          }}
        >
          <div>
            <label
              htmlFor="rename-input"
              className="text-[11px] font-medium block mb-1"
              style={{ color: "var(--text-muted)" }}
            >
              Node name
            </label>
            <input
              ref={inputRef}
              id="rename-input"
              name="name"
              type="text"
              defaultValue={defaultValue}
              className="w-full px-3 py-1.5 text-[13px] rounded-md focus:outline-none focus:ring-2"
              style={{
                background: "var(--bg-input)",
                border: "1px solid var(--border)",
                color: "var(--text-primary)",
                caretColor: "var(--accent)",
              }}
            />
          </div>
          <div className="flex justify-end gap-2">
            <button
              type="button"
              onClick={onCancel}
              className="px-3 py-1.5 text-[12px] font-medium rounded-md transition-colors"
              style={{ color: "var(--text-secondary)" }}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-1.5 text-[12px] font-semibold text-white rounded-md transition-colors"
              style={{ background: "#64748b" }}
              onMouseEnter={(e) => (e.currentTarget.style.background = "#94a3b8")}
              onMouseLeave={(e) => (e.currentTarget.style.background = "#64748b")}
            >
              Rename
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
