import { useEffect, useRef, useState, useMemo } from "react"
import { Trash2, Copy, Type, Ungroup, Link2 } from "lucide-react"

interface ContextMenuProps {
  x: number
  y: number
  nodeId: string
  nodeLabel: string
  isSubmodel?: boolean
  onClose: () => void
  onDelete: (id: string) => void
  onDuplicate: (id: string) => void
  onRename: (id: string) => void
  onCreateInstance?: (id: string) => void
  onDissolveSubmodel?: (name: string) => void
}

export default function ContextMenu({
  x,
  y,
  nodeLabel,
  onClose,
  onDelete,
  onDuplicate,
  onRename,
  onCreateInstance,
  onDissolveSubmodel,
  isSubmodel,
  nodeId,
}: ContextMenuProps) {
  const ref = useRef<HTMLDivElement>(null)
  const [focusIndex, setFocusIndex] = useState(0)
  const buttonRefs = useRef<(HTMLButtonElement | null)[]>([])

  const items = useMemo(() => {
    const list: { label: string; icon: typeof Type; action: () => void; danger?: boolean }[] = [
      { label: "Rename", icon: Type, action: () => onRename(nodeId) },
      { label: "Duplicate", icon: Copy, action: () => onDuplicate(nodeId) },
    ]
    if (onCreateInstance && !isSubmodel) {
      list.push({ label: "Create Instance", icon: Link2, action: () => onCreateInstance(nodeId) })
    }
    if (isSubmodel && onDissolveSubmodel) {
      const smName = nodeId.replace("submodel__", "")
      list.push({ label: "Dissolve Submodel", icon: Ungroup, action: () => onDissolveSubmodel(smName), danger: true })
    }
    list.push({ label: "Delete", icon: Trash2, action: () => onDelete(nodeId), danger: true })
    return list
  }, [nodeId, isSubmodel, onRename, onDuplicate, onDelete, onCreateInstance, onDissolveSubmodel])

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as HTMLElement)) {
        onClose()
      }
    }
    document.addEventListener("mousedown", handler)
    return () => document.removeEventListener("mousedown", handler)
  }, [onClose])

  // Auto-focus first item
  useEffect(() => {
    buttonRefs.current[0]?.focus()
  }, [])

  // Keyboard navigation: Escape, ArrowDown, ArrowUp
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") { onClose(); return }
      if (e.key === "ArrowDown") {
        e.preventDefault()
        setFocusIndex((prev) => {
          const next = (prev + 1) % items.length
          buttonRefs.current[next]?.focus()
          return next
        })
        return
      }
      if (e.key === "ArrowUp") {
        e.preventDefault()
        setFocusIndex((prev) => {
          const next = (prev - 1 + items.length) % items.length
          buttonRefs.current[next]?.focus()
          return next
        })
      }
    }
    document.addEventListener("keydown", handler)
    return () => document.removeEventListener("keydown", handler)
  }, [onClose, items.length])

  return (
    <div
      ref={ref}
      role="menu"
      aria-label={`Actions for ${nodeLabel}`}
      className="fixed z-50 bg-[#0f172a] rounded-lg shadow-2xl border border-slate-700/50 py-1 min-w-[160px] animate-fade-in"
      style={{ left: x, top: y }}
    >
      <div className="px-3 py-1.5 text-[9px] text-slate-500 font-bold uppercase tracking-[0.1em] border-b border-slate-700/50 mb-0.5">
        {nodeLabel}
      </div>
      {items.map((item, i) => {
        const Icon = item.icon
        return (
          <button
            key={item.label}
            ref={(el) => { buttonRefs.current[i] = el }}
            role="menuitem"
            tabIndex={i === focusIndex ? 0 : -1}
            onClick={() => {
              item.action()
              onClose()
            }}
            className={`w-full flex items-center gap-2.5 px-3 py-1.5 text-[12px] transition-colors ${
              item.danger
                ? "text-red-400 hover:bg-red-500/10"
                : "text-slate-300 hover:bg-slate-700/50 hover:text-white"
            }`}
          >
            <Icon size={13} aria-hidden="true" />
            {item.label}
          </button>
        )
      })}
    </div>
  )
}
