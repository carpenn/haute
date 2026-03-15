import { useEffect, type ReactNode } from "react"

export interface ModalShellProps {
  /** Accessible label for the dialog */
  ariaLabel: string
  /** Called when the user clicks the backdrop or presses Escape */
  onClose: () => void
  /** Optional extra keys (besides Escape) that close the modal */
  extraCloseKeys?: string[]
  /** Width class for the inner panel (default: "w-[360px]") */
  width?: string
  children: ReactNode
}

/**
 * Shared modal shell: full-screen overlay with backdrop click,
 * Escape key handling, and a centred panel.
 *
 * Used by SubmodelDialog, RenameDialog, and KeyboardShortcuts.
 */
export default function ModalShell({
  ariaLabel,
  onClose,
  extraCloseKeys,
  width = "w-[360px]",
  children,
}: ModalShellProps) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape" || (extraCloseKeys && extraCloseKeys.includes(e.key))) {
        e.preventDefault()
        onClose()
      }
    }
    document.addEventListener("keydown", handler)
    return () => {
      document.removeEventListener("keydown", handler)
    }
  }, [onClose, extraCloseKeys])

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      role="dialog"
      aria-modal="true"
      aria-label={ariaLabel}
      style={{ background: "rgba(0,0,0,.5)" }}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose()
      }}
    >
      <div
        className={`${width} flex flex-col rounded-xl overflow-hidden shadow-2xl`}
        style={{ background: "var(--bg-panel)", border: "1px solid var(--border)" }}
      >
        {children}
      </div>
    </div>
  )
}
