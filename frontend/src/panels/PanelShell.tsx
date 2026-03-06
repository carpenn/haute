import { useEffect, useRef, useCallback, type ReactNode } from "react"
import useUIStore from "../stores/useUIStore"

const MIN_PANEL_W = 320
const MAX_PANEL_W = 900

interface PanelShellProps {
  children: ReactNode
  /** Additional opacity/transition styles (e.g. dimmed NodePanel) */
  style?: React.CSSProperties
}

/**
 * Shared wrapper for all right-side panels (NodePanel, UtilityPanel,
 * ImportsPanel, GitPanel, TracePanel).  Provides:
 * - Width from the UI store (shared across all panels)
 * - A visible left-edge drag handle for resizing
 * - Slide-in animation
 * - Consistent background color
 */
export default function PanelShell({ children, style }: PanelShellProps) {
  const panelWidth = useUIStore((s) => s.nodePanelWidth)
  const setNodePanelWidth = useUIStore((s) => s.setNodePanelWidth)

  const isDragging = useRef(false)
  const startX = useRef(0)
  const startW = useRef(panelWidth)
  const widthRef = useRef(panelWidth)
  const panelRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!isDragging.current) return
      const delta = startX.current - e.clientX
      const newW = Math.min(MAX_PANEL_W, Math.max(MIN_PANEL_W, startW.current + delta))
      widthRef.current = newW
      if (panelRef.current) {
        panelRef.current.style.width = `${newW}px`
      }
    }
    const onMouseUp = () => {
      if (isDragging.current) {
        isDragging.current = false
        document.body.style.cursor = ""
        document.body.style.userSelect = ""
        setNodePanelWidth(widthRef.current)
      }
    }
    window.addEventListener("mousemove", onMouseMove)
    window.addEventListener("mouseup", onMouseUp)
    return () => {
      window.removeEventListener("mousemove", onMouseMove)
      window.removeEventListener("mouseup", onMouseUp)
    }
  }, [setNodePanelWidth])

  const onDragStart = useCallback(
    (e: React.MouseEvent) => {
      isDragging.current = true
      startX.current = e.clientX
      startW.current = panelWidth
      widthRef.current = panelWidth
      document.body.style.cursor = "col-resize"
      document.body.style.userSelect = "none"
    },
    [panelWidth],
  )

  return (
    <div
      ref={panelRef}
      className="h-full shrink-0 flex flex-row animate-slide-in"
      style={{ width: panelWidth, background: "var(--bg-panel)", ...style }}
    >
      {/* Drag handle */}
      <div
        onMouseDown={onDragStart}
        className="shrink-0 h-full w-1 cursor-col-resize transition-colors"
        style={{ background: "var(--chrome-border)" }}
        onMouseEnter={(e) => {
          e.currentTarget.style.background = "var(--accent)"
        }}
        onMouseLeave={(e) => {
          if (!isDragging.current) e.currentTarget.style.background = "var(--chrome-border)"
        }}
      />
      <div className="flex-1 min-w-0 h-full flex flex-col overflow-hidden">
        {children}
      </div>
    </div>
  )
}
