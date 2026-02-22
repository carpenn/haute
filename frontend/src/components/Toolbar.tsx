import { Settings, Undo2, Redo2, Grid3X3, Keyboard } from "lucide-react"
import type { WsStatus } from "../hooks/useWebSocketSync"
import useUIStore from "../stores/useUIStore"

const WS_STATUS_CONFIG: Record<WsStatus, { color: string; title: string }> = {
  connected: { color: "#22c55e", title: "Live sync connected" },
  reconnecting: { color: "#f59e0b", title: "Reconnecting to server\u2026" },
  disconnected: { color: "#ef4444", title: "Server unreachable \u2014 restart haute serve" },
}

interface ToolbarProps {
  nodeCount: number
  edgeCount: number
  dirty: boolean
  canUndo: boolean
  canRedo: boolean
  onUndo: () => void
  onRedo: () => void
  snapToGrid: boolean
  onToggleSnapToGrid: () => void
  onShowShortcuts: () => void
  onOpenSettings: () => void
  onAutoLayout: () => void
  onRun: () => void
  runStatus: string | null
  onSave: () => void
  wsStatus: WsStatus
}

export default function Toolbar({
  nodeCount, edgeCount, dirty,
  canUndo, canRedo, onUndo, onRedo,
  snapToGrid, onToggleSnapToGrid,
  onShowShortcuts,
  onOpenSettings, onAutoLayout,
  onRun, runStatus, onSave,
  wsStatus,
}: ToolbarProps) {
  const rowLimit = useUIStore((s) => s.rowLimit)
  const setRowLimit = useUIStore((s) => s.setRowLimit)
  const wsConfig = WS_STATUS_CONFIG[wsStatus]

  return (
    <header role="toolbar" aria-label="Pipeline toolbar" className="h-11 flex items-center px-4 shrink-0" style={{ background: 'var(--chrome)', borderBottom: '1px solid var(--chrome-border)' }}>
      <div className="flex items-center gap-2.5">
        <h1 className="text-sm font-bold tracking-tight" style={{ color: 'var(--text-primary)' }}>Haute</h1>
        <span className="text-[11px] font-mono" style={{ color: 'var(--text-muted)' }}>v0.1.0</span>
        {dirty && <span className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse-dot" title="Unsaved changes" />}
        <span
          className={`w-2 h-2 rounded-full shrink-0${wsStatus === "reconnecting" ? " animate-pulse-dot" : ""}`}
          style={{ background: wsConfig.color }}
          title={wsConfig.title}
        />
      </div>
      <div className="ml-auto flex items-center gap-1.5">
        <span className="text-[12px] mr-2" style={{ color: 'var(--text-muted)' }}>
          {nodeCount} nodes · {edgeCount} edges
        </span>
        {/* Undo / Redo */}
        <button
          onClick={onUndo}
          disabled={!canUndo}
          className="p-1.5 rounded-md transition-colors disabled:opacity-20"
          style={{ color: 'var(--text-secondary)' }}
          onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-primary)' }}
          onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)' }}
          title="Undo (Ctrl+Z)"
        >
          <Undo2 size={14} aria-hidden="true" />
        </button>
        <button
          onClick={onRedo}
          disabled={!canRedo}
          aria-label="Redo"
          className="p-1.5 rounded-md transition-colors disabled:opacity-20"
          style={{ color: 'var(--text-secondary)' }}
          onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-primary)' }}
          onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)' }}
          title="Redo (Ctrl+Shift+Z)"
        >
          <Redo2 size={14} aria-hidden="true" />
        </button>
        <div className="w-px h-4 mx-0.5" style={{ background: 'var(--chrome-border)' }} />
        {/* Snap to grid */}
        <button
          onClick={onToggleSnapToGrid}
          className="p-1.5 rounded-md transition-colors"
          style={{ color: snapToGrid ? 'var(--accent)' : 'var(--text-secondary)', background: snapToGrid ? 'var(--accent-soft)' : 'transparent' }}
          onMouseEnter={(e) => { if (!snapToGrid) { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-primary)' } }}
          onMouseLeave={(e) => { if (!snapToGrid) { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)' } }}
          title="Toggle snap-to-grid (G)"
        >
          <Grid3X3 size={14} aria-hidden="true" />
        </button>
        {/* Keyboard shortcuts */}
        <button
          onClick={onShowShortcuts}
          aria-label="Keyboard shortcuts"
          className="p-1.5 rounded-md transition-colors"
          style={{ color: 'var(--text-secondary)' }}
          onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-primary)' }}
          onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)' }}
          title="Keyboard shortcuts (?)"
        >
          <Keyboard size={14} aria-hidden="true" />
        </button>
        <div className="w-px h-4 mx-0.5" style={{ background: 'var(--chrome-border)' }} />
        <div className="flex items-center gap-1 mr-1" title="Row limit for preview (0 = no limit)">
          <label className="text-[11px] font-medium" style={{ color: 'var(--text-muted)' }}>Rows</label>
          <input
            type="number"
            min={0}
            step={100}
            value={rowLimit}
            onChange={(e) => setRowLimit(Math.max(0, parseInt(e.target.value) || 0))}
            className="w-16 px-1.5 py-0.5 text-[12px] font-mono rounded text-center focus:outline-none"
            style={{ background: 'var(--chrome-hover)', border: '1px solid var(--chrome-border)', color: 'var(--text-primary)' }}
          />
        </div>
        <button
          onClick={onOpenSettings}
          className="px-2.5 py-1 text-[12px] font-medium rounded-md transition-colors flex items-center gap-1"
          style={{ color: 'var(--text-secondary)' }}
          onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-primary)' }}
          onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)' }}
          title="Pipeline settings (imports, helpers)"
        >
          <Settings size={13} />
          Imports
        </button>
        <button
          onClick={onAutoLayout}
          disabled={nodeCount === 0}
          className="px-2.5 py-1 text-[12px] font-medium rounded-md transition-colors disabled:opacity-30"
          style={{ color: 'var(--text-secondary)' }}
          onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-primary)' }}
          onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)' }}
          title="Auto-arrange nodes"
        >
          Layout
        </button>
        <button
          onClick={onRun}
          disabled={nodeCount === 0}
          className="px-3 py-1 text-[12px] font-semibold text-white rounded-md transition-colors disabled:opacity-30"
          style={{ background: '#22c55e' }}
          onMouseEnter={(e) => e.currentTarget.style.background = '#4ade80'}
          onMouseLeave={(e) => e.currentTarget.style.background = '#22c55e'}
        >
          {runStatus === "Running..." ? "Running..." : "Run"}
        </button>
        <button
          onClick={onSave}
          className="px-3 py-1 text-[12px] font-semibold text-white rounded-md transition-colors"
          style={{ background: 'var(--accent)' }}
          onMouseEnter={(e) => e.currentTarget.style.background = '#60a5fa'}
          onMouseLeave={(e) => e.currentTarget.style.background = 'var(--accent)'}
          title="Ctrl+S"
        >
          Save
        </button>
      </div>
    </header>
  )
}
