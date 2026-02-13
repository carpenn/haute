import { useState, useCallback, useRef } from "react"
import { X, ChevronDown, ChevronUp, AlertCircle, CheckCircle2, Table2 } from "lucide-react"
import { getDtypeColor } from "../utils/dtypeColors"

interface Column {
  name: string
  dtype: string
}

export interface PreviewData {
  nodeId: string
  nodeLabel: string
  status: "ok" | "error" | "loading"
  row_count: number
  column_count: number
  columns: Column[]
  preview: Record<string, unknown>[]
  error: string | null
}

interface DataPreviewProps {
  data: PreviewData | null
  onClose: () => void
}


function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "null"
  if (typeof value === "number") {
    if (Number.isInteger(value)) return value.toLocaleString()
    return value.toLocaleString(undefined, { maximumFractionDigits: 4 })
  }
  return String(value)
}

export default function DataPreview({ data, onClose }: DataPreviewProps) {
  const [collapsed, setCollapsed] = useState(false)
  const [height, setHeight] = useState(256)
  const dragging = useRef(false)

  const onDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    dragging.current = true
    const startY = e.clientY
    const startH = height
    const onMove = (ev: MouseEvent) => {
      if (!dragging.current) return
      const newH = Math.max(120, Math.min(600, startH + (startY - ev.clientY)))
      setHeight(newH)
    }
    const onUp = () => {
      dragging.current = false
      document.removeEventListener("mousemove", onMove)
      document.removeEventListener("mouseup", onUp)
    }
    document.addEventListener("mousemove", onMove)
    document.addEventListener("mouseup", onUp)
  }, [height])

  if (!data) return null

  if (collapsed) {
    return (
      <div className="h-8 flex items-center px-4 shrink-0" style={{ borderTop: '1px solid var(--border)', background: 'var(--bg-panel)' }}>
        <button
          onClick={() => setCollapsed(false)}
          className="flex items-center gap-2 text-xs"
          style={{ color: 'var(--text-secondary)' }}
        >
          <ChevronUp size={14} />
          <Table2 size={14} />
          <span className="font-medium">{data.nodeLabel}</span>
          {data.status === "ok" && (
            <span style={{ color: 'var(--text-muted)' }}>
              {data.row_count.toLocaleString()} rows · {data.column_count} cols
            </span>
          )}
        </button>
      </div>
    )
  }

  return (
    <div style={{ height, borderTop: '1px solid var(--border)', background: 'var(--bg-panel)' }} className="flex flex-col shrink-0 relative">
      {/* Drag handle */}
      <div
        onMouseDown={onDragStart}
        className="absolute top-0 left-0 right-0 h-1 cursor-ns-resize z-10 transition-colors"
        style={{ background: 'transparent' }}
        onMouseEnter={(e) => e.currentTarget.style.background = 'var(--accent-soft)'}
        onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
      />
      {/* Header bar */}
      <div className="h-9 flex items-center px-4 shrink-0 gap-2" style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
        <Table2 size={14} style={{ color: 'var(--text-muted)' }} />
        <span className="text-xs font-bold" style={{ color: 'var(--text-primary)' }}>{data.nodeLabel}</span>

        {data.status === "ok" && (
          <>
            <CheckCircle2 size={13} className="ml-1" style={{ color: '#22c55e' }} />
            <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
              {data.row_count.toLocaleString()} rows · {data.column_count} cols
            </span>
          </>
        )}

        {data.status === "error" && (
          <>
            <AlertCircle size={13} className="ml-1" style={{ color: '#ef4444' }} />
            <span className="text-[11px] truncate" style={{ color: '#ef4444' }}>{data.error}</span>
          </>
        )}

        {data.status === "loading" && (
          <span className="text-[11px] animate-pulse" style={{ color: 'var(--text-muted)' }}>Running...</span>
        )}

        <div className="ml-auto flex items-center gap-1">
          <button
            onClick={() => setCollapsed(true)}
            className="p-1 rounded transition-colors"
            style={{ color: 'var(--text-muted)' }}
            onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-hover)'}
            onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
          >
            <ChevronDown size={14} />
          </button>
          <button onClick={onClose} className="p-1 rounded transition-colors"
            style={{ color: 'var(--text-muted)' }}
            onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-hover)'}
            onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
          >
            <X size={14} />
          </button>
        </div>
      </div>

      {/* Data table */}
      {data.status === "loading" ? (
        <div className="flex-1 flex items-center justify-center">
          <div className="text-xs animate-pulse" style={{ color: 'var(--text-muted)' }}>Executing pipeline...</div>
        </div>
      ) : data.status === "error" ? (
        <div className="flex-1 flex items-center justify-center p-4">
          <div className="text-center">
            <AlertCircle size={24} className="mx-auto mb-2" style={{ color: '#ef4444', opacity: 0.5 }} />
            <div className="text-xs max-w-md" style={{ color: '#ef4444' }}>{data.error}</div>
          </div>
        </div>
      ) : (
        <div className="flex-1 overflow-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 z-10" style={{ background: 'var(--bg-elevated)' }}>
              <tr>
                <th className="px-3 py-1.5 text-left text-[10px] font-semibold uppercase tracking-wider w-10"
                  style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border)' }}>
                  #
                </th>
                {data.columns.map((col) => (
                  <th
                    key={col.name}
                    className="px-3 py-1.5 text-left whitespace-nowrap"
                    style={{ borderBottom: '1px solid var(--border)' }}
                  >
                    <div className="font-semibold" style={{ color: 'var(--text-primary)' }}>{col.name}</div>
                    <div className={`text-[10px] font-normal ${getDtypeColor(col.dtype)}`}>
                      {col.dtype}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.preview.map((row, i) => (
                <tr
                  key={i}
                  style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,.02)' }}
                >
                  <td className="px-3 py-1 font-mono" style={{ color: 'var(--text-muted)', borderRight: '1px solid var(--border)' }}>
                    {i + 1}
                  </td>
                  {data.columns.map((col) => (
                    <td
                      key={col.name}
                      className="px-3 py-1 font-mono whitespace-nowrap max-w-[200px] truncate"
                      style={{ color: 'var(--text-secondary)' }}
                    >
                      <span style={row[col.name] === null ? { color: 'var(--text-muted)', fontStyle: 'italic' } : undefined}>
                        {formatCell(row[col.name])}
                      </span>
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>

          {data.row_count > data.preview.length && (
            <div className="px-3 py-1.5 text-[10px] text-center" style={{ color: 'var(--text-muted)', borderTop: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
              Showing {data.preview.length} of {data.row_count.toLocaleString()} rows
            </div>
          )}
        </div>
      )}
    </div>
  )
}
