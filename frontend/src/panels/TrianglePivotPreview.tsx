/**
 * TrianglePivotPreview — replaces the standard DataPreview bottom panel when
 * a Triangle_Viewer node is selected.  Renders a cross-tab (pivot) table:
 *   Rows    = unique Origin Period values
 *   Columns = unique Development Period values
 *   Cells   = SUM of Value field
 */

import { useMemo } from "react"
import { Grid3X3 } from "lucide-react"
import type { PreviewData } from "./DataPreview"
import { buildTrianglePivot } from "../utils/trianglePivot"
import { useDragResize } from "../hooks/useDragResize"

interface TrianglePivotPreviewProps {
  data: PreviewData | null
  config: Record<string, unknown>
}

export default function TrianglePivotPreview({ data, config }: TrianglePivotPreviewProps) {
  const originField = String(config.originField ?? "")
  const developmentField = String(config.developmentField ?? "")
  const valueField = String(config.valueField ?? "")

  const allMapped = originField && developmentField && valueField

  const pivot = useMemo(() => {
    if (!data || data.status !== "ok" || !allMapped) return null
    return buildTrianglePivot(data.preview, originField, developmentField, valueField)
  }, [data, originField, developmentField, valueField, allMapped])

  const { height, containerRef, onDragStart } = useDragResize({
    initialHeight: 256,
    minHeight: 120,
    maxHeight: 600,
  })

  // ── Header bar ────────────────────────────────────────────────────────────

  const headerBar = (
    <div
      className="flex items-center gap-2 px-4 shrink-0"
      style={{
        height: 32,
        borderTop: "1px solid var(--border)",
        background: "var(--bg-panel)",
        cursor: "ns-resize",
        userSelect: "none",
      }}
      onMouseDown={onDragStart}
    >
      <Grid3X3 size={13} style={{ color: "var(--text-muted)" }} />
      <span className="text-xs font-medium" style={{ color: "var(--text-secondary)" }}>
        {data?.nodeLabel ?? "Triangle Viewer"}
      </span>
      {pivot && (
        <span className="text-xs" style={{ color: "var(--text-muted)" }}>
          {pivot.origins.length} origins · {pivot.developments.length} development periods
        </span>
      )}
    </div>
  )

  // ── No data yet ───────────────────────────────────────────────────────────

  if (!data) return null

  // ── Error state ───────────────────────────────────────────────────────────

  if (data.status === "error") {
    return (
      <div style={{ borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }}>
        {headerBar}
        <div
          className="px-4 py-3 text-xs"
          style={{ color: "#ef4444" }}
        >
          {data.error ?? "Preview error"}
        </div>
      </div>
    )
  }

  // ── Mappings not yet configured ───────────────────────────────────────────

  if (!allMapped) {
    return (
      <div style={{ borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }}>
        {headerBar}
        <div
          className="px-4 py-3 text-xs"
          style={{ color: "var(--text-muted)" }}
        >
          Map Origin Period, Development Period, and Value in the config panel to see the triangle preview.
        </div>
      </div>
    )
  }

  // ── Empty pivot ───────────────────────────────────────────────────────────

  if (!pivot || pivot.origins.length === 0) {
    return (
      <div style={{ borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }}>
        {headerBar}
        <div
          className="px-4 py-3 text-xs"
          style={{ color: "var(--text-muted)" }}
        >
          No data to display. Refresh preview after connecting a Data Source.
        </div>
      </div>
    )
  }

  // ── Pivot table ───────────────────────────────────────────────────────────

  return (
    <div
      ref={containerRef}
      style={{
        height,
        borderTop: "1px solid var(--border)",
        background: "var(--bg-panel)",
        display: "flex",
        flexDirection: "column",
        flexShrink: 0,
      }}
    >
      {headerBar}
      <div style={{ flex: 1, overflow: "auto" }}>
        <table
          style={{
            borderCollapse: "collapse",
            fontSize: "12px",
            minWidth: "100%",
            tableLayout: "auto",
          }}
        >
          <thead>
            <tr>
              <th style={TH_CORNER}>
                <span style={{ color: "var(--text-muted)", fontSize: "10px" }}>
                  {originField} ↓ / {developmentField} →
                </span>
              </th>
              {pivot.developments.map((dev) => (
                <th key={dev} style={TH}>
                  {dev}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pivot.origins.map((origin, rowIdx) => (
              <tr
                key={origin}
                style={{
                  background:
                    rowIdx % 2 === 0 ? "transparent" : "rgba(255,255,255,.02)",
                }}
              >
                <td style={TD_HEADER}>{origin}</td>
                {pivot.developments.map((dev) => {
                  const val = pivot.cells.get(origin)?.get(dev)
                  return (
                    <td key={dev} style={TD}>
                      {val != null ? val.toLocaleString() : ""}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Shared cell styles ────────────────────────────────────────────────────

const CELL_BASE: React.CSSProperties = {
  padding: "5px 10px",
  borderBottom: "1px solid var(--border)",
  borderRight: "1px solid var(--border)",
  whiteSpace: "nowrap",
}

const TH: React.CSSProperties = {
  ...CELL_BASE,
  background: "var(--bg-surface)",
  color: "var(--text-muted)",
  fontWeight: 700,
  fontSize: "11px",
  textTransform: "uppercase" as const,
  letterSpacing: "0.04em",
  textAlign: "right",
  position: "sticky",
  top: 0,
  zIndex: 1,
}

const TH_CORNER: React.CSSProperties = {
  ...TH,
  textAlign: "left",
  minWidth: 90,
  left: 0,
  zIndex: 2,
}

const TD: React.CSSProperties = {
  ...CELL_BASE,
  textAlign: "right",
  color: "var(--text-primary)",
  fontVariantNumeric: "tabular-nums",
}

const TD_HEADER: React.CSSProperties = {
  ...CELL_BASE,
  textAlign: "left",
  fontWeight: 600,
  color: "var(--text-secondary)",
  background: "var(--bg-surface)",
  position: "sticky",
  left: 0,
}
