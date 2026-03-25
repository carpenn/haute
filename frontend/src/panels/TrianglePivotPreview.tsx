/**
 * TrianglePivotPreview — bottom-panel viewer for Triangle_Viewer nodes.
 *
 * Layout:
 *   ┌─ drag-resize handle ─────────────────────────────────────────────────┐
 *   │ [Icon] Triangle Viewer  [Incr|Cumul]  Origin grain [Y|Q|M]  Dev [Y|Q|M] │
 *   ├─ [Triangle] [Chart] ─────────────────────────────────────────────────┤
 *   │  <tab content>                                                        │
 *   └──────────────────────────────────────────────────────────────────────┘
 *
 * Controls in the header trigger a call to `POST /api/pipeline/triangle`,
 * which uses the Python chainladder package to return structured triangle data.
 * The Triangle tab shows a pivot table; the Chart tab shows a line chart with
 * one series per origin period.
 */

import { useMemo, useState, useEffect, useRef, useCallback } from "react"
import { Grid3X3 } from "lucide-react"
import type { PreviewData } from "./DataPreview"
import type { GraphPayload, TriangleResponse } from "../api/types"
import { fetchTriangle } from "../api/client"
import { useDragResize } from "../hooks/useDragResize"

// ── Series colours for the line chart ─────────────────────────────────────

const SERIES_COLOURS = [
  "#38bdf8", "#fb923c", "#4ade80", "#f472b6", "#a78bfa",
  "#fbbf24", "#34d399", "#f87171", "#818cf8", "#2dd4bf",
]

// ── Grain / toggle options ─────────────────────────────────────────────────

const GRAIN_OPTIONS = ["Y", "Q", "M"] as const
type Grain = typeof GRAIN_OPTIONS[number]
type TriangleType = "incremental" | "cumulative"

// ── Props ──────────────────────────────────────────────────────────────────

interface TrianglePivotPreviewProps {
  data: PreviewData | null
  config: Record<string, unknown>
  nodeId: string
  getGraph: () => GraphPayload
}

// ── Component ─────────────────────────────────────────────────────────────

export default function TrianglePivotPreview({
  data,
  config,
  nodeId,
  getGraph,
}: TrianglePivotPreviewProps) {
  const originField = String(config.originField ?? "")
  const developmentField = String(config.developmentField ?? "")
  const valueField = String(config.valueField ?? "")
  const allMapped = !!(originField && developmentField && valueField)

  // ── Header control state ─────────────────────────────────────────────────

  const [triangleType, setTriangleType] = useState<TriangleType>("incremental")
  const [originGrain, setOriginGrain] = useState<Grain>("Y")
  const [devGrain, setDevGrain] = useState<Grain>("Y")
  const [activeTab, setActiveTab] = useState<"triangle" | "chart">("triangle")

  // ── Triangle data fetching ───────────────────────────────────────────────

  const [triData, setTriData] = useState<TriangleResponse | null>(null)
  const [triLoading, setTriLoading] = useState(false)
  const [triError, setTriError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  const loadTriangle = useCallback(() => {
    if (!allMapped || !nodeId) return
    if (data?.status === "error") return

    abortRef.current?.abort()
    const ctrl = new AbortController()
    abortRef.current = ctrl

    setTriLoading(true)
    setTriError(null)

    fetchTriangle(getGraph(), nodeId, originGrain, devGrain, triangleType, "live", {
      signal: ctrl.signal,
      timeout: 120_000,
    })
      .then((result) => {
        if (ctrl.signal.aborted) return
        if (result.status === "error") {
          setTriError(result.error ?? "Triangle processing failed")
          setTriData(null)
        } else {
          setTriData(result)
        }
      })
      .catch((err) => {
        if (ctrl.signal.aborted) return
        setTriError(err?.detail ?? err?.message ?? String(err))
      })
      .finally(() => {
        if (!ctrl.signal.aborted) setTriLoading(false)
      })
  }, [allMapped, nodeId, getGraph, originGrain, devGrain, triangleType, data?.status])

  // Re-fetch whenever controls or underlying preview data changes
  useEffect(() => {
    const timer = window.setTimeout(loadTriangle, 0)
    return () => {
      window.clearTimeout(timer)
      abortRef.current?.abort()
    }
  }, [loadTriangle])

  // ── Drag-resize ──────────────────────────────────────────────────────────

  const { height, containerRef, onDragStart } = useDragResize({
    initialHeight: 300,
    minHeight: 160,
    maxHeight: 700,
  })

  // ── Derive pivot structure ────────────────────────────────────────────────

  const pivotData = useMemo(() => {
    if (!triData || triData.status !== "ok") return null
    const { origins, developments, values } = triData
    if (origins.length === 0) return null
    // Build sparse cells map for easy lookup
    const cells = new Map<string, Map<string, number | null>>()
    origins.forEach((origin, ri) => {
      const devMap = new Map<string, number | null>()
      developments.forEach((dev, ci) => {
        devMap.set(dev, values[ri]?.[ci] ?? null)
      })
      cells.set(origin, devMap)
    })
    return { origins, developments, cells, raw: values }
  }, [triData])

  // ── Header bar (drag handle + controls) ──────────────────────────────────

  const headerBar = (
    <div
      style={{
        height: 36,
        borderTop: "1px solid var(--border)",
        background: "var(--bg-panel)",
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "0 12px",
        cursor: "ns-resize",
        userSelect: "none",
        flexShrink: 0,
      }}
      onMouseDown={onDragStart}
    >
      {/* Icon + title */}
      <Grid3X3 size={13} style={{ color: "var(--text-muted)", flexShrink: 0 }} />
      <span
        className="text-xs font-medium"
        style={{ color: "var(--text-secondary)", flexShrink: 0 }}
      >
        {data?.nodeLabel ?? "Triangle Viewer"}
      </span>

      {/* Spacer */}
      <div style={{ flex: 1 }} />

      {/* Controls — stop drag propagation so clicks work */}
      <div
        style={{ display: "flex", alignItems: "center", gap: 6, cursor: "default" }}
        onMouseDown={(e) => e.stopPropagation()}
      >
        {/* Incremental / Cumulative toggle */}
        <ToggleButton
          left="Incremental"
          right="Cumulative"
          value={triangleType}
          onChange={(v) => setTriangleType(v as TriangleType)}
        />

        {/* Origin grain */}
        <label
          className="text-[10px]"
          style={{ color: "var(--text-muted)", letterSpacing: "0.04em" }}
        >
          ORIGIN
        </label>
        <GrainSelect value={originGrain} onChange={setOriginGrain} />

        {/* Development grain */}
        <label
          className="text-[10px]"
          style={{ color: "var(--text-muted)", letterSpacing: "0.04em" }}
        >
          DEV
        </label>
        <GrainSelect value={devGrain} onChange={setDevGrain} />
      </div>
    </div>
  )

  // ── Tab bar ───────────────────────────────────────────────────────────────

  const tabBar = (
    <div
      style={{
        height: 30,
        display: "flex",
        alignItems: "stretch",
        background: "var(--bg-surface)",
        borderBottom: "1px solid var(--border)",
        flexShrink: 0,
      }}
    >
      {(["triangle", "chart"] as const).map((tab) => (
        <button
          key={tab}
          onClick={() => setActiveTab(tab)}
          style={{
            padding: "0 14px",
            fontSize: 11,
            fontWeight: activeTab === tab ? 700 : 400,
            background: "transparent",
            border: "none",
            borderBottom: activeTab === tab ? "2px solid var(--accent)" : "2px solid transparent",
            color: activeTab === tab ? "var(--text-primary)" : "var(--text-muted)",
            cursor: "pointer",
            textTransform: "capitalize" as const,
            letterSpacing: "0.03em",
          }}
        >
          {tab === "triangle" ? "Triangle" : "Chart"}
        </button>
      ))}
    </div>
  )

  // ── Early-return states ───────────────────────────────────────────────────

  if (!data) return null

  if (data.status === "error") {
    return (
      <div style={{ borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }}>
        {headerBar}
        <div className="px-4 py-3 text-xs" style={{ color: "#ef4444" }}>
          {data.error ?? "Preview error"}
        </div>
      </div>
    )
  }

  if (!allMapped) {
    return (
      <div style={{ borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }}>
        {headerBar}
        <div className="px-4 py-3 text-xs" style={{ color: "var(--text-muted)" }}>
          Map Origin Period, Development Period, and Value in the config panel to see the triangle.
        </div>
      </div>
    )
  }

  // ── Mounted view ──────────────────────────────────────────────────────────

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
      {tabBar}

      {/* Tab content */}
      <div style={{ flex: 1, overflow: "hidden", position: "relative" }}>
        {triLoading && (
          <div
            style={{
              position: "absolute",
              inset: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              background: "rgba(0,0,0,.4)",
              zIndex: 10,
              fontSize: 12,
              color: "var(--text-muted)",
            }}
          >
            Computing triangle…
          </div>
        )}

        {triError && !triLoading && (
          <div className="px-4 py-3 text-xs" style={{ color: "#ef4444" }}>
            {triError}
          </div>
        )}

        {!triError && !triLoading && !pivotData && (
          <div className="px-4 py-3 text-xs" style={{ color: "var(--text-muted)" }}>
            No triangle data — connect a data source and refresh the preview.
          </div>
        )}

        {!triError && pivotData && (
          <>
            {activeTab === "triangle" && (
              <div style={{ height: "100%", overflow: "auto" }}>
                <PivotTable
                  origins={pivotData.origins}
                  developments={pivotData.developments}
                  cells={pivotData.cells}
                  originField={originField}
                  developmentField={developmentField}
                />
              </div>
            )}
            {activeTab === "chart" && (
              <div style={{ height: "100%", overflow: "hidden" }}>
                <TriangleLineChart
                  origins={pivotData.origins}
                  developments={pivotData.developments}
                  values={triData!.values}
                />
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

// ── Sub-components ────────────────────────────────────────────────────────

function ToggleButton({
  left,
  right,
  value,
  onChange,
}: {
  left: string
  right: string
  value: string
  onChange: (v: string) => void
}) {
  return (
    <div
      style={{
        display: "inline-flex",
        border: "1px solid var(--border)",
        borderRadius: 5,
        overflow: "hidden",
        fontSize: 10,
        fontWeight: 600,
        letterSpacing: "0.04em",
      }}
    >
      {[left, right].map((label) => {
        const active = value === label.toLowerCase()
        return (
          <button
            key={label}
            onClick={() => onChange(label.toLowerCase())}
            style={{
              padding: "2px 8px",
              background: active ? "var(--accent)" : "transparent",
              color: active ? "#fff" : "var(--text-muted)",
              border: "none",
              cursor: "pointer",
              lineHeight: 1.6,
            }}
          >
            {label}
          </button>
        )
      })}
    </div>
  )
}

function GrainSelect({ value, onChange }: { value: Grain; onChange: (g: Grain) => void }) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value as Grain)}
      style={{
        fontSize: 11,
        padding: "1px 4px",
        borderRadius: 4,
        border: "1px solid var(--border)",
        background: "var(--bg-surface)",
        color: "var(--text-primary)",
        cursor: "pointer",
      }}
    >
      {GRAIN_OPTIONS.map((g) => (
        <option key={g} value={g}>
          {g}
        </option>
      ))}
    </select>
  )
}

// ── Pivot Table ───────────────────────────────────────────────────────────

function PivotTable({
  origins,
  developments,
  cells,
  originField,
  developmentField,
}: {
  origins: string[]
  developments: string[]
  cells: Map<string, Map<string, number | null>>
  originField: string
  developmentField: string
}) {
  return (
    <table style={{ borderCollapse: "collapse", fontSize: 12, minWidth: "100%", tableLayout: "auto" }}>
      <thead>
        <tr>
          <th style={TH_CORNER}>
            <span style={{ color: "var(--text-muted)", fontSize: 10 }}>
              {originField} ↓ / {developmentField} →
            </span>
          </th>
          {developments.map((dev) => (
            <th key={dev} style={TH}>
              {dev}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {origins.map((origin, rowIdx) => (
          <tr
            key={origin}
            style={{ background: rowIdx % 2 === 0 ? "transparent" : "rgba(255,255,255,.02)" }}
          >
            <td style={TD_HEADER}>{origin}</td>
            {developments.map((dev) => {
              const val = cells.get(origin)?.get(dev) ?? null
              return (
                <td key={dev} style={TD}>
                  {val != null ? val.toLocaleString(undefined, { maximumFractionDigits: 4 }) : ""}
                </td>
              )
            })}
          </tr>
        ))}
      </tbody>
    </table>
  )
}

// ── Line Chart ────────────────────────────────────────────────────────────

function TriangleLineChart({
  origins,
  developments,
  values,
}: {
  origins: string[]
  developments: string[]
  values: (number | null)[][]
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 600, h: 200 })

  useEffect(() => {
    if (!containerRef.current) return
    const ro = new ResizeObserver((entries) => {
      const e = entries[0]
      if (e) setDims({ w: e.contentRect.width, h: e.contentRect.height })
    })
    ro.observe(containerRef.current)
    return () => ro.disconnect()
  }, [])

  const PAD = { top: 20, right: 20, bottom: 52, left: 64 }
  const plotW = Math.max(dims.w - PAD.left - PAD.right, 1)
  const plotH = Math.max(dims.h - PAD.top - PAD.bottom, 1)

  // Collect all non-null values for y-scale
  const allVals = values.flat().filter((v): v is number => v !== null)
  const yMin = allVals.length ? Math.min(0, ...allVals) : 0
  const yMax = allVals.length ? Math.max(...allVals) : 1
  const yRange = yMax - yMin || 1

  // Dynamic Y-axis number format based on value magnitude
  const yTickFmt = useMemo(() => {
    const absMax = Math.max(Math.abs(yMin), Math.abs(yMax))
    if (absMax >= 1000) return (v: number) => v.toLocaleString(undefined, { maximumFractionDigits: 0 })
    if (absMax >= 1) return (v: number) => v.toLocaleString(undefined, { maximumFractionDigits: 2 })
    return (v: number) => v.toLocaleString(undefined, { maximumFractionDigits: 4 })
  }, [yMin, yMax])

  const xStep = developments.length > 1 ? plotW / (developments.length - 1) : 0

  function toX(ci: number) {
    return ci * xStep
  }
  function toY(v: number) {
    return plotH - ((v - yMin) / yRange) * plotH
  }

  // Y-axis ticks
  const Y_TICKS = 5
  const yTicks = Array.from({ length: Y_TICKS + 1 }, (_, i) => yMin + (yRange * i) / Y_TICKS)

  // Build one polyline per origin (skip null gaps)
  const seriesLines = origins.map((origin, ri) => {
    const segments: [number, number][][] = []
    let current: [number, number][] = []
    developments.forEach((_, ci) => {
      const v = values[ri]?.[ci] ?? null
      if (v !== null) {
        current.push([toX(ci), toY(v)])
      } else if (current.length) {
        segments.push(current)
        current = []
      }
    })
    if (current.length) segments.push(current)
    return { origin, segments, colour: SERIES_COLOURS[ri % SERIES_COLOURS.length] }
  })

  const svgW = dims.w
  const svgH = dims.h
  const LEGEND_ITEM_W = 80
  const LEGEND_MAX_COLS = Math.max(1, Math.floor(plotW / LEGEND_ITEM_W))

  return (
    <div ref={containerRef} style={{ width: "100%", height: "100%", position: "relative" }}>
      <svg
        width={svgW}
        height={svgH}
        style={{ display: "block", fontFamily: "inherit", overflow: "visible" }}
      >
        <g transform={`translate(${PAD.left},${PAD.top})`}>
          {/* Y-axis gridlines + labels */}
          {yTicks.map((v, i) => {
            const y = toY(v)
            return (
              <g key={i}>
                <line x1={0} y1={y} x2={plotW} y2={y} stroke="rgba(255,255,255,.07)" strokeWidth={1} />
                <text
                  x={-6}
                  y={y}
                  textAnchor="end"
                  dominantBaseline="middle"
                  fill="var(--text-muted)"
                  fontSize={9}
                >
                  {yTickFmt(v)}
                </text>
              </g>
            )
          })}

          {/* X-axis labels */}
          {developments.map((dev, ci) => (
            <text
              key={dev}
              x={toX(ci)}
              y={plotH + 16}
              textAnchor={ci === 0 ? "start" : ci === developments.length - 1 ? "end" : "middle"}
              fill="var(--text-muted)"
              fontSize={9}
              transform={`rotate(-30,${toX(ci)},${plotH + 16})`}
            >
              {dev}
            </text>
          ))}

          {/* Axis lines */}
          <line x1={0} y1={0} x2={0} y2={plotH} stroke="var(--border)" strokeWidth={1} />
          <line x1={0} y1={plotH} x2={plotW} y2={plotH} stroke="var(--border)" strokeWidth={1} />

          {/* Series lines */}
          {seriesLines.map(({ origin, segments, colour }) =>
            segments.map((seg, si) => (
              <polyline
                key={`${origin}-${si}`}
                points={seg.map(([x, y]) => `${x},${y}`).join(" ")}
                fill="none"
                stroke={colour}
                strokeWidth={1.5}
                strokeLinejoin="round"
                strokeLinecap="round"
              />
            ))
          )}

          {/* Data points */}
          {seriesLines.map(({ origin, segments, colour }) =>
            segments.flatMap((seg, si) =>
              seg.map(([x, y], pi) => (
                <circle
                  key={`${origin}-${si}-${pi}`}
                  cx={x}
                  cy={y}
                  r={2.5}
                  fill={colour}
                  stroke="var(--bg-panel)"
                  strokeWidth={1}
                />
              ))
            )
          )}

          {/* Legend */}
          {seriesLines.map(({ origin, colour }, i) => {
            const col = i % LEGEND_MAX_COLS
            const row = Math.floor(i / LEGEND_MAX_COLS)
            const lx = col * LEGEND_ITEM_W
            const ly = plotH + 36 + row * 14
            return (
              <g key={origin} transform={`translate(${lx},${ly})`}>
                <line x1={0} y1={4} x2={12} y2={4} stroke={colour} strokeWidth={2} />
                <circle cx={6} cy={4} r={2.5} fill={colour} />
                <text x={15} y={8} fontSize={9} fill="var(--text-secondary)">
                  {origin}
                </text>
              </g>
            )
          })}
        </g>
      </svg>
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
  fontSize: 11,
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
