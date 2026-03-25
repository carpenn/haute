/**
 * EdaPreview — bottom-panel viewer for EDA Viewer nodes.
 *
 * Renders five tabs:
 *   1. Descriptive Statistics
 *   2. Outliers / Inliers
 *   3. Disguised Missings
 *   4. Correlations
 *   5. One-way Charts
 *
 * All data is fetched from POST /api/pipeline/eda and POST /api/pipeline/eda/one_way.
 */

import { useState, useEffect, useRef, useCallback } from "react"
import { SearchCheck, ChevronDown, ChevronUp, RefreshCw } from "lucide-react"
import type { PreviewData } from "./DataPreview"
import type { GraphPayload, EdaResponse, EdaOneWayResponse } from "../api/types"
import { fetchEda, fetchEdaOneWay } from "../api/client"
import { useDragResize } from "../hooks/useDragResize"

// ── Role constants ─────────────────────────────────────────────────────────

const ONE_WAY_X_ROLES = new Set([
  "underwriting_date", "accident_date", "reporting_date", "transaction_date",
  "covariate", "fold",
])

// ── Tab definitions ────────────────────────────────────────────────────────

const TABS = [
  "Descriptive Statistics",
  "Outliers/Inliers",
  "Disguised Missings",
  "Correlations",
  "One-way Charts",
] as const
type Tab = typeof TABS[number]

// ── Shared helpers ─────────────────────────────────────────────────────────

function fmtPct(v: number): string {
  return (v * 100).toFixed(2) + "%"
}

function fmtNum(v: number | null | undefined, dp = 4): string {
  if (v == null) return "—"
  return v.toFixed(dp)
}

function fmtVal(v: number | string | null): string {
  if (v === null) return "null"
  return String(v)
}

function CellNum({ v, dp = 4 }: { v: number | null | undefined; dp?: number }) {
  if (v == null) return <span style={{ color: "var(--text-muted)" }}>—</span>
  return <span>{fmtNum(v, dp)}</span>
}

// Heat-map colour for correlation values [-1 .. 1]
function corrColor(v: number | null): string {
  if (v == null) return "transparent"
  const intensity = Math.abs(v)
  if (v > 0) return `rgba(56,189,248,${intensity * 0.7})`
  return `rgba(251,113,133,${intensity * 0.7})`
}

// ── Props ──────────────────────────────────────────────────────────────────

interface EdaPreviewProps {
  data: PreviewData | null
  config: Record<string, unknown>
  graph: GraphPayload
  nodeId: string
}

// ── Loading / Error helpers ────────────────────────────────────────────────

function LoadingSpinner() {
  return (
    <div className="flex items-center gap-2 p-4" style={{ color: "var(--text-muted)" }}>
      <RefreshCw size={14} className="animate-spin" />
      <span className="text-xs">Analysing dataset…</span>
    </div>
  )
}

function ErrorBox({ msg }: { msg: string }) {
  return (
    <div
      className="m-4 p-3 rounded text-xs"
      style={{ background: "rgba(239,68,68,.08)", border: "1px solid rgba(239,68,68,.2)", color: "#ef4444" }}
    >
      {msg}
    </div>
  )
}

// ── Descriptive Statistics Tab ─────────────────────────────────────────────

function DescriptiveTab({ rows }: { rows: EdaResponse["descriptive"] }) {
  if (rows.length === 0) return <p className="p-4 text-xs" style={{ color: "var(--text-muted)" }}>No fields with assigned roles.</p>
  return (
    <div className="overflow-auto">
      <table className="w-full text-[11px] border-collapse">
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-input)" }}>
            {["Field","Role","Dtype","Count","Missing","Missing%","Mean","Std","Min","Q25","Median","Q75","Max","Skew","N Unique","Distribution"].map(h => (
              <th key={h} className="px-2 py-1.5 text-left font-semibold whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={r.field} style={{ borderBottom: "1px solid var(--border)", background: i % 2 === 0 ? "transparent" : "rgba(255,255,255,.02)" }}>
              <td className="px-2 py-1.5 font-mono whitespace-nowrap" style={{ color: "var(--text-primary)" }}>{r.field}</td>
              <td className="px-2 py-1.5 whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{r.role}</td>
              <td className="px-2 py-1.5 font-mono whitespace-nowrap text-[10px]" style={{ color: "var(--text-muted)" }}>{r.dtype}</td>
              <td className="px-2 py-1.5 text-right">{r.count.toLocaleString()}</td>
              <td className="px-2 py-1.5 text-right">{r.missing_count.toLocaleString()}</td>
              <td className="px-2 py-1.5 text-right">{fmtPct(r.missing_prop)}</td>
              <td className="px-2 py-1.5 text-right"><CellNum v={r.mean} /></td>
              <td className="px-2 py-1.5 text-right"><CellNum v={r.std} /></td>
              <td className="px-2 py-1.5 text-right">{typeof r.min === "string" ? r.min : <CellNum v={r.min} />}</td>
              <td className="px-2 py-1.5 text-right"><CellNum v={r.q25} /></td>
              <td className="px-2 py-1.5 text-right"><CellNum v={r.median} /></td>
              <td className="px-2 py-1.5 text-right"><CellNum v={r.q75} /></td>
              <td className="px-2 py-1.5 text-right">{typeof r.max === "string" ? r.max : <CellNum v={r.max} />}</td>
              <td className="px-2 py-1.5 text-right"><CellNum v={r.skewness} dp={2} /></td>
              <td className="px-2 py-1.5 text-right">{r.n_unique ?? "—"}</td>
              <td className="px-2 py-1.5">
                {r.sparkline
                  ? <img src={r.sparkline} alt="distribution" style={{ height: 20, width: 60, color: "#38bdf8", display: "block" }} />
                  : r.top_value != null
                    ? <span className="text-[10px] font-mono" style={{ color: "var(--text-muted)" }}>{String(r.top_value)}</span>
                    : null}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Outliers / Inliers Tab ─────────────────────────────────────────────────

function OutliersTab({ rows }: { rows: EdaResponse["outliers"] }) {
  if (rows.length === 0) return <p className="p-4 text-xs" style={{ color: "var(--text-muted)" }}>No fields with assigned roles.</p>
  return (
    <div className="overflow-auto">
      <table className="w-full text-[11px] border-collapse">
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-input)" }}>
            {["Field","Role","Dtype","Outlier Values","Outlier Count","Outlier%","Inlier Count","Inlier%"].map(h => (
              <th key={h} className="px-2 py-1.5 text-left font-semibold whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={r.field} style={{ borderBottom: "1px solid var(--border)", background: i % 2 === 0 ? "transparent" : "rgba(255,255,255,.02)" }}>
              <td className="px-2 py-1.5 font-mono whitespace-nowrap" style={{ color: "var(--text-primary)" }}>{r.field}</td>
              <td className="px-2 py-1.5 whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{r.role}</td>
              <td className="px-2 py-1.5 font-mono text-[10px]" style={{ color: "var(--text-muted)" }}>{r.dtype}</td>
              <td className="px-2 py-1.5 max-w-[200px]">
                {r.outlier_values.length === 0
                  ? <span style={{ color: "var(--text-muted)" }}>none</span>
                  : <span className="font-mono text-[10px] break-all">{r.outlier_values.map(fmtVal).join(", ")}</span>}
              </td>
              <td className="px-2 py-1.5 text-right">{r.outlier_count.toLocaleString()}</td>
              <td className="px-2 py-1.5 text-right" style={{ color: r.outlier_prop > 0.05 ? "#f87171" : "inherit" }}>{fmtPct(r.outlier_prop)}</td>
              <td className="px-2 py-1.5 text-right">{r.inlier_count.toLocaleString()}</td>
              <td className="px-2 py-1.5 text-right">{fmtPct(r.inlier_prop)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Disguised Missings Tab ─────────────────────────────────────────────────

function DisguisedMissingsTab({ rows }: { rows: EdaResponse["disguised_missings"] }) {
  if (rows.length === 0) return <p className="p-4 text-xs" style={{ color: "var(--text-muted)" }}>No fields with assigned roles.</p>
  return (
    <div className="overflow-auto">
      <table className="w-full text-[11px] border-collapse">
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-input)" }}>
            {["Field","Role","Dtype","Missing Values","Missing Count","Missing%"].map(h => (
              <th key={h} className="px-2 py-1.5 text-left font-semibold whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={r.field} style={{ borderBottom: "1px solid var(--border)", background: i % 2 === 0 ? "transparent" : "rgba(255,255,255,.02)" }}>
              <td className="px-2 py-1.5 font-mono whitespace-nowrap" style={{ color: "var(--text-primary)" }}>{r.field}</td>
              <td className="px-2 py-1.5 whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{r.role}</td>
              <td className="px-2 py-1.5 font-mono text-[10px]" style={{ color: "var(--text-muted)" }}>{r.dtype}</td>
              <td className="px-2 py-1.5 max-w-[200px]">
                {r.missing_values.length === 0
                  ? <span style={{ color: "var(--text-muted)" }}>none detected</span>
                  : <span className="font-mono text-[10px] break-all">{r.missing_values.map(fmtVal).join(", ")}</span>}
              </td>
              <td className="px-2 py-1.5 text-right">{r.missing_count.toLocaleString()}</td>
              <td className="px-2 py-1.5 text-right" style={{ color: r.missing_prop > 0.05 ? "#f87171" : "inherit" }}>{fmtPct(r.missing_prop)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Correlations Tab ───────────────────────────────────────────────────────

function CorrelationsTab({ corr }: { corr: EdaResponse["correlations"] }) {
  const [activeCorr, setActiveCorr] = useState<"pearson" | "spearman" | "cramer">("pearson")
  const { fields } = corr
  if (fields.length === 0) {
    return <p className="p-4 text-xs" style={{ color: "var(--text-muted)" }}>No analysable fields with assigned roles.</p>
  }

  const matrix = corr[activeCorr]

  return (
    <div className="p-3 space-y-3">
      {/* Type selector */}
      <div className="flex gap-2">
        {(["pearson", "spearman", "cramer"] as const).map(k => (
          <button
            key={k}
            onClick={() => setActiveCorr(k)}
            className="px-3 py-1 rounded text-[11px] font-medium transition-colors"
            style={{
              background: activeCorr === k ? "rgba(124,58,237,.15)" : "var(--bg-input)",
              border: `1px solid ${activeCorr === k ? "rgba(124,58,237,.4)" : "var(--border)"}`,
              color: activeCorr === k ? "#a78bfa" : "var(--text-muted)",
            }}
          >
            {k === "cramer" ? "Cramér's V" : k.charAt(0).toUpperCase() + k.slice(1)}
          </button>
        ))}
      </div>

      {/* Correlation matrix heatmap */}
      <div className="overflow-auto">
        <table className="text-[10px] border-collapse">
          <thead>
            <tr>
              <th className="px-2 py-1" />
              {fields.map(f => (
                <th key={f} className="px-1 py-1 font-mono whitespace-nowrap" style={{ color: "var(--text-muted)", writingMode: "vertical-rl", transform: "rotate(180deg)", maxWidth: 80 }}>{f}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {fields.map((rowField, ri) => (
              <tr key={rowField}>
                <td className="px-2 py-0.5 font-mono whitespace-nowrap text-right" style={{ color: "var(--text-muted)" }}>{rowField}</td>
                {fields.map((_, ci) => {
                  const v = matrix[ri]?.[ci] ?? null
                  return (
                    <td
                      key={ci}
                      className="px-1 py-0.5 text-center"
                      style={{
                        background: corrColor(v),
                        border: "1px solid var(--border)",
                        minWidth: 40,
                        color: "var(--text-primary)",
                      }}
                      title={`${rowField} × ${fields[ci]}: ${v != null ? v.toFixed(4) : "—"}`}
                    >
                      {v != null ? v.toFixed(2) : "—"}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex items-center gap-4 text-[10px]" style={{ color: "var(--text-muted)" }}>
        <span>Colour scale:</span>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
          <span style={{ width: 16, height: 12, background: "rgba(251,113,133,.7)", borderRadius: 2, display: "inline-block" }} /> negative
        </span>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
          <span style={{ width: 16, height: 12, background: "rgba(56,189,248,.7)", borderRadius: 2, display: "inline-block" }} /> positive
        </span>
      </div>
    </div>
  )
}

// ── One-way Charts Tab ─────────────────────────────────────────────────────

function OneWayTab({
  graph,
  nodeId,
  fieldRoles,
}: {
  graph: GraphPayload
  nodeId: string
  fieldRoles: Record<string, string>
}) {
  const xOptions = Object.entries(fieldRoles)
    .filter(([, role]) => ONE_WAY_X_ROLES.has(role))
    .map(([field]) => field)

  const [xField, setXField] = useState<string>(xOptions[0] ?? "")
  const [chartData, setChartData] = useState<EdaOneWayResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  const loadChart = useCallback((field: string) => {
    if (!field) return
    abortRef.current?.abort()
    const ctrl = new AbortController()
    abortRef.current = ctrl
    setLoading(true)
    setError(null)
    fetchEdaOneWay(graph, nodeId, fieldRoles, field, "live", { signal: ctrl.signal })
      .then(res => {
        if (ctrl.signal.aborted) return
        if (res.status === "error") setError(res.error ?? "Failed to load chart")
        else setChartData(res)
      })
      .catch(err => { if (!ctrl.signal.aborted) setError(err?.message ?? String(err)) })
      .finally(() => { if (!ctrl.signal.aborted) setLoading(false) })
  }, [graph, nodeId, fieldRoles])

  useEffect(() => {
    if (xField) loadChart(xField)
    return () => { abortRef.current?.abort() }
  }, [xField, loadChart])

  if (xOptions.length === 0) {
    return <p className="p-4 text-xs" style={{ color: "var(--text-muted)" }}>Assign at least one field to a date or covariate/fold role to enable one-way charts.</p>
  }

  return (
    <div className="p-3 space-y-3">
      {/* X-axis selector */}
      <div className="flex items-center gap-2">
        <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>X axis:</label>
        <select
          value={xField}
          onChange={e => setXField(e.target.value)}
          className="text-[11px] font-mono px-2 py-1 rounded appearance-none cursor-pointer"
          style={{ background: "var(--bg-input)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
        >
          {xOptions.map(f => <option key={f} value={f}>{f} ({fieldRoles[f]})</option>)}
        </select>
        {loading && <RefreshCw size={12} className="animate-spin" style={{ color: "var(--text-muted)" }} />}
      </div>

      {error && <ErrorBox msg={error} />}

      {chartData && chartData.x_labels.length > 0 && (
        <OneWaySvgChart data={chartData} />
      )}

      {!loading && !error && chartData && chartData.x_labels.length === 0 && (
        <p className="text-xs" style={{ color: "var(--text-muted)" }}>No data returned for this field.</p>
      )}
    </div>
  )
}

// ── SVG Bar-Line chart ─────────────────────────────────────────────────────

const CHART_W = 580
const CHART_H = 220
const PAD = { top: 16, right: 40, bottom: 64, left: 60 }

function OneWaySvgChart({ data }: { data: EdaOneWayResponse }) {
  const { x_labels, claim_counts, target_sums } = data
  const n = x_labels.length
  if (n === 0) return null

  const innerW = CHART_W - PAD.left - PAD.right
  const innerH = CHART_H - PAD.top - PAD.bottom

  const maxBar = Math.max(...claim_counts, 1)
  const maxLine = Math.max(...target_sums, 1)
  const barW = innerW / n
  const halfBar = barW * 0.7

  const hasTarget = target_sums.some(v => v !== 0)

  // Bar rects
  const bars = claim_counts.map((cnt, i) => {
    const bh = (cnt / maxBar) * innerH
    const x = PAD.left + i * barW + (barW - halfBar) / 2
    const y = PAD.top + innerH - bh
    return <rect key={i} x={x} y={y} width={halfBar} height={bh} fill="#38bdf8" opacity={0.7} rx={2} />
  })

  // Line points
  const linePoints = target_sums.map((v, i) => {
    const cx = PAD.left + i * barW + barW / 2
    const cy = PAD.top + innerH - (v / maxLine) * innerH
    return `${cx},${cy}`
  }).join(" ")

  // X-axis ticks: show at most 15 labels
  const tickStep = Math.max(1, Math.ceil(n / 15))
  const xTicks = x_labels
    .map((lbl, i) => ({ lbl, x: PAD.left + i * barW + barW / 2, i }))
    .filter(({ i }) => i % tickStep === 0)

  // Y left ticks (claims)
  const yLeftTicks = [0, 0.25, 0.5, 0.75, 1].map(t => ({
    y: PAD.top + innerH - t * innerH,
    v: Math.round(t * maxBar),
  }))

  // Y right ticks (target)
  const yRightTicks = hasTarget ? [0, 0.25, 0.5, 0.75, 1].map(t => ({
    y: PAD.top + innerH - t * innerH,
    v: (t * maxLine).toLocaleString(undefined, { maximumFractionDigits: 0 }),
  })) : []

  return (
    <svg
      width={CHART_W}
      height={CHART_H}
      viewBox={`0 0 ${CHART_W} ${CHART_H}`}
      style={{ maxWidth: "100%", overflow: "visible" }}
      aria-label="One-way chart"
    >
      {/* Grid lines */}
      {yLeftTicks.map((t, i) => (
        <line key={i} x1={PAD.left} y1={t.y} x2={PAD.left + innerW} y2={t.y} stroke="var(--border)" strokeWidth={0.5} />
      ))}

      {/* Bars */}
      {bars}

      {/* Line */}
      {hasTarget && (
        <>
          <polyline
            points={linePoints}
            fill="none"
            stroke="#fb923c"
            strokeWidth={2}
            strokeLinejoin="round"
          />
          {target_sums.map((v, i) => (
            <circle key={i} cx={PAD.left + i * barW + barW / 2} cy={PAD.top + innerH - (v / maxLine) * innerH} r={3} fill="#fb923c" />
          ))}
        </>
      )}

      {/* X axis */}
      <line x1={PAD.left} y1={PAD.top + innerH} x2={PAD.left + innerW} y2={PAD.top + innerH} stroke="var(--border)" strokeWidth={1} />
      {xTicks.map(({ lbl, x }) => (
        <text key={x} x={x} y={PAD.top + innerH + 14} textAnchor="end" fontSize={9} fill="var(--text-muted)" transform={`rotate(-45,${x},${PAD.top + innerH + 14})`}>{lbl}</text>
      ))}

      {/* Y left axis (bar) */}
      <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={PAD.top + innerH} stroke="var(--border)" strokeWidth={1} />
      {yLeftTicks.map((t, i) => (
        <text key={i} x={PAD.left - 5} y={t.y + 4} textAnchor="end" fontSize={9} fill="var(--text-muted)">{t.v.toLocaleString()}</text>
      ))}
      <text x={16} y={PAD.top + innerH / 2} textAnchor="middle" fontSize={9} fill="#38bdf8" transform={`rotate(-90,16,${PAD.top + innerH / 2})`}>Count</text>

      {/* Y right axis (line) */}
      {hasTarget && (
        <>
          <line x1={PAD.left + innerW} y1={PAD.top} x2={PAD.left + innerW} y2={PAD.top + innerH} stroke="var(--border)" strokeWidth={1} />
          {yRightTicks.map((t, i) => (
            <text key={i} x={PAD.left + innerW + 5} y={t.y + 4} textAnchor="start" fontSize={9} fill="var(--text-muted)">{t.v}</text>
          ))}
          <text x={CHART_W - 10} y={PAD.top + innerH / 2} textAnchor="middle" fontSize={9} fill="#fb923c" transform={`rotate(90,${CHART_W - 10},${PAD.top + innerH / 2})`}>Target sum</text>
        </>
      )}

      {/* Legend */}
      <g transform={`translate(${PAD.left + 8},${PAD.top + 4})`}>
        <rect x={0} y={0} width={10} height={10} fill="#38bdf8" opacity={0.7} rx={1} />
        <text x={14} y={9} fontSize={9} fill="var(--text-muted)">Claim / row count</text>
        {hasTarget && (
          <>
            <line x1={0} y1={20} x2={10} y2={20} stroke="#fb923c" strokeWidth={2} />
            <circle cx={5} cy={20} r={2.5} fill="#fb923c" />
            <text x={14} y={24} fontSize={9} fill="var(--text-muted)">Target sum</text>
          </>
        )}
      </g>
    </svg>
  )
}

// ── Main component ─────────────────────────────────────────────────────────

export default function EdaPreview({ data, config, graph, nodeId }: EdaPreviewProps) {
  const fieldRoles = (config.fieldRoles ?? {}) as Record<string, string>
  const hasRoles = Object.keys(fieldRoles).length > 0

  const [collapsed, setCollapsed] = useState(false)
  const { height, containerRef, onDragStart } = useDragResize({ initialHeight: 360, minHeight: 200, maxHeight: 700 })
  const [activeTab, setActiveTab] = useState<Tab>("Descriptive Statistics")

  const [edaData, setEdaData] = useState<EdaResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  const loadEda = useCallback(() => {
    if (!hasRoles || !nodeId) return
    if (data?.status === "error") return

    abortRef.current?.abort()
    const ctrl = new AbortController()
    abortRef.current = ctrl

    setLoading(true)
    setError(null)

    fetchEda(graph, nodeId, fieldRoles, "live", { signal: ctrl.signal })
      .then(res => {
        if (ctrl.signal.aborted) return
        if (res.status === "error") {
          setError(res.error ?? "EDA analysis failed")
          setEdaData(null)
        } else {
          setEdaData(res)
        }
      })
      .catch(err => { if (!ctrl.signal.aborted) setError(err?.detail ?? err?.message ?? String(err)) })
      .finally(() => { if (!ctrl.signal.aborted) setLoading(false) })
  }, [hasRoles, nodeId, graph, fieldRoles, data?.status])

  useEffect(() => {
    loadEda()
    return () => { abortRef.current?.abort() }
  }, [loadEda])

  // ── Header bar ──────────────────────────────────────────────────────────

  const headerBar = (
    <div
      onMouseDown={onDragStart}
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
    >
      <SearchCheck size={14} style={{ color: "#7C3AED", flexShrink: 0 }} />
      <span className="text-[12px] font-semibold" style={{ color: "var(--text-primary)", flexShrink: 0 }}>
        EDA
      </span>
      {loading && <RefreshCw size={12} className="animate-spin" style={{ color: "var(--text-muted)", flexShrink: 0 }} />}
      <div style={{ flex: 1 }} />
      {/* Refresh button */}
      <button
        onClick={loadEda}
        className="flex items-center gap-1 px-2 py-0.5 rounded text-[11px]"
        style={{ background: "var(--bg-input)", border: "1px solid var(--border)", color: "var(--text-muted)", cursor: "pointer" }}
        title="Re-run analysis"
        onMouseDown={e => e.stopPropagation()}
      >
        <RefreshCw size={11} /> Refresh
      </button>
      <button
        onClick={() => setCollapsed(!collapsed)}
        onMouseDown={e => e.stopPropagation()}
        style={{ cursor: "pointer", color: "var(--text-muted)", background: "none", border: "none" }}
      >
        {collapsed ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
      </button>
    </div>
  )

  if (collapsed) {
    return (
      <div className="h-9 flex items-center px-4 shrink-0" style={{ borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }}>
        <button onClick={() => setCollapsed(false)} className="flex items-center gap-2 text-xs" style={{ color: "var(--text-secondary)" }}>
          <ChevronUp size={14} />
          <SearchCheck size={14} style={{ color: "#7C3AED" }} />
          <span>EDA</span>
        </button>
      </div>
    )
  }

  if (!hasRoles) {
    return (
      <>
        {headerBar}
        <div
          ref={containerRef}
          style={{ height, overflow: "hidden", flexShrink: 0 }}
          className="flex items-center justify-center"
        >
          <p className="text-xs" style={{ color: "var(--text-muted)" }}>
            Assign field roles in the config panel to enable EDA analysis.
          </p>
        </div>
      </>
    )
  }

  return (
    <>
      {headerBar}
      <div
        ref={containerRef}
        style={{ height, overflow: "hidden", display: "flex", flexDirection: "column", flexShrink: 0 }}
      >
        {/* Tab bar */}
        <div
          className="flex gap-0 shrink-0"
          style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-panel)" }}
        >
          {TABS.map(t => (
            <button
              key={t}
              onClick={() => setActiveTab(t)}
              className="px-3 py-2 text-[11px] whitespace-nowrap transition-colors"
              style={{
                borderTop: "none",
                borderLeft: "none",
                borderRight: "none",
                borderBottom: activeTab === t ? "2px solid #7C3AED" : "2px solid transparent",
                color: activeTab === t ? "#a78bfa" : "var(--text-muted)",
                background: "none",
                cursor: "pointer",
              }}
            >
              {t}
            </button>
          ))}
        </div>

        {/* Tab content */}
        <div style={{ flex: 1, overflow: "auto" }}>
          {loading && <LoadingSpinner />}
          {!loading && error && <ErrorBox msg={error} />}
          {!loading && !error && edaData && (
            <>
              {activeTab === "Descriptive Statistics" && <DescriptiveTab rows={edaData.descriptive} />}
              {activeTab === "Outliers/Inliers" && <OutliersTab rows={edaData.outliers} />}
              {activeTab === "Disguised Missings" && <DisguisedMissingsTab rows={edaData.disguised_missings} />}
              {activeTab === "Correlations" && <CorrelationsTab corr={edaData.correlations} />}
              {activeTab === "One-way Charts" && (
                <OneWayTab graph={graph} nodeId={nodeId} fieldRoles={fieldRoles} />
              )}
            </>
          )}
          {!loading && !error && !edaData && (
            <p className="p-4 text-xs" style={{ color: "var(--text-muted)" }}>
              Run preview on the upstream node first, then the EDA analysis will load automatically.
            </p>
          )}
        </div>
      </div>
    </>
  )
}
