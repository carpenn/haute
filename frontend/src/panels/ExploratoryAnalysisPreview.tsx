import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { ChevronDown, ChevronUp, Search } from "lucide-react"
import type { GraphPayload, ExploratoryAnalysisResponse, ExploratoryOneWayChart } from "../api/types"
import { fetchExploratoryAnalysis, fetchExploratoryOneWayChart } from "../api/client"
import type { PreviewData } from "./DataPreview"
import { useDragResize } from "../hooks/useDragResize"
import { formatAxisLabel } from "../utils/chartHelpers"

const TAB_KEYS = [
  "descriptive",
  "outliers",
  "missings",
  "correlations",
  "oneway",
] as const
type TabKey = (typeof TAB_KEYS)[number]

const TAB_LABELS: Record<TabKey, string> = {
  descriptive: "Descriptive Statistics",
  outliers: "Outliers/Inliers",
  missings: "Disguised Missings",
  correlations: "Correlations",
  oneway: "One-way charts",
}

interface ExploratoryAnalysisPreviewProps {
  data: PreviewData | null
  config: Record<string, unknown>
  nodeId: string
  getGraph: () => GraphPayload
}

function formatCell(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") return "—"
  if (typeof value === "number") {
    if (Number.isInteger(value)) return value.toLocaleString()
    return value.toFixed(4)
  }
  return String(value)
}

function formatPercent(value: number): string {
  return `${(value * 100).toFixed(2)}%`
}

function DistributionSparkline({ values }: { values?: number[] }) {
  if (!values || values.length === 0) return <span style={{ color: "var(--text-muted)" }}>—</span>
  const max = Math.max(...values, 1)
  const points = values.map((value, idx) => {
    const x = values.length === 1 ? 0 : (idx / (values.length - 1)) * 100
    const y = 100 - ((value / max) * 100)
    return `${x},${y}`
  }).join(" ")
  return (
    <svg viewBox="0 0 100 100" className="w-24 h-8">
      <polyline fill="none" stroke="var(--accent)" strokeWidth="4" points={points} />
    </svg>
  )
}

function OneWayChart({ chart }: { chart: ExploratoryOneWayChart | null | undefined }) {
  if (!chart) {
    return <div className="text-xs" style={{ color: "var(--text-muted)" }}>No chart available.</div>
  }
  if (chart.error) {
    return <div className="text-xs" style={{ color: "#ef4444" }}>{chart.error}</div>
  }
  const points = chart.points ?? []
  if (points.length === 0) {
    return <div className="text-xs" style={{ color: "var(--text-muted)" }}>No chart points available.</div>
  }

  const width = 920
  const height = 320
  const left = 52
  const right = 52
  const top = 18
  const bottom = 42
  const chartW = width - left - right
  const chartH = height - top - bottom
  const maxBar = Math.max(...points.map((point) => point.bar_value), 1)
  const maxLine = Math.max(...points.map((point) => point.line_value), 1)
  const barWidth = chartW / Math.max(points.length, 1) * 0.7
  const step = chartW / Math.max(points.length, 1)

  const linePath = points.map((point, idx) => {
    const x = left + (idx * step) + (step / 2)
    const y = top + chartH - ((point.line_value / maxLine) * chartH)
    return `${idx === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`
  }).join(" ")

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3 text-[11px]" style={{ color: "var(--text-muted)" }}>
        <span>{chart.bar_label}</span>
        <span>{chart.line_label}</span>
        {chart.binned && <span>Binned x-axis</span>}
      </div>
      <div className="overflow-x-auto rounded-lg" style={{ border: "1px solid var(--border)", background: "var(--bg-input)" }}>
        <svg viewBox={`0 0 ${width} ${height}`} className="min-w-[920px] w-full h-80">
          {[0, 0.25, 0.5, 0.75, 1].map((tick) => {
            const y = top + chartH - (tick * chartH)
            return (
              <g key={tick}>
                <line x1={left} y1={y} x2={left + chartW} y2={y} stroke="var(--border)" strokeWidth="0.75" />
                <text x={left - 6} y={y + 4} textAnchor="end" fontSize="10" fill="var(--text-muted)">
                  {formatAxisLabel(tick * maxBar)}
                </text>
                <text x={left + chartW + 6} y={y + 4} fontSize="10" fill="var(--text-muted)">
                  {formatAxisLabel(tick * maxLine)}
                </text>
              </g>
            )
          })}
          {points.map((point, idx) => {
            const x = left + (idx * step) + ((step - barWidth) / 2)
            const barHeight = (point.bar_value / maxBar) * chartH
            const barY = top + chartH - barHeight
            return (
              <g key={point.x}>
                <rect x={x} y={barY} width={barWidth} height={barHeight} fill="rgba(59,130,246,.55)" rx="3" />
                <text
                  x={left + (idx * step) + (step / 2)}
                  y={height - 16}
                  textAnchor="middle"
                  fontSize="9"
                  fill="var(--text-muted)"
                  transform={`rotate(0 ${left + (idx * step) + (step / 2)} ${height - 16})`}
                >
                  {point.x}
                </text>
              </g>
            )
          })}
          <path d={linePath} fill="none" stroke="#f97316" strokeWidth="2.5" />
          {points.map((point, idx) => {
            const cx = left + (idx * step) + (step / 2)
            const cy = top + chartH - ((point.line_value / maxLine) * chartH)
            return <circle key={`${point.x}-line`} cx={cx} cy={cy} r="3" fill="#f97316" />
          })}
        </svg>
      </div>
    </div>
  )
}

export default function ExploratoryAnalysisPreview({
  data,
  config,
  nodeId,
  getGraph,
}: ExploratoryAnalysisPreviewProps) {
  const [collapsed, setCollapsed] = useState(false)
  const [tab, setTab] = useState<TabKey>("descriptive")
  const [analysis, setAnalysis] = useState<ExploratoryAnalysisResponse | null>(null)
  const [analysisLoading, setAnalysisLoading] = useState(false)
  const [analysisError, setAnalysisError] = useState<string | null>(null)
  const [chartLoading, setChartLoading] = useState(false)
  const [selectedXField, setSelectedXField] = useState("")
  const analysisAbort = useRef<AbortController | null>(null)
  const chartAbort = useRef<AbortController | null>(null)
  const { height, containerRef, onDragStart } = useDragResize({ initialHeight: 360, minHeight: 180, maxHeight: 700 })

  const configHash = useMemo(() => JSON.stringify(config ?? {}), [config])

  const loadAnalysis = useCallback(() => {
    if (data?.status === "error") return
    analysisAbort.current?.abort()
    const controller = new AbortController()
    analysisAbort.current = controller
    setAnalysisLoading(true)
    setAnalysisError(null)

    fetchExploratoryAnalysis(getGraph(), nodeId, "live", { signal: controller.signal })
      .then((result) => {
        if (controller.signal.aborted) return
        if (result.status === "error") {
          setAnalysisError(result.error ?? "Exploratory analysis failed")
          setAnalysis(null)
          return
        }
        setAnalysis(result)
        setSelectedXField(result.default_x_field ?? "")
      })
      .catch((err) => {
        if (controller.signal.aborted) return
        setAnalysisError(err?.detail ?? err?.message ?? String(err))
      })
      .finally(() => {
        if (!controller.signal.aborted) setAnalysisLoading(false)
      })
  }, [data?.status, getGraph, nodeId])

  const loadChart = useCallback((xField: string) => {
    if (!xField) return
    chartAbort.current?.abort()
    const controller = new AbortController()
    chartAbort.current = controller
    setChartLoading(true)

    fetchExploratoryOneWayChart(getGraph(), nodeId, xField, "live", { signal: controller.signal })
      .then((result) => {
        if (controller.signal.aborted || result.status === "error") return
        setAnalysis((current) => current ? { ...current, chart: result.chart ?? null } : current)
      })
      .finally(() => {
        if (!controller.signal.aborted) setChartLoading(false)
      })
  }, [getGraph, nodeId])

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => { loadAnalysis() })
    return () => {
      window.cancelAnimationFrame(frame)
      analysisAbort.current?.abort()
    }
  }, [loadAnalysis, configHash])

  useEffect(() => {
    return () => { chartAbort.current?.abort() }
  }, [])

  if (!data) return null

  if (collapsed) {
    return (
      <div className="h-8 flex items-center px-4 shrink-0" style={{ borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }}>
        <button onClick={() => setCollapsed(false)} className="flex items-center gap-2 text-xs" style={{ color: "var(--text-secondary)" }}>
          <ChevronUp size={14} />
          <Search size={14} />
          <span className="font-medium">{data.nodeLabel}</span>
        </button>
      </div>
    )
  }

  return (
    <div ref={containerRef} style={{ height, borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }} className="flex flex-col shrink-0 relative">
      <div
        onMouseDown={onDragStart}
        className="absolute top-0 left-0 right-0 h-1 cursor-ns-resize z-10 transition-colors"
        style={{ background: "var(--chrome-border)" }}
        onMouseEnter={(e) => { e.currentTarget.style.background = "var(--accent)" }}
        onMouseLeave={(e) => { e.currentTarget.style.background = "var(--chrome-border)" }}
      />

      <div className="min-h-9 flex items-center flex-wrap px-4 shrink-0 gap-x-2 gap-y-1 py-1.5" style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-elevated)" }}>
        <Search size={14} style={{ color: "#0f766e" }} />
        <span className="text-xs font-bold" style={{ color: "var(--text-primary)" }}>{data.nodeLabel}</span>
        {analysis && (
          <span className="text-[11px]" style={{ color: "var(--text-muted)" }}>
            {analysis.row_count.toLocaleString()} rows analysed
          </span>
        )}
        <div className="flex gap-1 ml-3">
          {TAB_KEYS.map((key) => (
            <button
              key={key}
              onClick={() => setTab(key)}
              className="px-2 py-0.5 rounded text-[10px] font-medium"
              style={{
                background: tab === key ? "var(--accent-soft)" : "var(--chrome-hover)",
                color: tab === key ? "var(--accent)" : "var(--text-muted)",
              }}
            >
              {TAB_LABELS[key]}
            </button>
          ))}
        </div>
        <div className="ml-auto flex items-center gap-1">
          <button onClick={() => setCollapsed(true)} className="p-1 rounded transition-colors" style={{ color: "var(--text-muted)" }}>
            <ChevronDown size={14} />
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-auto px-4 py-3">
        {data.status === "error" ? (
          <div className="text-xs" style={{ color: "#ef4444" }}>{data.error ?? "Preview error"}</div>
        ) : analysisLoading ? (
          <div className="text-xs animate-pulse" style={{ color: "var(--text-muted)" }}>Profiling dataset...</div>
        ) : analysisError ? (
          <div className="text-xs" style={{ color: "#ef4444" }}>{analysisError}</div>
        ) : !analysis ? (
          <div className="text-xs" style={{ color: "var(--text-muted)" }}>No exploratory analysis available.</div>
        ) : (
          <>
            {tab === "descriptive" && (
              <div className="overflow-x-auto rounded-lg" style={{ border: "1px solid var(--border)", background: "var(--bg-input)" }}>
                <table className="w-full text-xs">
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-elevated)" }}>
                      {["Field", "Role", "Dtype", "Profile", "Distinct", "Missing", "Mean", "Std", "Min", "Median", "Max", "Top values", "Distribution"].map((label) => (
                        <th key={label} className="text-left px-2.5 py-1.5 font-semibold whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{label}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {analysis.descriptive_statistics.map((row) => (
                      <tr key={row.field} style={{ borderBottom: "1px solid var(--border)" }}>
                        <td className="px-2.5 py-1.5 font-mono">{row.field}</td>
                        <td className="px-2.5 py-1.5">{row.role}</td>
                        <td className="px-2.5 py-1.5">{row.dtype}</td>
                        <td className="px-2.5 py-1.5">{formatCell(row.profile_type)}</td>
                        <td className="px-2.5 py-1.5">{row.distinct_count.toLocaleString()}</td>
                        <td className="px-2.5 py-1.5">{row.missing_count.toLocaleString()} ({formatPercent(row.missing_proportion)})</td>
                        <td className="px-2.5 py-1.5">{formatCell(row.mean)}</td>
                        <td className="px-2.5 py-1.5">{formatCell(row.std)}</td>
                        <td className="px-2.5 py-1.5">{formatCell(row.min)}</td>
                        <td className="px-2.5 py-1.5">{formatCell(row.median)}</td>
                        <td className="px-2.5 py-1.5">{formatCell(row.max)}</td>
                        <td className="px-2.5 py-1.5">{row.top_values.length > 0 ? row.top_values.join(", ") : "—"}</td>
                        <td className="px-2.5 py-1.5"><DistributionSparkline values={row.distribution?.values} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {tab === "outliers" && (
              <div className="overflow-x-auto rounded-lg" style={{ border: "1px solid var(--border)", background: "var(--bg-input)" }}>
                <table className="w-full text-xs">
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-elevated)" }}>
                      {["Field", "Role", "Dtype", "Outlier", "Outlier_proportion", "Inlier", "Inlier_proportion"].map((label) => (
                        <th key={label} className="text-left px-2.5 py-1.5 font-semibold whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{label}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {analysis.outliers_inliers.map((row) => (
                      <tr key={row.field} style={{ borderBottom: "1px solid var(--border)" }}>
                        <td className="px-2.5 py-1.5 font-mono">{row.field}</td>
                        <td className="px-2.5 py-1.5">{row.role}</td>
                        <td className="px-2.5 py-1.5">{row.dtype}</td>
                        <td className="px-2.5 py-1.5">{row.outlier.length > 0 ? row.outlier.join(", ") : "—"}</td>
                        <td className="px-2.5 py-1.5">{formatPercent(row.outlier_proportion)}</td>
                        <td className="px-2.5 py-1.5">{row.inlier.length > 0 ? row.inlier.join(", ") : "—"}</td>
                        <td className="px-2.5 py-1.5">{formatPercent(row.inlier_proportion)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {tab === "missings" && (
              <div className="overflow-x-auto rounded-lg" style={{ border: "1px solid var(--border)", background: "var(--bg-input)" }}>
                <table className="w-full text-xs">
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-elevated)" }}>
                      {["Field", "Role", "Dtype", "Missing values", "Missing_proportion"].map((label) => (
                        <th key={label} className="text-left px-2.5 py-1.5 font-semibold whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{label}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {analysis.disguised_missings.map((row) => (
                      <tr key={row.field} style={{ borderBottom: "1px solid var(--border)" }}>
                        <td className="px-2.5 py-1.5 font-mono">{row.field}</td>
                        <td className="px-2.5 py-1.5">{row.role}</td>
                        <td className="px-2.5 py-1.5">{row.dtype}</td>
                        <td className="px-2.5 py-1.5">{row.missing_values.length > 0 ? row.missing_values.join(", ") : "—"}</td>
                        <td className="px-2.5 py-1.5">{formatPercent(row.missing_proportion)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {tab === "correlations" && (
              analysis.correlations.fields.length === 0 ? (
                <div className="text-xs" style={{ color: "var(--text-muted)" }}>No correlation matrix available.</div>
              ) : (
                <div className="overflow-auto rounded-lg" style={{ border: "1px solid var(--border)", background: "var(--bg-input)", maxHeight: 520 }}>
                  <table className="w-full text-[11px]">
                    <thead className="sticky top-0" style={{ background: "var(--bg-elevated)" }}>
                      <tr>
                        <th className="px-2.5 py-1.5 text-left font-semibold" style={{ color: "var(--text-muted)" }}>Field</th>
                        {analysis.correlations.fields.map((field) => (
                          <th key={field} className="px-2.5 py-1.5 text-left font-semibold whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{field}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {analysis.correlations.fields.map((rowField, rowIndex) => (
                        <tr key={rowField} style={{ borderBottom: "1px solid var(--border)" }}>
                          <td className="px-2.5 py-1.5 font-mono">{rowField}</td>
                          {analysis.correlations.cells[rowIndex].map((cell, cellIndex) => (
                            <td key={`${rowField}-${cellIndex}`} className="px-2.5 py-1.5 align-top">
                              {rowIndex === cellIndex ? "—" : (
                                <div className="space-y-0.5">
                                  {analysis.correlations.types.map((type) => (
                                    <div key={type} className="flex items-center gap-1.5">
                                      <span style={{ color: "var(--text-muted)" }}>{type}:</span>
                                      <span>{formatCell(cell[type] ?? null)}</span>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )
            )}

            {tab === "oneway" && (
              <div className="space-y-3">
                <div className="flex items-center gap-2">
                  <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>
                    X-axis
                  </label>
                  <select
                    aria-label="One-way chart x-axis"
                    value={selectedXField}
                    onChange={(e) => {
                      const nextField = e.target.value
                      setSelectedXField(nextField)
                      loadChart(nextField)
                    }}
                    className="px-2.5 py-1.5 rounded-md text-[12px] font-mono"
                    style={{ background: "var(--bg-input)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                  >
                    {analysis.one_way_options.map((option) => (
                      <option key={option.field} value={option.field}>
                        {option.field} ({option.role})
                      </option>
                    ))}
                  </select>
                  {chartLoading && <span className="text-[11px] animate-pulse" style={{ color: "var(--text-muted)" }}>Updating chart...</span>}
                </div>
                <OneWayChart chart={analysis.chart} />
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}
