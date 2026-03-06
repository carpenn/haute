/**
 * Bottom-panel visualisations for the optimiser node.
 *
 * Renders in the same slot as DataPreview when an optimiser solve has
 * completed.  Three tabs: Summary, Frontier, Convergence.
 */

import { useState, useCallback } from "react"
import { X, ChevronDown, ChevronUp, Loader2, Target } from "lucide-react"
import { runFrontier } from "../api/client"
import { formatNumber } from "../utils/formatValue"
import { useDragResize } from "../hooks/useDragResize"

// ─── Types (shared with OptimiserConfig) ─────────────────────────

export type SolveResult = {
  mode?: string
  total_objective: number
  baseline_objective: number
  constraints: Record<string, number>
  baseline_constraints: Record<string, number>
  lambdas: Record<string, number>
  converged: boolean
  iterations?: number
  n_quotes?: number
  n_steps?: number
  cd_iterations?: number
  factor_tables?: Record<string, Record<string, unknown>[]>
  history?: {
    iteration: number
    total_objective: number
    max_lambda_change: number
    all_constraints_satisfied?: boolean
    lambdas?: Record<string, number>
    total_constraints?: Record<string, number>
  }[] | null
  warning?: string
  scenario_value_stats?: {
    mean: number; std: number; min: number; max: number
    p5: number; p25: number; p50: number; p75: number; p95: number
    pct_increase: number; pct_decrease: number
  }
  scenario_value_histogram?: { counts: number[]; edges: number[] }
  clamp_rate?: number | null
}

export type OptimiserPreviewData = {
  result: SolveResult
  jobId: string
  constraints: Record<string, Record<string, number>>
  nodeLabel: string
}

type FrontierPoint = Record<string, unknown>

// ─── Component ───────────────────────────────────────────────────

interface OptimiserPreviewProps {
  data: OptimiserPreviewData
  onClose: () => void
}

export default function OptimiserPreview({ data, onClose }: OptimiserPreviewProps) {
  const { result, jobId, constraints } = data
  const [collapsed, setCollapsed] = useState(false)
  const { height, containerRef, onDragStart } = useDragResize({ initialHeight: 320, minHeight: 160, maxHeight: 600 })
  const [tab, setTab] = useState<"summary" | "frontier" | "convergence">("summary")

  // Frontier state
  const [frontierData, setFrontierData] = useState<FrontierPoint[] | null>(null)
  const [frontierLoading, setFrontierLoading] = useState(false)
  const [frontierError, setFrontierError] = useState<string | null>(null)
  const [selectedPoint, setSelectedPoint] = useState<number | null>(null)

  const handleRunFrontier = useCallback(async () => {
    setFrontierLoading(true)
    setFrontierError(null)
    setFrontierData(null)
    setSelectedPoint(null)
    try {
      const ranges: Record<string, [number, number]> = {}
      for (const name of Object.keys(constraints)) {
        const baseline = result.baseline_constraints[name]
        if (baseline != null) ranges[name] = [baseline * 0.85, baseline * 1.05]
      }
      const res = await runFrontier({ job_id: jobId, threshold_ranges: ranges, n_points_per_dim: 5 })
      setFrontierData(res.points)
    } catch (e) {
      setFrontierError(String(e))
    } finally {
      setFrontierLoading(false)
    }
  }, [jobId, result, constraints])

  // ── Collapsed ──
  if (collapsed) {
    return (
      <div className="h-8 flex items-center px-4 shrink-0" style={{ borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }}>
        <button onClick={() => setCollapsed(false)} className="flex items-center gap-2 text-xs" style={{ color: "var(--text-secondary)" }}>
          <ChevronUp size={14} />
          <Target size={14} />
          <span className="font-medium">{data.nodeLabel}</span>
          <span style={{ color: "var(--text-muted)" }}>
            {result.converged ? "Converged" : "Not converged"}
            {" — "}Objective: {formatNumber(result.total_objective)}
          </span>
        </button>
      </div>
    )
  }

  // ── Expanded ──
  return (
    <div ref={containerRef} style={{ height, borderTop: "1px solid var(--border)", background: "var(--bg-panel)" }} className="flex flex-col shrink-0 relative">
      {/* Drag handle */}
      <div
        onMouseDown={onDragStart}
        className="absolute top-0 left-0 right-0 h-1 cursor-ns-resize z-10 transition-colors"
        style={{ background: "var(--chrome-border)" }}
        onMouseEnter={e => { e.currentTarget.style.background = "var(--accent)" }}
        onMouseLeave={e => { e.currentTarget.style.background = "var(--chrome-border)" }}
      />

      {/* Header */}
      <div className="h-9 flex items-center px-4 shrink-0 gap-2" style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-elevated)" }}>
        <Target size={14} style={{ color: "#f97316" }} />
        <span className="text-xs font-bold" style={{ color: "var(--text-primary)" }}>{data.nodeLabel}</span>
        <span className="text-[11px]" style={{ color: result.converged ? "#22c55e" : "#f59e0b" }}>
          {result.converged ? "Converged" : "Not converged"}
          {result.mode === "ratebook"
            ? ` · ${result.cd_iterations ?? "?"} CD iters`
            : ` · ${result.iterations ?? "?"} iters`}
          {result.n_quotes != null && <> · {result.n_quotes.toLocaleString()} quotes</>}
        </span>

        {/* Tab selector */}
        <div className="flex gap-1 ml-3">
          {(["summary", "frontier", "convergence"] as const)
            .filter(t => t !== "convergence" || result.history)
            .map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className="px-2 py-0.5 rounded text-[10px] font-medium"
              style={{
                background: tab === t ? "var(--accent-soft)" : "var(--chrome-hover)",
                color: tab === t ? "var(--accent)" : "var(--text-muted)",
              }}
            >
              {t === "summary" ? "Summary" : t === "frontier" ? "Frontier" : "Convergence"}
            </button>
          ))}
        </div>

        <div className="ml-auto flex items-center gap-1">
          <button onClick={() => setCollapsed(true)} className="p-1 rounded transition-colors" style={{ color: "var(--text-muted)" }}
            onMouseEnter={e => { e.currentTarget.style.background = "var(--bg-hover)" }}
            onMouseLeave={e => { e.currentTarget.style.background = "transparent" }}
          >
            <ChevronDown size={14} />
          </button>
          <button onClick={onClose} className="p-1 rounded transition-colors" style={{ color: "var(--text-muted)" }}
            onMouseEnter={e => { e.currentTarget.style.background = "var(--bg-hover)" }}
            onMouseLeave={e => { e.currentTarget.style.background = "transparent" }}
          >
            <X size={14} />
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto px-4 py-3">
        {/* ── Summary ── */}
        {tab === "summary" && (
          <div className="flex gap-6 flex-wrap">
            {/* Left column: objective + constraints */}
            <div className="space-y-3 min-w-[200px]">
              <div>
                <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Objective</label>
                <div className="mt-1 space-y-0.5">
                  <div className="flex justify-between text-xs font-mono gap-4">
                    <span style={{ color: "var(--text-secondary)" }}>Optimised</span>
                    <span style={{ color: "var(--text-primary)" }}>{formatNumber(result.total_objective)}</span>
                  </div>
                  <div className="flex justify-between text-xs font-mono gap-4">
                    <span style={{ color: "var(--text-secondary)" }}>Baseline</span>
                    <span style={{ color: "var(--text-muted)" }}>{formatNumber(result.baseline_objective)}</span>
                  </div>
                  {result.baseline_objective !== 0 && (
                    <div className="flex justify-between text-xs font-mono gap-4">
                      <span style={{ color: "var(--text-secondary)" }}>Uplift</span>
                      <span style={{ color: "#f97316" }}>
                        {((result.total_objective / result.baseline_objective - 1) * 100).toFixed(2)}%
                      </span>
                    </div>
                  )}
                </div>
              </div>

              {/* Constraints with binding indicators */}
              {Object.keys(result.constraints).length > 0 && (
                <div>
                  <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Constraints</label>
                  <div className="mt-1 space-y-0.5">
                    {Object.entries(result.constraints).map(([name, value]) => {
                      const baseline = result.baseline_constraints[name]
                      const ratio = baseline ? value / baseline : 0
                      const spec = constraints[name] || {}
                      const thresholdType = Object.keys(spec)[0]
                      const thresholdVal = spec[thresholdType] ?? 0
                      const met = thresholdType === "min" ? ratio >= thresholdVal
                        : thresholdType === "max" ? ratio <= thresholdVal
                        : thresholdType === "min_abs" ? value >= thresholdVal
                        : thresholdType === "max_abs" ? value <= thresholdVal
                        : true
                      return (
                        <div key={name} className="flex items-center justify-between text-xs font-mono gap-4">
                          <span className="flex items-center gap-1.5">
                            <span className="inline-block w-1.5 h-1.5 rounded-full shrink-0" style={{ background: met ? "#22c55e" : "#ef4444" }} />
                            <span style={{ color: "var(--text-secondary)" }}>{name}</span>
                          </span>
                          <span>
                            <span style={{ color: "var(--text-primary)" }}>{formatNumber(value)}</span>
                            {baseline !== undefined && (
                              <span style={{ color: "var(--text-muted)" }}> ({(ratio * 100).toFixed(1)}%)</span>
                            )}
                          </span>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}

              {/* Lambdas (online) / Factor tables (ratebook) */}
              {result.mode !== "ratebook" && Object.keys(result.lambdas).length > 0 && (
                <div>
                  <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Lambdas</label>
                  <div className="mt-1 space-y-0.5">
                    {Object.entries(result.lambdas).map(([name, value]) => (
                      <div key={name} className="flex justify-between text-xs font-mono gap-4">
                        <span style={{ color: "var(--text-secondary)" }}>{name}</span>
                        <span style={{ color: "var(--text-primary)" }}>{value.toFixed(6)}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {result.mode === "ratebook" && result.clamp_rate != null && (
                <div className="flex justify-between text-xs font-mono">
                  <span style={{ color: "var(--text-muted)" }}>Clamp rate</span>
                  <span style={{ color: "#f59e0b" }}>{(result.clamp_rate * 100).toFixed(1)}%</span>
                </div>
              )}
            </div>

            {/* Middle column: histogram + stats */}
            {result.scenario_value_histogram && (() => {
              const { counts, edges } = result.scenario_value_histogram
              if (!counts || counts.length === 0) return null
              const maxCount = Math.max(...counts)
              const w = 320, h = 100, px = 2, py = 2
              const chartW = w - px * 2, chartH = h - py * 2
              const barW = chartW / counts.length
              const eMin = edges[0], eMax = edges[edges.length - 1]
              const oneX = eMax > eMin ? px + ((1.0 - eMin) / (eMax - eMin)) * chartW : null
              return (
                <div className="min-w-[200px]">
                  <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Scenario Value Distribution</label>
                  <svg width={w} height={h} className="mt-1" style={{ background: "var(--input-bg)", borderRadius: 6, border: "1px solid var(--border)" }}>
                    {counts.map((c, i) => {
                      const barH = maxCount > 0 ? (c / maxCount) * chartH : 0
                      return (
                        <rect key={i} x={px + i * barW + 0.5} y={py + chartH - barH} width={Math.max(barW - 1, 1)} height={barH} fill="#f97316" opacity={0.7} />
                      )
                    })}
                    {oneX != null && oneX >= px && oneX <= px + chartW && (
                      <line x1={oneX} y1={py} x2={oneX} y2={py + chartH} stroke="#ef4444" strokeWidth={1} strokeDasharray="3,2" />
                    )}
                  </svg>
                  <div className="flex gap-3 mt-0.5 text-[10px]" style={{ color: "var(--text-muted)" }}>
                    <span>{eMin.toFixed(2)}</span>
                    <span className="flex-1" />
                    {oneX != null && <span><span style={{ color: "#ef4444" }}>|</span> 1.0</span>}
                    <span className="flex-1" />
                    <span>{eMax.toFixed(2)}</span>
                  </div>

                  {/* Stats grid */}
                  {result.scenario_value_stats && (
                    <div className="mt-2 grid grid-cols-2 gap-x-6 gap-y-0.5 text-xs font-mono">
                      <div className="flex justify-between"><span style={{ color: "var(--text-muted)" }}>Mean</span><span style={{ color: "var(--text-primary)" }}>{result.scenario_value_stats.mean.toFixed(4)}</span></div>
                      <div className="flex justify-between"><span style={{ color: "var(--text-muted)" }}>Std</span><span style={{ color: "var(--text-primary)" }}>{result.scenario_value_stats.std.toFixed(4)}</span></div>
                      <div className="flex justify-between"><span style={{ color: "var(--text-muted)" }}>P5–P95</span><span style={{ color: "var(--text-primary)" }}>{result.scenario_value_stats.p5.toFixed(3)}–{result.scenario_value_stats.p95.toFixed(3)}</span></div>
                      <div className="flex justify-between"><span style={{ color: "var(--text-muted)" }}>Min–Max</span><span style={{ color: "var(--text-primary)" }}>{result.scenario_value_stats.min.toFixed(3)}–{result.scenario_value_stats.max.toFixed(3)}</span></div>
                      <div className="flex justify-between"><span style={{ color: "#22c55e" }}>Increase</span><span style={{ color: "#22c55e" }}>{(result.scenario_value_stats.pct_increase * 100).toFixed(1)}%</span></div>
                      <div className="flex justify-between"><span style={{ color: "#ef4444" }}>Decrease</span><span style={{ color: "#ef4444" }}>{(result.scenario_value_stats.pct_decrease * 100).toFixed(1)}%</span></div>
                    </div>
                  )}
                </div>
              )
            })()}

            {/* Factor tables (ratebook) */}
            {result.mode === "ratebook" && result.factor_tables && (
              <div className="min-w-[180px]">
                <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Factor Tables</label>
                {Object.entries(result.factor_tables).map(([factorName, rows]) => (
                  <div key={factorName} className="mt-1.5">
                    <div className="text-[11px] font-medium mb-1" style={{ color: "var(--text-secondary)" }}>{factorName}</div>
                    <div className="space-y-0.5">
                      {rows.map((row, i) => {
                        const levelName = row.__factor_group__ as string ?? row[Object.keys(row)[0]] as string ?? `Level ${i}`
                        const mult = row.optimal_scenario_value as number
                        return (
                          <div key={i} className="flex justify-between text-xs font-mono gap-4">
                            <span style={{ color: "var(--text-secondary)" }}>{levelName}</span>
                            <span style={{ color: "var(--text-primary)" }}>{typeof mult === "number" ? mult.toFixed(2) : "?"}</span>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* ── Frontier ── */}
        {tab === "frontier" && (
          <div className="flex gap-6 flex-wrap">
            <div className="space-y-2">
              <button
                onClick={handleRunFrontier}
                disabled={frontierLoading}
                className="flex items-center justify-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium transition-colors"
                style={{
                  background: frontierLoading ? "var(--chrome-hover)" : "rgba(168,85,247,.15)",
                  color: frontierLoading ? "var(--text-muted)" : "#a855f7",
                  border: "1px solid rgba(168,85,247,.3)",
                }}
              >
                {frontierLoading ? <Loader2 size={14} className="animate-spin" /> : <Target size={14} />}
                {frontierLoading ? "Computing..." : "Run Efficient Frontier"}
              </button>

              {frontierError && (
                <div className="px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(239,68,68,.08)", border: "1px solid rgba(239,68,68,.2)", color: "#fca5a5" }}>
                  {frontierError}
                </div>
              )}

              {frontierData && frontierData.length > 0 && (() => {
                const w = 400, h = 200, px = 35, py = 12
                const chartW = w - px - 10, chartH = h - py * 2 - 12
                const constraintNames = Object.keys(constraints)
                const xKey = constraintNames.length > 0 ? `total_${constraintNames[0]}` : null
                const yKey = "total_objective"
                if (!xKey) return null
                const xVals = frontierData.map(p => p[xKey] as number).filter(v => v != null)
                const yVals = frontierData.map(p => p[yKey] as number).filter(v => v != null)
                if (xVals.length < 2) return null
                const xMin = Math.min(...xVals), xMax = Math.max(...xVals)
                const yMin = Math.min(...yVals), yMax = Math.max(...yVals)
                const xRange = xMax - xMin || 1, yRange = yMax - yMin || 1
                const xScale = (v: number) => px + ((v - xMin) / xRange) * chartW
                const yScale = (v: number) => py + chartH - ((v - yMin) / yRange) * chartH

                const sorted = [...frontierData]
                  .filter(p => p[xKey] != null && p[yKey] != null)
                  .sort((a, b) => (a[xKey] as number) - (b[xKey] as number))
                const linePath = sorted
                  .map((p, i) => `${i === 0 ? "M" : "L"}${xScale(p[xKey] as number).toFixed(1)},${yScale(p[yKey] as number).toFixed(1)}`)
                  .join(" ")

                return (
                  <div>
                    <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Efficient Frontier</label>
                    <svg width={w} height={h} className="mt-1" style={{ background: "var(--input-bg)", borderRadius: 6, border: "1px solid var(--border)" }}>
                      <text x={px + chartW / 2} y={h - 1} textAnchor="middle" fontSize={9} fill="var(--text-muted)">{constraintNames[0]}</text>
                      <text x={4} y={py + chartH / 2} textAnchor="middle" fontSize={9} fill="var(--text-muted)" transform={`rotate(-90,4,${py + chartH / 2})`}>objective</text>
                      <path d={linePath} fill="none" stroke="#a855f7" strokeWidth={1.5} opacity={0.6} />
                      {frontierData.map((p, i) => {
                        const cx = xScale(p[xKey] as number)
                        const cy = yScale(p[yKey] as number)
                        const sel = selectedPoint === i
                        return (
                          <circle key={i} cx={cx} cy={cy} r={sel ? 5 : 3.5}
                            fill={sel ? "#f97316" : "#a855f7"} stroke={sel ? "#fff" : "none"} strokeWidth={1.5}
                            style={{ cursor: "pointer" }} onClick={() => setSelectedPoint(sel ? null : i)} />
                        )
                      })}
                    </svg>
                    <div className="text-[10px] mt-0.5" style={{ color: "var(--text-muted)" }}>
                      {frontierData.length} points. Click a point for details.
                    </div>
                  </div>
                )
              })()}

              {frontierData && frontierData.length === 0 && (
                <div className="text-[11px] py-2" style={{ color: "var(--text-muted)" }}>No frontier points returned.</div>
              )}
            </div>

            {/* Selected point details */}
            {frontierData && selectedPoint != null && frontierData[selectedPoint] && (
              <div className="min-w-[200px]">
                <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Point {selectedPoint + 1}</label>
                <div className="mt-1 space-y-0.5">
                  {Object.entries(frontierData[selectedPoint]).map(([k, v]) => (
                    <div key={k} className="flex justify-between text-xs font-mono gap-4">
                      <span style={{ color: "var(--text-muted)" }}>{k}</span>
                      <span style={{ color: "var(--text-primary)" }}>{typeof v === "number" ? v.toFixed(4) : String(v)}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* ── Convergence ── */}
        {tab === "convergence" && result.history && result.history.length > 0 && (() => {
          const hist = result.history
          const w = 400, h = 140, px = 6, py = 6
          const chartW = w - px * 2, chartH = h - py * 2

          const objVals = hist.map(e => e.total_objective)
          const lcVals = hist.map(e => e.max_lambda_change)
          const objMin = Math.min(...objVals), objMax = Math.max(...objVals)
          const lcMin = Math.min(...lcVals), lcMax = Math.max(...lcVals)
          const objRange = objMax - objMin || 1, lcRange = lcMax - lcMin || 1

          const xScale = (i: number) => px + (i / Math.max(hist.length - 1, 1)) * chartW
          const yObj = (v: number) => py + chartH - ((v - objMin) / objRange) * chartH
          const yLc = (v: number) => py + chartH - ((v - lcMin) / lcRange) * chartH

          const objPath = hist.map((e, i) => `${i === 0 ? "M" : "L"}${xScale(i).toFixed(1)},${yObj(e.total_objective).toFixed(1)}`).join(" ")
          const lcPath = hist.map((e, i) => `${i === 0 ? "M" : "L"}${xScale(i).toFixed(1)},${yLc(e.max_lambda_change).toFixed(1)}`).join(" ")

          return (
            <div className="flex gap-6 flex-wrap">
              <div>
                <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Convergence</label>
                <svg width={w} height={h} className="mt-1" style={{ background: "var(--input-bg)", borderRadius: 6, border: "1px solid var(--border)" }}>
                  <path d={objPath} fill="none" stroke="#f97316" strokeWidth={1.5} />
                  <path d={lcPath} fill="none" stroke="#3b82f6" strokeWidth={1.5} />
                </svg>
                <div className="flex gap-3 mt-0.5 text-[10px]" style={{ color: "var(--text-muted)" }}>
                  <span><span style={{ color: "#f97316" }}>--</span> Objective</span>
                  <span><span style={{ color: "#3b82f6" }}>--</span> Lambda change</span>
                </div>
              </div>

              {/* Iteration table */}
              <div className="min-w-[280px]">
                <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Iterations</label>
                <div className="mt-1 max-h-48 overflow-y-auto">
                  <div className="flex text-[10px] font-bold py-0.5 sticky top-0" style={{ color: "var(--text-muted)", background: "var(--bg-panel)" }}>
                    <span className="w-8 text-center">#</span>
                    <span className="flex-1 text-right">Objective</span>
                    <span className="flex-1 text-right">Max dLambda</span>
                    <span className="w-10 text-center">OK</span>
                  </div>
                  {hist.map(e => (
                    <div key={e.iteration} className="flex text-[10px] font-mono py-0.5" style={{ color: "var(--text-secondary)" }}>
                      <span className="w-8 text-center">{e.iteration}</span>
                      <span className="flex-1 text-right">{formatNumber(e.total_objective)}</span>
                      <span className="flex-1 text-right">{e.max_lambda_change.toExponential(2)}</span>
                      <span className="w-10 text-center">{e.all_constraints_satisfied ? "Y" : "N"}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )
        })()}
      </div>
    </div>
  )
}
