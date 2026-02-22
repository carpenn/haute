import { useState, useMemo } from "react"
import { X, Plus, Table2 } from "lucide-react"
import { InputSourcesBar } from "./_shared"
import type { InputSource, SimpleNode } from "./_shared"
import { NODE_TYPES } from "../../utils/nodeTypes"

// ─── Types & helpers ──────────────────────────────────────────────

type ContinuousRule = { op1: string; val1: string; op2: string; val2: string; assignment: string }
type CategoricalRule = { value: string; assignment: string }
type BandingFactor = {
  banding: string
  column: string
  outputColumn: string
  rules: (ContinuousRule | CategoricalRule)[]
  default?: string | null
}

type RatingTable = {
  name: string
  factors: string[]
  outputColumn: string
  defaultValue: string | null
  entries: Record<string, string | number>[]
}

const INPUT_STYLE = { background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }

function normaliseRatingTables(config: Record<string, unknown>): RatingTable[] {
  const raw = config.tables as RatingTable[] | undefined
  if (Array.isArray(raw) && raw.length > 0) return raw
  return [{ name: "Table 1", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }]
}

function extractBandingLevels(allNodes: SimpleNode[]): Record<string, string[]> {
  const levelSets: Record<string, Set<string>> = {}
  for (const n of allNodes) {
    if (n.data.nodeType !== NODE_TYPES.BANDING) continue
    const cfg = (n.data.config || {}) as Record<string, unknown>
    const factors = cfg.factors as BandingFactor[] | undefined
    if (!Array.isArray(factors)) continue
    for (const f of factors) {
      if (!f.outputColumn) continue
      if (!levelSets[f.outputColumn]) levelSets[f.outputColumn] = new Set()
      for (const r of f.rules || []) {
        const a = (r as Record<string, string>).assignment
        if (a) levelSets[f.outputColumn].add(a)
      }
    }
  }
  const levels: Record<string, string[]> = {}
  for (const [col, s] of Object.entries(levelSets)) {
    if (s.size > 0) levels[col] = [...s]
  }
  return levels
}

/** Heatmap color for actuarial relativity values. */
function relativityColor(value: number): string {
  if (isNaN(value)) return 'transparent'
  const dev = value - 1.0
  const t = Math.min(Math.abs(dev) / 0.5, 1)
  if (dev > 0.005)  return `rgba(239, 68, 68, ${(t * 0.22).toFixed(3)})`
  if (dev < -0.005) return `rgba(59, 130, 246, ${(t * 0.22).toFixed(3)})`
  return 'transparent'
}

function relativityTextColor(value: number): string {
  if (isNaN(value)) return 'var(--text-secondary)'
  const dev = value - 1.0
  if (dev > 0.005) return '#dc2626'
  if (dev < -0.005) return '#2563eb'
  return '#10b981'
}

function tableStats(entries: Record<string, string | number>[]): { min: number; max: number; avg: number; count: number } | null {
  const vals = entries.map(e => typeof e.value === 'number' ? e.value : parseFloat(String(e.value ?? ''))).filter(v => !isNaN(v))
  if (vals.length === 0) return null
  const min = Math.min(...vals)
  const max = Math.max(...vals)
  const avg = vals.reduce((s, v) => s + v, 0) / vals.length
  return { min, max, avg, count: vals.length }
}

function buildCartesianEntries(
  factors: string[],
  bandingLevels: Record<string, string[]>,
  existing: Record<string, string | number>[],
  defaultValue: string | null,
): Record<string, string | number>[] {
  if (factors.length === 0) return []
  const levelArrays = factors.map(f => bandingLevels[f] || [])
  if (levelArrays.some(a => a.length === 0)) return existing

  const existingLookup = new Map<string, number>()
  for (const e of existing) {
    const key = factors.map(f => String(e[f] ?? "")).join("|")
    const v = e.value
    if (v !== undefined && v !== null && v !== "") {
      existingLookup.set(key, typeof v === "number" ? v : parseFloat(String(v)))
    }
  }

  const defVal = defaultValue != null && String(defaultValue).trim() ? parseFloat(String(defaultValue)) : 1.0
  const entries: Record<string, string | number>[] = []

  function recurse(depth: number, current: Record<string, string>) {
    if (depth === factors.length) {
      const key = factors.map(f => current[f]).join("|")
      entries.push({ ...current, value: existingLookup.get(key) ?? defVal })
      return
    }
    for (const level of levelArrays[depth]) {
      recurse(depth + 1, { ...current, [factors[depth]]: level })
    }
  }
  recurse(0, {})
  return entries
}

function StatsFooter({ stats }: { stats: { min: number; max: number; avg: number; count: number } | null }) {
  if (!stats) return null
  return (
    <div className="flex items-center gap-3 px-2.5 py-1.5 text-[10px] font-mono rounded-b-lg"
      style={{ background: 'var(--bg-elevated)', borderTop: '1px solid var(--border)', color: 'var(--text-muted)' }}>
      <span>n={stats.count}</span>
      <span style={{ color: '#2563eb' }}>min {stats.min.toFixed(3)}</span>
      <span style={{ color: 'var(--text-secondary)' }}>avg {stats.avg.toFixed(3)}</span>
      <span style={{ color: '#dc2626' }}>max {stats.max.toFixed(3)}</span>
    </div>
  )
}

// ─── OneWayEditor ─────────────────────────────────────────────────

function OneWayEditor({ table, bandingLevels, onUpdateEntries }: {
  table: RatingTable
  bandingLevels: Record<string, string[]>
  onUpdateEntries: (entries: Record<string, string | number>[]) => void
}) {
  const factor = table.factors[0]
  const entries = useMemo(() => table.entries || [], [table.entries])
  const stats = useMemo(() => tableStats(entries), [entries])

  if (!factor) return null
  const levels = bandingLevels[factor] || []

  const lookup = new Map<string, number>()
  for (const e of entries) {
    const k = String(e[factor] ?? "")
    if (k) lookup.set(k, typeof e.value === "number" ? e.value : parseFloat(String(e.value ?? "1")))
  }
  const maxVal = stats ? Math.max(Math.abs(stats.max), Math.abs(stats.min), 1) : 1

  const updateCell = (level: string, val: string) => {
    const num = val === "" ? 0 : parseFloat(val)
    const next = entries.map(e => String(e[factor]) === level ? { ...e, value: isNaN(num) ? 0 : num } : e)
    if (!next.some(e => String(e[factor]) === level)) {
      next.push({ [factor]: level, value: isNaN(num) ? 0 : num })
    }
    onUpdateEntries(next)
  }

  return (
    <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)' }}>
      <table className="w-full text-[11px]" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
        <thead>
          <tr style={{ background: 'var(--bg-elevated)' }}>
            <th className="text-left px-2.5 py-2 font-bold uppercase tracking-[0.06em] text-[10px]"
              style={{ color: 'var(--text-muted)', borderBottom: '2px solid var(--border)' }}>{factor}</th>
            <th className="text-center px-2 py-2 font-bold uppercase tracking-[0.06em] text-[10px]"
              style={{ color: 'var(--text-muted)', borderBottom: '2px solid var(--border)', width: 80 }}>Relativity</th>
            <th className="px-2 py-2 text-[10px]"
              style={{ color: 'var(--text-muted)', borderBottom: '2px solid var(--border)', width: '40%' }}></th>
          </tr>
        </thead>
        <tbody>
          {levels.length === 0 ? (
            <tr><td colSpan={3} className="px-2 py-4 text-center" style={{ color: 'var(--text-muted)' }}>No banding levels found</td></tr>
          ) : levels.map((level, ri) => {
            const val = lookup.get(level) ?? 1
            const barWidth = Math.min((Math.abs(val) / maxVal) * 100, 100)
            return (
              <tr key={level} style={{
                borderBottom: '1px solid var(--border)',
                background: ri % 2 === 0 ? 'var(--bg-input)' : 'var(--bg-surface)',
              }}>
                <td className="px-2.5 py-1.5 font-mono text-[11px] font-medium"
                  style={{ color: 'var(--text-primary)', borderBottom: '1px solid var(--border)' }}>{level}</td>
                <td className="px-0.5 py-0.5" style={{ borderBottom: '1px solid var(--border)' }}>
                  <input type="number" step="0.01"
                    defaultValue={val}
                    onBlur={(e) => updateCell(level, e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono text-center focus:outline-none focus:ring-1 focus:ring-emerald-500/40"
                    style={{
                      background: relativityColor(val),
                      border: '1px solid var(--border)',
                      color: relativityTextColor(val),
                      fontWeight: 600,
                    }} />
                </td>
                <td className="px-2 py-1.5" style={{ borderBottom: '1px solid var(--border)' }}>
                  <div className="relative h-3 rounded-full overflow-hidden" style={{ background: 'var(--bg-elevated)' }}>
                    <div className="absolute inset-y-0 left-0 rounded-full transition-all"
                      style={{
                        width: `${barWidth}%`,
                        background: val >= 1 ? 'rgba(239, 68, 68, 0.35)' : 'rgba(59, 130, 246, 0.35)',
                      }} />
                  </div>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
      <StatsFooter stats={stats} />
    </div>
  )
}

// ─── TwoWayGrid ───────────────────────────────────────────────────

function TwoWayGrid({ table, bandingLevels, onUpdateEntries, factorOverrides }: {
  table: RatingTable
  bandingLevels: Record<string, string[]>
  onUpdateEntries: (entries: Record<string, string | number>[]) => void
  factorOverrides?: { factors: string[]; sliceKey?: Record<string, string> }
}) {
  const usedFactors = factorOverrides?.factors || table.factors.slice(0, 2)
  const sliceKey = factorOverrides?.sliceKey || {}
  const rowFactor = usedFactors[0]
  const colFactor = usedFactors[1]
  const entries = useMemo(() => table.entries || [], [table.entries])
  const stats = useMemo(() => tableStats(entries), [entries])

  if (!rowFactor || !colFactor) return null

  const rowLabels = bandingLevels[rowFactor] || []
  const colLabels = bandingLevels[colFactor] || []

  const lookup = new Map<string, number>()
  for (const e of entries) {
    const matchSlice = Object.entries(sliceKey).every(([k, v]) => String(e[k]) === v)
    if (!matchSlice) continue
    const key = `${e[rowFactor]}|${e[colFactor]}`
    lookup.set(key, typeof e.value === "number" ? e.value : parseFloat(String(e.value ?? "1")))
  }

  const updateCell = (row: string, col: string, val: string) => {
    const num = val === "" ? 0 : parseFloat(val)
    const numVal = isNaN(num) ? 0 : num
    const matchRow = (e: Record<string, string | number>) =>
      String(e[rowFactor]) === row && String(e[colFactor]) === col &&
      Object.entries(sliceKey).every(([k, v]) => String(e[k]) === v)

    let found = false
    const next = entries.map(e => {
      if (matchRow(e)) { found = true; return { ...e, value: numVal } }
      return e
    })
    if (!found) {
      next.push({ ...sliceKey, [rowFactor]: row, [colFactor]: col, value: numVal })
    }
    onUpdateEntries(next)
  }

  if (rowLabels.length === 0 || colLabels.length === 0) {
    return <div className="px-2 py-3 text-center text-[11px]" style={{ color: 'var(--text-muted)' }}>No banding levels found for selected factors</div>
  }

  return (
    <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)' }}>
      <div className="overflow-x-auto">
        <table className="w-full text-[11px]" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
          <thead>
            <tr style={{ background: 'var(--bg-elevated)' }}>
              <th className="text-left px-2.5 py-2 font-bold uppercase tracking-[0.06em] text-[10px] sticky left-0 z-10"
                style={{ color: 'var(--text-muted)', borderBottom: '2px solid var(--border)', background: 'var(--bg-elevated)' }}>
                <span style={{ color: 'var(--text-secondary)' }}>{rowFactor}</span>
                <span style={{ color: 'var(--text-muted)', margin: '0 4px' }}>↓</span>
                <span style={{ color: 'var(--text-secondary)' }}>{colFactor}</span>
                <span style={{ color: 'var(--text-muted)', margin: '0 2px' }}>→</span>
              </th>
              {colLabels.map(col => (
                <th key={col} className="text-center px-1 py-2 font-bold font-mono text-[10px] uppercase tracking-[0.04em]"
                  style={{ color: 'var(--text-muted)', minWidth: 64, borderBottom: '2px solid var(--border)' }}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rowLabels.map((row, ri) => (
              <tr key={row} style={{ background: ri % 2 === 0 ? 'var(--bg-input)' : 'var(--bg-surface)' }}>
                <td className="px-2.5 py-1 font-mono text-[11px] font-medium sticky left-0 z-10"
                  style={{
                    color: 'var(--text-primary)',
                    whiteSpace: 'nowrap',
                    borderBottom: '1px solid var(--border)',
                    background: ri % 2 === 0 ? 'var(--bg-input)' : 'var(--bg-surface)',
                  }}>{row}</td>
                {colLabels.map(col => {
                  const val = lookup.get(`${row}|${col}`) ?? 1
                  return (
                    <td key={col} className="px-0.5 py-0.5" style={{ borderBottom: '1px solid var(--border)' }}>
                      <input type="number" step="0.01"
                        defaultValue={val}
                        onBlur={(e) => updateCell(row, col, e.target.value)}
                        className="w-full px-1 py-1 rounded text-[11px] font-mono text-center focus:outline-none focus:ring-1 focus:ring-emerald-500/40"
                        style={{
                          background: relativityColor(val),
                          border: '1px solid var(--border)',
                          color: relativityTextColor(val),
                          fontWeight: 600,
                          minWidth: 56,
                        }} />
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <StatsFooter stats={stats} />
    </div>
  )
}

// ─── Main Editor ──────────────────────────────────────────────────

export default function RatingStepEditor({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
  allNodes,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  allNodes: SimpleNode[]
}) {
  const [activeTab, setActiveTab] = useState(0)
  const [sliceIdx, setSliceIdx] = useState(0)
  const tables = normaliseRatingTables(config)
  const bandingLevels = extractBandingLevels(allNodes)
  const operation = (config.operation as string) || "multiply"
  const combinedColumn = (config.combinedColumn as string) || ""

  const availableColumns = Object.keys(bandingLevels)
  const safeIdx = Math.min(activeTab, tables.length - 1)
  const table = tables[safeIdx] || { name: "Table 1", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }

  const commitTables = (next: RatingTable[]) => onUpdate("tables", next)

  const updateTable = (idx: number, patch: Partial<RatingTable>) => {
    const next = tables.map((t, i) => i === idx ? { ...t, ...patch } : t)
    commitTables(next)
  }

  const setFactors = (idx: number, newFactors: string[]) => {
    const t = tables[idx]
    const rebuilt = buildCartesianEntries(newFactors, bandingLevels, t.entries, t.defaultValue)
    updateTable(idx, { factors: newFactors, entries: rebuilt })
  }

  const addFactor = (idx: number, col: string) => {
    const t = tables[idx]
    if (t.factors.length >= 3 || t.factors.includes(col)) return
    setFactors(idx, [...t.factors, col])
  }

  const removeFactor = (idx: number, factorIdx: number) => {
    const t = tables[idx]
    const next = t.factors.filter((_, i) => i !== factorIdx)
    setFactors(idx, next)
  }

  const onUpdateEntries = (idx: number, entries: Record<string, string | number>[]) => {
    updateTable(idx, { entries })
  }

  const addTable = () => {
    commitTables([...tables, { name: `Table ${tables.length + 1}`, factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }])
    setActiveTab(tables.length)
  }

  const removeTable = (idx: number) => {
    if (tables.length <= 1) return
    const next = tables.filter((_, i) => i !== idx)
    commitTables(next)
    setActiveTab(Math.min(activeTab, next.length - 1))
  }

  const rebuildCurrentEntries = () => {
    const t = tables[safeIdx]
    const rebuilt = buildCartesianEntries(t.factors, bandingLevels, t.entries, t.defaultValue)
    updateTable(safeIdx, { entries: rebuilt })
  }

  const factorCount = table.factors.length

  // For 3-way: factor[2] is the slice dimension
  const sliceFactor = factorCount === 3 ? table.factors[2] : null
  const sliceLevels = sliceFactor ? (bandingLevels[sliceFactor] || []) : []
  const safeSliceIdx = Math.min(sliceIdx, Math.max(0, sliceLevels.length - 1))

  return (
    <div className="px-4 py-3 space-y-3 overflow-y-auto">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs font-medium"
        style={{ background: 'rgba(16,185,129,.1)', border: '1px solid rgba(16,185,129,.3)', color: '#10b981' }}>
        <Table2 size={13} />
        <span>Rating Tables · {tables.length} table{tables.length !== 1 ? 's' : ''}</span>
      </div>

      {/* Combination controls -- shown when 2+ tables exist */}
      {tables.length >= 2 && (
        <div className="space-y-2 p-2.5 rounded-lg" style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)' }}>
          <div className="flex items-center gap-2">
            <label className="text-[11px] font-bold uppercase tracking-[0.08em] shrink-0" style={{ color: 'var(--text-muted)' }}>Combine</label>
            <select value={operation}
              onChange={(e) => onUpdate("operation", e.target.value)}
              className="flex-1 px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none"
              style={INPUT_STYLE}>
              <option value="multiply">× Multiply (relativities)</option>
              <option value="add">+ Add (loadings)</option>
              <option value="min">↓ Min</option>
              <option value="max">↑ Max</option>
            </select>
          </div>
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Combined Output Column</label>
            <input type="text" defaultValue={combinedColumn}
              onBlur={(e) => onUpdate("combinedColumn", e.target.value)}
              className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
              style={INPUT_STYLE} placeholder="combined_factor" />
          </div>
          {/* Formula summary */}
          {tables.some(t => t.outputColumn) && (() => {
            const cols = tables.filter(t => t.outputColumn).map(t => t.outputColumn)
            const lhs = combinedColumn || '?'
            let formula = ''
            if (operation === 'multiply') formula = cols.join(' × ')
            else if (operation === 'add') formula = cols.join(' + ')
            else if (operation === 'min') formula = `min(${cols.join(', ')})`
            else if (operation === 'max') formula = `max(${cols.join(', ')})`
            return (
              <div className="text-[10px] font-mono px-2 py-1.5 rounded flex items-center gap-1.5"
                style={{ background: 'var(--bg-elevated)', color: 'var(--text-secondary)', border: '1px solid var(--border)' }}>
                <span style={{ color: '#10b981', fontWeight: 600 }}>{lhs}</span>
                <span style={{ color: 'var(--text-muted)' }}>=</span>
                <span>{formula}</span>
                {!combinedColumn && <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}> — name the output above</span>}
              </div>
            )
          })()}
        </div>
      )}

      {/* Tab bar */}
      <div className="flex items-center gap-1 overflow-x-auto pb-1">
        {tables.map((t, i) => {
          const tStats = tableStats(t.entries || [])
          return (
            <button key={i} onClick={() => { setActiveTab(i); setSliceIdx(0) }}
              className="group flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[11px] font-medium whitespace-nowrap transition-colors"
              style={{
                background: i === safeIdx ? 'rgba(16,185,129,.12)' : 'var(--bg-surface)',
                border: i === safeIdx ? '1px solid rgba(16,185,129,.4)' : '1px solid var(--border)',
                color: i === safeIdx ? '#10b981' : 'var(--text-secondary)',
              }}>
              {t.name || `Table ${i + 1}`}
              {tStats && (
                <span className="text-[9px] font-mono px-1 py-0.5 rounded"
                  style={{ background: i === safeIdx ? 'rgba(16,185,129,.15)' : 'var(--bg-elevated)', color: 'var(--text-muted)' }}>
                  {tStats.count}
                </span>
              )}
              {tables.length > 1 && (
                <span onClick={(e) => { e.stopPropagation(); removeTable(i) }}
                  className="ml-0.5 opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer"
                  style={{ color: 'var(--text-muted)' }}><X size={10} /></span>
              )}
            </button>
          )
        })}
        <button onClick={addTable}
          className="p-1.5 rounded-lg transition-colors" style={{ color: 'var(--text-muted)', border: '1px dashed var(--border)' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#10b981'; e.currentTarget.style.color = '#10b981' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.color = 'var(--text-muted)' }}>
          <Plus size={12} />
        </button>
      </div>

      {/* Table name */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Table Name</label>
        <input key={`name-${safeIdx}`} type="text" defaultValue={table.name}
          onBlur={(e) => updateTable(safeIdx, { name: e.target.value })}
          className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
          style={INPUT_STYLE} placeholder="Age Factor" />
      </div>

      {/* Factor selection */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>
          Factors ({factorCount}/3)
        </label>
        <div className="space-y-1.5">
          {table.factors.map((f, fi) => (
            <div key={fi} className="flex items-center gap-1.5">
              <span className="text-[10px] font-bold w-4 text-center" style={{ color: 'var(--text-muted)' }}>{fi + 1}</span>
              <select key={`fsel-${safeIdx}-${fi}`} value={f}
                onChange={(e) => {
                  const next = [...table.factors]
                  next[fi] = e.target.value
                  setFactors(safeIdx, next)
                }}
                className="flex-1 px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none"
                style={INPUT_STYLE}>
                <option value="">Select column...</option>
                {availableColumns.map(c => (
                  <option key={c} value={c}>{c} ({(bandingLevels[c] || []).length} levels)</option>
                ))}
              </select>
              <button onClick={() => removeFactor(safeIdx, fi)}
                className="p-1 rounded transition-colors" style={{ color: 'var(--text-muted)' }}
                onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}>
                <X size={11} />
              </button>
            </div>
          ))}
          {factorCount < 3 && (
            <select value="" onChange={(e) => { if (e.target.value) addFactor(safeIdx, e.target.value) }}
              className="w-full px-2 py-1.5 text-xs rounded-lg focus:outline-none"
              style={{ ...INPUT_STYLE, color: 'var(--text-muted)' }}>
              <option value="">+ Add factor...</option>
              {availableColumns.filter(c => !table.factors.includes(c)).map(c => (
                <option key={c} value={c}>{c} ({(bandingLevels[c] || []).length} levels)</option>
              ))}
            </select>
          )}
        </div>
      </div>

      {/* Output column + default */}
      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Output Column</label>
          <input key={`out-${safeIdx}`} type="text" defaultValue={table.outputColumn}
            onBlur={(e) => updateTable(safeIdx, { outputColumn: e.target.value })}
            className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
            style={INPUT_STYLE} placeholder="age_factor" />
        </div>
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Default</label>
          <input key={`def-${safeIdx}`} type="number" step="0.01" defaultValue={table.defaultValue ?? "1.0"}
            onBlur={(e) => updateTable(safeIdx, { defaultValue: e.target.value })}
            className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
            style={INPUT_STYLE} placeholder="1.0" />
        </div>
      </div>

      {/* Rebuild button */}
      {factorCount > 0 && (
        <button onClick={rebuildCurrentEntries}
          className="w-full px-2 py-1.5 text-[11px] font-medium rounded-lg transition-colors"
          style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#10b981'; e.currentTarget.style.color = '#10b981' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.color = 'var(--text-secondary)' }}>
          ↻ Rebuild from banding levels
        </button>
      )}

      {/* Table editor */}
      {factorCount === 0 && (
        <div className="px-2 py-4 text-center text-[11px]" style={{ color: 'var(--text-muted)' }}>
          Select at least one factor to populate the rating table
        </div>
      )}
      {factorCount === 1 && (
        <OneWayEditor table={table} bandingLevels={bandingLevels}
          onUpdateEntries={(e) => onUpdateEntries(safeIdx, e)} />
      )}
      {factorCount === 2 && (
        <TwoWayGrid table={table} bandingLevels={bandingLevels}
          onUpdateEntries={(e) => onUpdateEntries(safeIdx, e)} />
      )}
      {factorCount === 3 && (
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
              {sliceFactor}
            </label>
            <select value={safeSliceIdx} onChange={(e) => setSliceIdx(Number(e.target.value))}
              className="flex-1 px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none"
              style={INPUT_STYLE}>
              {sliceLevels.map((level, i) => (
                <option key={level} value={i}>{level}</option>
              ))}
            </select>
          </div>
          {sliceLevels.length > 0 && (
            <TwoWayGrid table={table} bandingLevels={bandingLevels}
              onUpdateEntries={(e) => onUpdateEntries(safeIdx, e)}
              factorOverrides={{
                factors: [table.factors[0], table.factors[1]],
                sliceKey: { [table.factors[2]]: sliceLevels[safeSliceIdx] },
              }} />
          )}
        </div>
      )}

      {/* Summary */}
      {table.entries.length > 0 && (() => {
        const s = tableStats(table.entries)
        return (
          <div className="flex items-center justify-between text-[10px] font-mono px-1"
            style={{ color: 'var(--text-muted)' }}>
            <span>{table.outputColumn ? <span style={{ color: 'var(--text-secondary)' }}>{table.outputColumn}</span> : 'untitled'}</span>
            <span>{table.entries.length} entries{s ? ` · range ${s.min.toFixed(2)}–${s.max.toFixed(2)}` : ''}</span>
          </div>
        )
      })()}
    </div>
  )
}
