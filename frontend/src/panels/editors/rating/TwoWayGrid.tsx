import { useMemo } from "react"
import type { RatingTable } from "./ratingTableUtils"
import { relativityColor, relativityTextColor, tableStats } from "./ratingTableUtils"
import { StatsFooter } from "./StatsFooter"

export function TwoWayGrid({ table, bandingLevels, onUpdateEntries, factorOverrides }: {
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
