import { useState } from "react"
import { X, Plus, Trash, SlidersHorizontal } from "lucide-react"
import { InputSourcesBar, INPUT_STYLE } from "./_shared"
import type { InputSource, OnUpdateConfig } from "./_shared"
import type { ContinuousRule, CategoricalRule, BandingFactor } from "../../types/banding"

function normaliseBandingFactors(config: Record<string, unknown>): BandingFactor[] {
  const raw = config.factors as BandingFactor[] | undefined
  if (Array.isArray(raw) && raw.length > 0) return raw
  return [{ banding: "continuous", column: "", outputColumn: "", rules: [], default: null }]
}

const EMPTY_CONTINUOUS: ContinuousRule = { op1: ">", val1: "", op2: "", val2: "", assignment: "" }
const EMPTY_CATEGORICAL: CategoricalRule = { value: "", assignment: "" }
const OPS = ["<", "<=", ">", ">=", "="]

function BandingRulesGrid({ factor, onUpdateFactor }: { factor: BandingFactor; onUpdateFactor: (patch: Partial<BandingFactor>) => void }) {
  const rules = factor.rules || []
  const bt = factor.banding || "continuous"

  const setRules = (r: (ContinuousRule | CategoricalRule)[]) => onUpdateFactor({ rules: r })
  const updateRule = (idx: number, field: string, value: string) => {
    const next = [...rules]; next[idx] = { ...next[idx], [field]: value }; setRules(next)
  }
  const removeRule = (idx: number) => setRules(rules.filter((_, i) => i !== idx))

  return (
    <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)', background: 'var(--bg-input)' }}>
      {bt === "continuous" ? (
        <table className="w-full text-[11px]">
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 52 }}>Op</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 60 }}>Value</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 52, opacity: 0.55 }}>Op</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 60, opacity: 0.55 }}>Value</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Band</th>
              <th style={{ width: 28 }}></th>
            </tr>
          </thead>
          <tbody>
            {rules.length === 0 ? (
              <tr><td colSpan={6} className="px-2 py-3 text-center" style={{ color: 'var(--text-muted)' }}>No rules yet</td></tr>
            ) : (rules as ContinuousRule[]).map((rule, i) => (
              <tr key={i} style={{ borderBottom: '1px solid var(--border)' }}>
                <td className="px-1 py-1">
                  <select value={rule.op1 || ""} onChange={(e) => updateRule(i, "op1", e.target.value)}
                    className="w-full px-1 py-1 rounded text-[11px] font-mono appearance-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}>
                    <option value="">—</option>
                    {OPS.map((o) => <option key={o} value={o}>{o}</option>)}
                  </select>
                </td>
                <td className="px-1 py-1">
                  <input type="text" value={rule.val1 ?? ""} onChange={(e) => updateRule(i, "val1", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }} placeholder="0" />
                </td>
                <td className="px-1 py-1">
                  <select value={rule.op2 || ""} onChange={(e) => updateRule(i, "op2", e.target.value)}
                    className="w-full px-1 py-1 rounded text-[11px] font-mono appearance-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}>
                    <option value="">—</option>
                    {OPS.map((o) => <option key={o} value={o}>{o}</option>)}
                  </select>
                </td>
                <td className="px-1 py-1">
                  <input type="text" value={rule.val2 ?? ""} onChange={(e) => updateRule(i, "val2", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }} placeholder="" />
                </td>
                <td className="px-1 py-1">
                  <input type="text" value={rule.assignment ?? ""} onChange={(e) => updateRule(i, "assignment", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono font-semibold focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: '#14b8a6' }} placeholder="band" />
                </td>
                <td className="px-1 py-1 text-center">
                  <button onClick={() => removeRule(i)} className="p-0.5 rounded transition-colors" style={{ color: 'var(--text-muted)' }}
                    onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                    onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}><Trash size={11} /></button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <table className="w-full text-[11px]">
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Value</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Group</th>
              <th style={{ width: 28 }}></th>
            </tr>
          </thead>
          <tbody>
            {rules.length === 0 ? (
              <tr><td colSpan={3} className="px-2 py-3 text-center" style={{ color: 'var(--text-muted)' }}>No rules yet</td></tr>
            ) : (rules as CategoricalRule[]).map((rule, i) => (
              <tr key={i} style={{ borderBottom: '1px solid var(--border)' }}>
                <td className="px-1 py-1">
                  <input type="text" value={rule.value ?? ""} onChange={(e) => updateRule(i, "value", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }} placeholder="Semi-detached House" />
                </td>
                <td className="px-1 py-1">
                  <input type="text" value={rule.assignment ?? ""} onChange={(e) => updateRule(i, "assignment", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono font-semibold focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: '#14b8a6' }} placeholder="House" />
                </td>
                <td className="px-1 py-1 text-center">
                  <button onClick={() => removeRule(i)} className="p-0.5 rounded transition-colors" style={{ color: 'var(--text-muted)' }}
                    onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                    onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}><Trash size={11} /></button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

export default function BandingEditor({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
  upstreamColumns = [],
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  upstreamColumns?: { name: string; dtype: string }[]
}) {
  const factors = normaliseBandingFactors(config)
  const [activeIdx, setActiveIdx] = useState(0)
  const safeIdx = Math.min(activeIdx, factors.length - 1)
  const factor = factors[safeIdx]

  const colMap = Object.fromEntries(upstreamColumns.map(c => [c.name, c.dtype]))

  const isNumericDtype = (dtype: string) => {
    const d = dtype.toLowerCase()
    return d.startsWith("int") || d.startsWith("uint") || d.startsWith("float") || d === "f32" || d === "f64" || d === "i8" || d === "i16" || d === "i32" || d === "i64" || d === "u8" || d === "u16" || d === "u32" || d === "u64"
  }

  const inferBandingType = (colName: string): string | null => {
    const dtype = colMap[colName]
    if (!dtype) return null
    return isNumericDtype(dtype) ? "continuous" : "categorical"
  }

  const commitFactors = (next: BandingFactor[]) => {
    onUpdate("factors", next)
  }

  const updateFactor = (idx: number, patch: Partial<BandingFactor>) => {
    const next = factors.map((f, i) => i === idx ? { ...f, ...patch } : f)
    commitFactors(next)
  }

  const setColumnWithAutoDetect = (idx: number, colName: string) => {
    const patch: Partial<BandingFactor> = { column: colName }
    const detected = inferBandingType(colName)
    if (detected && detected !== factors[idx].banding) {
      patch.banding = detected
      patch.rules = []
    }
    updateFactor(idx, patch)
  }

  const addFactor = () => {
    const next = [...factors, { banding: "continuous" as const, column: "", outputColumn: "", rules: [] as (ContinuousRule | CategoricalRule)[], default: null }]
    commitFactors(next)
    setActiveIdx(next.length - 1)
  }

  const removeFactor = (idx: number) => {
    if (factors.length <= 1) return
    const next = factors.filter((_, i) => i !== idx)
    commitFactors(next)
    if (safeIdx >= next.length) setActiveIdx(next.length - 1)
  }

  const tabLabel = (f: BandingFactor, i: number) => {
    if (f.outputColumn) return f.outputColumn
    if (f.column) return f.column
    return `Factor ${i + 1}`
  }

  return (
    <div className="px-4 py-3 space-y-3 overflow-y-auto">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs font-medium"
        style={{ background: 'rgba(20,184,166,.1)', border: '1px solid rgba(20,184,166,.3)', color: '#14b8a6' }}>
        <SlidersHorizontal size={14} />
        <span>Group values into bands — {factors.length} factor{factors.length !== 1 ? 's' : ''}</span>
      </div>

      {/* Factor tabs */}
      <div>
        <div className="flex items-center gap-1 flex-wrap">
          {factors.map((f, i) => (
            <button
              key={i}
              onClick={() => setActiveIdx(i)}
              className="relative flex items-center gap-1 px-2.5 py-1.5 rounded-t-lg text-[11px] font-medium transition-colors"
              style={{
                background: i === safeIdx ? 'var(--bg-input)' : 'transparent',
                border: i === safeIdx ? '1px solid var(--border)' : '1px solid transparent',
                borderBottom: i === safeIdx ? '1px solid var(--bg-input)' : '1px solid var(--border)',
                color: i === safeIdx ? '#14b8a6' : 'var(--text-muted)',
              }}
            >
              <span className="font-mono truncate max-w-[100px]">{tabLabel(f, i)}</span>
              {factors.length > 1 && (
                <span
                  onClick={(e) => { e.stopPropagation(); removeFactor(i) }}
                  className="ml-0.5 p-0.5 rounded transition-colors cursor-pointer"
                  style={{ color: 'var(--text-muted)' }}
                  onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                  onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}
                >
                  <X size={9} />
                </span>
              )}
            </button>
          ))}
          <button
            onClick={addFactor}
            className="flex items-center gap-0.5 px-2 py-1.5 rounded-lg text-[11px] font-medium transition-colors"
            style={{ color: '#14b8a6' }}
            onMouseEnter={(e) => { e.currentTarget.style.background = 'rgba(20,184,166,.1)' }}
            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent' }}
          >
            <Plus size={11} />
          </button>
        </div>
        <div style={{ borderTop: '1px solid var(--border)', marginTop: -1 }} />
      </div>

      {/* Active factor config */}
      <div>
        <div className="flex items-center gap-1.5">
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Type</label>
          {factor.column && colMap[factor.column] && (
            <span className="text-[10px] font-medium" style={{ color: 'var(--text-muted)', opacity: 0.7 }}>
              auto: {colMap[factor.column]}
            </span>
          )}
        </div>
        <div className="mt-1 flex gap-1.5">
          {(["continuous", "categorical"] as const).map((bt) => (
            <button
              key={bt}
              onClick={() => updateFactor(safeIdx, { banding: bt, rules: [] })}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: factor.banding === bt ? 'rgba(20,184,166,.1)' : 'var(--bg-input)',
                border: factor.banding === bt ? '1px solid #14b8a6' : '1px solid var(--border)',
                color: factor.banding === bt ? '#14b8a6' : 'var(--text-secondary)',
              }}
            >
              {bt.charAt(0).toUpperCase() + bt.slice(1)}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Input Column</label>
          {upstreamColumns.length > 0 ? (
            <select
              key={`col-${safeIdx}`}
              value={factor.column}
              onChange={(e) => setColumnWithAutoDetect(safeIdx, e.target.value)}
              className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
              style={INPUT_STYLE}
            >
              <option value="">Select column...</option>
              {upstreamColumns.map(c => (
                <option key={c.name} value={c.name}>
                  {c.name} ({c.dtype})
                </option>
              ))}
            </select>
          ) : (
            <input
              key={`col-${safeIdx}`}
              type="text" placeholder="driver_age" defaultValue={factor.column}
              onChange={(e) => updateFactor(safeIdx, { column: e.target.value })}
              className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
              style={INPUT_STYLE} />
          )}
        </div>
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Output Column</label>
          <input
            key={`out-${safeIdx}`}
            type="text" placeholder="age_band" defaultValue={factor.outputColumn}
            onChange={(e) => updateFactor(safeIdx, { outputColumn: e.target.value })}
            className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
            style={INPUT_STYLE} />
        </div>
      </div>

      {/* Rules grid + add button */}
      <div>
        <div className="flex items-center justify-between mb-1.5">
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
            Rules ({(factor.rules || []).length})
          </label>
          <button
            onClick={() => {
              const empty = factor.banding === "continuous" ? { ...EMPTY_CONTINUOUS } : { ...EMPTY_CATEGORICAL }
              updateFactor(safeIdx, { rules: [...(factor.rules || []), empty] })
            }}
            className="flex items-center gap-1 px-2 py-1 rounded-md text-[11px] font-medium transition-colors"
            style={{ background: 'rgba(20,184,166,.1)', color: '#14b8a6', border: '1px solid rgba(20,184,166,.3)' }}
            onMouseEnter={(e) => { e.currentTarget.style.background = 'rgba(20,184,166,.2)' }}
            onMouseLeave={(e) => { e.currentTarget.style.background = 'rgba(20,184,166,.1)' }}
          >
            <Plus size={11} /> Add
          </button>
        </div>
        <BandingRulesGrid
          key={safeIdx}
          factor={factor}
          onUpdateFactor={(patch) => updateFactor(safeIdx, patch)}
        />
      </div>

      {/* Default value */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>
          Default <span className="ml-1.5 normal-case tracking-normal font-normal">(unmatched rows)</span>
        </label>
        <input
          key={`def-${safeIdx}`}
          type="text" placeholder="null" defaultValue={factor.default || ""}
          onChange={(e) => updateFactor(safeIdx, { default: e.target.value || null })}
          className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
          style={INPUT_STYLE} />
      </div>

      {/* Summary across all factors */}
      {factors.some(f => f.column && f.outputColumn && (f.rules || []).length > 0) && (
        <div className="rounded-lg px-3 py-2 space-y-1" style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border)' }}>
          {factors.map((f, i) => {
            if (!f.column || !f.outputColumn || !(f.rules || []).length) return null
            return (
              <div key={i} className="text-[10px] leading-relaxed" style={{ color: 'var(--text-muted)' }}>
                <span className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{f.column}</span>
                {' → '}
                <span className="font-mono font-medium" style={{ color: '#14b8a6' }}>{f.outputColumn}</span>
                {' · '}{f.rules.length} rule{f.rules.length !== 1 ? 's' : ''}
                {' · '}{f.banding}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
