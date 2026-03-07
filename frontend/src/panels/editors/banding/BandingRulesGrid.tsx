import { Trash } from "lucide-react"
import type { BandingFactor, ContinuousRule, CategoricalRule } from "../../../types/banding"

const OPS = ["<", "<=", ">", ">=", "="]

export function BandingRulesGrid({ factor, onUpdateFactor }: { factor: BandingFactor; onUpdateFactor: (patch: Partial<BandingFactor>) => void }) {
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
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: '#22d3ee' }} placeholder="band" />
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
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: '#22d3ee' }} placeholder="House" />
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
