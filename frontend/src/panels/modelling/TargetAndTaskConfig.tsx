import type { OnUpdateConfig } from "../editors"
import { configField } from "../../utils/configField"

type Column = { name: string; dtype: string }

export type TargetAndTaskConfigProps = {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  columns: Column[]
  target: string
  weight: string
  task: string
}

export function TargetAndTaskConfig({ config, onUpdate, columns, target, weight, task }: TargetAndTaskConfigProps) {
  return (
    <div>
      <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Target & Weight</label>
      <div className="mt-1.5 space-y-2">
        <div>
          <label className="text-xs" style={{ color: "var(--text-secondary)" }}>Target column</label>
          <select
            value={target}
            onChange={(e) => onUpdate("target", e.target.value)}
            className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
            style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
          >
            <option value="">Select target...</option>
            {columns.map(c => <option key={c.name} value={c.name}>{c.name} ({c.dtype})</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs" style={{ color: "var(--text-secondary)" }}>Weight column (optional)</label>
          <select
            value={weight}
            onChange={(e) => onUpdate("weight", e.target.value)}
            className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
            style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
          >
            <option value="">None</option>
            {columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs" style={{ color: "var(--text-secondary)" }}>Offset column (optional, e.g. log-exposure)</label>
          <select
            value={configField(config, "offset", "")}
            onChange={(e) => onUpdate("offset", e.target.value || null)}
            className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
            style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
          >
            <option value="">None</option>
            {columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs" style={{ color: "var(--text-secondary)" }}>Task</label>
          <div className="flex gap-2 mt-0.5">
            {["regression", "classification"].map(t => (
              <button
                key={t}
                onClick={() => {
                  onUpdate("task", t)
                  onUpdate("metrics", t === "regression" ? ["gini", "rmse"] : ["auc", "logloss"])
                }}
                className="px-3 py-1 rounded-md text-xs font-medium transition-colors"
                style={{
                  background: task === t ? "var(--accent-soft)" : "var(--input-bg)",
                  color: task === t ? "var(--accent)" : "var(--text-secondary)",
                  border: `1px solid ${task === t ? "var(--accent)" : "var(--border)"}`,
                }}
              >
                {t}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
