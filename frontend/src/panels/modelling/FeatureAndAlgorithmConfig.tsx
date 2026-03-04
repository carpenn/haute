import { ChevronDown, ChevronRight } from "lucide-react"
import type { OnUpdateConfig } from "../editors"
import { configField } from "../../utils/configField"

type Column = { name: string; dtype: string }

const REGRESSION_LOSSES = ["RMSE", "MAE", "Poisson", "Tweedie"]
const CLASSIFICATION_LOSSES = ["Logloss", "CrossEntropy"]

export type FeatureAndAlgorithmConfigProps = {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  columns: Column[]
  target: string
  weight: string
  exclude: string[]
  algorithm: string
  task: string
  params: Record<string, unknown>
  featureCount: number
  advancedOpen: boolean
  toggleSection: (section: string) => void
  onParamUpdate: (key: string, value: unknown) => void
}

export function FeatureAndAlgorithmConfig({
  config,
  onUpdate,
  columns,
  target,
  weight,
  exclude,
  algorithm,
  task,
  params,
  featureCount,
  advancedOpen,
  toggleSection,
  onParamUpdate,
}: FeatureAndAlgorithmConfigProps) {
  return (
    <>
      {/* Feature Selection */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>
          Features {columns.length > 0 && <span className="font-normal">({featureCount} of {columns.length})</span>}
        </label>
        <div className="mt-1.5">
          <label className="text-xs" style={{ color: "var(--text-secondary)" }}>Exclude columns</label>
          <div className="mt-1 flex flex-wrap gap-1">
            {columns.filter(c => c.name !== target && c.name !== weight).map(c => {
              const excluded = exclude.includes(c.name)
              return (
                <button
                  key={c.name}
                  onClick={() => {
                    const newExclude = excluded ? exclude.filter(e => e !== c.name) : [...exclude, c.name]
                    onUpdate("exclude", newExclude)
                  }}
                  className="px-2 py-0.5 rounded text-[11px] font-mono transition-colors"
                  style={{
                    background: excluded ? "rgba(239,68,68,.15)" : "var(--chrome-hover)",
                    color: excluded ? "#ef4444" : "var(--text-secondary)",
                    textDecoration: excluded ? "line-through" : "none",
                  }}
                >
                  {c.name}
                </button>
              )
            })}
          </div>
        </div>
      </div>

      {/* Algorithm & Key Params */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Algorithm</label>
        <div className="mt-1.5 space-y-2">
          <select
            value={algorithm}
            onChange={(e) => onUpdate("algorithm", e.target.value)}
            className="w-full px-2.5 py-1.5 rounded-lg text-xs font-mono"
            style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
          >
            <option value="catboost">CatBoost</option>
          </select>
          <div>
            <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Loss function</label>
            <select
              value={configField(config, "loss_function", "")}
              onChange={(e) => onUpdate("loss_function", e.target.value || null)}
              className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
              style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
            >
              <option value="">Default</option>
              {(task === "classification" ? CLASSIFICATION_LOSSES : REGRESSION_LOSSES).map(l => (
                <option key={l} value={l}>{l}</option>
              ))}
            </select>
          </div>
          {configField(config, "loss_function", "") === "Tweedie" && (
            <div>
              <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Variance power (1.0=Poisson, 2.0=Gamma)</label>
              <input
                type="range" min={1.0} max={2.0} step={0.05}
                value={configField(config, "variance_power", 1.5)}
                onChange={(e) => onUpdate("variance_power", parseFloat(e.target.value))}
                className="w-full mt-0.5"
              />
              <div className="text-[11px] font-mono text-right" style={{ color: "var(--text-muted)" }}>
                {configField(config, "variance_power", 1.5).toFixed(2)}
              </div>
            </div>
          )}
          {/* Core params */}
          <div className="grid grid-cols-2 gap-2">
            {[
              { key: "iterations", label: "Iterations", default: 1000, step: 1 },
              { key: "learning_rate", label: "Learning Rate", default: 0.05, step: 0.01 },
              { key: "depth", label: "Depth", default: 6, step: 1 },
              { key: "l2_leaf_reg", label: "L2 Reg", default: 3, step: 0.1 },
            ].map(p => (
              <div key={p.key}>
                <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>{p.label}</label>
                <input
                  type="number"
                  step={p.step}
                  value={(params[p.key] as number) ?? p.default}
                  onChange={(e) => onParamUpdate(p.key, parseFloat(e.target.value) || p.default)}
                  className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                />
              </div>
            ))}
          </div>
          {/* Regularisation params */}
          <div className="grid grid-cols-2 gap-2">
            {[
              { key: "random_strength", label: "Random Strength", default: 1, step: 0.1 },
              { key: "bagging_temperature", label: "Bagging Temp", default: 1, step: 0.1 },
              { key: "min_data_in_leaf", label: "Min Data in Leaf", default: 1, step: 1 },
              { key: "border_count", label: "Border Count", default: 254, step: 1 },
            ].map(p => (
              <div key={p.key}>
                <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>{p.label}</label>
                <input
                  type="number"
                  step={p.step}
                  value={(params[p.key] as number) ?? p.default}
                  onChange={(e) => onParamUpdate(p.key, parseFloat(e.target.value) || p.default)}
                  className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                />
              </div>
            ))}
          </div>
          {/* Grow policy */}
          <div>
            <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Grow policy</label>
            <select
              value={configField(params, "grow_policy", "SymmetricTree")}
              onChange={(e) => onParamUpdate("grow_policy", e.target.value)}
              className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
              style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
            >
              {["SymmetricTree", "Lossguide", "Depthwise"].map(p => (
                <option key={p} value={p}>{p}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Early stopping rounds (0 = disabled)</label>
            <input
              type="number"
              min={0}
              step={1}
              value={(params.early_stopping_rounds as number) ?? 50}
              onChange={(e) => onParamUpdate("early_stopping_rounds", parseInt(e.target.value) || 0)}
              className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
              style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
            />
          </div>
          {/* GPU toggle */}
          <label className="flex items-center gap-2 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={(params.task_type as string) === "GPU"}
              onChange={(e) => onParamUpdate("task_type", e.target.checked ? "GPU" : "CPU")}
              className="accent-purple-500"
            />
            <span className="text-[11px]" style={{ color: "var(--text-primary)" }}>
              GPU training
            </span>
            <span className="text-[10px]" style={{ color: "var(--text-muted)" }}>
              (CUDA)
            </span>
          </label>
          <button
            onClick={() => toggleSection("modelling.advanced")}
            className="flex items-center gap-1 text-[11px]"
            style={{ color: "var(--text-muted)" }}
          >
            {advancedOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
            Advanced params (JSON)
          </button>
          {advancedOpen && (
            <textarea
              value={JSON.stringify(params, null, 2)}
              onChange={(e) => {
                try { onUpdate("params", JSON.parse(e.target.value)) } catch { /* invalid JSON */ }
              }}
              rows={6}
              className="w-full px-2.5 py-2 rounded-lg text-xs font-mono"
              style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
            />
          )}
        </div>
      </div>
    </>
  )
}
