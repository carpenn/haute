import { useState, useCallback, useRef, useEffect } from "react"
import { Play, Download, Loader2, ChevronDown, ChevronRight, AlertTriangle, FlaskConical } from "lucide-react"
import type { SimpleNode, SimpleEdge } from "./editors"
import { checkMlflow, getTrainStatus, trainModel, exportTraining, logToMlflow } from "../api/client"

type ModellingConfigProps = {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  upstreamColumns?: { name: string; dtype: string }[]
  allNodes: SimpleNode[]
  edges: SimpleEdge[]
  submodels?: Record<string, unknown>
}

type TrainResult = {
  status: string
  metrics: Record<string, number>
  feature_importance: { feature: string; importance: number }[]
  model_path: string
  train_rows: number
  test_rows: number
  error?: string
  best_iteration?: number | null
  loss_history?: { iteration: number; [key: string]: number }[]
  double_lift?: { decile: number; actual: number; predicted: number; count: number }[]
  shap_summary?: { feature: string; mean_abs_shap: number }[]
  feature_importance_loss?: { feature: string; importance: number }[]
  cv_results?: { mean_metrics: Record<string, number>; std_metrics: Record<string, number>; n_folds: number } | null
}

type TrainProgress = {
  status: string
  progress: number
  message: string
  iteration: number
  total_iterations: number
  train_loss: Record<string, number>
  elapsed_seconds: number
  result?: TrainResult
}

const REGRESSION_METRICS = ["gini", "rmse", "mae", "mse", "r2", "poisson_deviance", "tweedie_deviance"]
const CLASSIFICATION_METRICS = ["auc", "logloss"]
const REGRESSION_LOSSES = ["RMSE", "MAE", "Poisson", "Tweedie"]
const CLASSIFICATION_LOSSES = ["Logloss", "CrossEntropy"]

function formatElapsed(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(0)}s`
  const mins = Math.floor(seconds / 60)
  const secs = Math.floor(seconds % 60)
  return `${mins}m ${secs}s`
}

function LossChart({ lossHistory, bestIteration }: { lossHistory: { iteration: number; [key: string]: number }[]; bestIteration?: number | null }) {
  if (!lossHistory || lossHistory.length < 2) return null

  // Find train and eval loss keys
  const keys = Object.keys(lossHistory[0]).filter(k => k !== "iteration")
  const trainKey = keys.find(k => k.startsWith("train_"))
  const evalKey = keys.find(k => k.startsWith("eval_"))
  if (!trainKey) return null

  const w = 280, h = 80, px = 4, py = 4
  const chartW = w - px * 2, chartH = h - py * 2

  // Gather all loss values to find y range
  const allVals: number[] = []
  for (const entry of lossHistory) {
    if (trainKey && entry[trainKey] != null) allVals.push(entry[trainKey])
    if (evalKey && entry[evalKey] != null) allVals.push(entry[evalKey])
  }
  const yMin = Math.min(...allVals)
  const yMax = Math.max(...allVals)
  const yRange = yMax - yMin || 1

  const xScale = (i: number) => px + (i / (lossHistory.length - 1)) * chartW
  const yScale = (v: number) => py + chartH - ((v - yMin) / yRange) * chartH

  const makePath = (key: string) => {
    const points = lossHistory
      .map((e, i) => e[key] != null ? `${i === 0 ? "M" : "L"}${xScale(i).toFixed(1)},${yScale(e[key]).toFixed(1)}` : null)
      .filter(Boolean)
    return points.join(" ")
  }

  // Best iteration vertical line position
  const bestX = bestIteration != null ? xScale(Math.min(bestIteration, lossHistory.length - 1)) : null

  return (
    <div>
      <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Loss Curve</label>
      <svg width={w} height={h} className="mt-1" style={{ background: "var(--input-bg)", borderRadius: 6, border: "1px solid var(--border)" }}>
        <path d={makePath(trainKey)} fill="none" stroke="#a855f7" strokeWidth={1.5} />
        {evalKey && <path d={makePath(evalKey)} fill="none" stroke="#22c55e" strokeWidth={1.5} />}
        {bestX != null && <line x1={bestX} y1={py} x2={bestX} y2={py + chartH} stroke="#f59e0b" strokeWidth={1} strokeDasharray="3,2" />}
      </svg>
      <div className="flex gap-3 mt-1 text-[10px]" style={{ color: "var(--text-muted)" }}>
        <span><span style={{ color: "#a855f7" }}>--</span> Train</span>
        {evalKey && <span><span style={{ color: "#22c55e" }}>--</span> Eval</span>}
        {bestX != null && <span><span style={{ color: "#f59e0b" }}>|</span> Best iter</span>}
      </div>
    </div>
  )
}

export default function ModellingConfig({ config, onUpdate, upstreamColumns, allNodes, edges, submodels }: ModellingConfigProps) {
  const [training, setTraining] = useState(false)
  const [trainResult, setTrainResult] = useState<TrainResult | null>(null)
  const [trainProgress, setTrainProgress] = useState<TrainProgress | null>(null)
  const [exporting, setExporting] = useState(false)
  const [exportedScript, setExportedScript] = useState<string | null>(null)
  const [advancedOpen, setAdvancedOpen] = useState(false)
  const [mlflowOpen, setMlflowOpen] = useState(false)
  const [monotonicOpen, setMonotonicOpen] = useState(false)
  const [importanceType, setImportanceType] = useState<"prediction" | "loss" | "shap">("prediction")
  const [trainJobId, setTrainJobId] = useState<string | null>(null)
  const [loggingToMlflow, setLoggingToMlflow] = useState(false)
  const [mlflowResult, setMlflowResult] = useState<{ status: string; backend?: string; experiment_name?: string; run_id?: string; run_url?: string | null; tracking_uri?: string; error?: string } | null>(null)
  const [mlflowBackend, setMlflowBackend] = useState<{ installed: boolean; backend: string; host: string } | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Clean up polling on unmount
  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [])

  // Check MLflow backend on mount
  useEffect(() => {
    checkMlflow()
      .then(data => setMlflowBackend({ installed: !!data.mlflow_installed, backend: data.backend || "", host: data.databricks_host || "" }))
      .catch(() => {})
  }, [])

  const target = (config.target as string) || ""
  const weight = (config.weight as string) || ""
  const exclude = (config.exclude as string[]) || []
  const algorithm = (config.algorithm as string) || "catboost"
  const task = (config.task as string) || "regression"
  const params = (config.params as Record<string, unknown>) || {}
  const split = (config.split as Record<string, unknown>) || { strategy: "random", test_size: 0.2, seed: 42 }
  const metrics = (config.metrics as string[]) || (task === "regression" ? ["gini", "rmse"] : ["auc", "logloss"])

  const columns = upstreamColumns || []
  const featureCount = columns.filter(c => c.name !== target && c.name !== weight && !exclude.includes(c.name)).length

  const handleParamUpdate = useCallback((key: string, value: unknown) => {
    onUpdate("params", { ...params, [key]: value })
  }, [params, onUpdate])

  const handleSplitUpdate = useCallback((key: string, value: unknown) => {
    onUpdate("split", { ...split, [key]: value })
  }, [split, onUpdate])

  const buildGraph = useCallback(() => ({
    nodes: allNodes.map((n) => ({ id: n.id, type: n.type || n.data.nodeType, data: n.data, position: { x: 0, y: 0 } })),
    edges,
    submodels,
  }), [allNodes, edges, submodels])

  const pollStatus = useCallback((jobId: string) => {
    pollRef.current = setInterval(async () => {
      try {
        const status = await getTrainStatus<TrainProgress>(jobId)
        setTrainProgress(status)

        if (status.status === "completed" || status.status === "error") {
          if (pollRef.current) clearInterval(pollRef.current)
          pollRef.current = null
          setTraining(false)
          if (status.status === "completed" && status.result) {
            setTrainResult(status.result)
          } else if (status.status === "error") {
            setTrainResult({
              status: "error", metrics: {}, feature_importance: [],
              model_path: "", train_rows: 0, test_rows: 0,
              error: status.message,
            })
          }
          setTrainProgress(null)
        }
      } catch {
        // Network error — keep polling
      }
    }, 500)
  }, [])

  const handleTrain = useCallback(async () => {
    setTraining(true)
    setTrainResult(null)
    setTrainProgress(null)
    setTrainJobId(null)
    setMlflowResult(null)
    try {
      const result = await trainModel({ graph: buildGraph(), node_id: config._nodeId as string })

      if (result.status === "started" && result.job_id) {
        // Training running in background — poll for updates
        setTrainJobId(result.job_id as string)
        setTrainProgress({ status: "running", progress: 0, message: "Starting...", iteration: 0, total_iterations: 0, train_loss: {}, elapsed_seconds: 0 })
        pollStatus(result.job_id as string)
      } else if (result.status === "error") {
        // Immediate validation error
        setTrainResult(result as unknown as TrainResult)
        setTraining(false)
      } else {
        // Synchronous completion (shouldn't happen with new backend, but handle it)
        setTrainResult(result as unknown as TrainResult)
        setTraining(false)
      }
    } catch (e) {
      setTrainResult({ status: "error", metrics: {}, feature_importance: [], model_path: "", train_rows: 0, test_rows: 0, error: String(e) })
      setTraining(false)
    }
  }, [config._nodeId, buildGraph, pollStatus])

  const handleExport = useCallback(async () => {
    setExporting(true)
    try {
      const result = await exportTraining({ graph: buildGraph(), node_id: config._nodeId as string, data_path: "" })
      setExportedScript(result.script || null)
    } catch {
      // silently fail
    } finally {
      setExporting(false)
    }
  }, [config._nodeId, buildGraph])

  const handleLogExperiment = useCallback(async () => {
    if (!trainJobId) return
    setLoggingToMlflow(true)
    setMlflowResult(null)
    try {
      const result = await logToMlflow({
        job_id: trainJobId,
        experiment_name: (config.mlflow_experiment as string) || null,
        model_name: (config.model_name as string) || null,
      })
      setMlflowResult(result)
    } catch (e) {
      setMlflowResult({ status: "error", error: String(e) })
    } finally {
      setLoggingToMlflow(false)
    }
  }, [trainJobId, config.mlflow_experiment, config.model_name])

  const availableMetrics = task === "classification" ? CLASSIFICATION_METRICS : REGRESSION_METRICS

  return (
    <div className="px-4 py-3 space-y-4">
      {/* Target & Weight */}
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
              value={(config.offset as string) || ""}
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
              value={(config.loss_function as string) || ""}
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
          {(config.loss_function as string) === "Tweedie" && (
            <div>
              <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Variance power (1.0=Poisson, 2.0=Gamma)</label>
              <input
                type="range" min={1.0} max={2.0} step={0.05}
                value={(config.variance_power as number) ?? 1.5}
                onChange={(e) => onUpdate("variance_power", parseFloat(e.target.value))}
                className="w-full mt-0.5"
              />
              <div className="text-[11px] font-mono text-right" style={{ color: "var(--text-muted)" }}>
                {((config.variance_power as number) ?? 1.5).toFixed(2)}
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
                  onChange={(e) => handleParamUpdate(p.key, parseFloat(e.target.value) || p.default)}
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
                  onChange={(e) => handleParamUpdate(p.key, parseFloat(e.target.value) || p.default)}
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
              value={(params.grow_policy as string) || "SymmetricTree"}
              onChange={(e) => handleParamUpdate("grow_policy", e.target.value)}
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
              onChange={(e) => handleParamUpdate("early_stopping_rounds", parseInt(e.target.value) || 0)}
              className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
              style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
            />
          </div>
          <button
            onClick={() => setAdvancedOpen(!advancedOpen)}
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

      {/* Split Strategy */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Split Strategy</label>
        <div className="mt-1.5 space-y-2">
          <div className="flex gap-2">
            {["random", "temporal", "group"].map(s => (
              <button
                key={s}
                onClick={() => handleSplitUpdate("strategy", s)}
                className="px-3 py-1 rounded-md text-xs font-medium transition-colors"
                style={{
                  background: split.strategy === s ? "var(--accent-soft)" : "var(--input-bg)",
                  color: split.strategy === s ? "var(--accent)" : "var(--text-secondary)",
                  border: `1px solid ${split.strategy === s ? "var(--accent)" : "var(--border)"}`,
                }}
              >
                {s}
              </button>
            ))}
          </div>
          {split.strategy === "random" && (
            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Test size</label>
                <input
                  type="number" step={0.05} min={0.05} max={0.5}
                  value={(split.test_size as number) ?? 0.2}
                  onChange={(e) => handleSplitUpdate("test_size", parseFloat(e.target.value) || 0.2)}
                  className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                />
              </div>
              <div>
                <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Seed</label>
                <input
                  type="number"
                  value={(split.seed as number) ?? 42}
                  onChange={(e) => handleSplitUpdate("seed", parseInt(e.target.value) || 42)}
                  className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                />
              </div>
            </div>
          )}
          {split.strategy === "temporal" && (
            <div className="space-y-2">
              <div>
                <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Date column</label>
                <select
                  value={(split.date_column as string) || ""}
                  onChange={(e) => handleSplitUpdate("date_column", e.target.value)}
                  className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                >
                  <option value="">Select...</option>
                  {columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
                </select>
              </div>
              <div>
                <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Cutoff date</label>
                <input
                  type="date"
                  value={(split.cutoff_date as string) || ""}
                  onChange={(e) => handleSplitUpdate("cutoff_date", e.target.value)}
                  className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                />
              </div>
            </div>
          )}
          {split.strategy === "group" && (
            <div className="space-y-2">
              <div>
                <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Group column</label>
                <select
                  value={(split.group_column as string) || ""}
                  onChange={(e) => handleSplitUpdate("group_column", e.target.value)}
                  className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                >
                  <option value="">Select...</option>
                  {columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
                </select>
              </div>
              <div>
                <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Test size</label>
                <input
                  type="number" step={0.05} min={0.05} max={0.5}
                  value={(split.test_size as number) ?? 0.2}
                  onChange={(e) => handleSplitUpdate("test_size", parseFloat(e.target.value) || 0.2)}
                  className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                />
              </div>
            </div>
          )}
          {/* Cross-validation */}
          <div className="flex items-center gap-2 mt-2">
            <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Cross-validate</label>
            <button
              onClick={() => onUpdate("cv_folds", config.cv_folds ? null : 5)}
              className="px-2 py-0.5 rounded text-[11px] font-mono"
              style={{
                background: config.cv_folds ? "rgba(168,85,247,.15)" : "var(--chrome-hover)",
                color: config.cv_folds ? "#a855f7" : "var(--text-muted)",
                border: `1px solid ${config.cv_folds ? "rgba(168,85,247,.3)" : "transparent"}`,
              }}
            >
              {config.cv_folds ? "On" : "Off"}
            </button>
            {!!config.cv_folds && (
              <div className="flex items-center gap-1">
                <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Folds:</label>
                <input
                  type="number" min={2} max={20} step={1}
                  value={(config.cv_folds as number) || 5}
                  onChange={(e) => onUpdate("cv_folds", parseInt(e.target.value) || 5)}
                  className="w-14 px-2 py-0.5 rounded text-xs font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                />
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Metrics */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Metrics</label>
        <div className="mt-1.5 flex flex-wrap gap-1.5">
          {availableMetrics.map(m => {
            const selected = metrics.includes(m)
            return (
              <button
                key={m}
                onClick={() => {
                  const newMetrics = selected ? metrics.filter(x => x !== m) : [...metrics, m]
                  onUpdate("metrics", newMetrics)
                }}
                className="px-2.5 py-1 rounded-md text-xs font-mono transition-colors"
                style={{
                  background: selected ? "rgba(168,85,247,.15)" : "var(--chrome-hover)",
                  color: selected ? "#a855f7" : "var(--text-muted)",
                  border: `1px solid ${selected ? "rgba(168,85,247,.3)" : "transparent"}`,
                }}
              >
                {m}
              </button>
            )
          })}
        </div>
      </div>

      {/* MLflow (collapsible) */}
      <div>
        <button
          onClick={() => setMlflowOpen(!mlflowOpen)}
          className="flex items-center gap-1 text-[11px] font-bold uppercase tracking-[0.08em]"
          style={{ color: "var(--text-muted)" }}
        >
          {mlflowOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          MLflow Logging
        </button>
        {mlflowOpen && (
          <div className="mt-1.5 space-y-2">
            <div>
              <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Experiment path</label>
              <input
                type="text"
                placeholder="/Shared/haute/experiment"
                value={(config.mlflow_experiment as string) || ""}
                onChange={(e) => onUpdate("mlflow_experiment", e.target.value)}
                className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
                style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
              />
            </div>
            <div>
              <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Model name (registered model)</label>
              <input
                type="text"
                placeholder="Optional"
                value={(config.model_name as string) || ""}
                onChange={(e) => onUpdate("model_name", e.target.value)}
                className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
                style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
              />
            </div>
          </div>
        )}
      </div>

      {/* Monotonic Constraints (collapsible) */}
      {columns.length > 0 && (
        <div>
          <button
            onClick={() => setMonotonicOpen(!monotonicOpen)}
            className="flex items-center gap-1 text-[11px] font-bold uppercase tracking-[0.08em]"
            style={{ color: "var(--text-muted)" }}
          >
            {monotonicOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
            Monotonic Constraints
          </button>
          {monotonicOpen && (
            <div className="mt-1.5 space-y-1">
              <div className="text-[10px]" style={{ color: "var(--text-muted)" }}>Set per-feature constraints (numeric features only)</div>
              {columns
                .filter(c => c.name !== target && c.name !== weight && !exclude.includes(c.name) && !["Utf8", "Categorical", "String"].includes(c.dtype))
                .map(c => {
                  const mc = (config.monotone_constraints as Record<string, number>) || {}
                  const val = mc[c.name] ?? 0
                  return (
                    <div key={c.name} className="flex items-center gap-2">
                      <span className="text-[11px] font-mono flex-1 truncate" style={{ color: "var(--text-secondary)" }}>{c.name}</span>
                      {([-1, 0, 1] as const).map(v => (
                        <button
                          key={v}
                          onClick={() => {
                            const newMc = { ...mc }
                            if (v === 0) { delete newMc[c.name] } else { newMc[c.name] = v }
                            onUpdate("monotone_constraints", Object.keys(newMc).length > 0 ? newMc : null)
                          }}
                          className="px-1.5 py-0.5 rounded text-[10px] font-mono"
                          style={{
                            background: val === v ? (v === 1 ? "rgba(34,197,94,.15)" : v === -1 ? "rgba(239,68,68,.15)" : "var(--accent-soft)") : "var(--chrome-hover)",
                            color: val === v ? (v === 1 ? "#22c55e" : v === -1 ? "#ef4444" : "var(--accent)") : "var(--text-muted)",
                            border: `1px solid ${val === v ? (v === 1 ? "rgba(34,197,94,.3)" : v === -1 ? "rgba(239,68,68,.3)" : "var(--accent)") : "transparent"}`,
                          }}
                        >
                          {v === 1 ? "+1" : v === -1 ? "-1" : "0"}
                        </button>
                      ))}
                    </div>
                  )
                })}
            </div>
          )}
        </div>
      )}

      {/* Actions */}
      <div className="space-y-2 pt-2" style={{ borderTop: "1px solid var(--border)" }}>
        <button
          onClick={handleTrain}
          disabled={training || !target}
          className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-colors"
          style={{
            background: training ? "var(--chrome-hover)" : "#a855f7",
            color: training ? "var(--text-muted)" : "#fff",
            opacity: !target ? 0.5 : 1,
          }}
        >
          {training ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
          {training ? "Training..." : "Train Model"}
        </button>

        <button
          onClick={handleExport}
          disabled={exporting || !target}
          className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-colors"
          style={{
            background: "var(--chrome-hover)",
            color: "var(--text-secondary)",
            opacity: !target ? 0.5 : 1,
          }}
        >
          {exporting ? <Loader2 size={14} className="animate-spin" /> : <Download size={14} />}
          Export Training Script
        </button>
      </div>

      {/* Live Training Progress */}
      {trainProgress && (
        <div className="px-3 py-2.5 rounded-lg text-xs space-y-2" style={{ background: "rgba(168,85,247,.06)", border: "1px solid rgba(168,85,247,.2)" }}>
          {/* Progress bar */}
          <div className="space-y-1">
            <div className="flex justify-between text-[11px]">
              <span style={{ color: "#a855f7" }}>{trainProgress.message || "Training..."}</span>
              <span style={{ color: "var(--text-muted)" }}>{formatElapsed(trainProgress.elapsed_seconds)}</span>
            </div>
            <div className="w-full h-1.5 rounded-full overflow-hidden" style={{ background: "rgba(168,85,247,.15)" }}>
              <div
                className="h-full rounded-full transition-all duration-300"
                style={{ width: `${Math.max(trainProgress.progress * 100, 2)}%`, background: "#a855f7" }}
              />
            </div>
          </div>

          {/* Iteration + loss stats */}
          {trainProgress.total_iterations > 0 && (
            <div className="flex gap-4 text-[11px] font-mono" style={{ color: "var(--text-secondary)" }}>
              <span>
                Round <span style={{ color: "var(--text-primary)" }}>{trainProgress.iteration}</span>
                /{trainProgress.total_iterations}
              </span>
              {Object.entries(trainProgress.train_loss).map(([name, value]) => (
                <span key={name}>
                  {name}: <span style={{ color: "var(--text-primary)" }}>{value.toFixed(4)}</span>
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Train Results */}
      {trainResult && (
        <div className="space-y-2">
          {trainResult.status === "error" ? (
            <div className="px-3 py-2.5 rounded-lg text-xs space-y-1.5" style={{ background: "rgba(239,68,68,.08)", border: "1px solid rgba(239,68,68,.2)" }}>
              <div className="flex items-start gap-2">
                <AlertTriangle size={14} className="shrink-0 mt-0.5" style={{ color: "#ef4444" }} />
                <div className="space-y-1 min-w-0">
                  <div className="font-semibold" style={{ color: "#ef4444" }}>Training failed</div>
                  <div style={{ color: "#fca5a5", lineHeight: "1.5" }}>{trainResult.error}</div>
                </div>
              </div>
            </div>
          ) : (
            <>
              <div className="px-3 py-2 rounded-lg text-xs space-y-0.5" style={{ background: "rgba(34,197,94,.1)", color: "#22c55e" }}>
                <div>Model saved to {trainResult.model_path} ({trainResult.train_rows.toLocaleString()} train / {trainResult.test_rows.toLocaleString()} test)</div>
                {trainResult.best_iteration != null && (
                  <div style={{ color: "#f59e0b" }}>
                    Stopped early at iteration {trainResult.best_iteration} / {(params.iterations as number) ?? 1000}
                  </div>
                )}
              </div>
              {/* Log to MLflow button */}
              {mlflowBackend?.installed && trainJobId && (
                <div className="space-y-1.5">
                  <button
                    onClick={handleLogExperiment}
                    disabled={loggingToMlflow}
                    className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-colors"
                    style={{
                      background: loggingToMlflow ? "var(--chrome-hover)" : "rgba(59,130,246,.15)",
                      color: loggingToMlflow ? "var(--text-muted)" : "#3b82f6",
                      border: "1px solid rgba(59,130,246,.3)",
                    }}
                  >
                    {loggingToMlflow ? <Loader2 size={14} className="animate-spin" /> : <FlaskConical size={14} />}
                    {loggingToMlflow ? "Logging..." : `Log to MLflow (${mlflowBackend.backend})`}
                  </button>
                  {mlflowResult && mlflowResult.status === "ok" && (
                    <div className="px-3 py-2 rounded-lg text-xs space-y-0.5" style={{ background: "rgba(59,130,246,.08)", border: "1px solid rgba(59,130,246,.2)" }}>
                      <div style={{ color: "#3b82f6" }}>Logged to {mlflowResult.experiment_name}</div>
                      {mlflowResult.run_url && (
                        <a href={mlflowResult.run_url} target="_blank" rel="noreferrer" className="underline" style={{ color: "#60a5fa" }}>
                          Open in Databricks
                        </a>
                      )}
                      {!mlflowResult.run_url && mlflowResult.tracking_uri && (
                        <div style={{ color: "var(--text-muted)" }}>Run ID: {mlflowResult.run_id}</div>
                      )}
                    </div>
                  )}
                  {mlflowResult && mlflowResult.status === "error" && (
                    <div className="px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(239,68,68,.08)", border: "1px solid rgba(239,68,68,.2)", color: "#fca5a5" }}>
                      {mlflowResult.error}
                    </div>
                  )}
                </div>
              )}
              <div>
                <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Metrics</label>
                <div className="mt-1 space-y-0.5">
                  {Object.entries(trainResult.metrics).map(([k, v]) => (
                    <div key={k} className="flex justify-between text-xs font-mono">
                      <span style={{ color: "var(--text-secondary)" }}>{k}</span>
                      <span style={{ color: "var(--text-primary)" }}>{v.toFixed(4)}</span>
                    </div>
                  ))}
                </div>
              </div>
              {trainResult.cv_results && (
                <div>
                  <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>
                    Cross-Validation ({trainResult.cv_results.n_folds}-fold)
                  </label>
                  <div className="mt-1 space-y-0.5">
                    {Object.entries(trainResult.cv_results.mean_metrics).map(([k, v]) => (
                      <div key={k} className="flex justify-between text-xs font-mono">
                        <span style={{ color: "var(--text-secondary)" }}>{k}</span>
                        <span style={{ color: "var(--text-primary)" }}>
                          {v.toFixed(4)}
                          {trainResult.cv_results?.std_metrics[k] != null && (
                            <span style={{ color: "var(--text-muted)" }}> +/- {trainResult.cv_results.std_metrics[k].toFixed(4)}</span>
                          )}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {trainResult.loss_history && trainResult.loss_history.length > 1 && (
                <LossChart lossHistory={trainResult.loss_history} bestIteration={trainResult.best_iteration} />
              )}
              {trainResult.double_lift && trainResult.double_lift.length > 0 && (
                <div>
                  <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Double Lift (Actual vs Predicted by Decile)</label>
                  <div className="mt-1 text-[11px] font-mono" style={{ color: "var(--text-secondary)" }}>
                    <div className="grid grid-cols-4 gap-1 pb-0.5 mb-0.5" style={{ borderBottom: "1px solid var(--border)" }}>
                      <span style={{ color: "var(--text-muted)" }}>Decile</span>
                      <span style={{ color: "var(--text-muted)" }}>Actual</span>
                      <span style={{ color: "var(--text-muted)" }}>Predicted</span>
                      <span style={{ color: "var(--text-muted)" }}>Count</span>
                    </div>
                    {trainResult.double_lift.map(row => (
                      <div key={row.decile} className="grid grid-cols-4 gap-1">
                        <span>{row.decile}</span>
                        <span style={{ color: "var(--text-primary)" }}>{row.actual.toFixed(4)}</span>
                        <span style={{ color: "#a855f7" }}>{row.predicted.toFixed(4)}</span>
                        <span>{row.count}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {trainResult.feature_importance.length > 0 && (() => {
                const types: { key: "prediction" | "loss" | "shap"; label: string }[] = [
                  { key: "prediction", label: "Prediction" },
                  ...(trainResult.feature_importance_loss?.length ? [{ key: "loss" as const, label: "Loss" }] : []),
                  ...(trainResult.shap_summary?.length ? [{ key: "shap" as const, label: "SHAP" }] : []),
                ]
                const items = importanceType === "shap"
                  ? (trainResult.shap_summary || []).slice(0, 10).map(s => ({ feature: s.feature, importance: s.mean_abs_shap }))
                  : importanceType === "loss"
                    ? (trainResult.feature_importance_loss || []).slice(0, 10)
                    : trainResult.feature_importance.slice(0, 10)
                const maxVal = items[0]?.importance || 1
                return (
                  <div>
                    <div className="flex items-center gap-2 mb-1">
                      <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Top Features</label>
                      {types.length > 1 && (
                        <div className="flex gap-1">
                          {types.map(t => (
                            <button
                              key={t.key}
                              onClick={() => setImportanceType(t.key)}
                              className="px-1.5 py-0.5 rounded text-[10px]"
                              style={{
                                background: importanceType === t.key ? "var(--accent-soft)" : "var(--chrome-hover)",
                                color: importanceType === t.key ? "var(--accent)" : "var(--text-muted)",
                              }}
                            >
                              {t.label}
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                    <div className="space-y-0.5">
                      {items.map((fi, i) => (
                        <div key={i} className="flex items-center gap-2 text-xs font-mono">
                          <span className="truncate flex-1" style={{ color: "var(--text-secondary)" }}>{fi.feature}</span>
                          <div className="w-20 h-1.5 rounded-full overflow-hidden" style={{ background: "var(--chrome-hover)" }}>
                            <div
                              className="h-full rounded-full"
                              style={{
                                width: `${(Math.abs(fi.importance) / Math.abs(maxVal)) * 100}%`,
                                background: "#a855f7",
                              }}
                            />
                          </div>
                          <span className="w-12 text-right" style={{ color: "var(--text-muted)" }}>{fi.importance.toFixed(1)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )
              })()}
            </>
          )}
        </div>
      )}

      {/* Exported Script */}
      {exportedScript && (
        <div>
          <div className="flex items-center justify-between mb-1">
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Generated Script</label>
            <button
              onClick={() => { navigator.clipboard.writeText(exportedScript) }}
              className="text-[11px] px-2 py-0.5 rounded"
              style={{ color: "var(--accent)", background: "var(--accent-soft)" }}
            >
              Copy
            </button>
          </div>
          <pre
            className="px-3 py-2 rounded-lg text-[11px] font-mono overflow-x-auto max-h-60 overflow-y-auto"
            style={{ background: "var(--input-bg)", color: "var(--text-primary)", border: "1px solid var(--border)" }}
          >
            {exportedScript}
          </pre>
        </div>
      )}
    </div>
  )
}
