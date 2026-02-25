import { useState, useRef } from "react"
import { Loader2, Check, AlertTriangle, ChevronDown } from "lucide-react"
import { InputSourcesBar, CodeEditor } from "./_shared"
import type { InputSource } from "./_shared"
import {
  getExperiments,
  getRuns,
  getModels,
  getModelVersions,
  ApiError,
} from "../../api/client"
import useUIStore from "../../stores/useUIStore"

// Derive MLflow status from the global store for instant rendering
function useMlflowFromStore() {
  const mlflow = useUIStore((s) => s.mlflow)
  return {
    mlflowStatus: mlflow.status === "pending" ? "loading" as const : mlflow.status,
    mlflowBackend: mlflow.backend,
  }
}

export default function ModelScoreEditor({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
}: {
  config: Record<string, unknown>
  onUpdate: (keyOrUpdates: string | Record<string, unknown>, value?: unknown) => void
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
}) {
  const sourceType = (config.sourceType as string) || "registered"
  const task = (config.task as string) || "regression"
  const outputColumn = (config.output_column as string) || "prediction"
  const defaultCode = (config.code as string) || ""
  const selectedModel = (config.registered_model as string) || ""

  // MLflow connection status — from global store (fetched once on app startup)
  const { mlflowStatus, mlflowBackend } = useMlflowFromStore()

  // Lazy-loaded dropdown data -- fetched on focus only, like Databricks selects
  const [experiments, setExperiments] = useState<{ experiment_id: string; name: string }[]>([])
  const [runs, setRuns] = useState<{ run_id: string; run_name: string; metrics: Record<string, number>; artifacts: string[] }[]>([])
  const [models, setModels] = useState<{ name: string; latest_versions: { version: string; status: string; run_id: string }[] }[]>([])
  const [modelVersions, setModelVersions] = useState<{ version: string; run_id: string; status: string; description: string }[]>([])
  const [loadingExperiments, setLoadingExperiments] = useState(false)
  const [loadingRuns, setLoadingRuns] = useState(false)
  const [loadingModels, setLoadingModels] = useState(false)
  const [, setLoadingVersions] = useState(false)
  const [errorExperiments, setErrorExperiments] = useState("")
  const [errorRuns, setErrorRuns] = useState("")
  const [errorModels, setErrorModels] = useState("")
  const [errorVersions, setErrorVersions] = useState("")

  const [browseExpId, setBrowseExpId] = useState((config.experiment_id as string) || "")

  // Fetch guards -- only fetch once per mount, not on every focus
  const fetchedExperiments = useRef(false)
  const fetchedModels = useRef(false)
  const fetchedRunsFor = useRef("")
  const fetchedVersionsFor = useRef("")

  const errorMsg = (e: Error) => e instanceof ApiError ? e.detail || e.message : e.message

  const refreshExperiments = () => {
    if (fetchedExperiments.current) return
    fetchedExperiments.current = true
    setLoadingExperiments(true)
    setErrorExperiments("")
    getExperiments()
      .then((data) => { setExperiments(Array.isArray(data) ? data : []); setLoadingExperiments(false) })
      .catch((e: Error) => { setExperiments([]); setLoadingExperiments(false); setErrorExperiments(errorMsg(e) || "Failed to load experiments"); fetchedExperiments.current = false })
  }

  const refreshRuns = (expId: string) => {
    if (!expId) return
    if (fetchedRunsFor.current === expId) return
    fetchedRunsFor.current = expId
    setLoadingRuns(true)
    setErrorRuns("")
    getRuns(expId)
      .then((data) => { setRuns(Array.isArray(data) ? data : []); setLoadingRuns(false) })
      .catch((e: Error) => { setRuns([]); setLoadingRuns(false); setErrorRuns(errorMsg(e) || "Failed to load runs"); fetchedRunsFor.current = "" })
  }

  const refreshModels = () => {
    if (fetchedModels.current) return
    fetchedModels.current = true
    setLoadingModels(true)
    setErrorModels("")
    getModels()
      .then((data) => { setModels(Array.isArray(data) ? data : []); setLoadingModels(false) })
      .catch((e: Error) => { setModels([]); setLoadingModels(false); setErrorModels(errorMsg(e) || "Failed to load models"); fetchedModels.current = false })
  }

  const refreshVersions = (modelName: string) => {
    if (!modelName) return
    if (fetchedVersionsFor.current === modelName) return
    fetchedVersionsFor.current = modelName
    setLoadingVersions(true)
    setErrorVersions("")
    getModelVersions(modelName)
      .then((data) => { setModelVersions(Array.isArray(data) ? data : []); setLoadingVersions(false) })
      .catch((e: Error) => { setModelVersions([]); setLoadingVersions(false); setErrorVersions(errorMsg(e) || "Failed to load versions"); fetchedVersionsFor.current = "" })
  }

  const selectStyle = {
    background: 'var(--bg-input)',
    border: '1px solid var(--border)',
    color: 'var(--text-primary)',
  }

  const [showCode, setShowCode] = useState(!!defaultCode)

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-3">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      {/* MLflow Status */}
      <div className="flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-[11px]" style={{
        background: mlflowStatus === "connected" ? "rgba(34,197,94,.06)" : mlflowStatus === "error" ? "rgba(239,68,68,.06)" : "var(--bg-surface)",
        border: `1px solid ${mlflowStatus === "connected" ? "rgba(34,197,94,.2)" : mlflowStatus === "error" ? "rgba(239,68,68,.2)" : "var(--border)"}`,
      }}>
        {mlflowStatus === "loading" ? (
          <><Loader2 size={11} className="animate-spin" style={{ color: "var(--text-muted)" }} /><span style={{ color: "var(--text-muted)" }}>Connecting to MLflow...</span></>
        ) : mlflowStatus === "connected" ? (
          <><Check size={11} style={{ color: "#22c55e" }} /><span style={{ color: "var(--text-secondary)" }}>MLflow ({mlflowBackend})</span></>
        ) : (
          <><AlertTriangle size={11} style={{ color: "#ef4444" }} /><span style={{ color: "#ef4444" }}>MLflow not available</span></>
        )}
      </div>

      {/* Source Type Toggle */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Model Source</label>
        <div className="mt-1 flex gap-1.5">
          {[
            { key: "registered", label: "Registered Model" },
            { key: "run", label: "Experiment Run" },
          ].map((opt) => (
            <button
              key={opt.key}
              onClick={() => onUpdate("sourceType", opt.key)}
              className="flex-1 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: sourceType === opt.key ? "rgba(139,92,246,.1)" : "var(--bg-input)",
                border: sourceType === opt.key ? "1px solid #8b5cf6" : "1px solid var(--border)",
                color: sourceType === opt.key ? "#8b5cf6" : "var(--text-secondary)",
              }}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* Registered Model Selection */}
      {sourceType === "registered" && (
        <div className="flex flex-col gap-2">
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Model Name</label>
            <select
              className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
              style={selectStyle}
              value={selectedModel}
              onFocus={refreshModels}
              onChange={(e) => onUpdate({ registered_model: e.target.value, version: "latest" })}
            >
              <option value="">{loadingModels ? "Loading..." : "Select a model..."}</option>
              {selectedModel && models.every((m) => m.name !== selectedModel) && (
                <option value={selectedModel}>{selectedModel}</option>
              )}
              {models.map((m) => (
                <option key={m.name} value={m.name}>{m.name}</option>
              ))}
            </select>
            {errorModels && <span className="text-[10px] mt-0.5" style={{ color: "#ef4444" }}>{errorModels}</span>}
          </div>
          {selectedModel && (
            <div>
              <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Version</label>
              <select
                className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
                style={selectStyle}
                value={(config.version as string) || "latest"}
                onFocus={() => refreshVersions(selectedModel)}
                onChange={(e) => onUpdate("version", e.target.value)}
              >
                <option value="latest">latest</option>
                {modelVersions.map((v) => (
                  <option key={v.version} value={v.version}>v{v.version} — {v.status}{v.description ? ` (${v.description})` : ""}</option>
                ))}
              </select>
              {errorVersions && <span className="text-[10px] mt-0.5" style={{ color: "#ef4444" }}>{errorVersions}</span>}
            </div>
          )}
        </div>
      )}

      {/* Run-based Selection */}
      {sourceType === "run" && (
        <div className="flex flex-col gap-2">
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Experiment</label>
            <select
              className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
              style={selectStyle}
              value={browseExpId}
              onFocus={refreshExperiments}
              onChange={(e) => {
                const eid = e.target.value
                const exp = experiments.find((x) => x.experiment_id === eid)
                setBrowseExpId(eid)
                onUpdate({ experiment_id: eid, experiment_name: exp?.name || eid })
                setRuns([])
                fetchedRunsFor.current = ""
                if (eid) refreshRuns(eid)
              }}
            >
              <option value="">{loadingExperiments ? "Loading..." : "Select an experiment..."}</option>
              {browseExpId && experiments.every((e) => e.experiment_id !== browseExpId) && (
                <option value={browseExpId}>{(config.experiment_name as string) || browseExpId}</option>
              )}
              {experiments.map((exp) => (
                <option key={exp.experiment_id} value={exp.experiment_id}>{exp.name}</option>
              ))}
            </select>
            {errorExperiments && <span className="text-[10px] mt-0.5" style={{ color: "#ef4444" }}>{errorExperiments}</span>}
          </div>
          {browseExpId && (
            <div>
              <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Run</label>
              <select
                className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
                style={selectStyle}
                value={(config.run_id as string) || ""}
                onFocus={() => refreshRuns(browseExpId)}
                onChange={(e) => {
                  const runId = e.target.value
                  const run = runs.find((r) => r.run_id === runId)
                  onUpdate({ run_id: runId, run_name: run?.run_name || "", artifact_path: run?.artifacts[0] || "" })
                }}
              >
                <option value="">{loadingRuns ? "Loading..." : "Select a run..."}</option>
                {(config.run_id as string) && runs.every((r) => r.run_id !== config.run_id) && (
                  <option value={config.run_id as string}>{(config.run_name as string) || (config.run_id as string).slice(0, 8) + "..."}</option>
                )}
                {runs.map((r) => (
                  <option key={r.run_id} value={r.run_id}>
                    {r.run_name || r.run_id.slice(0, 8)}
                    {Object.entries(r.metrics).slice(0, 2).map(([k, v]) => ` ${k}=${typeof v === "number" ? v.toFixed(4) : v}`).join("")}
                  </option>
                ))}
              </select>
              {errorRuns && <span className="text-[10px] mt-0.5" style={{ color: "#ef4444" }}>{errorRuns}</span>}
            </div>
          )}
          {/* Persisted values -- always visible when set, editable directly */}
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Run ID</label>
            <input
              type="text"
              className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg font-mono focus:outline-none focus:ring-2"
              style={selectStyle}
              value={(config.run_id as string) || ""}
              onChange={(e) => onUpdate("run_id", e.target.value)}
              placeholder="e.g. a1b2c3d4e5f6..."
            />
          </div>
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Artifact Path</label>
            <input
              type="text"
              className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg font-mono focus:outline-none focus:ring-2"
              style={selectStyle}
              value={(config.artifact_path as string) || ""}
              onChange={(e) => onUpdate("artifact_path", e.target.value)}
              placeholder="e.g. model.cbm"
            />
          </div>
        </div>
      )}

      {/* Task and Output Column */}
      <div className="flex gap-2">
        <div className="flex-1">
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Task</label>
          <select
            className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
            style={selectStyle}
            value={task}
            onChange={(e) => onUpdate("task", e.target.value)}
          >
            <option value="regression">Regression</option>
            <option value="classification">Classification</option>
          </select>
        </div>
        <div className="flex-1">
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Output Column</label>
          <input
            type="text"
            className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
            style={selectStyle}
            value={outputColumn}
            onChange={(e) => onUpdate("output_column", e.target.value)}
            placeholder="prediction"
          />
        </div>
      </div>

      {task === "classification" && (
        <p className="text-[10px]" style={{ color: "var(--text-muted)" }}>
          Classification models also generate a <code className="px-0.5 rounded" style={{ background: "var(--bg-hover)" }}>{outputColumn}_proba</code> column.
        </p>
      )}

      {/* Optional Post-processing Code */}
      <div>
        <button
          onClick={() => setShowCode(!showCode)}
          className="flex items-center gap-1.5 text-[11px] font-medium transition-colors"
          style={{ color: "var(--text-muted)" }}
        >
          <ChevronDown size={11} style={{ transform: showCode ? "rotate(0deg)" : "rotate(-90deg)", transition: "transform 0.15s" }} />
          Post-processing Code (optional)
        </button>
      </div>
      {showCode && (
        <CodeEditor
          defaultValue={defaultCode}
          onChange={(val) => onUpdate("code", val)}
          placeholder={`# df has the prediction column already\n# model is the loaded CatBoost model\ndf = df.with_columns(\n    risk_band=pl.when(pl.col("${outputColumn}") > 0.5).then(pl.lit("high")).otherwise(pl.lit("low"))\n)`}
        />
      )}
    </div>
  )
}
