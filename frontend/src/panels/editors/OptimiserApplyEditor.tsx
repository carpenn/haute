import { useState, useEffect, useRef } from "react"
import { Loader2, Check, AlertTriangle } from "lucide-react"
import { InputSourcesBar } from "./_shared"
import type { InputSource } from "./_shared"
import {
  getExperiments,
  getRuns,
  getModels,
  getModelVersions,
  ApiError,
} from "../../api/client"
import { useMlflowStatus } from "../../stores/useUIStore"

const INPUT_STYLE = { background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }
const ACCENT = "#22c55e"

type ArtifactMeta = {
  version: string
  created_at: string
  mode: string
  objective: string
  lambdas: Record<string, number>
  constraints: Record<string, Record<string, number>>
  factor_tables?: Record<string, unknown[]>
}

export default function OptimiserApplyEditor({
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
  const sourceType = (config.sourceType as string) || "file"
  const artifactPath = (config.artifact_path as string) || ""
  const versionColumn = (config.version_column as string) || "__optimiser_version__"
  const selectedModel = (config.registered_model as string) || ""

  const [meta, setMeta] = useState<ArtifactMeta | null>(null)
  const [loadError, setLoadError] = useState("")

  // MLflow connection status — from global store (fetched once on app startup)
  const { mlflowStatus, mlflowBackend } = useMlflowStatus()

  // MLflow dropdown data (lazy-loaded on focus)
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

  // Fetch guards
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
    getRuns(expId, "optimiser")
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

  // Load artifact metadata when file path changes
  useEffect(() => {
    if (sourceType !== "file" || !artifactPath) {
      if (sourceType === "file") { setMeta(null); setLoadError("") }
      return
    }
    fetch("/api/pipeline/read-json", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path: artifactPath }),
    })
      .then(async (res) => {
        if (!res.ok) {
          setLoadError(`Could not read artifact: ${res.statusText}`)
          setMeta(null)
          return
        }
        const data = await res.json()
        setMeta(data as ArtifactMeta)
        setLoadError("")
      })
      .catch((e: unknown) => {
        console.warn("Artifact load failed:", e)
        setLoadError("Could not load artifact file")
        setMeta(null)
      })
  }, [artifactPath, sourceType])

  const selectStyle = {
    background: 'var(--bg-input)',
    border: '1px solid var(--border)',
    color: 'var(--text-primary)',
  }

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-3">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      {/* MLflow Status (shown when not in file mode) */}
      {sourceType !== "file" && (
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
      )}

      {/* Source Type Toggle */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Artifact Source</label>
        <div className="mt-1 flex gap-1.5">
          {[
            { key: "file", label: "File Path" },
            { key: "registered", label: "Registered" },
            { key: "run", label: "Experiment Run" },
          ].map((opt) => (
            <button
              key={opt.key}
              onClick={() => onUpdate("sourceType", opt.key)}
              className="flex-1 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: sourceType === opt.key ? `${ACCENT}18` : "var(--bg-input)",
                border: sourceType === opt.key ? `1px solid ${ACCENT}` : "1px solid var(--border)",
                color: sourceType === opt.key ? ACCENT : "var(--text-secondary)",
              }}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* File Path Mode */}
      {sourceType === "file" && (
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>
            Artifact Path
          </label>
          <input
            type="text"
            className="w-full px-2.5 py-1.5 rounded-lg text-[12px] font-mono focus:outline-none focus:ring-2"
            style={INPUT_STYLE}
            value={artifactPath}
            onChange={(e) => onUpdate("artifact_path", e.target.value)}
            placeholder="artifacts/optimiser_v1.json"
          />
          <p className="text-[10px] mt-1" style={{ color: 'var(--text-muted)' }}>
            Path to saved optimiser result (JSON from the Optimiser Save action)
          </p>
        </div>
      )}

      {/* Registered Model Mode */}
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

      {/* Experiment Run Mode */}
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
                  onUpdate({ run_id: runId, run_name: run?.run_name || "" })
                }}
              >
                <option value="">{loadingRuns ? "Loading..." : "Select a run..."}</option>
                {(config.run_id as string) && runs.every((r) => r.run_id !== config.run_id) && (
                  <option value={config.run_id as string}>{(config.run_name as string) || (config.run_id as string).slice(0, 8) + "..."}</option>
                )}
                {runs.map((r) => {
                  const mode = r.metrics.converged !== undefined ? (r.metrics.cd_iterations !== undefined ? "ratebook" : "online") : ""
                  return (
                    <option key={r.run_id} value={r.run_id}>
                      {r.run_name || r.run_id.slice(0, 8)}
                      {mode ? ` [${mode}]` : ""}
                      {r.metrics.total_objective !== undefined ? ` obj=${r.metrics.total_objective.toFixed(2)}` : ""}
                    </option>
                  )
                })}
              </select>
              {errorRuns && <span className="text-[10px] mt-0.5" style={{ color: "#ef4444" }}>{errorRuns}</span>}
            </div>
          )}
          {/* Persisted run ID — always visible when set */}
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
        </div>
      )}

      {/* Version Column */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>
          Version Column
        </label>
        <input
          type="text"
          className="w-full px-2.5 py-1.5 rounded-lg text-[12px] font-mono focus:outline-none focus:ring-2"
          style={INPUT_STYLE}
          value={versionColumn}
          onChange={(e) => onUpdate("version_column", e.target.value)}
          placeholder="__optimiser_version__"
        />
        <p className="text-[10px] mt-1" style={{ color: 'var(--text-muted)' }}>
          Column added to output for monitoring / version tracking
        </p>
      </div>

      {/* Artifact metadata display (file mode) */}
      {sourceType === "file" && loadError && (
        <div className="rounded-lg px-3 py-2" style={{ background: 'var(--bg-elevated)', border: '1px solid #f97316' }}>
          <div className="text-[11px]" style={{ color: '#f97316' }}>{loadError}</div>
        </div>
      )}

      {sourceType === "file" && meta && <ArtifactMetaPanel meta={meta} />}
    </div>
  )
}


function ArtifactMetaPanel({ meta }: { meta: ArtifactMeta }) {
  return (
    <div className="rounded-lg px-3 py-2.5 space-y-2" style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border)' }}>
      <div className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
        Loaded Artifact
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] font-mono">
        <span style={{ color: 'var(--text-muted)' }}>Mode</span>
        <span style={{ color: 'var(--text-primary)', fontWeight: 600 }}>{meta.mode}</span>

        <span style={{ color: 'var(--text-muted)' }}>Version</span>
        <span style={{ color: 'var(--text-primary)' }}>{meta.version || "\u2014"}</span>

        <span style={{ color: 'var(--text-muted)' }}>Created</span>
        <span style={{ color: 'var(--text-primary)' }}>
          {meta.created_at ? new Date(meta.created_at).toLocaleDateString() : "\u2014"}
        </span>

        <span style={{ color: 'var(--text-muted)' }}>Objective</span>
        <span style={{ color: 'var(--text-primary)' }}>{meta.objective || "\u2014"}</span>
      </div>

      {/* Lambdas (online mode) */}
      {meta.mode === "online" && meta.lambdas && Object.keys(meta.lambdas).length > 0 && (
        <div>
          <div className="text-[10px] font-bold uppercase tracking-[0.08em] mt-1 mb-0.5" style={{ color: 'var(--text-muted)' }}>
            Lambdas
          </div>
          {Object.entries(meta.lambdas).map(([k, v]) => (
            <div key={k} className="flex justify-between text-[11px] font-mono px-1">
              <span style={{ color: 'var(--text-secondary)' }}>{k}</span>
              <span style={{ color: '#22c55e', fontWeight: 600 }}>{typeof v === 'number' ? v.toFixed(4) : String(v)}</span>
            </div>
          ))}
        </div>
      )}

      {/* Factor tables (ratebook mode) */}
      {meta.mode === "ratebook" && meta.factor_tables && Object.keys(meta.factor_tables).length > 0 && (
        <div>
          <div className="text-[10px] font-bold uppercase tracking-[0.08em] mt-1 mb-0.5" style={{ color: 'var(--text-muted)' }}>
            Factor Tables
          </div>
          {Object.entries(meta.factor_tables).map(([name, entries]) => (
            <div key={name} className="flex justify-between text-[11px] font-mono px-1">
              <span style={{ color: 'var(--text-secondary)' }}>{name}</span>
              <span style={{ color: 'var(--text-muted)' }}>{Array.isArray(entries) ? entries.length : 0} levels</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
