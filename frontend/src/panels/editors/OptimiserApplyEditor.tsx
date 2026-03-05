import { useState, useEffect } from "react"
import { InputSourcesBar, MlflowStatusBadge, INPUT_STYLE, SELECT_STYLE } from "./_shared"
import type { InputSource, OnUpdateConfig } from "./_shared"
import { useMlflowBrowser } from "../../hooks/useMlflowBrowser"
import { configField } from "../../utils/configField"
import ToggleButtonGroup from "../../components/ToggleButtonGroup"

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
  accentColor,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  accentColor: string
}) {
  const sourceType = configField(config, "sourceType", "file")
  const artifactPath = configField(config, "artifact_path", "")
  const versionColumn = configField(config, "version_column", "__optimiser_version__")
  const selectedModel = configField(config, "registered_model", "")

  const [meta, setMeta] = useState<ArtifactMeta | null>(null)
  const [loadError, setLoadError] = useState("")

  const {
    experiments, runs, models, modelVersions,
    loadingExperiments, loadingRuns, loadingModels,
    errorExperiments, errorRuns, errorModels, errorVersions,
    browseExpId, setBrowseExpId, setRuns, resetRunsGuard,
    refreshExperiments, refreshRuns, refreshModels, refreshVersions,
  } = useMlflowBrowser({ runTag: "optimiser", initialExpId: configField(config, "experiment_id", "") })

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

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-3">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      {/* MLflow Status (shown when not in file mode) */}
      {sourceType !== "file" && <MlflowStatusBadge />}

      {/* Source Type Toggle */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Artifact Source</label>
        <div className="mt-1">
          <ToggleButtonGroup
            value={sourceType}
            onChange={(v) => onUpdate("sourceType", v)}
            options={[
              { key: "file", label: "File Path" },
              { key: "registered", label: "Registered" },
              { key: "run", label: "Experiment Run" },
            ]}
            accentColor={accentColor}
          />
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
              style={SELECT_STYLE}
              value={selectedModel}
              onFocus={refreshModels}
              onChange={(e) => onUpdate({ registered_model: e.target.value, version: "latest" })}
            >
              <option value="">{loadingModels ? "Loading..." : "Select a model..."}</option>
              {selectedModel && !models.some((m) => m.name === selectedModel) && (
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
                style={SELECT_STYLE}
                value={configField(config, "version", "latest")}
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
              style={SELECT_STYLE}
              value={browseExpId}
              onFocus={refreshExperiments}
              onChange={(e) => {
                const eid = e.target.value
                const exp = experiments.find((x) => x.experiment_id === eid)
                setBrowseExpId(eid)
                onUpdate({ experiment_id: eid, experiment_name: exp?.name || eid })
                setRuns([])
                resetRunsGuard()
                if (eid) refreshRuns(eid)
              }}
            >
              <option value="">{loadingExperiments ? "Loading..." : "Select an experiment..."}</option>
              {browseExpId && !experiments.some((e) => e.experiment_id === browseExpId) && (
                <option value={browseExpId}>{configField(config, "experiment_name", "") || browseExpId}</option>
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
                style={SELECT_STYLE}
                value={configField(config, "run_id", "")}
                onFocus={() => refreshRuns(browseExpId)}
                onChange={(e) => {
                  const runId = e.target.value
                  const run = runs.find((r) => r.run_id === runId)
                  onUpdate({ run_id: runId, run_name: run?.run_name || "" })
                }}
              >
                <option value="">{loadingRuns ? "Loading..." : "Select a run..."}</option>
                {configField(config, "run_id", "") && !runs.some((r) => r.run_id === config.run_id) && (
                  <option value={configField(config, "run_id", "")}>{configField(config, "run_name", "") || configField(config, "run_id", "")}</option>
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
              style={SELECT_STYLE}
              value={configField(config, "run_id", "")}
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
        <div className="rounded-lg px-3 py-2" style={{ background: 'var(--bg-elevated)', border: `1px solid ${accentColor}` }}>
          <div className="text-[11px]" style={{ color: accentColor }}>{loadError}</div>
        </div>
      )}

      {sourceType === "file" && meta && <ArtifactMetaPanel meta={meta} accentColor={accentColor} />}
    </div>
  )
}


function ArtifactMetaPanel({ meta, accentColor }: { meta: ArtifactMeta; accentColor: string }) {
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
              <span style={{ color: accentColor, fontWeight: 600 }}>{typeof v === 'number' ? v.toFixed(4) : String(v)}</span>
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
