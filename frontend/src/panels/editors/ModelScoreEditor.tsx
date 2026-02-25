import { useState } from "react"
import { ChevronDown } from "lucide-react"
import { InputSourcesBar, CodeEditor, MlflowStatusBadge, SELECT_STYLE } from "./_shared"
import type { InputSource, OnUpdateConfig } from "./_shared"
import { useMlflowBrowser } from "../../hooks/useMlflowBrowser"
import { configField } from "../../utils/configField"

export default function ModelScoreEditor({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
}) {
  const sourceType = configField(config, "sourceType", "registered")
  const task = configField(config, "task", "regression")
  const outputColumn = configField(config, "output_column", "prediction")
  const defaultCode = configField(config, "code", "")
  const selectedModel = configField(config, "registered_model", "")

  const {
    experiments, runs, models, modelVersions,
    loadingExperiments, loadingRuns, loadingModels,
    errorExperiments, errorRuns, errorModels, errorVersions,
    browseExpId, setBrowseExpId, setRuns, resetRunsGuard,
    refreshExperiments, refreshRuns, refreshModels, refreshVersions,
  } = useMlflowBrowser({ initialExpId: configField(config, "experiment_id", "") })

  const [showCode, setShowCode] = useState(!!defaultCode)

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-3">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      {/* MLflow Status */}
      <MlflowStatusBadge />

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

      {/* Run-based Selection */}
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
                  onUpdate({ run_id: runId, run_name: run?.run_name || "", artifact_path: run?.artifacts[0] || "" })
                }}
              >
                <option value="">{loadingRuns ? "Loading..." : "Select a run..."}</option>
                {configField(config, "run_id", "") && !runs.some((r) => r.run_id === config.run_id) && (
                  <option value={configField(config, "run_id", "")}>{configField(config, "run_name", "") || configField(config, "run_id", "")}</option>
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
              style={SELECT_STYLE}
              value={configField(config, "run_id", "")}
              onChange={(e) => onUpdate("run_id", e.target.value)}
              placeholder="e.g. a1b2c3d4e5f6..."
            />
          </div>
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Artifact Path</label>
            <input
              type="text"
              className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg font-mono focus:outline-none focus:ring-2"
              style={SELECT_STYLE}
              value={configField(config, "artifact_path", "")}
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
            style={SELECT_STYLE}
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
            style={SELECT_STYLE}
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
