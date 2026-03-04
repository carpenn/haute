import { Play, Download, Loader2, AlertTriangle, RefreshCw } from "lucide-react"
import type { TrainResult, TrainProgress } from "../../stores/useNodeResultsStore"
import type { TrainEstimate } from "../../api/client"
import { LossChart } from "./LossChart"
import { FeatureImportance } from "./FeatureImportance"
import { MlflowExportSection } from "./MlflowExportSection"
import { TrainingProgress as TrainingProgressPanel } from "./TrainingProgress"

export type TrainingActionsAndResultsProps = {
  config: Record<string, unknown>
  target: string
  params: Record<string, unknown>
  training: boolean
  trainProgress: TrainProgress | null
  trainResult: TrainResult | null
  trainJobId: string | null
  isStale: boolean
  exporting: boolean
  exportedScript: string | null
  mlflowBackend: { installed: boolean; backend: string; host: string } | null
  ramEstimate: TrainEstimate | null
  ramEstimateLoading: boolean
  onTrain: () => void
  onExport: () => void
  onMlflowResult: (result: { status: string; error?: string } | null) => void
}

export function TrainingActionsAndResults({
  config,
  target,
  params,
  training,
  trainProgress,
  trainResult,
  trainJobId,
  isStale,
  exporting,
  exportedScript,
  mlflowBackend,
  ramEstimate,
  ramEstimateLoading,
  onTrain,
  onExport,
  onMlflowResult,
}: TrainingActionsAndResultsProps) {
  return (
    <>
      {/* Staleness indicator */}
      {isStale && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(245,158,11,.08)", border: "1px solid rgba(245,158,11,.2)" }}>
          <RefreshCw size={12} style={{ color: "#f59e0b" }} className="shrink-0" />
          <span style={{ color: "#fbbf24" }}>Config changed since last training</span>
          <button
            onClick={onTrain}
            disabled={training || !target}
            className="ml-auto px-2 py-0.5 rounded text-[11px] font-medium"
            style={{ background: "rgba(168,85,247,.15)", color: "#a855f7" }}
          >
            Re-train
          </button>
        </div>
      )}

      {/* RAM Estimate */}
      {ramEstimateLoading && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(168,85,247,.06)", border: "1px solid rgba(168,85,247,.15)" }}>
          <Loader2 size={12} className="animate-spin" style={{ color: "#a855f7" }} />
          <span style={{ color: "var(--text-muted)" }}>Estimating dataset size...</span>
        </div>
      )}
      {ramEstimate && !ramEstimateLoading && ramEstimate.total_rows != null && (
        <div className="px-3 py-2.5 rounded-lg text-xs space-y-1.5" style={{
          background: ramEstimate.was_downsampled ? "rgba(245,158,11,.06)" : "rgba(34,197,94,.06)",
          border: `1px solid ${ramEstimate.was_downsampled ? "rgba(245,158,11,.2)" : "rgba(34,197,94,.15)"}`,
        }}>
          <div className="flex items-center gap-2">
            {ramEstimate.was_downsampled && <AlertTriangle size={12} className="shrink-0" style={{ color: "#f59e0b" }} />}
            <span className="font-medium" style={{ color: ramEstimate.was_downsampled ? "#fbbf24" : "#22c55e" }}>
              {ramEstimate.was_downsampled ? "Will downsample" : "Dataset fits in memory"}
            </span>
          </div>
          <div className="grid grid-cols-2 gap-x-4 gap-y-0.5 text-[11px] font-mono" style={{ color: "var(--text-secondary)" }}>
            <span>Source rows</span>
            <span style={{ color: "var(--text-primary)" }}>{ramEstimate.total_rows!.toLocaleString()}</span>
            <span>Est. training RAM</span>
            <span style={{ color: "var(--text-primary)" }}>{ramEstimate.training_mb < 1024 ? `${ramEstimate.training_mb.toFixed(0)} MB` : `${(ramEstimate.training_mb / 1024).toFixed(1)} GB`}</span>
            <span>Available RAM</span>
            <span style={{ color: "var(--text-primary)" }}>{ramEstimate.available_mb < 1024 ? `${ramEstimate.available_mb.toFixed(0)} MB` : `${(ramEstimate.available_mb / 1024).toFixed(1)} GB`}</span>
            {ramEstimate.was_downsampled && ramEstimate.safe_row_limit != null && (
              <>
                <span>Training rows</span>
                <span style={{ color: "#f59e0b" }}>{ramEstimate.safe_row_limit.toLocaleString()}</span>
              </>
            )}
            {ramEstimate.gpu_vram_estimated_mb != null && (
              <>
                <span>Est. GPU VRAM</span>
                <span style={{ color: ramEstimate.gpu_warning ? "#f59e0b" : "var(--text-primary)" }}>
                  {ramEstimate.gpu_vram_estimated_mb < 1024 ? `${ramEstimate.gpu_vram_estimated_mb.toFixed(0)} MB` : `${(ramEstimate.gpu_vram_estimated_mb / 1024).toFixed(1)} GB`}
                </span>
                {ramEstimate.gpu_vram_available_mb != null && (
                  <>
                    <span>GPU VRAM</span>
                    <span style={{ color: "var(--text-primary)" }}>
                      {ramEstimate.gpu_vram_available_mb < 1024 ? `${ramEstimate.gpu_vram_available_mb.toFixed(0)} MB` : `${(ramEstimate.gpu_vram_available_mb / 1024).toFixed(1)} GB`}
                    </span>
                  </>
                )}
              </>
            )}
          </div>
          {ramEstimate.gpu_warning && (
            <div className="flex items-center gap-2 mt-1" style={{ color: "#f59e0b" }}>
              <AlertTriangle size={12} className="shrink-0" />
              <span>{ramEstimate.gpu_warning}</span>
            </div>
          )}
        </div>
      )}

      {/* Actions */}
      <div className="space-y-2 pt-2" style={{ borderTop: "1px solid var(--border)" }}>
        <button
          onClick={onTrain}
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
          onClick={onExport}
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
      {trainProgress && <TrainingProgressPanel trainProgress={trainProgress} />}

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
              {trainResult.warning && (
                <div className="flex items-start gap-2 px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(245,158,11,.06)", border: "1px solid rgba(245,158,11,.2)" }}>
                  <AlertTriangle size={12} className="shrink-0 mt-0.5" style={{ color: "#f59e0b" }} />
                  <span style={{ color: "#fbbf24" }}>{trainResult.warning}</span>
                </div>
              )}
              {/* Log to MLflow button */}
              {mlflowBackend?.installed && trainJobId && (
                <MlflowExportSection
                  trainJobId={trainJobId}
                  mlflowBackend={mlflowBackend}
                  config={config}
                  onMlflowResult={onMlflowResult}
                />
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
              {trainResult.feature_importance.length > 0 && (
                <FeatureImportance trainResult={trainResult} />
              )}
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
    </>
  )
}
