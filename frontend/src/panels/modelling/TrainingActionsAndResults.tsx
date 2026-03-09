import { Play, Loader2, AlertTriangle, RefreshCw, CheckCircle2 } from "lucide-react"
import type { TrainResult, TrainProgress } from "../../stores/useNodeResultsStore"
import type { TrainEstimate } from "../../api/client"
import { TrainingProgress as TrainingProgressPanel } from "./TrainingProgress"

export type TrainingActionsAndResultsProps = {
  target: string
  training: boolean
  trainProgress: TrainProgress | null
  trainResult: TrainResult | null
  isStale: boolean
  ramEstimate: TrainEstimate | null
  ramEstimateLoading: boolean
  onTrain: () => void
}

export function TrainingActionsAndResults({
  target,
  training,
  trainProgress,
  trainResult,
  isStale,
  ramEstimate,
  ramEstimateLoading,
  onTrain,
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
      <div className="pt-2" style={{ borderTop: "1px solid var(--border)" }}>
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
      </div>

      {/* Live Training Progress */}
      {trainProgress && <TrainingProgressPanel trainProgress={trainProgress} />}

      {/* Completion badge — results are in the preview panel below */}
      {trainResult && trainResult.status !== "error" && !training && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(34,197,94,.08)", border: "1px solid rgba(34,197,94,.2)" }}>
          <CheckCircle2 size={12} style={{ color: "#22c55e" }} className="shrink-0" />
          <span style={{ color: "#22c55e" }}>
            Model trained — results in preview panel below
          </span>
        </div>
      )}

      {/* Error display — keep in config panel since there's no preview to show */}
      {trainResult && trainResult.status === "error" && (
        <div className="px-3 py-2.5 rounded-lg text-xs space-y-1.5" style={{ background: "rgba(239,68,68,.08)", border: "1px solid rgba(239,68,68,.2)" }}>
          <div className="flex items-start gap-2">
            <AlertTriangle size={14} className="shrink-0 mt-0.5" style={{ color: "#ef4444" }} />
            <div className="space-y-1 min-w-0">
              <div className="font-semibold" style={{ color: "#ef4444" }}>Training failed</div>
              <div style={{ color: "#fca5a5", lineHeight: "1.5" }}>{trainResult.error}</div>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
