import { useMemo } from "react"
import { Play, Loader2, AlertTriangle, RefreshCw, CheckCircle2, Database } from "lucide-react"
import type { TrainResult, TrainProgress } from "../../stores/useNodeResultsStore"
import type { TrainEstimate } from "../../api/client"
import { TrainingProgress as TrainingProgressPanel } from "./TrainingProgress"

// The backend's bytes_per_row already includes full phase-model overhead
// (split, pools, CatBoost internals, diagnostics, CV).  No extra multiplier.
const TRAINING_OVERHEAD = 1.0

function formatMb(mb: number): string {
  return mb < 1024 ? `${mb.toFixed(0)} MB` : `${(mb / 1024).toFixed(1)} GB`
}

export type TrainingActionsAndResultsProps = {
  target: string
  training: boolean
  trainProgress: TrainProgress | null
  trainResult: TrainResult | null
  isStale: boolean
  ramEstimate: TrainEstimate | null
  ramEstimateLoading: boolean
  ramEstimateError?: string | null
  rowLimit: number | null
  /** True while the initial POST /api/modelling/train is in flight (pipeline executing). */
  submitting?: boolean
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
  ramEstimateError = null,
  rowLimit,
  submitting = false,
  onTrain,
}: TrainingActionsAndResultsProps) {
  // Recalculate training MB and GPU VRAM reactively as row_limit changes
  const adjusted = useMemo(() => {
    if (!ramEstimate || ramEstimate.total_rows == null) return null
    const sourceRows = ramEstimate.total_rows
    const hasUserLimit = rowLimit != null && rowLimit > 0

    // Effective rows for RAM: user limit, then RAM-safe limit, capped at source
    let rows = sourceRows
    if (hasUserLimit) rows = Math.min(rows, rowLimit)
    if (ramEstimate.safe_row_limit != null) rows = Math.min(rows, ramEstimate.safe_row_limit)

    const trainingMb = rows * ramEstimate.bytes_per_row * TRAINING_OVERHEAD / (1024 * 1024)
    const isLimited = rows < sourceRows

    // Amber when RAM requires downsampling, unless the user's limit
    // is already at or below the safe limit (they've preempted it)
    const wasDownsampled = ramEstimate.was_downsampled
      && !(hasUserLimit && rowLimit <= (ramEstimate.safe_row_limit ?? sourceRows))

    // GPU VRAM: scale from the backend estimate using effective rows.
    // Unlike RAM, don't clamp to safe_row_limit — show what the user's
    // chosen row count would actually need on the GPU.
    let gpuVramMb = ramEstimate.gpu_vram_estimated_mb ?? null
    if (gpuVramMb != null) {
      const gpuRows = hasUserLimit ? Math.min(rowLimit, sourceRows) : sourceRows
      const originalRows = ramEstimate.safe_row_limit ?? sourceRows
      if (originalRows > 0 && gpuRows !== originalRows) {
        gpuVramMb = gpuVramMb * gpuRows / originalRows
      }
    }

    return { rows, trainingMb, wasDownsampled, isLimited, gpuVramMb }
  }, [ramEstimate, rowLimit])

  const busy = submitting || training
  const trainIcon = submitting
    ? <Database size={14} className="animate-pulse" />
    : training
      ? <Loader2 size={14} className="animate-spin" />
      : <Play size={14} />
  const trainLabel = submitting
    ? "Preparing training data..."
    : training
      ? (trainProgress?.message || "Training...")
      : "Train Model"

  return (
    <>
      {/* Staleness indicator */}
      {isStale && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(245,158,11,.08)", border: "1px solid rgba(245,158,11,.2)" }}>
          <RefreshCw size={12} style={{ color: "#f59e0b" }} className="shrink-0" />
          <span style={{ color: "#fbbf24" }}>Config changed since last training</span>
          <button
            onClick={onTrain}
            disabled={training || submitting || !target}
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
      {ramEstimateError && !ramEstimateLoading && !ramEstimate && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(245,158,11,.06)", border: "1px solid rgba(245,158,11,.2)" }}>
          <AlertTriangle size={12} className="shrink-0" style={{ color: "#f59e0b" }} />
          <span style={{ color: "#fbbf24" }}>RAM estimate unavailable — training will still work</span>
        </div>
      )}
      {ramEstimate && !ramEstimateLoading && adjusted && (
        <div className="px-3 py-2.5 rounded-lg text-xs space-y-1.5" style={{
          background: adjusted.wasDownsampled ? "rgba(245,158,11,.06)" : "rgba(34,197,94,.06)",
          border: `1px solid ${adjusted.wasDownsampled ? "rgba(245,158,11,.2)" : "rgba(34,197,94,.15)"}`,
        }}>
          <div className="flex items-center gap-2">
            {adjusted.wasDownsampled && <AlertTriangle size={12} className="shrink-0" style={{ color: "#f59e0b" }} />}
            <span className="font-medium" style={{ color: adjusted.wasDownsampled ? "#fbbf24" : "#22c55e" }}>
              {adjusted.wasDownsampled ? "Will downsample" : "Dataset fits in memory"}
            </span>
          </div>
          <div className="grid grid-cols-2 gap-x-4 gap-y-0.5 text-[11px] font-mono" style={{ color: "var(--text-secondary)" }}>
            <span>Source rows</span>
            <span style={{ color: "var(--text-primary)" }}>{ramEstimate.total_rows!.toLocaleString()}</span>
            {adjusted.isLimited && (
              <>
                <span>Training rows</span>
                <span style={{ color: adjusted.wasDownsampled ? "#f59e0b" : "var(--text-primary)" }}>{adjusted.rows.toLocaleString()}</span>
              </>
            )}
            <span>Est. training RAM</span>
            <span style={{ color: "var(--text-primary)" }}>{formatMb(adjusted.trainingMb)}</span>
            <span>Available RAM</span>
            <span style={{ color: "var(--text-primary)" }}>{formatMb(ramEstimate.available_mb)}</span>
            {adjusted.gpuVramMb != null && (
              <>
                <span>Est. GPU VRAM</span>
                <span style={{ color: ramEstimate.gpu_vram_available_mb != null && adjusted.gpuVramMb > ramEstimate.gpu_vram_available_mb ? "#f59e0b" : "var(--text-primary)" }}>
                  {formatMb(adjusted.gpuVramMb)}
                </span>
                {ramEstimate.gpu_vram_available_mb != null && (
                  <>
                    <span>GPU VRAM</span>
                    <span style={{ color: "var(--text-primary)" }}>
                      {formatMb(ramEstimate.gpu_vram_available_mb)}
                    </span>
                  </>
                )}
              </>
            )}
          </div>
          {adjusted.gpuVramMb != null && ramEstimate.gpu_vram_available_mb != null && adjusted.gpuVramMb > ramEstimate.gpu_vram_available_mb && (
            <div className="flex items-center gap-2 mt-1" style={{ color: "#f59e0b" }}>
              <AlertTriangle size={12} className="shrink-0" />
              <span>
                GPU training needs ~{formatMb(adjusted.gpuVramMb)} but GPU has {formatMb(ramEstimate.gpu_vram_available_mb)}. Training will fall back to CPU automatically.
              </span>
            </div>
          )}
        </div>
      )}

      {/* Actions */}
      <div className="pt-2" style={{ borderTop: "1px solid var(--border)" }}>
        <button
          onClick={onTrain}
          disabled={busy || !target}
          className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-colors"
          style={{
            background: busy ? "var(--chrome-hover)" : "#a855f7",
            color: busy ? "var(--text-muted)" : "#fff",
            opacity: !target ? 0.5 : 1,
          }}
        >
          {trainIcon}
          {trainLabel}
        </button>
      </div>

      {/* Live Training Progress */}
      {trainProgress && <TrainingProgressPanel trainProgress={trainProgress} />}

      {/* Completion badge — results are in the preview panel below */}
      {trainResult && trainResult.status !== "error" && !training && !submitting && (
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
