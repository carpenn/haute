import { useState, useCallback, useMemo, useEffect, useRef } from "react"
import type { SimpleNode, SimpleEdge, OnUpdateConfig } from "./editors"
import { trainModel, estimateTrainingRam } from "../api/client"
import type { TrainEstimate } from "../api/client"
import useNodeResultsStore, { hashConfig } from "../stores/useNodeResultsStore"
import useSettingsStore from "../stores/useSettingsStore"
import useToastStore from "../stores/useToastStore"
import { configField } from "../utils/configField"
import { buildGraph } from "../utils/buildGraph"
import { TargetAndTaskConfig } from "./modelling/TargetAndTaskConfig"
import { FeatureAndAlgorithmConfig } from "./modelling/FeatureAndAlgorithmConfig"
import { SplitAndMetricsConfig } from "./modelling/SplitAndMetricsConfig"
import { TrainingActionsAndResults } from "./modelling/TrainingActionsAndResults"
import { GLMTargetConfig } from "./modelling/GLMTargetConfig"
import { GLMFactorConfig } from "./modelling/GLMFactorConfig"
import { GLMRegularizationConfig } from "./modelling/GLMRegularizationConfig"

type ModellingConfigProps = {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  upstreamColumns?: { name: string; dtype: string }[]
  allNodes: SimpleNode[]
  edges: SimpleEdge[]
  submodels?: Record<string, unknown>
  preamble?: string
}

import type { TrainResult, TrainProgress } from "../stores/useNodeResultsStore"

export default function ModellingConfig({ config, onUpdate, upstreamColumns, allNodes, edges, submodels, preamble }: ModellingConfigProps) {
  // ── Store-backed state (survives panel unmount) ──
  const nodeId = config._nodeId as string
  const trainJob = useNodeResultsStore((s) => s.trainJobs[nodeId])
  const cachedResult = useNodeResultsStore((s) => s.trainResults[nodeId])
  const startTrainJob = useNodeResultsStore((s) => s.startTrainJob)

  const [submitting, setSubmitting] = useState(false)
  const training = !!trainJob
  const trainProgress: TrainProgress | null = trainJob?.progress ?? null
  const trainResult: TrainResult | null = cachedResult?.result ?? null

  // Staleness detection
  const currentConfigHash = useMemo(() => hashConfig(config), [config])
  const isStale = !!cachedResult && cachedResult.configHash !== currentConfigHash

  // ── RAM + VRAM estimate (re-fetched when GPU is toggled) ──
  const [ramEstimate, setRamEstimate] = useState<TrainEstimate | null>(null)
  const [ramEstimateLoading, setRamEstimateLoading] = useState(false)
  const [ramEstimateError, setRamEstimateError] = useState<string | null>(null)
  const estimateAbortRef = useRef<AbortController | null>(null)
  const addToast = useToastStore((s) => s.addToast)
  // Ref to capture latest graph inputs without re-triggering the effect
  const graphInputsRef = useRef({ allNodes, edges, submodels, preamble })
  graphInputsRef.current = { allNodes, edges, submodels, preamble }

  // Track config that affects the estimate so it re-fetches
  const isGpu = String((config.params as Record<string, unknown>)?.task_type ?? "").toUpperCase() === "GPU"
  const excludeCount = (configField<string[]>(config, "exclude", [])).length

  useEffect(() => {
    if (!nodeId) return
    // Cancel any in-flight estimate
    estimateAbortRef.current?.abort()
    const controller = new AbortController()
    estimateAbortRef.current = controller

    setRamEstimateLoading(true)
    setRamEstimate(null)
    setRamEstimateError(null)

    const { allNodes: n, edges: e, submodels: s, preamble: p } = graphInputsRef.current
    estimateTrainingRam(
      { graph: buildGraph(n, e, s, p), node_id: nodeId, source: useSettingsStore.getState().activeSource },
      { signal: controller.signal },
    )
      .then((est) => {
        if (!controller.signal.aborted) setRamEstimate(est)
      })
      .catch((err) => {
        if (!controller.signal.aborted) {
          const msg = err instanceof Error ? err.message : String(err)
          setRamEstimateError(msg)
          addToast("warning", `RAM estimate failed: ${msg}`)
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setRamEstimateLoading(false)
      })

    return () => controller.abort()
  }, [nodeId, isGpu, excludeCount, addToast])

  // Collapse state from UI store (persisted)
  const featuresOpen = useSettingsStore((s) => s.isSectionOpen("modelling.features"))
  const mlflowOpen = useSettingsStore((s) => s.isSectionOpen("modelling.mlflow"))
  const monotonicOpen = useSettingsStore((s) => s.isSectionOpen("modelling.monotonic"))
  const toggleSection = useSettingsStore((s) => s.toggleSection)

  const target = configField(config, "target", "")
  const weight = configField(config, "weight", "")
  const exclude = configField<string[]>(config, "exclude", [])
  const algorithm = configField(config, "algorithm", "")
  const task = configField(config, "task", "regression")
  const params = configField<Record<string, unknown>>(config, "params", {})
  const split = configField<Record<string, unknown>>(config, "split", { strategy: "random", validation_size: 0.2, holdout_size: 0, seed: 42 })
  const metrics = configField<string[]>(config, "metrics", task === "regression" ? ["gini", "rmse"] : ["auc", "logloss"])

  const columns = upstreamColumns || []
  const featureCount = columns.filter(c => c.name !== target && c.name !== weight && !exclude.includes(c.name)).length

  const handleSplitUpdate = useCallback((key: string, value: unknown) => {
    onUpdate("split", { ...split, [key]: value })
  }, [split, onUpdate])

  const buildGraphCb = useCallback(
    () => buildGraph(allNodes, edges, submodels, preamble),
    [allNodes, edges, submodels, preamble],
  )

  const handleTrain = useCallback(async () => {
    const nodeLabel = allNodes.find(n => n.id === nodeId)?.data.label || "Model Training"
    setSubmitting(true)
    try {
      const result = await trainModel({ graph: buildGraphCb(), node_id: nodeId, source: useSettingsStore.getState().activeSource })

      if (result.status === "started" && result.job_id) {
        // Register job in store — background hook picks up polling
        startTrainJob(nodeId, result.job_id as string, nodeLabel, currentConfigHash)
      } else if (result.status === "error") {
        // Immediate validation error — store as completed with error result
        useNodeResultsStore.getState().completeTrainJob(nodeId, result as unknown as TrainResult)
      } else {
        // Synchronous completion
        useNodeResultsStore.getState().completeTrainJob(nodeId, result as unknown as TrainResult)
      }
    } catch (e) {
      useNodeResultsStore.getState().completeTrainJob(nodeId, {
        status: "error", metrics: {}, feature_importance: [],
        model_path: "", train_rows: 0, test_rows: 0, error: String(e),
      })
    } finally {
      setSubmitting(false)
    }
  }, [nodeId, allNodes, buildGraphCb, currentConfigHash, startTrainJob])

  // ── Gateway: pick algorithm before showing full config ──
  if (!algorithm) {
    return (
      <div className="px-4 py-3 space-y-3">
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>
          Select Algorithm
        </label>
        {[
          { id: "catboost", name: "CatBoost", desc: "Gradient boosting — handles categoricals natively, fast GPU training" },
          { id: "glm", name: "GLM", desc: "Generalised linear model — interpretable coefficients, regulatory-friendly" },
        ].map(algo => (
          <button
            key={algo.id}
            onClick={() => onUpdate("algorithm", algo.id)}
            className="w-full flex items-start gap-3 px-3 py-3 rounded-lg text-left transition-colors"
            style={{ background: "var(--chrome-hover)", border: "1px solid var(--border)" }}
            onMouseEnter={(e) => { e.currentTarget.style.borderColor = "var(--accent)"; e.currentTarget.style.background = "var(--accent-soft)" }}
            onMouseLeave={(e) => { e.currentTarget.style.borderColor = "var(--border)"; e.currentTarget.style.background = "var(--chrome-hover)" }}
          >
            <div className="min-w-0">
              <div className="text-xs font-semibold" style={{ color: "var(--text-primary)" }}>{algo.name}</div>
              <div className="text-[11px] mt-0.5" style={{ color: "var(--text-muted)" }}>{algo.desc}</div>
            </div>
          </button>
        ))}
      </div>
    )
  }

  // ── GLM config panels ──
  if (algorithm === "glm") {
    return (
      <div className="px-4 py-3 space-y-4">
        <GLMTargetConfig
          config={config}
          onUpdate={onUpdate}
          columns={columns}
        />

        <GLMFactorConfig
          config={config}
          onUpdate={onUpdate}
          columns={columns}
          target={target}
          weight={weight}
          exclude={exclude}
        />

        <GLMRegularizationConfig
          config={config}
          onUpdate={onUpdate}
        />

        <SplitAndMetricsConfig
          config={config}
          onUpdate={onUpdate}
          columns={columns}
          target={target}
          weight={weight}
          exclude={exclude}
          split={split}
          mlflowOpen={mlflowOpen}
          monotonicOpen={monotonicOpen}
          toggleSection={toggleSection}
          onSplitUpdate={handleSplitUpdate}
        />

        <TrainingActionsAndResults
          target={target}
          training={training}
          trainProgress={trainProgress}
          trainResult={trainResult}
          isStale={isStale}
          ramEstimate={ramEstimate}
          ramEstimateLoading={ramEstimateLoading}
          ramEstimateError={ramEstimateError}
          rowLimit={typeof config.row_limit === "number" ? config.row_limit : null}
          submitting={submitting}
          onTrain={handleTrain}
        />
      </div>
    )
  }

  // ── CatBoost config panels ──
  return (
    <div className="px-4 py-3 space-y-4">
      <TargetAndTaskConfig
        config={config}
        onUpdate={onUpdate}
        columns={columns}
        target={target}
        weight={weight}
        task={task}
        metrics={metrics}
      />

      <FeatureAndAlgorithmConfig
        onUpdate={onUpdate}
        columns={columns}
        target={target}
        weight={weight}
        exclude={exclude}
        params={params}
        featureCount={featureCount}
        featuresOpen={featuresOpen}
        toggleSection={toggleSection}
      />

      <SplitAndMetricsConfig
        config={config}
        onUpdate={onUpdate}
        columns={columns}
        target={target}
        weight={weight}
        exclude={exclude}
        split={split}
        mlflowOpen={mlflowOpen}
        monotonicOpen={monotonicOpen}
        toggleSection={toggleSection}
        onSplitUpdate={handleSplitUpdate}
      />

      <TrainingActionsAndResults
        target={target}
        training={training}
        trainProgress={trainProgress}
        trainResult={trainResult}
        isStale={isStale}
        ramEstimate={ramEstimate}
        ramEstimateLoading={ramEstimateLoading}
        ramEstimateError={ramEstimateError}
        rowLimit={typeof config.row_limit === "number" ? config.row_limit : null}
        submitting={submitting}
        onTrain={handleTrain}
      />
    </div>
  )
}
