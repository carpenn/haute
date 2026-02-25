import { useState, useCallback, useEffect, useMemo } from "react"
import { Save, Loader2, ChevronDown, ChevronRight, AlertTriangle, Plus, X, Target, FlaskConical, Layers, RefreshCw } from "lucide-react"
import type { SimpleNode, SimpleEdge } from "./editors"
import { solveOptimiser, saveOptimiser, logOptimiserToMlflow, previewNode } from "../api/client"
import type { SolveResult } from "./OptimiserPreview"
import { NODE_TYPES } from "../utils/nodeTypes"
import useNodeResultsStore, { hashConfig } from "../stores/useNodeResultsStore"
import useUIStore from "../stores/useUIStore"
import { formatElapsed } from "../utils/formatValue"

// ─── Banding factor extraction ───

type BandingFactor = {
  banding: string
  column: string
  outputColumn: string
  rules: Record<string, string>[]
  default?: string | null
}

type BandingNodeInfo = { id: string; label: string }
type InputNodeInfo = { id: string; label: string; nodeType: string }

/** List all nodes that are direct inputs to a given node. */
function findInputNodes(
  nodeId: string,
  allNodes: SimpleNode[],
  edges: SimpleEdge[],
): InputNodeInfo[] {
  const sourceIds = edges.filter(e => e.target === nodeId).map(e => e.source)
  const nodeMap = new Map(allNodes.map(n => [n.id, n]))
  return sourceIds
    .map(id => nodeMap.get(id))
    .filter((n): n is SimpleNode => !!n)
    .map(n => ({ id: n.id, label: n.data.label || n.id, nodeType: n.data.nodeType }))
}

/** List banding nodes among the inputs to a given node. */
function findInputBandingNodes(
  nodeId: string,
  allNodes: SimpleNode[],
  edges: SimpleEdge[],
): BandingNodeInfo[] {
  return findInputNodes(nodeId, allNodes, edges)
    .filter(n => n.nodeType === NODE_TYPES.BANDING)
    .map(({ id, label }) => ({ id, label }))
}

/** Extract factor column → level names from a single banding node. */
function extractBandingLevelsForNode(
  allNodes: SimpleNode[],
  nodeId: string,
): Record<string, string[]> {
  const node = allNodes.find(n => n.id === nodeId)
  if (!node || node.data.nodeType !== NODE_TYPES.BANDING) return {}
  const cfg = (node.data.config || {}) as Record<string, unknown>
  const factors = cfg.factors as BandingFactor[] | undefined
  if (!Array.isArray(factors)) return {}
  const levels: Record<string, string[]> = {}
  for (const f of factors) {
    if (!f.outputColumn) continue
    const vals = new Set<string>()
    for (const r of f.rules || []) {
      const a = r.assignment
      if (a) vals.add(a)
    }
    if (vals.size > 0) levels[f.outputColumn] = [...vals]
  }
  return levels
}

type OptimiserConfigProps = {
  config: Record<string, unknown>
  onUpdate: (keyOrUpdates: string | Record<string, unknown>, value?: unknown) => void
  upstreamColumns?: { name: string; dtype: string }[]
  allNodes: SimpleNode[]
  edges: SimpleEdge[]
  submodels?: Record<string, unknown>
}

const CONSTRAINT_TYPES = [
  { value: "min", label: "Min (relative)" },
  { value: "max", label: "Max (relative)" },
  { value: "min_abs", label: "Min (absolute)" },
  { value: "max_abs", label: "Max (absolute)" },
]

export default function OptimiserConfig({ config, onUpdate, allNodes, edges, submodels }: OptimiserConfigProps) {
  // ── Store-backed state (survives panel unmount) ──
  const nodeId = config._nodeId as string
  const solveJob = useNodeResultsStore((s) => s.solveJobs[nodeId])
  const cachedResult = useNodeResultsStore((s) => s.solveResults[nodeId])
  const startSolveJob = useNodeResultsStore((s) => s.startSolveJob)

  const solving = !!solveJob
  const solveProgress = solveJob?.progress ?? null
  const solveError = solveJob?.error ?? null
  const solveResult: SolveResult | null = cachedResult?.result ?? null
  const solveJobId: string | null = cachedResult?.jobId ?? solveJob?.jobId ?? null

  // Staleness detection: has config changed since last solve?
  const currentConfigHash = useMemo(() => hashConfig(config), [config])
  const isStale = !!cachedResult && cachedResult.configHash !== currentConfigHash

  // ── Local UI state (cheap, ok to recreate) ──
  const [saving, setSaving] = useState(false)
  const [saveMessage, setSaveMessage] = useState<string | null>(null)
  const [loggingToMlflow, setLoggingToMlflow] = useState(false)
  const [mlflowResult, setMlflowResult] = useState<{ status: string; backend?: string; experiment_name?: string; run_id?: string; run_url?: string | null; tracking_uri?: string; error?: string } | null>(null)

  // Global MLflow status from store (fetched once on app startup)
  const mlflow = useUIStore((s) => s.mlflow)
  const mlflowBackend = mlflow.status === "connected" ? { installed: true, backend: mlflow.backend, host: mlflow.host } : null

  // Collapse state from UI store (persisted)
  const advancedOpen = useUIStore((s) => s.isSectionOpen("optimiser.advanced"))
  const toggleAdvanced = useUIStore((s) => s.toggleSection)

  const mode = (config.mode as string) || "online"
  const factorColumns = (config.factor_columns as string[][]) || []
  const objective = (config.objective as string) || ""
  const constraints = (config.constraints as Record<string, Record<string, number>>) || {}
  const quoteId = (config.quote_id as string) || "quote_id"
  const scenarioIndex = (config.scenario_index as string) || "scenario_index"
  const scenarioValue = (config.scenario_value as string) || "scenario_value"
  const maxIter = (config.max_iter as number) ?? 50
  const tolerance = (config.tolerance as number) ?? 1e-6
  const chunkSize = (config.chunk_size as number) ?? 500_000
  const recordHistory = (config.record_history as boolean) ?? true
  const maxCdIterations = (config.max_cd_iterations as number) ?? 10
  const cdTolerance = (config.cd_tolerance as number) ?? 1e-4

  // Input nodes connected to this optimiser
  const inputNodes = useMemo(
    () => nodeId ? findInputNodes(nodeId, allNodes, edges) : [],
    [nodeId, allNodes, edges],
  )

  // Data input selection — which connected input provides objectives & constraints
  const dataInput = (config.data_input as string) || ""

  // Columns from the selected data input node — cached in store
  const setColumnsCache = useNodeResultsStore((s) => s.setColumns)
  const cachedColumns = useNodeResultsStore((s) => dataInput ? s.columnCache[dataInput] : undefined)
  const [dataInputColumns, setDataInputColumns] = useState<{ name: string; dtype: string }[]>(
    cachedColumns?.columns ?? []
  )

  useEffect(() => {
    if (!dataInput) {
      setDataInputColumns([])
      return
    }
    // Check store cache first
    const cached = useNodeResultsStore.getState().getColumns(dataInput)
    if (cached) {
      setDataInputColumns(cached.columns)
      if (cached.fresh) return // cache is current, skip API call
    }
    // Fetch fresh columns (cached value shown meanwhile — no loading flash)
    const graph = {
      nodes: allNodes.map(n => ({ id: n.id, type: n.type || n.data.nodeType, data: n.data, position: { x: 0, y: 0 } })),
      edges,
      submodels,
    }
    previewNode(graph, dataInput, 1)
      .then(result => {
        if (result.columns) {
          setDataInputColumns(result.columns)
          setColumnsCache(dataInput, result.columns, useNodeResultsStore.getState().graphVersion)
        }
      })
      .catch((e) => {
        console.warn("Column fetch failed", e)
        if (!cached) setDataInputColumns([])
      })
  }, [dataInput, allNodes, edges, submodels, setColumnsCache]) // re-fetch when input or graph changes

  const buildGraph = useCallback(() => ({
    nodes: allNodes.map((n) => ({ id: n.id, type: n.type || n.data.nodeType, data: n.data, position: { x: 0, y: 0 } })),
    edges,
    submodels,
  }), [allNodes, edges, submodels])

  // --- Constraints helpers ---

  const handleAddConstraint = useCallback(() => {
    const usedCols = new Set(Object.keys(constraints))
    const available = dataInputColumns.find(c => !usedCols.has(c.name) && c.name !== objective)
    const colName = available ? available.name : `constraint_${Object.keys(constraints).length + 1}`
    const newConstraints = { ...constraints, [colName]: { min: 0.9 } }
    onUpdate("constraints", newConstraints)
  }, [constraints, dataInputColumns, objective, onUpdate])

  const handleRemoveConstraint = useCallback((name: string) => {
    const newConstraints = { ...constraints }
    delete newConstraints[name]
    onUpdate("constraints", newConstraints)
  }, [constraints, onUpdate])

  const handleConstraintColumnChange = useCallback((oldName: string, newName: string) => {
    if (oldName === newName) return
    const newConstraints: Record<string, Record<string, number>> = {}
    for (const [k, v] of Object.entries(constraints)) {
      newConstraints[k === oldName ? newName : k] = v
    }
    onUpdate("constraints", newConstraints)
  }, [constraints, onUpdate])

  const handleConstraintValueChange = useCallback((name: string, type: string, value: number) => {
    onUpdate("constraints", { ...constraints, [name]: { [type]: value } })
  }, [constraints, onUpdate])

  // --- Factor toggle helpers (ratebook) ---

  const handleToggleFactor = useCallback((factorName: string) => {
    // Each banding factor maps to a factor group of [factorName]
    const isSelected = factorColumns.some(g => g.length === 1 && g[0] === factorName)
    if (isSelected) {
      onUpdate("factor_columns", factorColumns.filter(g => !(g.length === 1 && g[0] === factorName)))
    } else {
      onUpdate("factor_columns", [...factorColumns, [factorName]])
    }
  }, [factorColumns, onUpdate])

  // --- Actions (polling is handled by useBackgroundJobs hook in App.tsx) ---

  const handleSolve = useCallback(async () => {
    setSaveMessage(null)
    setMlflowResult(null)
    const nodeLabel = allNodes.find(n => n.id === nodeId)?.data.label || "Optimiser"
    try {
      const result = await solveOptimiser({ graph: buildGraph(), node_id: nodeId })
      if (result.status === "started" && result.job_id) {
        // Register job in store — background hook picks up polling
        startSolveJob(nodeId, result.job_id, nodeLabel, constraints, currentConfigHash)
      } else if (result.status === "error") {
        useNodeResultsStore.getState().failSolveJob(nodeId, result.error || "Unknown error")
      }
    } catch (e) {
      useNodeResultsStore.getState().failSolveJob(nodeId, String(e))
    }
  }, [nodeId, allNodes, buildGraph, constraints, currentConfigHash, startSolveJob])

  const handleSave = useCallback(async () => {
    if (!solveJobId) return
    setSaving(true)
    setSaveMessage(null)
    try {
      const result = await saveOptimiser({
        job_id: solveJobId,
        output_path: `output/optimiser_result.json`,
      })
      setSaveMessage(result.message || `Saved to ${result.path}`)
    } catch (e) {
      setSaveMessage(`Error: ${e}`)
    } finally {
      setSaving(false)
    }
  }, [solveJobId])

  const handleLogMlflow = useCallback(async () => {
    if (!solveJobId) return
    setLoggingToMlflow(true)
    setMlflowResult(null)
    try {
      const result = await logOptimiserToMlflow({ job_id: solveJobId })
      setMlflowResult(result)
    } catch (e) {
      setMlflowResult({ status: "error", error: String(e) })
    } finally {
      setLoggingToMlflow(false)
    }
  }, [solveJobId])

  // Banding node selection — only from connected inputs
  const bandingNodes = useMemo(
    () => nodeId ? findInputBandingNodes(nodeId, allNodes, edges) : [],
    [nodeId, allNodes, edges],
  )
  const bandingSource = (config.banding_source as string) || ""
  const effectiveBandingSource = bandingSource || (bandingNodes.length > 0 ? bandingNodes[0].id : "")

  // Auto-persist the effective banding source so the backend can read it
  useEffect(() => {
    if (mode === "ratebook" && !bandingSource && effectiveBandingSource) {
      onUpdate("banding_source", effectiveBandingSource)
    }
  }, [mode, bandingSource, effectiveBandingSource, onUpdate])

  const bandingLevels = useMemo(
    () => effectiveBandingSource ? extractBandingLevelsForNode(allNodes, effectiveBandingSource) : {},
    [allNodes, effectiveBandingSource],
  )
  const bandingFactorNames = useMemo(() => Object.keys(bandingLevels).sort(), [bandingLevels])

  // When banding source changes, auto-select all its factors
  const handleBandingSourceChange = useCallback((bandingNodeId: string) => {
    onUpdate("banding_source", bandingNodeId)
    const levels = extractBandingLevelsForNode(allNodes, bandingNodeId)
    const allFactors = Object.keys(levels).map(name => [name])
    onUpdate("factor_columns", allFactors)
  }, [allNodes, onUpdate])

  const canSolve = !!objective && Object.keys(constraints).length > 0 &&
    (mode !== "ratebook" || factorColumns.length > 0)

  return (
    <div className="px-4 py-3 space-y-4">
      {/* Mode Toggle */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Mode</label>
        <div className="mt-1.5 flex gap-1">
          {(["online", "ratebook"] as const).map(m => (
            <button
              key={m}
              onClick={() => onUpdate("mode", m)}
              className="flex-1 px-3 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: mode === m ? "rgba(249,115,22,.15)" : "var(--chrome-hover)",
                color: mode === m ? "#f97316" : "var(--text-muted)",
                border: `1px solid ${mode === m ? "rgba(249,115,22,.3)" : "transparent"}`,
              }}
            >
              {m === "online" ? "Online" : "Ratebook"}
            </button>
          ))}
        </div>
      </div>

      {/* Objectives & Constraints Input */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Objectives & Constraints</label>
        <div className="mt-1.5">
          {inputNodes.length > 0 ? (
            <select
              value={dataInput}
              onChange={(e) => onUpdate("data_input", e.target.value)}
              className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs"
              style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
            >
              <option value="">Select input...</option>
              {inputNodes.map(n => (
                <option key={n.id} value={n.id}>{n.label}</option>
              ))}
            </select>
          ) : (
            <div className="mt-0.5 text-[11px] py-2 text-center" style={{ color: "var(--text-muted)" }}>
              No inputs connected.
            </div>
          )}
        </div>
      </div>

      {/* Objective */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Objective</label>
        <div className="mt-1.5">
          <label className="text-xs" style={{ color: "var(--text-secondary)" }}>Column to maximise</label>
          <select
            value={objective}
            onChange={(e) => onUpdate("objective", e.target.value)}
            className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
            style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
          >
            <option value="">Select objective...</option>
            {dataInputColumns.map(c => <option key={c.name} value={c.name}>{c.name} ({c.dtype})</option>)}
          </select>
        </div>
      </div>

      {/* Ratebook: Banding Source + Rating Factors */}
      {mode === "ratebook" && (
        <div className="space-y-3">
          {/* Banding source selector */}
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>
              Rating Factor Source
            </label>
            {bandingNodes.length > 0 ? (
              <select
                value={effectiveBandingSource}
                onChange={(e) => handleBandingSourceChange(e.target.value)}
                className="w-full mt-1 px-2.5 py-1.5 rounded-lg text-xs"
                style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
              >
                {bandingNodes.map(bn => (
                  <option key={bn.id} value={bn.id}>{bn.label}</option>
                ))}
              </select>
            ) : (
              <div className="mt-1 text-[11px] py-2 text-center" style={{ color: "var(--text-muted)" }}>
                No Banding nodes found. Add a Banding node to define rating factors.
              </div>
            )}
          </div>

          {/* Factor toggles from selected banding node */}
          {bandingFactorNames.length > 0 && (
            <div>
              <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>
                <Layers size={10} className="inline mr-1" />
                Rating Factors ({factorColumns.length} selected)
              </label>
              <div className="mt-1.5 space-y-1">
                {bandingFactorNames.map(name => {
                  const levels = bandingLevels[name] || []
                  const selected = factorColumns.some(g => g.length === 1 && g[0] === name)
                  return (
                    <button
                      key={name}
                      onClick={() => handleToggleFactor(name)}
                      className="w-full flex items-center justify-between px-2.5 py-1.5 rounded-lg text-xs transition-colors"
                      style={{
                        background: selected ? "rgba(249,115,22,.1)" : "var(--bg-surface)",
                        border: `1px solid ${selected ? "rgba(249,115,22,.3)" : "var(--border)"}`,
                      }}
                    >
                      <span className="font-mono" style={{ color: selected ? "#f97316" : "var(--text-primary)" }}>{name}</span>
                      <span className="text-[10px]" style={{ color: selected ? "rgba(249,115,22,.7)" : "var(--text-muted)" }}>
                        {levels.length} levels
                      </span>
                    </button>
                  )
                })}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Column Mappings */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Column Mappings</label>
        <div className="mt-1.5 space-y-2">
          {[
            { key: "quote_id", label: "Quote ID", value: quoteId, default: "quote_id" },
            { key: "scenario_index", label: "Scenario Index", value: scenarioIndex, default: "scenario_index" },
            { key: "scenario_value", label: "Scenario Value", value: scenarioValue, default: "scenario_value" },
          ].map(field => (
            <div key={field.key}>
              <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>{field.label}</label>
              <select
                value={field.value}
                onChange={(e) => onUpdate(field.key, e.target.value)}
                className="w-full mt-0.5 px-2.5 py-1.5 rounded-lg text-xs font-mono"
                style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
              >
                <option value="">Select {field.label.toLowerCase()}...</option>
                {dataInputColumns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
              </select>
            </div>
          ))}
        </div>
      </div>

      {/* Constraints */}
      <div>
        <div className="flex items-center justify-between">
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>
            Constraints ({Object.keys(constraints).length})
          </label>
          <button
            onClick={handleAddConstraint}
            className="flex items-center gap-1 px-2 py-0.5 rounded text-[11px] transition-colors"
            style={{ color: "var(--accent)", background: "var(--accent-soft)" }}
          >
            <Plus size={10} /> Add
          </button>
        </div>
        <div className="mt-1.5 space-y-2">
          {Object.entries(constraints).map(([name, spec]) => {
            const constraintType = Object.keys(spec)[0] || "min"
            const constraintValue = spec[constraintType] ?? 0.9
            return (
              <div key={name} className="flex items-center gap-1.5 p-2 rounded-lg" style={{ background: "var(--bg-surface)", border: "1px solid var(--border)" }}>
                <select
                  value={name}
                  onChange={(e) => handleConstraintColumnChange(name, e.target.value)}
                  className="flex-1 min-w-0 px-1.5 py-1 rounded text-[11px] font-mono"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                >
                  <option value={name}>{name}</option>
                  {dataInputColumns.filter(c => c.name !== name && c.name !== objective && !constraints[c.name]).map(c => (
                    <option key={c.name} value={c.name}>{c.name}</option>
                  ))}
                </select>
                <select
                  value={constraintType}
                  onChange={(e) => handleConstraintValueChange(name, e.target.value, constraintValue)}
                  className="w-[90px] px-1 py-1 rounded text-[10px]"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-secondary)" }}
                >
                  {CONSTRAINT_TYPES.map(ct => <option key={ct.value} value={ct.value}>{ct.label}</option>)}
                </select>
                <input
                  type="number"
                  step={0.01}
                  value={constraintValue}
                  onChange={(e) => handleConstraintValueChange(name, constraintType, parseFloat(e.target.value) || 0)}
                  className="w-16 px-1.5 py-1 rounded text-[11px] font-mono text-right"
                  style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                />
                <button
                  onClick={() => handleRemoveConstraint(name)}
                  className="p-0.5 rounded transition-colors shrink-0"
                  style={{ color: "var(--text-muted)" }}
                >
                  <X size={12} />
                </button>
              </div>
            )
          })}
          {Object.keys(constraints).length === 0 && (
            <div className="text-[11px] py-2 text-center" style={{ color: "var(--text-muted)" }}>
              No constraints added. Click "Add" to set volume or loss ratio bounds.
            </div>
          )}
        </div>
      </div>

      {/* Solver Tuning */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Solver</label>
        <div className="mt-1.5 grid grid-cols-2 gap-2">
          <div>
            <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Max iterations</label>
            <input
              type="number" min={1} step={1}
              value={maxIter}
              onChange={(e) => onUpdate("max_iter", parseInt(e.target.value) || 50)}
              className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
              style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
            />
          </div>
          <div>
            <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Tolerance</label>
            <input
              type="number" step={0.000001}
              value={tolerance}
              onChange={(e) => onUpdate("tolerance", parseFloat(e.target.value) || 1e-6)}
              className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
              style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
            />
          </div>
        </div>
      </div>

      {/* Advanced (collapsible) */}
      <div>
        <button
          onClick={() => toggleAdvanced("optimiser.advanced")}
          className="flex items-center gap-1 text-[11px] font-bold uppercase tracking-[0.08em]"
          style={{ color: "var(--text-muted)" }}
        >
          {advancedOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          Advanced
        </button>
        {advancedOpen && (
          <div className="mt-1.5 space-y-2">
            <div>
              <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Chunk size</label>
              <input
                type="number" min={1000} step={10000}
                value={chunkSize}
                onChange={(e) => onUpdate("chunk_size", parseInt(e.target.value) || 500_000)}
                className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
                style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
              />
            </div>
            <div className="flex items-center gap-2">
              <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>Record history</label>
              <button
                onClick={() => onUpdate("record_history", !recordHistory)}
                className="px-2 py-0.5 rounded text-[11px] font-mono"
                style={{
                  background: recordHistory ? "rgba(249,115,22,.15)" : "var(--chrome-hover)",
                  color: recordHistory ? "#f97316" : "var(--text-muted)",
                  border: `1px solid ${recordHistory ? "rgba(249,115,22,.3)" : "transparent"}`,
                }}
              >
                {recordHistory ? "On" : "Off"}
              </button>
            </div>
            {mode === "ratebook" && (
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>CD iterations</label>
                  <input
                    type="number" min={1} step={1}
                    value={maxCdIterations}
                    onChange={(e) => onUpdate("max_cd_iterations", parseInt(e.target.value) || 10)}
                    className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
                    style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                  />
                </div>
                <div>
                  <label className="text-[11px]" style={{ color: "var(--text-muted)" }}>CD tolerance</label>
                  <input
                    type="number" step={0.0001}
                    value={cdTolerance}
                    onChange={(e) => onUpdate("cd_tolerance", parseFloat(e.target.value) || 1e-4)}
                    className="w-full mt-0.5 px-2 py-1 rounded text-xs font-mono"
                    style={{ background: "var(--input-bg)", border: "1px solid var(--border)", color: "var(--text-primary)" }}
                  />
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Staleness indicator */}
      {isStale && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(245,158,11,.08)", border: "1px solid rgba(245,158,11,.2)" }}>
          <RefreshCw size={12} style={{ color: "#f59e0b" }} className="shrink-0" />
          <span style={{ color: "#fbbf24" }}>Config changed since last solve</span>
          <button
            onClick={handleSolve}
            disabled={solving || !canSolve}
            className="ml-auto px-2 py-0.5 rounded text-[11px] font-medium"
            style={{ background: "rgba(249,115,22,.15)", color: "#f97316" }}
          >
            Re-run
          </button>
        </div>
      )}

      {/* Actions */}
      <div className="space-y-2 pt-2" style={{ borderTop: "1px solid var(--border)" }}>
        <button
          onClick={handleSolve}
          disabled={solving || !canSolve}
          className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-colors"
          style={{
            background: solving ? "var(--chrome-hover)" : "#f97316",
            color: solving ? "var(--text-muted)" : "#fff",
            opacity: !canSolve ? 0.5 : 1,
          }}
        >
          {solving ? <Loader2 size={14} className="animate-spin" /> : <Target size={14} />}
          {solving ? "Optimising..." : "Optimise"}
        </button>
      </div>

      {/* Live Progress */}
      {solveProgress && (
        <div className="px-3 py-2.5 rounded-lg text-xs space-y-2" style={{ background: "rgba(249,115,22,.06)", border: "1px solid rgba(249,115,22,.2)" }}>
          <div className="space-y-1">
            <div className="flex justify-between text-[11px]">
              <span style={{ color: "#f97316" }}>{solveProgress.message || "Solving..."}</span>
              <span style={{ color: "var(--text-muted)" }}>{formatElapsed(solveProgress.elapsed_seconds)}</span>
            </div>
            <div className="w-full h-1.5 rounded-full overflow-hidden" style={{ background: "rgba(249,115,22,.15)" }}>
              <div
                className="h-full rounded-full transition-all duration-300"
                style={{ width: `${Math.max(solveProgress.progress * 100, 2)}%`, background: "#f97316" }}
              />
            </div>
          </div>
        </div>
      )}

      {/* Error */}
      {solveError && (
        <div className="px-3 py-2.5 rounded-lg text-xs space-y-1.5" style={{ background: "rgba(239,68,68,.08)", border: "1px solid rgba(239,68,68,.2)" }}>
          <div className="flex items-start gap-2">
            <AlertTriangle size={14} className="shrink-0 mt-0.5" style={{ color: "#ef4444" }} />
            <div className="space-y-1 min-w-0">
              <div className="font-semibold" style={{ color: "#ef4444" }}>Optimisation failed</div>
              <div style={{ color: "#fca5a5", lineHeight: "1.5" }}>{solveError}</div>
            </div>
          </div>
        </div>
      )}

      {/* Results */}
      {solveResult && (
        <div className="space-y-2">
          {/* Non-convergence warning banner */}
          {!solveResult.converged && (
            <div className="flex items-start gap-2 px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(245,158,11,.1)", border: "1px solid rgba(245,158,11,.25)" }}>
              <AlertTriangle size={14} className="shrink-0 mt-0.5" style={{ color: "#f59e0b" }} />
              <div>
                <div className="font-semibold" style={{ color: "#f59e0b" }}>Solver did not converge</div>
                <div style={{ color: "#fbbf24", lineHeight: "1.5" }}>
                  {solveResult.warning || "Try increasing max iterations or relaxing the tolerance."}
                </div>
              </div>
            </div>
          )}

          {/* Convergence status */}
          <div className="px-3 py-2 rounded-lg text-xs space-y-1" style={{ background: solveResult.converged ? "rgba(34,197,94,.1)" : "rgba(245,158,11,.06)", border: `1px solid ${solveResult.converged ? "rgba(34,197,94,.2)" : "rgba(245,158,11,.15)"}` }}>
            <div style={{ color: solveResult.converged ? "#22c55e" : "#f59e0b" }}>
              {solveResult.converged ? "Converged" : "Did not converge"}
              {solveResult.mode === "ratebook"
                ? ` in ${solveResult.cd_iterations ?? "?"} CD iterations`
                : ` in ${solveResult.iterations ?? "?"} iterations`}
              {solveResult.n_quotes != null && solveResult.n_steps != null && (
                <> ({solveResult.n_quotes.toLocaleString()} quotes, {solveResult.n_steps} steps)</>
              )}
            </div>
          </div>


          {/* Save & MLflow buttons */}
          <div className="space-y-2 pt-1">
            <button
              onClick={handleSave}
              disabled={saving}
              className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: saving ? "var(--chrome-hover)" : "rgba(249,115,22,.15)",
                color: saving ? "var(--text-muted)" : "#f97316",
                border: "1px solid rgba(249,115,22,.3)",
              }}
            >
              {saving ? <Loader2 size={14} className="animate-spin" /> : <Save size={14} />}
              {saving ? "Saving..." : "Save Result"}
            </button>
            {saveMessage && (
              <div className="text-[11px] px-3 py-1.5 rounded-lg" style={{ background: "var(--bg-surface)", color: "var(--text-secondary)", border: "1px solid var(--border)" }}>
                {saveMessage}
              </div>
            )}

            {mlflowBackend?.installed && solveJobId && (
              <>
                <button
                  onClick={handleLogMlflow}
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
                  <div className="px-3 py-2 rounded-lg text-xs space-y-1" style={{ background: "rgba(59,130,246,.08)", border: "1px solid rgba(59,130,246,.2)", color: "#3b82f6" }}>
                    <div>Logged successfully</div>
                    {mlflowResult.experiment_name && (
                      <div className="font-mono text-[11px]" style={{ color: "#93c5fd" }}>{mlflowResult.experiment_name}</div>
                    )}
                    {mlflowResult.run_url ? (
                      <a href={mlflowResult.run_url} target="_blank" rel="noreferrer" className="underline" style={{ color: "#60a5fa" }}>
                        Open in Databricks
                      </a>
                    ) : mlflowResult.run_id ? (
                      <div className="font-mono text-[11px]" style={{ color: "#93c5fd" }}>Run: {mlflowResult.run_id.slice(0, 8)}</div>
                    ) : null}
                  </div>
                )}
                {mlflowResult && mlflowResult.status === "error" && (
                  <div className="px-3 py-2 rounded-lg text-xs" style={{ background: "rgba(239,68,68,.08)", border: "1px solid rgba(239,68,68,.2)", color: "#fca5a5" }}>
                    {mlflowResult.error}
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
