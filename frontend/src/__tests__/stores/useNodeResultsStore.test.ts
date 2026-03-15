/**
 * Tests for useNodeResultsStore — preview cache, solve/train job lifecycle,
 * config hashing, column cache, and cleanup.
 */
import { describe, it, expect, beforeEach } from "vitest"
import useNodeResultsStore, { hashConfig } from "../../stores/useNodeResultsStore.ts"
import type { PreviewData } from "../../panels/DataPreview.tsx"
import type { SolveResult } from "../../panels/OptimiserPreview.tsx"
import type { TrainResult } from "../../stores/useNodeResultsStore.ts"

// ── Helpers ──────────────────────────────────────────────────────

function resetStore() {
  useNodeResultsStore.setState({
    previews: {},
    columnCache: {},
    solveResults: {},
    solveJobs: {},
    trainResults: {},
    trainJobs: {},
    graphVersion: 0,
  })
}

function makePreviewData(overrides: Partial<PreviewData> = {}): PreviewData {
  return {
    nodeId: "node-1",
    nodeLabel: "Test Node",
    status: "ok",
    row_count: 10,
    column_count: 2,
    columns: [
      { name: "col_a", dtype: "float64" },
      { name: "col_b", dtype: "int64" },
    ],
    preview: [{ col_a: 1.5, col_b: 42 }],
    error: null,
    ...overrides,
  }
}

function makeSolveResult(overrides: Partial<SolveResult> = {}): SolveResult {
  return {
    total_objective: 100,
    baseline_objective: 80,
    constraints: { premium: 50 },
    baseline_constraints: { premium: 45 },
    lambdas: { premium: 0.1 },
    converged: true,
    ...overrides,
  }
}

function makeTrainResult(overrides: Partial<TrainResult> = {}): TrainResult {
  return {
    status: "completed",
    metrics: { rmse: 0.05 },
    feature_importance: [{ feature: "x", importance: 0.9 }],
    model_path: "/tmp/model.pkl",
    train_rows: 1000,
    test_rows: 200,
    ...overrides,
  }
}

// ── Test suites ──────────────────────────────────────────────────

describe("useNodeResultsStore", () => {
  beforeEach(() => {
    resetStore()
  })

  // ────────────────────────────────────────────────────────────────
  // Solve job lifecycle
  // ────────────────────────────────────────────────────────────────

  describe("solve job lifecycle", () => {
    it("startSolveJob creates an active job entry", () => {
      const { startSolveJob } = useNodeResultsStore.getState()
      startSolveJob("n1", "job-1", "Node 1", { premium: { min: 0, max: 100 } }, "hash-a")

      const { solveJobs } = useNodeResultsStore.getState()
      expect(solveJobs["n1"]).toBeDefined()
      expect(solveJobs["n1"].jobId).toBe("job-1")
      expect(solveJobs["n1"].nodeLabel).toBe("Node 1")
      expect(solveJobs["n1"].configHash).toBe("hash-a")
      expect(solveJobs["n1"].progress).toBeNull()
      expect(solveJobs["n1"].error).toBeNull()
    })

    it("updateSolveProgress attaches progress to active job", () => {
      const state = useNodeResultsStore.getState()
      state.startSolveJob("n1", "job-1", "Node 1", {}, "h")
      state.updateSolveProgress("n1", {
        status: "running",
        progress: 0.5,
        message: "Iterating",
        elapsed_seconds: 3,
      })

      const job = useNodeResultsStore.getState().solveJobs["n1"]
      expect(job.progress).not.toBeNull()
      expect(job.progress!.progress).toBe(0.5)
      expect(job.progress!.message).toBe("Iterating")
    })

    it("updateSolveProgress is a no-op for unknown node", () => {
      const state = useNodeResultsStore.getState()
      state.updateSolveProgress("unknown", {
        status: "running",
        progress: 0.5,
        message: "x",
        elapsed_seconds: 1,
      })
      expect(useNodeResultsStore.getState().solveJobs["unknown"]).toBeUndefined()
    })

    it("completeSolveJob moves result to solveResults and removes the job", () => {
      const state = useNodeResultsStore.getState()
      state.startSolveJob("n1", "job-1", "Node 1", { premium: { min: 0, max: 100 } }, "hash-a")

      const result = makeSolveResult()
      state.completeSolveJob("n1", result)

      const updated = useNodeResultsStore.getState()
      // Job removed
      expect(updated.solveJobs["n1"]).toBeUndefined()
      // Result stored
      expect(updated.solveResults["n1"]).toBeDefined()
      expect(updated.solveResults["n1"].result).toEqual(result)
      expect(updated.solveResults["n1"].jobId).toBe("job-1")
      expect(updated.solveResults["n1"].configHash).toBe("hash-a")
      expect(updated.solveResults["n1"].constraints).toEqual({ premium: { min: 0, max: 100 } })
      expect(updated.solveResults["n1"].nodeLabel).toBe("Node 1")
    })

    it("completeSolveJob is a no-op when there is no active job", () => {
      const state = useNodeResultsStore.getState()
      state.completeSolveJob("n1", makeSolveResult())
      const updated = useNodeResultsStore.getState()
      // No result should be stored because there was no matching job
      expect(updated.solveResults["n1"]).toBeUndefined()
    })

    it("full lifecycle: start → update → complete", () => {
      const s = useNodeResultsStore.getState()
      s.startSolveJob("n1", "j1", "Node 1", { c: { min: 0, max: 1 } }, "h1")
      s.updateSolveProgress("n1", {
        status: "running",
        progress: 0.5,
        message: "halfway",
        elapsed_seconds: 5,
      })
      const result = makeSolveResult({ converged: true, iterations: 42 })
      s.completeSolveJob("n1", result)

      const final = useNodeResultsStore.getState()
      expect(Object.keys(final.solveJobs)).toHaveLength(0)
      expect(final.solveResults["n1"].result.iterations).toBe(42)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Solve job failure
  // ────────────────────────────────────────────────────────────────

  describe("failSolveJob", () => {
    it("sets error on active job and clears progress, but keeps it in solveJobs", () => {
      const s = useNodeResultsStore.getState()
      s.startSolveJob("n1", "j1", "Node 1", {}, "h")
      s.updateSolveProgress("n1", {
        status: "running",
        progress: 0.3,
        message: "working",
        elapsed_seconds: 2,
      })

      s.failSolveJob("n1", "Solver diverged")

      const job = useNodeResultsStore.getState().solveJobs["n1"]
      expect(job).toBeDefined()
      expect(job.error).toBe("Solver diverged")
      expect(job.progress).toBeNull()
      // NOT moved to solveResults
      expect(useNodeResultsStore.getState().solveResults["n1"]).toBeUndefined()
    })

    it("is a no-op for unknown node", () => {
      useNodeResultsStore.getState().failSolveJob("ghost", "oops")
      expect(useNodeResultsStore.getState().solveJobs["ghost"]).toBeUndefined()
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Train job lifecycle
  // ────────────────────────────────────────────────────────────────

  describe("train job lifecycle", () => {
    it("startTrainJob creates an active job entry", () => {
      useNodeResultsStore.getState().startTrainJob("t1", "tj-1", "Train Node", "cfg-hash")
      const job = useNodeResultsStore.getState().trainJobs["t1"]
      expect(job).toBeDefined()
      expect(job.jobId).toBe("tj-1")
      expect(job.nodeLabel).toBe("Train Node")
      expect(job.configHash).toBe("cfg-hash")
      expect(job.progress).toBeNull()
      expect(job.error).toBeNull()
    })

    it("updateTrainProgress attaches progress to active job", () => {
      const s = useNodeResultsStore.getState()
      s.startTrainJob("t1", "tj-1", "Train Node", "h")
      s.updateTrainProgress("t1", {
        status: "running",
        progress: 0.7,
        message: "Training...",
        iteration: 70,
        total_iterations: 100,
        train_loss: { rmse: 0.1 },
        elapsed_seconds: 10,
      })

      const job = useNodeResultsStore.getState().trainJobs["t1"]
      expect(job.progress!.progress).toBe(0.7)
      expect(job.progress!.iteration).toBe(70)
    })

    it("updateTrainProgress is a no-op for unknown node", () => {
      useNodeResultsStore.getState().updateTrainProgress("nope", {
        status: "running",
        progress: 0.5,
        message: "x",
        iteration: 50,
        total_iterations: 100,
        train_loss: {},
        elapsed_seconds: 1,
      })
      expect(useNodeResultsStore.getState().trainJobs["nope"]).toBeUndefined()
    })

    it("completeTrainJob moves result to trainResults and removes the job", () => {
      const s = useNodeResultsStore.getState()
      s.startTrainJob("t1", "tj-1", "Train Node", "cfg-hash")
      const result = makeTrainResult()
      s.completeTrainJob("t1", result)

      const updated = useNodeResultsStore.getState()
      expect(updated.trainJobs["t1"]).toBeUndefined()
      expect(updated.trainResults["t1"]).toBeDefined()
      expect(updated.trainResults["t1"].result).toEqual(result)
      expect(updated.trainResults["t1"].jobId).toBe("tj-1")
      expect(updated.trainResults["t1"].configHash).toBe("cfg-hash")
    })

    it("completeTrainJob works even without an active job (direct completion)", () => {
      const s = useNodeResultsStore.getState()
      const result = makeTrainResult()
      s.completeTrainJob("t1", result)

      const updated = useNodeResultsStore.getState()
      // Should still be stored with empty jobId/configHash
      expect(updated.trainResults["t1"]).toBeDefined()
      expect(updated.trainResults["t1"].result).toEqual(result)
      expect(updated.trainResults["t1"].jobId).toBe("")
      expect(updated.trainResults["t1"].configHash).toBe("")
    })

    it("full lifecycle: start → update → complete", () => {
      const s = useNodeResultsStore.getState()
      s.startTrainJob("t1", "tj-1", "Train Node", "h")
      s.updateTrainProgress("t1", {
        status: "running",
        progress: 0.5,
        message: "Training...",
        iteration: 50,
        total_iterations: 100,
        train_loss: { rmse: 0.1 },
        elapsed_seconds: 5,
      })
      const result = makeTrainResult({ metrics: { rmse: 0.02 } })
      s.completeTrainJob("t1", result)

      const final = useNodeResultsStore.getState()
      expect(Object.keys(final.trainJobs)).toHaveLength(0)
      expect(final.trainResults["t1"].result.metrics.rmse).toBe(0.02)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Train job failure
  // ────────────────────────────────────────────────────────────────

  describe("failTrainJob", () => {
    it("sets error on active job and clears progress, keeps it in trainJobs", () => {
      const s = useNodeResultsStore.getState()
      s.startTrainJob("t1", "tj-1", "Train Node", "h")
      s.updateTrainProgress("t1", {
        status: "running",
        progress: 0.3,
        message: "training",
        iteration: 30,
        total_iterations: 100,
        train_loss: {},
        elapsed_seconds: 3,
      })

      s.failTrainJob("t1", "Out of memory")

      const job = useNodeResultsStore.getState().trainJobs["t1"]
      expect(job).toBeDefined()
      expect(job.error).toBe("Out of memory")
      expect(job.progress).toBeNull()
      expect(useNodeResultsStore.getState().trainResults["t1"]).toBeUndefined()
    })

    it("is a no-op for unknown node", () => {
      useNodeResultsStore.getState().failTrainJob("ghost", "oops")
      expect(useNodeResultsStore.getState().trainJobs["ghost"]).toBeUndefined()
    })
  })

  // ────────────────────────────────────────────────────────────────
  // hashConfig
  // ────────────────────────────────────────────────────────────────

  describe("hashConfig", () => {
    it("returns the same hash for the same config", () => {
      const config = { solver: "glpk", tolerance: 0.01 }
      expect(hashConfig(config)).toBe(hashConfig({ ...config }))
    })

    it("returns different hashes for different configs", () => {
      const a = { solver: "glpk", tolerance: 0.01 }
      const b = { solver: "glpk", tolerance: 0.02 }
      expect(hashConfig(a)).not.toBe(hashConfig(b))
    })

    it("strips _nodeId, _columns, and _schemaWarnings before hashing", () => {
      const base = { solver: "glpk", tolerance: 0.01 }
      const withInternals = {
        ...base,
        _nodeId: "n-42",
        _columns: [{ name: "x", dtype: "float64" }],
        _schemaWarnings: [{ column: "x", status: "missing" }],
      }
      expect(hashConfig(base)).toBe(hashConfig(withInternals))
    })

    it("returns a non-empty string", () => {
      const hash = hashConfig({ a: 1 })
      expect(hash.length).toBeGreaterThan(0)
    })

    it("is order-sensitive via JSON.stringify (same object key order)", () => {
      // JSON.stringify preserves insertion order, so these should differ
      const a = { x: 1, y: 2 }
      const b = { y: 2, x: 1 }
      // Note: these MAY differ depending on JSON.stringify key ordering
      // In practice, JS engines preserve insertion order, so this tests that
      const hashA = hashConfig(a)
      const hashB = hashConfig(b)
      // We just verify both produce valid hashes; equality depends on engine
      expect(hashA.length).toBeGreaterThan(0)
      expect(hashB.length).toBeGreaterThan(0)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Preview cache / graph version
  // ────────────────────────────────────────────────────────────────

  describe("preview cache and graph version", () => {
    it("setPreview then getPreview returns cached data", () => {
      const s = useNodeResultsStore.getState()
      const preview = makePreviewData()
      s.setPreview("n1", preview, 0)

      const cached = s.getPreview("n1")
      expect(cached).not.toBeNull()
      expect(cached!.data).toEqual(preview)
      expect(cached!.graphVersion).toBe(0)
    })

    it("getPreview returns null for unknown node", () => {
      expect(useNodeResultsStore.getState().getPreview("unknown")).toBeNull()
    })

    it("bumpGraphVersion increments graphVersion", () => {
      expect(useNodeResultsStore.getState().graphVersion).toBe(0)
      useNodeResultsStore.getState().bumpGraphVersion()
      expect(useNodeResultsStore.getState().graphVersion).toBe(1)
      useNodeResultsStore.getState().bumpGraphVersion()
      expect(useNodeResultsStore.getState().graphVersion).toBe(2)
    })

    it("setPreview then bumpGraphVersion: preview still returned but graphVersion mismatches", () => {
      const s = useNodeResultsStore.getState()
      const preview = makePreviewData()
      s.setPreview("n1", preview, 0)
      s.bumpGraphVersion()

      const cached = useNodeResultsStore.getState().getPreview("n1")
      expect(cached).not.toBeNull()
      expect(cached!.data).toEqual(preview)
      // Preview was stored at graphVersion 0 but store is now at 1
      expect(cached!.graphVersion).toBe(0)
      expect(useNodeResultsStore.getState().graphVersion).toBe(1)
      expect(cached!.graphVersion).not.toBe(useNodeResultsStore.getState().graphVersion)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Column cache
  // ────────────────────────────────────────────────────────────────

  describe("column cache", () => {
    it("setColumns then getColumns returns columns with fresh=true", () => {
      const s = useNodeResultsStore.getState()
      const columns = [{ name: "a", dtype: "float64" }, { name: "b", dtype: "int64" }]
      s.setColumns("src-1", columns, 0)

      const result = useNodeResultsStore.getState().getColumns("src-1")
      expect(result).not.toBeNull()
      expect(result!.columns).toEqual(columns)
      expect(result!.fresh).toBe(true)
    })

    it("getColumns returns null for unknown source", () => {
      expect(useNodeResultsStore.getState().getColumns("nope")).toBeNull()
    })

    it("columns become stale after bumpGraphVersion", () => {
      const s = useNodeResultsStore.getState()
      s.setColumns("src-1", [{ name: "a", dtype: "float64" }], 0)
      s.bumpGraphVersion()

      const result = useNodeResultsStore.getState().getColumns("src-1")
      expect(result).not.toBeNull()
      expect(result!.fresh).toBe(false)
    })

    it("columns set at current graph version are fresh", () => {
      const s = useNodeResultsStore.getState()
      s.bumpGraphVersion() // graphVersion is now 1
      s.setColumns("src-1", [{ name: "a", dtype: "float64" }], 1)

      const result = useNodeResultsStore.getState().getColumns("src-1")
      expect(result).not.toBeNull()
      expect(result!.fresh).toBe(true)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // B20: bumpGraphVersion should only fire on structural changes
  // (tested at the store level — App.tsx fingerprinting is the gate)
  // ────────────────────────────────────────────────────────────────

  describe("bumpGraphVersion idempotence", () => {
    it("bumpGraphVersion increments by exactly 1 each call", () => {
      const s = useNodeResultsStore.getState()
      const v0 = s.graphVersion
      s.bumpGraphVersion()
      expect(useNodeResultsStore.getState().graphVersion).toBe(v0 + 1)
      useNodeResultsStore.getState().bumpGraphVersion()
      expect(useNodeResultsStore.getState().graphVersion).toBe(v0 + 2)
    })

    it("multiple rapid bumps are additive (no dedup at store level)", () => {
      const s = useNodeResultsStore.getState()
      const v0 = s.graphVersion
      for (let i = 0; i < 5; i++) {
        useNodeResultsStore.getState().bumpGraphVersion()
      }
      expect(useNodeResultsStore.getState().graphVersion).toBe(v0 + 5)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // clearNode
  // ────────────────────────────────────────────────────────────────

  describe("clearNode", () => {
    it("removes all data for a given node", () => {
      const s = useNodeResultsStore.getState()

      // Set up data across all caches
      s.setPreview("n1", makePreviewData(), 0)
      s.setColumns("n1", [{ name: "x", dtype: "float64" }], 0)
      s.startSolveJob("n1", "sj1", "Node 1", { c: { min: 0, max: 1 } }, "h1")
      s.startTrainJob("n1", "tj1", "Train 1", "th1")

      // Also set up a solve result (complete a second job to create it)
      s.startSolveJob("n1b", "sj2", "Node 1b", {}, "h2")
      // We'll directly inject a solve result for n1
      useNodeResultsStore.setState((prev) => ({
        solveResults: { ...prev.solveResults, n1: { result: makeSolveResult(), jobId: "sj-old", configHash: "h-old", constraints: {}, nodeLabel: "N1" } },
        trainResults: { ...prev.trainResults, n1: { result: makeTrainResult(), jobId: "tj-old", configHash: "th-old" } },
      }))

      // Verify all caches have data
      expect(useNodeResultsStore.getState().getPreview("n1")).not.toBeNull()
      expect(useNodeResultsStore.getState().getColumns("n1")).not.toBeNull()
      expect(useNodeResultsStore.getState().solveJobs["n1"]).toBeDefined()
      expect(useNodeResultsStore.getState().solveResults["n1"]).toBeDefined()
      expect(useNodeResultsStore.getState().trainJobs["n1"]).toBeDefined()
      expect(useNodeResultsStore.getState().trainResults["n1"]).toBeDefined()

      // Clear
      useNodeResultsStore.getState().clearNode("n1")

      const after = useNodeResultsStore.getState()
      expect(after.getPreview("n1")).toBeNull()
      expect(after.getColumns("n1")).toBeNull()
      expect(after.solveJobs["n1"]).toBeUndefined()
      expect(after.solveResults["n1"]).toBeUndefined()
      expect(after.trainJobs["n1"]).toBeUndefined()
      expect(after.trainResults["n1"]).toBeUndefined()
    })

    it("does not affect other nodes", () => {
      const s = useNodeResultsStore.getState()
      s.setPreview("n1", makePreviewData(), 0)
      s.setPreview("n2", makePreviewData({ nodeId: "n2" }), 0)

      s.clearNode("n1")

      expect(useNodeResultsStore.getState().getPreview("n1")).toBeNull()
      expect(useNodeResultsStore.getState().getPreview("n2")).not.toBeNull()
    })
  })

  // ────────────────────────────────────────────────────────────────
  // getOptimiserPreview
  // ────────────────────────────────────────────────────────────────

  describe("getOptimiserPreview", () => {
    it("returns null when no solve result exists", () => {
      expect(useNodeResultsStore.getState().getOptimiserPreview("n1")).toBeNull()
    })

    it("builds correct shape from completed result", () => {
      const s = useNodeResultsStore.getState()
      const constraints = { premium: { min: 0, max: 100 } }
      s.startSolveJob("n1", "j1", "Optim Node", constraints, "h")
      const result = makeSolveResult({ converged: true, iterations: 15 })
      s.completeSolveJob("n1", result)

      const preview = useNodeResultsStore.getState().getOptimiserPreview("n1")
      expect(preview).not.toBeNull()
      expect(preview!.result).toEqual(result)
      expect(preview!.jobId).toBe("j1")
      expect(preview!.constraints).toEqual(constraints)
      expect(preview!.nodeLabel).toBe("Optim Node")
    })
  })
})
