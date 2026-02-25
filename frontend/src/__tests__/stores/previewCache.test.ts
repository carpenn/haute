/**
 * Tests for preview cache hit/miss/stale paths using useNodeResultsStore.
 *
 * These tests focus specifically on the preview cache behaviour and
 * staleness detection via graphVersion.
 */
import { describe, it, expect, beforeEach } from "vitest"
import useNodeResultsStore from "../../stores/useNodeResultsStore.ts"
import type { PreviewData } from "../../panels/DataPreview.tsx"

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
    row_count: 5,
    column_count: 2,
    columns: [
      { name: "x", dtype: "float64" },
      { name: "y", dtype: "int64" },
    ],
    preview: [{ x: 1.0, y: 2 }],
    error: null,
    ...overrides,
  }
}

// ── Tests ────────────────────────────────────────────────────────

describe("preview cache", () => {
  beforeEach(() => {
    resetStore()
  })

  // ────────────────────────────────────────────────────────────────
  // Cache miss
  // ────────────────────────────────────────────────────────────────

  describe("cache miss", () => {
    it("getPreview returns null for a node that has never been cached", () => {
      expect(useNodeResultsStore.getState().getPreview("unknown-node")).toBeNull()
    })

    it("getPreview returns null for a different node ID", () => {
      const s = useNodeResultsStore.getState()
      s.setPreview("node-a", makePreviewData({ nodeId: "node-a" }), 0)
      expect(useNodeResultsStore.getState().getPreview("node-b")).toBeNull()
    })

    it("getPreview returns null after clearNode", () => {
      const s = useNodeResultsStore.getState()
      s.setPreview("n1", makePreviewData(), 0)
      s.clearNode("n1")
      expect(useNodeResultsStore.getState().getPreview("n1")).toBeNull()
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Cache hit
  // ────────────────────────────────────────────────────────────────

  describe("cache hit", () => {
    it("returns cached data with matching graphVersion", () => {
      const s = useNodeResultsStore.getState()
      const preview = makePreviewData()
      s.setPreview("n1", preview, 0)

      const cached = useNodeResultsStore.getState().getPreview("n1")
      expect(cached).not.toBeNull()
      expect(cached!.data).toEqual(preview)
      expect(cached!.graphVersion).toBe(0)
      // graphVersion matches store's current version
      expect(cached!.graphVersion).toBe(useNodeResultsStore.getState().graphVersion)
    })

    it("returns the latest preview when set multiple times", () => {
      const s = useNodeResultsStore.getState()
      s.setPreview("n1", makePreviewData({ row_count: 5 }), 0)
      s.setPreview("n1", makePreviewData({ row_count: 10 }), 0)

      const cached = useNodeResultsStore.getState().getPreview("n1")
      expect(cached!.data.row_count).toBe(10)
    })

    it("stores previews for multiple nodes independently", () => {
      const s = useNodeResultsStore.getState()
      s.setPreview("a", makePreviewData({ nodeId: "a", row_count: 1 }), 0)
      s.setPreview("b", makePreviewData({ nodeId: "b", row_count: 2 }), 0)

      expect(useNodeResultsStore.getState().getPreview("a")!.data.row_count).toBe(1)
      expect(useNodeResultsStore.getState().getPreview("b")!.data.row_count).toBe(2)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Cache stale
  // ────────────────────────────────────────────────────────────────

  describe("cache stale", () => {
    it("preview becomes stale after bumpGraphVersion", () => {
      const s = useNodeResultsStore.getState()
      const preview = makePreviewData()
      s.setPreview("n1", preview, 0)

      // Bump graph version (simulates node/edge change)
      s.bumpGraphVersion()

      const cached = useNodeResultsStore.getState().getPreview("n1")
      // Data is still returned for instant display
      expect(cached).not.toBeNull()
      expect(cached!.data).toEqual(preview)
      // But graphVersion doesn't match — caller detects staleness
      expect(cached!.graphVersion).toBe(0)
      expect(useNodeResultsStore.getState().graphVersion).toBe(1)
      expect(cached!.graphVersion).not.toBe(useNodeResultsStore.getState().graphVersion)
    })

    it("preview remains stale across multiple bumps", () => {
      const s = useNodeResultsStore.getState()
      s.setPreview("n1", makePreviewData(), 0)
      s.bumpGraphVersion()
      s.bumpGraphVersion()
      s.bumpGraphVersion()

      const cached = useNodeResultsStore.getState().getPreview("n1")
      expect(cached!.graphVersion).toBe(0)
      expect(useNodeResultsStore.getState().graphVersion).toBe(3)
    })

    it("re-setting preview at current graphVersion makes it fresh again", () => {
      const s = useNodeResultsStore.getState()
      s.setPreview("n1", makePreviewData(), 0)
      s.bumpGraphVersion()

      // Verify stale
      expect(useNodeResultsStore.getState().getPreview("n1")!.graphVersion)
        .not.toBe(useNodeResultsStore.getState().graphVersion)

      // Re-set at new graphVersion
      const freshPreview = makePreviewData({ row_count: 99 })
      useNodeResultsStore.getState().setPreview("n1", freshPreview, 1)

      const cached = useNodeResultsStore.getState().getPreview("n1")
      expect(cached!.graphVersion).toBe(1)
      expect(cached!.graphVersion).toBe(useNodeResultsStore.getState().graphVersion)
      expect(cached!.data.row_count).toBe(99)
    })

    it("stale preview for one node does not affect freshness of another", () => {
      const s = useNodeResultsStore.getState()
      s.setPreview("a", makePreviewData({ nodeId: "a" }), 0)
      s.bumpGraphVersion()
      // Set "b" at the new graphVersion — it should be fresh
      s.setPreview("b", makePreviewData({ nodeId: "b" }), 1)

      const cachedA = useNodeResultsStore.getState().getPreview("a")
      const cachedB = useNodeResultsStore.getState().getPreview("b")
      const currentVersion = useNodeResultsStore.getState().graphVersion

      expect(cachedA!.graphVersion).toBe(0)
      expect(cachedA!.graphVersion).not.toBe(currentVersion)

      expect(cachedB!.graphVersion).toBe(1)
      expect(cachedB!.graphVersion).toBe(currentVersion)
    })
  })
})
