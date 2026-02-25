/**
 * Tests for useMlflowBrowser — lazy-loading MLflow dropdown data with
 * fetch guards, error handling, and guard-reset logic.
 *
 * All four API functions are vi.mock'd so no network calls are made.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { renderHook, act, cleanup } from "@testing-library/react"

// Mock API functions BEFORE importing the hook
vi.mock("../../api/client.ts", () => ({
  getExperiments: vi.fn(),
  getRuns: vi.fn(),
  getModels: vi.fn(),
  getModelVersions: vi.fn(),
  ApiError: class ApiError extends Error {
    status: number
    detail?: string
    constructor(message: string, status: number, detail?: string) {
      super(message)
      this.name = "ApiError"
      this.status = status
      this.detail = detail
    }
  },
}))

import {
  getExperiments,
  getRuns,
  getModels,
  getModelVersions,
} from "../../api/client.ts"
import { useMlflowBrowser } from "../../hooks/useMlflowBrowser.ts"

// ── Helpers ──────────────────────────────────────────────────────────

const mockGetExperiments = vi.mocked(getExperiments)
const mockGetRuns = vi.mocked(getRuns)
const mockGetModels = vi.mocked(getModels)
const mockGetModelVersions = vi.mocked(getModelVersions)

const fakeExperiments = [
  { experiment_id: "1", name: "exp-1" },
  { experiment_id: "2", name: "exp-2" },
]

const fakeRuns = [
  { run_id: "r1", run_name: "run-1", metrics: { rmse: 0.1 }, artifacts: ["model.pkl"] },
]

const fakeModels = [
  { name: "model-a", latest_versions: [{ version: "1", status: "READY", run_id: "r1" }] },
]

const fakeVersions = [
  { version: "1", run_id: "r1", status: "READY", description: "v1" },
  { version: "2", run_id: "r2", status: "READY", description: "v2" },
]

/**
 * Flush all pending microtasks so that .then/.catch handlers on mocked
 * promises resolve and React state updates are applied.
 */
async function flushPromises() {
  await act(async () => {
    await new Promise((r) => setTimeout(r, 0))
  })
}

// ── Test suites ──────────────────────────────────────────────────────

describe("useMlflowBrowser", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    cleanup()
  })

  // ────────────────────────────────────────────────────────────────
  // initialExpId
  // ────────────────────────────────────────────────────────────────

  describe("initialExpId", () => {
    it("initializes browseExpId from the option", () => {
      const { result } = renderHook(() =>
        useMlflowBrowser({ initialExpId: "42" }),
      )
      expect(result.current.browseExpId).toBe("42")
    })

    it("defaults browseExpId to empty string when no option", () => {
      const { result } = renderHook(() => useMlflowBrowser())
      expect(result.current.browseExpId).toBe("")
    })
  })

  // ────────────────────────────────────────────────────────────────
  // refreshExperiments — fetch guard
  // ────────────────────────────────────────────────────────────────

  describe("refreshExperiments fetch guard", () => {
    it("only calls API once; second call is a no-op", async () => {
      mockGetExperiments.mockResolvedValue(fakeExperiments)

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshExperiments() })
      await flushPromises()

      expect(mockGetExperiments).toHaveBeenCalledTimes(1)
      expect(result.current.experiments).toEqual(fakeExperiments)

      // Second call — guard should prevent fetch
      act(() => { result.current.refreshExperiments() })
      await flushPromises()

      expect(mockGetExperiments).toHaveBeenCalledTimes(1)
    })

    it("resets guard on error so next call attempts fetch again", async () => {
      mockGetExperiments.mockRejectedValueOnce(new Error("Network error"))

      const { result } = renderHook(() => useMlflowBrowser())

      // First call — fails
      act(() => { result.current.refreshExperiments() })
      await flushPromises()

      expect(mockGetExperiments).toHaveBeenCalledTimes(1)
      expect(result.current.errorExperiments).toBe("Network error")
      expect(result.current.experiments).toEqual([])

      // Second call — guard was reset, so it should fetch again
      mockGetExperiments.mockResolvedValueOnce(fakeExperiments)
      act(() => { result.current.refreshExperiments() })
      await flushPromises()

      expect(mockGetExperiments).toHaveBeenCalledTimes(2)
      expect(result.current.experiments).toEqual(fakeExperiments)
      expect(result.current.errorExperiments).toBe("")
    })
  })

  // ────────────────────────────────────────────────────────────────
  // refreshExperiments — loading state
  // ────────────────────────────────────────────────────────────────

  describe("refreshExperiments loading state", () => {
    it("loading is true during fetch, false after", async () => {
      let resolvePromise!: (v: typeof fakeExperiments) => void
      mockGetExperiments.mockReturnValue(
        new Promise((resolve) => { resolvePromise = resolve }),
      )

      const { result } = renderHook(() => useMlflowBrowser())

      // Trigger fetch
      act(() => { result.current.refreshExperiments() })

      // loading should be true while promise is pending
      expect(result.current.loadingExperiments).toBe(true)

      // Resolve the promise
      await act(async () => { resolvePromise(fakeExperiments) })

      expect(result.current.loadingExperiments).toBe(false)
      expect(result.current.experiments).toEqual(fakeExperiments)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // refreshRuns — with and without tag
  // ────────────────────────────────────────────────────────────────

  describe("refreshRuns", () => {
    it("forwards runTag to getRuns when provided", async () => {
      mockGetRuns.mockResolvedValue(fakeRuns)

      const { result } = renderHook(() =>
        useMlflowBrowser({ runTag: "optimiser" }),
      )

      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()

      expect(mockGetRuns).toHaveBeenCalledWith("exp-1", "optimiser")
      expect(result.current.runs).toEqual(fakeRuns)
    })

    it("calls getRuns with undefined tag when no runTag provided", async () => {
      mockGetRuns.mockResolvedValue(fakeRuns)

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()

      expect(mockGetRuns).toHaveBeenCalledWith("exp-1", undefined)
    })

    it("does nothing when expId is empty string", async () => {
      mockGetRuns.mockResolvedValue(fakeRuns)

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshRuns("") })
      await flushPromises()

      expect(mockGetRuns).not.toHaveBeenCalled()
    })
  })

  // ────────────────────────────────────────────────────────────────
  // refreshRuns — guard by expId
  // ────────────────────────────────────────────────────────────────

  describe("refreshRuns guard by expId", () => {
    it("calling with same expId twice only fetches once", async () => {
      mockGetRuns.mockResolvedValue(fakeRuns)

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(1)

      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(1)
    })

    it("calling with different expId fetches again", async () => {
      mockGetRuns.mockResolvedValue(fakeRuns)

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(1)

      act(() => { result.current.refreshRuns("exp-2") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(2)
    })

    it("resets guard on error so same expId can be retried", async () => {
      mockGetRuns.mockRejectedValueOnce(new Error("Timeout"))

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(1)
      expect(result.current.errorRuns).toBe("Timeout")

      // Guard was reset — same expId should fetch again
      mockGetRuns.mockResolvedValueOnce(fakeRuns)
      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(2)
      expect(result.current.runs).toEqual(fakeRuns)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // refreshRuns — loading state
  // ────────────────────────────────────────────────────────────────

  describe("refreshRuns loading state", () => {
    it("loading is true during fetch, false after", async () => {
      let resolvePromise!: (v: typeof fakeRuns) => void
      mockGetRuns.mockReturnValue(
        new Promise((resolve) => { resolvePromise = resolve }),
      )

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshRuns("exp-1") })
      expect(result.current.loadingRuns).toBe(true)

      await act(async () => { resolvePromise(fakeRuns) })
      expect(result.current.loadingRuns).toBe(false)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // resetRunsGuard
  // ────────────────────────────────────────────────────────────────

  describe("resetRunsGuard", () => {
    it("after calling resetRunsGuard, next refreshRuns with same expId fetches again", async () => {
      mockGetRuns.mockResolvedValue(fakeRuns)

      const { result } = renderHook(() => useMlflowBrowser())

      // First fetch
      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(1)

      // Same expId — should be blocked by guard
      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(1)

      // Reset guard
      act(() => { result.current.resetRunsGuard() })

      // Now same expId should fetch again
      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(2)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // refreshModels — fetch guard
  // ────────────────────────────────────────────────────────────────

  describe("refreshModels fetch guard", () => {
    it("only calls API once; second call is a no-op", async () => {
      mockGetModels.mockResolvedValue(fakeModels)

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshModels() })
      await flushPromises()
      expect(mockGetModels).toHaveBeenCalledTimes(1)
      expect(result.current.models).toEqual(fakeModels)

      act(() => { result.current.refreshModels() })
      await flushPromises()
      expect(mockGetModels).toHaveBeenCalledTimes(1)
    })

    it("resets guard on error so next call attempts fetch again", async () => {
      mockGetModels.mockRejectedValueOnce(new Error("Server error"))

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshModels() })
      await flushPromises()
      expect(mockGetModels).toHaveBeenCalledTimes(1)
      expect(result.current.errorModels).toBe("Server error")

      mockGetModels.mockResolvedValueOnce(fakeModels)
      act(() => { result.current.refreshModels() })
      await flushPromises()
      expect(mockGetModels).toHaveBeenCalledTimes(2)
      expect(result.current.models).toEqual(fakeModels)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // refreshVersions — guard by model name
  // ────────────────────────────────────────────────────────────────

  describe("refreshVersions guard by model name", () => {
    it("calling with same model name twice only fetches once", async () => {
      mockGetModelVersions.mockResolvedValue(fakeVersions)

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshVersions("model-a") })
      await flushPromises()
      expect(mockGetModelVersions).toHaveBeenCalledTimes(1)
      expect(result.current.modelVersions).toEqual(fakeVersions)

      act(() => { result.current.refreshVersions("model-a") })
      await flushPromises()
      expect(mockGetModelVersions).toHaveBeenCalledTimes(1)
    })

    it("calling with different model name fetches again", async () => {
      mockGetModelVersions.mockResolvedValue(fakeVersions)

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshVersions("model-a") })
      await flushPromises()
      expect(mockGetModelVersions).toHaveBeenCalledTimes(1)

      act(() => { result.current.refreshVersions("model-b") })
      await flushPromises()
      expect(mockGetModelVersions).toHaveBeenCalledTimes(2)
    })

    it("does nothing when modelName is empty string", async () => {
      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshVersions("") })
      await flushPromises()
      expect(mockGetModelVersions).not.toHaveBeenCalled()
    })

    it("resets guard on error so same model name can be retried", async () => {
      mockGetModelVersions.mockRejectedValueOnce(new Error("Not found"))

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshVersions("model-a") })
      await flushPromises()
      expect(mockGetModelVersions).toHaveBeenCalledTimes(1)
      expect(result.current.errorVersions).toBe("Not found")

      mockGetModelVersions.mockResolvedValueOnce(fakeVersions)
      act(() => { result.current.refreshVersions("model-a") })
      await flushPromises()
      expect(mockGetModelVersions).toHaveBeenCalledTimes(2)
      expect(result.current.modelVersions).toEqual(fakeVersions)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Error handling — ApiError with detail
  // ────────────────────────────────────────────────────────────────

  describe("error handling", () => {
    it("API error sets error state and resets fetch guard for experiments", async () => {
      const { ApiError } = await import("../../api/client.ts") as { ApiError: new (msg: string, status: number, detail?: string) => Error & { detail?: string } }
      mockGetExperiments.mockRejectedValueOnce(new ApiError("HTTP 500", 500, "Internal server error"))

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshExperiments() })
      await flushPromises()

      expect(result.current.errorExperiments).toBe("Internal server error")
      expect(result.current.loadingExperiments).toBe(false)
      expect(result.current.experiments).toEqual([])

      // Guard was reset — can try again
      mockGetExperiments.mockResolvedValueOnce(fakeExperiments)
      act(() => { result.current.refreshExperiments() })
      await flushPromises()
      expect(mockGetExperiments).toHaveBeenCalledTimes(2)
    })

    it("API error sets error state and resets fetch guard for runs", async () => {
      mockGetRuns.mockRejectedValueOnce(new Error("Connection refused"))

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()

      expect(result.current.errorRuns).toBe("Connection refused")
      expect(result.current.loadingRuns).toBe(false)

      // Guard was reset
      mockGetRuns.mockResolvedValueOnce(fakeRuns)
      act(() => { result.current.refreshRuns("exp-1") })
      await flushPromises()
      expect(mockGetRuns).toHaveBeenCalledTimes(2)
    })

    it("uses fallback message when error message is empty", async () => {
      mockGetModels.mockRejectedValueOnce(new Error(""))

      const { result } = renderHook(() => useMlflowBrowser())

      act(() => { result.current.refreshModels() })
      await flushPromises()

      // The hook uses `errorMsg(e) || "Failed to load models"`
      expect(result.current.errorModels).toBe("Failed to load models")
    })
  })
})
