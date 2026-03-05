/**
 * Tests for useJobPolling — the generic polling hook extracted from
 * useBackgroundJobs.
 *
 * These tests exercise the hook directly (not through the orchestrator)
 * to verify: start/stop lifecycle, exponential backoff, max lifetime
 * timeout, cleanup on unmount, and cleanup on job removal.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { renderHook, act, cleanup } from "@testing-library/react"
import useJobPolling from "../../hooks/useJobPolling"
import type { UseJobPollingConfig } from "../../hooks/useJobPolling"

// ── Types for test jobs ──────────────────────────────────────────

interface TestJob {
  jobId: string
  nodeLabel: string
}

interface TestStatus {
  status: string
  progress: number
  message: string
  result?: { value: number }
}

// ── Helpers ──────────────────────────────────────────────────────

function makeConfig(
  overrides: Partial<UseJobPollingConfig<TestJob, TestStatus>> = {},
): UseJobPollingConfig<TestJob, TestStatus> {
  return {
    jobs: {},
    pollFn: vi.fn().mockResolvedValue({ status: "running", progress: 0.5, message: "Working" }),
    onProgress: vi.fn(),
    onComplete: vi.fn(),
    onFail: vi.fn(),
    labelFn: (job) => job.nodeLabel,
    jobIdFn: (job) => job.jobId,
    isComplete: (s) => s.status === "completed",
    isError: (s) => s.status === "error",
    getResult: (s) => (s.result ? s : undefined),
    getErrorMessage: (s) => s.message || "Unknown error",
    addToast: vi.fn(),
    successLabel: "Job complete",
    failLabel: "Job failed",
    ...overrides,
  }
}

async function advance(ms: number): Promise<void> {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

// ── Test suites ──────────────────────────────────────────────────

describe("useJobPolling", () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.clearAllMocks()
  })

  afterEach(() => {
    cleanup()
    vi.useRealTimers()
  })

  // ────────────────────────────────────────────────────────────────
  // Basic lifecycle
  // ────────────────────────────────────────────────────────────────

  describe("basic lifecycle", () => {
    it("starts polling when a job appears and calls onProgress", async () => {
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockResolvedValue({
        status: "running",
        progress: 0.5,
        message: "Working",
      })
      const onProgress = vi.fn()

      const config = makeConfig({
        jobs: { n1: { jobId: "j1", nodeLabel: "Node 1" } },
        pollFn,
        onProgress,
      })

      renderHook(() => useJobPolling(config))

      // First poll at 500ms
      await advance(500)

      expect(pollFn).toHaveBeenCalledWith("j1")
      expect(onProgress).toHaveBeenCalledWith("n1", {
        status: "running",
        progress: 0.5,
        message: "Working",
      })
    })

    it("calls onComplete and shows success toast when job completes", async () => {
      const result = { value: 42 }
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockResolvedValue({
        status: "completed",
        progress: 1.0,
        message: "Done",
        result,
      })
      const onComplete = vi.fn()
      const addToast = vi.fn()

      const config = makeConfig({
        jobs: { n1: { jobId: "j1", nodeLabel: "Node 1" } },
        pollFn,
        onComplete,
        addToast,
      })

      renderHook(() => useJobPolling(config))

      await advance(500)

      expect(onComplete).toHaveBeenCalledTimes(1)
      expect(onComplete).toHaveBeenCalledWith("n1", expect.objectContaining({ result }))
      expect(addToast).toHaveBeenCalledWith("success", "Job complete: Node 1")
    })

    it("calls onFail and shows error toast when API returns error status", async () => {
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockResolvedValue({
        status: "error",
        progress: 0,
        message: "Infeasible",
      })
      const onFail = vi.fn()
      const addToast = vi.fn()

      const config = makeConfig({
        jobs: { n1: { jobId: "j1", nodeLabel: "Node 1" } },
        pollFn,
        onFail,
        addToast,
      })

      renderHook(() => useJobPolling(config))

      await advance(500)

      expect(onFail).toHaveBeenCalledWith("n1", "Infeasible")
      expect(addToast).toHaveBeenCalledWith("error", "Job failed: Node 1 — Infeasible")
    })

    it("stops polling after job completes (no more polls scheduled)", async () => {
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockResolvedValue({
        status: "completed",
        progress: 1.0,
        message: "Done",
        result: { value: 1 },
      })

      const config = makeConfig({
        jobs: { n1: { jobId: "j1", nodeLabel: "Node 1" } },
        pollFn,
      })

      renderHook(() => useJobPolling(config))

      await advance(500)
      expect(pollFn).toHaveBeenCalledTimes(1)

      // Advance well past several poll intervals
      await advance(5000)
      expect(pollFn).toHaveBeenCalledTimes(1) // no additional calls
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Exponential backoff
  // ────────────────────────────────────────────────────────────────

  describe("exponential backoff", () => {
    it("increases delay after network errors", async () => {
      const callTimes: number[] = []
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockImplementation(() => {
        callTimes.push(Date.now())
        if (callTimes.length <= 3) {
          return Promise.reject(new Error("Network error"))
        }
        return Promise.resolve({
          status: "completed",
          progress: 1.0,
          message: "Done",
          result: { value: 1 },
        })
      })

      const config = makeConfig({
        jobs: { n1: { jobId: "j1", nodeLabel: "Node 1" } },
        pollFn,
      })

      renderHook(() => useJobPolling(config))

      // Error 1 at +500ms, Error 2 at +500+1000=+1500ms, Error 3 at +1500+2000=+3500ms
      // Success at +3500+4000=+7500ms
      await advance(8000)

      expect(callTimes.length).toBe(4)

      // Verify backoff: gap between call 1->2 should be >= 1000ms (500 * 2^1)
      const gap1 = callTimes[1] - callTimes[0]
      expect(gap1).toBeGreaterThanOrEqual(1000)

      // Gap 2->3 should be >= 2000ms (500 * 2^2)
      const gap2 = callTimes[2] - callTimes[1]
      expect(gap2).toBeGreaterThanOrEqual(2000)
    })

    it("shows warning toast after 5 consecutive failures", async () => {
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockRejectedValue(
        new Error("Network error"),
      )
      const addToast = vi.fn()

      const config = makeConfig({
        jobs: { n1: { jobId: "j1", nodeLabel: "Node 1" } },
        pollFn,
        addToast,
      })

      renderHook(() => useJobPolling(config))

      // Advance enough for 5 error polls:
      // Poll 1: +500, Poll 2: +1500, Poll 3: +3500, Poll 4: +7500, Poll 5: +12500
      await advance(13000)

      const warningCalls = addToast.mock.calls.filter(
        ([type]: unknown[]) => type === "warning",
      )
      expect(warningCalls.length).toBe(1)
      expect(warningCalls[0][1]).toContain("Polling is struggling")
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Max lifetime timeout
  // ────────────────────────────────────────────────────────────────

  describe("max lifetime timeout", () => {
    it("fails job after 24 hours", async () => {
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockResolvedValue({
        status: "running",
        progress: 0.5,
        message: "Working",
      })
      const onFail = vi.fn()
      const addToast = vi.fn()

      const config = makeConfig({
        jobs: { n1: { jobId: "j1", nodeLabel: "Node 1" } },
        pollFn,
        onFail,
        addToast,
      })

      renderHook(() => useJobPolling(config))

      // MAX_LIFETIME_MS = 24 * 60 * 60 * 1000
      await advance(86_400_500)

      expect(onFail).toHaveBeenCalledWith("n1", "Job timed out after 24 hours")
      expect(addToast).toHaveBeenCalledWith(
        "error",
        "Job failed: Node 1 — Job timed out after 24 hours",
      )
    }, 120_000)
  })

  // ────────────────────────────────────────────────────────────────
  // Cleanup
  // ────────────────────────────────────────────────────────────────

  describe("cleanup", () => {
    it("stops polling when job is removed from the jobs map via rerender", async () => {
      let callCount = 0
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockImplementation(() => {
        callCount++
        return Promise.resolve({ status: "running", progress: 0.5, message: "Working" })
      })

      const initialConfig = makeConfig({
        jobs: { n1: { jobId: "j1", nodeLabel: "Node 1" } },
        pollFn,
      })

      const { rerender } = renderHook(
        (props: { config: UseJobPollingConfig<TestJob, TestStatus> }) =>
          useJobPolling(props.config),
        { initialProps: { config: initialConfig } },
      )

      // Let first poll happen
      await advance(500)
      const afterFirst = callCount
      expect(afterFirst).toBeGreaterThanOrEqual(1)

      // Remove the job
      const updatedConfig = makeConfig({ jobs: {}, pollFn })
      rerender({ config: updatedConfig })

      const afterCleanup = callCount

      // Advance significantly -- no more polls
      await advance(5000)
      expect(callCount).toBeLessThanOrEqual(afterCleanup + 1)
    })

    it("clears all timeouts on unmount", async () => {
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockResolvedValue({
        status: "running",
        progress: 0.5,
        message: "Working",
      })

      const config = makeConfig({
        jobs: { n1: { jobId: "j1", nodeLabel: "Node 1" } },
        pollFn,
      })

      const { unmount } = renderHook(() => useJobPolling(config))

      await advance(500)
      const callsBeforeUnmount = pollFn.mock.calls.length

      unmount()

      // Advance well past several intervals
      await advance(10_000)

      // No new calls after unmount
      expect(pollFn.mock.calls.length).toBe(callsBeforeUnmount)
    })
  })

  // ────────────────────────────────────────────────────────────────
  // Multiple independent jobs
  // ────────────────────────────────────────────────────────────────

  describe("multiple jobs", () => {
    it("polls multiple jobs independently", async () => {
      let pollCalls: string[] = []
      const pollFn = vi.fn<(jobId: string) => Promise<TestStatus>>().mockImplementation((jobId) => {
        pollCalls.push(jobId)
        return Promise.resolve({ status: "running", progress: 0.5, message: "Working" })
      })

      const config = makeConfig({
        jobs: {
          n1: { jobId: "j1", nodeLabel: "Node 1" },
          n2: { jobId: "j2", nodeLabel: "Node 2" },
        },
        pollFn,
      })

      renderHook(() => useJobPolling(config))

      await advance(500)

      // Both jobs should have been polled
      expect(pollCalls).toContain("j1")
      expect(pollCalls).toContain("j2")
    })
  })
})
