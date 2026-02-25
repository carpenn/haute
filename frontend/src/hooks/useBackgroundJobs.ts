/**
 * Background job polling hook — mounted once in App.tsx.
 *
 * Manages polling for all active optimiser and training jobs regardless of
 * which panel is open. This means clicking away from a node mid-solve no
 * longer kills the polling loop — results are captured in useNodeResultsStore
 * and a toast notifies the user on completion.
 */
import { useEffect, useRef } from "react"
import { getOptimiserStatus, getTrainStatus } from "../api/client"
import useNodeResultsStore from "../stores/useNodeResultsStore"
import type { SolveProgress, TrainProgress } from "../stores/useNodeResultsStore"
import useUIStore from "../stores/useUIStore"

// ── Poller configuration ──

const BASE_INTERVAL_MS = 500
const MAX_INTERVAL_MS = 5_000
const MAX_LIFETIME_MS = 30 * 60 * 1_000 // 30 minutes
const CONSECUTIVE_FAILURES_FOR_TOAST = 5

// ── Per-job polling state tracked alongside the timeout handle ──

interface JobPollerState {
  timeoutId?: ReturnType<typeof setTimeout>
  startedAt: number
  consecutiveErrors: number
  toastedWarning: boolean
}

// ── Generic job poller ──

interface CreateJobPollerArgs<TJob, TStatus> {
  jobs: Record<string, TJob>
  stateRef: React.MutableRefObject<Record<string, JobPollerState>>
  pollFn: (jobId: string) => Promise<TStatus>
  onProgress: (nodeId: string, status: TStatus) => void
  onComplete: (nodeId: string, result: TStatus) => void
  onFail: (nodeId: string, errorMsg: string) => void
  labelFn: (job: TJob) => string
  jobIdFn: (job: TJob) => string
  isComplete: (status: TStatus) => boolean
  isError: (status: TStatus) => boolean
  getResult: (status: TStatus) => TStatus | undefined
  getErrorMessage: (status: TStatus) => string
  addToast: (type: "success" | "error" | "warning" | "info", text: string) => void
  successLabel: string
  failLabel: string
}

/**
 * Encapsulates shared interval/timeout management logic for polling background
 * jobs. Uses `setTimeout` with exponential backoff instead of `setInterval`.
 *
 * This is a plain function (NOT a hook). It is called from within useEffect
 * blocks to reconcile the current set of active jobs with the running pollers.
 */
function createJobPoller<
  TJob,
  TStatus,
>(args: CreateJobPollerArgs<TJob, TStatus>): void {
  const {
    jobs,
    stateRef,
    pollFn,
    onProgress,
    onComplete,
    onFail,
    labelFn,
    jobIdFn,
    isComplete,
    isError,
    getResult,
    getErrorMessage,
    addToast,
    successLabel,
    failLabel,
  } = args

  const activeNodeIds = Object.keys(jobs)
  const pollingNodeIds = Object.keys(stateRef.current)

  // Start polling for new jobs
  for (const nodeId of activeNodeIds) {
    if (stateRef.current[nodeId]) continue // already polling

    const job = jobs[nodeId]
    const now = Date.now()

    function schedulePoll(state: JobPollerState): void {
      const elapsed = Date.now() - state.startedAt

      // ── Max lifetime check ──
      if (elapsed >= MAX_LIFETIME_MS) {
        delete stateRef.current[nodeId]
        onFail(nodeId, "Job timed out after 30 minutes")
        addToast("error", `${failLabel}: ${labelFn(job)} — Job timed out after 30 minutes`)
        return
      }

      // Compute delay: base interval with exponential backoff on errors
      const delay =
        state.consecutiveErrors === 0
          ? BASE_INTERVAL_MS
          : Math.min(BASE_INTERVAL_MS * Math.pow(2, state.consecutiveErrors), MAX_INTERVAL_MS)

      state.timeoutId = setTimeout(async () => {
        try {
          const status = await pollFn(jobIdFn(job))

          // Reset backoff on successful network call
          state.consecutiveErrors = 0
          state.toastedWarning = false

          if (isComplete(status) || isError(status)) {
            delete stateRef.current[nodeId]

            if (isComplete(status) && getResult(status)) {
              onComplete(nodeId, getResult(status)!)
              addToast("success", `${successLabel}: ${labelFn(job)}`)
            } else {
              const msg = getErrorMessage(status) || "Unknown error"
              onFail(nodeId, msg)
              addToast("error", `${failLabel}: ${labelFn(job)} — ${msg}`)
            }
            return
          }

          // Still in progress
          onProgress(nodeId, status)
        } catch (e) {
          state.consecutiveErrors += 1
          console.warn(`${failLabel} poll failed (attempt ${state.consecutiveErrors}, will retry):`, e)

          if (state.consecutiveErrors >= CONSECUTIVE_FAILURES_FOR_TOAST && !state.toastedWarning) {
            state.toastedWarning = true
            addToast("warning", `Polling is struggling for ${labelFn(job)} — ${state.consecutiveErrors} consecutive errors`)
          }
        }

        // Schedule next poll (whether success-in-progress or error)
        if (stateRef.current[nodeId]) {
          schedulePoll(state)
        }
      }, delay)
    }

    const initialState: JobPollerState = {
      startedAt: now,
      consecutiveErrors: 0,
      toastedWarning: false,
    }
    stateRef.current[nodeId] = initialState
    schedulePoll(initialState)
  }

  // Stop polling for jobs that are no longer active (completed/cleared)
  for (const nodeId of pollingNodeIds) {
    if (!jobs[nodeId]) {
      clearTimeout(stateRef.current[nodeId].timeoutId)
      delete stateRef.current[nodeId]
    }
  }
}

// ── Hook ──

export default function useBackgroundJobs() {
  const addToast = useUIStore((s) => s.addToast)

  // Track active polling state so we can clean up
  const solvePollerState = useRef<Record<string, JobPollerState>>({})
  const trainPollerState = useRef<Record<string, JobPollerState>>({})

  // ── Optimiser job polling ──

  const solveJobs = useNodeResultsStore((s) => s.solveJobs)
  const updateSolveProgress = useNodeResultsStore((s) => s.updateSolveProgress)
  const completeSolveJob = useNodeResultsStore((s) => s.completeSolveJob)
  const failSolveJob = useNodeResultsStore((s) => s.failSolveJob)

  useEffect(() => {
    createJobPoller<typeof solveJobs[string], SolveProgress>({
      jobs: solveJobs,
      stateRef: solvePollerState,
      pollFn: (jobId) => getOptimiserStatus<SolveProgress>(jobId),
      onProgress: updateSolveProgress,
      onComplete: (nodeId, status) => completeSolveJob(nodeId, status.result!),
      onFail: failSolveJob,
      labelFn: (job) => job.nodeLabel,
      jobIdFn: (job) => job.jobId,
      isComplete: (s) => s.status === "completed",
      isError: (s) => s.status === "error",
      getResult: (s) => (s.result ? s : undefined),
      getErrorMessage: (s) => s.message || "Unknown error",
      addToast,
      successLabel: "Optimisation complete",
      failLabel: "Optimisation failed",
    })
  }, [solveJobs, updateSolveProgress, completeSolveJob, failSolveJob, addToast])

  // ── Training job polling ──

  const trainJobs = useNodeResultsStore((s) => s.trainJobs)
  const updateTrainProgress = useNodeResultsStore((s) => s.updateTrainProgress)
  const completeTrainJob = useNodeResultsStore((s) => s.completeTrainJob)
  const failTrainJob = useNodeResultsStore((s) => s.failTrainJob)

  useEffect(() => {
    createJobPoller<typeof trainJobs[string], TrainProgress>({
      jobs: trainJobs,
      stateRef: trainPollerState,
      pollFn: (jobId) => getTrainStatus<TrainProgress>(jobId),
      onProgress: updateTrainProgress,
      onComplete: (nodeId, status) => completeTrainJob(nodeId, status.result!),
      onFail: failTrainJob,
      labelFn: (job) => job.nodeLabel,
      jobIdFn: (job) => job.jobId,
      isComplete: (s) => s.status === "completed",
      isError: (s) => s.status === "error",
      getResult: (s) => (s.result ? s : undefined),
      getErrorMessage: (s) => s.message || "Unknown error",
      addToast,
      successLabel: "Training complete",
      failLabel: "Training failed",
    })
  }, [trainJobs, updateTrainProgress, completeTrainJob, failTrainJob, addToast])

  // ── Cleanup all timeouts on unmount ──

  useEffect(() => {
    const solveRef = solvePollerState.current
    const trainRef = trainPollerState.current
    return () => {
      for (const state of Object.values(solveRef))
        clearTimeout(state.timeoutId)
      for (const state of Object.values(trainRef))
        clearTimeout(state.timeoutId)
    }
  }, [])
}
