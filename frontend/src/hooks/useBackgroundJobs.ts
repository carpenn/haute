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

const POLL_INTERVAL_MS = 500

export default function useBackgroundJobs() {
  const addToast = useUIStore((s) => s.addToast)

  // Track active polling intervals so we can clean up
  const solveIntervals = useRef<Record<string, ReturnType<typeof setInterval>>>({})
  const trainIntervals = useRef<Record<string, ReturnType<typeof setInterval>>>({})

  // ── Optimiser job polling ──

  const solveJobs = useNodeResultsStore((s) => s.solveJobs)
  const updateSolveProgress = useNodeResultsStore((s) => s.updateSolveProgress)
  const completeSolveJob = useNodeResultsStore((s) => s.completeSolveJob)
  const failSolveJob = useNodeResultsStore((s) => s.failSolveJob)

  useEffect(() => {
    const activeNodeIds = Object.keys(solveJobs)
    const pollingNodeIds = Object.keys(solveIntervals.current)

    // Start polling for new jobs
    for (const nodeId of activeNodeIds) {
      if (solveIntervals.current[nodeId]) continue // already polling

      const job = solveJobs[nodeId]
      solveIntervals.current[nodeId] = setInterval(async () => {
        try {
          const status = await getOptimiserStatus<SolveProgress>(job.jobId)
          if (status.status === "completed" || status.status === "error") {
            clearInterval(solveIntervals.current[nodeId])
            delete solveIntervals.current[nodeId]

            if (status.status === "completed" && status.result) {
              completeSolveJob(nodeId, status.result)
              addToast("success", `Optimisation complete: ${job.nodeLabel}`)
            } else {
              const msg = status.message || "Unknown error"
              failSolveJob(nodeId, msg)
              addToast("error", `Optimisation failed: ${job.nodeLabel} — ${msg}`)
            }
          } else {
            updateSolveProgress(nodeId, status)
          }
        } catch {
          // Network error — keep polling, don't kill the interval
        }
      }, POLL_INTERVAL_MS)
    }

    // Stop polling for jobs that are no longer active (completed/cleared)
    for (const nodeId of pollingNodeIds) {
      if (!solveJobs[nodeId]) {
        clearInterval(solveIntervals.current[nodeId])
        delete solveIntervals.current[nodeId]
      }
    }
  }, [solveJobs, updateSolveProgress, completeSolveJob, failSolveJob, addToast])

  // ── Training job polling ──

  const trainJobs = useNodeResultsStore((s) => s.trainJobs)
  const updateTrainProgress = useNodeResultsStore((s) => s.updateTrainProgress)
  const completeTrainJob = useNodeResultsStore((s) => s.completeTrainJob)
  const failTrainJob = useNodeResultsStore((s) => s.failTrainJob)

  useEffect(() => {
    const activeNodeIds = Object.keys(trainJobs)
    const pollingNodeIds = Object.keys(trainIntervals.current)

    for (const nodeId of activeNodeIds) {
      if (trainIntervals.current[nodeId]) continue

      const job = trainJobs[nodeId]
      trainIntervals.current[nodeId] = setInterval(async () => {
        try {
          const status = await getTrainStatus<TrainProgress>(job.jobId)
          if (status.status === "completed" || status.status === "error") {
            clearInterval(trainIntervals.current[nodeId])
            delete trainIntervals.current[nodeId]

            if (status.status === "completed" && status.result) {
              completeTrainJob(nodeId, status.result)
              addToast("success", `Training complete: ${job.nodeLabel}`)
            } else {
              const msg = status.message || "Unknown error"
              failTrainJob(nodeId, msg)
              addToast("error", `Training failed: ${job.nodeLabel} — ${msg}`)
            }
          } else {
            updateTrainProgress(nodeId, status)
          }
        } catch {
          // Network error — keep polling
        }
      }, POLL_INTERVAL_MS)
    }

    for (const nodeId of pollingNodeIds) {
      if (!trainJobs[nodeId]) {
        clearInterval(trainIntervals.current[nodeId])
        delete trainIntervals.current[nodeId]
      }
    }
  }, [trainJobs, updateTrainProgress, completeTrainJob, failTrainJob, addToast])

  // ── Cleanup all intervals on unmount ──

  useEffect(() => {
    return () => {
      for (const id of Object.values(solveIntervals.current)) clearInterval(id)
      for (const id of Object.values(trainIntervals.current)) clearInterval(id)
    }
  }, [])
}
