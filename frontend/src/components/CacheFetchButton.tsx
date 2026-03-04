import { useState, useEffect } from "react"
import { Loader2, HardDriveDownload, Trash2 } from "lucide-react"
import { ApiError } from "../api/client"
import { formatBytes } from "../utils/formatBytes"
import { formatTime } from "../utils/formatTime"

// ─── Types ───────────────────────────────────────────────────────

/** Minimal cache status shape – consumers extend with their own fields. */
export type BaseCacheStatus = {
  cached: boolean
  row_count: number
  column_count: number
  size_bytes: number
}

type ProgressPayload = { active: boolean; rows?: number; elapsed?: number }

export type CacheFetchButtonProps<TStatus extends BaseCacheStatus> = {
  /** The key that identifies the resource (path, table name, etc.). */
  resourceKey: string

  /** API: check current cache status. */
  getStatus: (key: string) => Promise<TStatus>
  /** API: kick off a fetch / build. */
  startFetch: (key: string) => Promise<TStatus>
  /** API: poll progress while building. */
  getProgress: (key: string) => Promise<ProgressPayload>
  /** API: delete the cached data. */
  deleteCache: (key: string) => Promise<TStatus>

  /** Field on TStatus that holds the "cached at" unix timestamp. */
  timestampField: keyof TStatus

  /** Labels */
  labels: {
    /** Button text when nothing is cached (e.g. "Cache as Parquet"). */
    fetchLabel: string
    /** Button text when cache exists (e.g. "Refresh Cache"). */
    refreshLabel: string
    /** Hint shown below the button when not yet cached. */
    notCachedHint: string
    /** Text shown while waiting for the first progress tick. */
    pendingLabel: string
  }

  /** Called after a successful fetch or when the initial status load finds a cache. */
  onCacheReady?: (status: TStatus) => void
}

// ─── Component ───────────────────────────────────────────────────

export function CacheFetchButton<TStatus extends BaseCacheStatus>({
  resourceKey,
  getStatus,
  startFetch,
  getProgress,
  deleteCache: deleteCacheFn,
  timestampField,
  labels,
  onCacheReady,
}: CacheFetchButtonProps<TStatus>) {
  const [cache, setCache] = useState<TStatus | null>(null)
  const [building, setBuilding] = useState(false)
  const [progress, setProgress] = useState<{ rows: number; elapsed: number } | null>(null)
  const [error, setError] = useState("")

  // Load initial status
  useEffect(() => {
    if (!resourceKey) return
    getStatus(resourceKey)
      .then((data) => {
        setCache(data)
        if (data.cached) onCacheReady?.(data)
      })
      .catch(() => setCache(null))
  }, [resourceKey])

  // Poll progress while building
  useEffect(() => {
    if (!building || !resourceKey) return
    const id = setInterval(() => {
      getProgress(resourceKey)
        .then((data) => {
          if (data.active) setProgress({ rows: data.rows || 0, elapsed: data.elapsed || 0 })
        })
        .catch(() => { /* polling retry on next interval */ })
    }, 1000)
    return () => { clearInterval(id); setProgress(null) }
  }, [building, resourceKey])

  const doFetch = () => {
    if (!resourceKey) return
    setBuilding(true)
    setError("")
    startFetch(resourceKey)
      .then((data) => {
        setCache(data)
        setBuilding(false)
        onCacheReady?.(data)
      })
      .catch((e: Error) => {
        setError(e instanceof ApiError ? e.detail || e.message : e.message)
        setBuilding(false)
      })
  }

  const doDelete = () => {
    deleteCacheFn(resourceKey)
      .then((data) => setCache(data))
      .catch((e: Error) => setError(e instanceof ApiError ? e.detail || e.message : e.message))
  }

  const cachedAt = cache ? (cache[timestampField] as number) : 0

  return (
    <div>
      <button
        onClick={doFetch}
        disabled={!resourceKey || building}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-colors disabled:opacity-40"
        style={{
          background: cache?.cached ? 'rgba(34,197,94,.1)' : 'var(--accent-soft)',
          border: cache?.cached ? '1px solid rgba(34,197,94,.3)' : '1px solid var(--accent)',
          color: cache?.cached ? '#22c55e' : 'var(--accent)',
        }}
      >
        {building ? (
          <><Loader2 size={14} className="animate-spin" /> {progress ? `${progress.rows.toLocaleString()} rows \u00b7 ${progress.elapsed}s` : labels.pendingLabel}</>
        ) : cache?.cached ? (
          <><HardDriveDownload size={14} /> {labels.refreshLabel}</>
        ) : (
          <><HardDriveDownload size={14} /> {labels.fetchLabel}</>
        )}
      </button>

      {cache?.cached && (
        <div className="mt-1.5 flex items-center gap-2 text-[10px] px-1" style={{ color: 'var(--text-muted)' }}>
          <span>{cache.row_count.toLocaleString()} rows</span>
          <span>&middot;</span>
          <span>{cache.column_count} cols</span>
          <span>&middot;</span>
          <span>{formatBytes(cache.size_bytes)}</span>
          {cachedAt > 0 && (
            <><span>&middot;</span><span>{formatTime(cachedAt)}</span></>
          )}
          <span>&middot;</span>
          <button
            onClick={doDelete}
            className="inline-flex items-center gap-0.5 hover:opacity-70 transition-opacity"
            style={{ color: '#ef4444' }}
            title="Delete cached data"
          >
            <Trash2 size={10} /> clear
          </button>
        </div>
      )}

      {!cache?.cached && resourceKey && !building && (
        <div className="mt-1.5 text-[10px] px-1" style={{ color: '#f59e0b' }}>
          {labels.notCachedHint}
        </div>
      )}

      {error && (
        <div className="mt-1.5 text-[10px] px-2 py-1 rounded" style={{ background: 'rgba(239,68,68,.1)', color: '#ef4444' }}>
          {error}
        </div>
      )}
    </div>
  )
}
