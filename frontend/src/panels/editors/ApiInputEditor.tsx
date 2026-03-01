import { useState, useEffect } from "react"
import { Radio, AlertTriangle, Loader2, HardDriveDownload, Trash2 } from "lucide-react"
import { FileBrowser, SchemaPreview } from "./_shared"
import type { OnUpdateConfig } from "./_shared"
import { useSchemaFetch } from "../../hooks/useSchemaFetch"
import { configField } from "../../utils/configField"
import {
  buildJsonCache,
  getJsonCacheProgress,
  getJsonCacheStatus,
  deleteJsonCache,
  ApiError,
} from "../../api/client"

// ─── JsonCacheButton ──────────────────────────────────────────────

type JsonCacheStatus = {
  cached: boolean
  path?: string
  data_path: string
  row_count: number
  column_count: number
  size_bytes: number
  cached_at: number
}

function JsonCacheButton({ dataPath }: { dataPath: string }) {
  const [cache, setCache] = useState<JsonCacheStatus | null>(null)
  const [building, setBuilding] = useState(false)
  const [progress, setProgress] = useState<{ rows: number; elapsed: number } | null>(null)
  const [error, setError] = useState("")

  useEffect(() => {
    if (!dataPath) return
    getJsonCacheStatus(dataPath)
      .then((data) => setCache(data))
      .catch(() => setCache(null))
  }, [dataPath])

  useEffect(() => {
    if (!building || !dataPath) return
    const id = setInterval(() => {
      getJsonCacheProgress(dataPath)
        .then((data) => { if (data.active) setProgress({ rows: data.rows || 0, elapsed: data.elapsed || 0 }) })
        .catch(() => { /* polling retry on next interval */ })
    }, 1000)
    return () => { clearInterval(id); setProgress(null) }
  }, [building, dataPath])

  const doBuild = () => {
    if (!dataPath) return
    setBuilding(true)
    setError("")
    buildJsonCache({ path: dataPath })
      .then((data) => {
        const info: JsonCacheStatus = { cached: true, ...data } as JsonCacheStatus
        setCache(info)
        setBuilding(false)
      })
      .catch((e: Error) => {
        setError(e instanceof ApiError ? e.detail || e.message : e.message)
        setBuilding(false)
      })
  }

  const formatBytes = (b: number) => {
    if (b < 1024) return `${b} B`
    if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
    return `${(b / (1024 * 1024)).toFixed(1)} MB`
  }

  const formatTime = (ts: number) => {
    if (!ts) return ""
    const d = new Date(ts * 1000)
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
  }

  return (
    <div>
      <button
        onClick={doBuild}
        disabled={!dataPath || building}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-colors disabled:opacity-40"
        style={{
          background: cache?.cached ? 'rgba(34,197,94,.1)' : 'var(--accent-soft)',
          border: cache?.cached ? '1px solid rgba(34,197,94,.3)' : '1px solid var(--accent)',
          color: cache?.cached ? '#22c55e' : 'var(--accent)',
        }}
      >
        {building ? (
          <><Loader2 size={14} className="animate-spin" /> {progress ? `${progress.rows.toLocaleString()} rows · ${progress.elapsed}s` : "Processing..."}</>
        ) : cache?.cached ? (
          <><HardDriveDownload size={14} /> Refresh Cache</>
        ) : (
          <><HardDriveDownload size={14} /> Cache as Parquet</>
        )}
      </button>

      {cache?.cached && (
        <div className="mt-1.5 flex items-center gap-2 text-[10px] px-1" style={{ color: 'var(--text-muted)' }}>
          <span>{cache.row_count.toLocaleString()} rows</span>
          <span>&middot;</span>
          <span>{cache.column_count} cols</span>
          <span>&middot;</span>
          <span>{formatBytes(cache.size_bytes)}</span>
          {cache.cached_at > 0 && (
            <><span>&middot;</span><span>{formatTime(cache.cached_at)}</span></>
          )}
          <span>&middot;</span>
          <button
            onClick={() => {
              deleteJsonCache(dataPath)
                .then((data) => setCache(data as JsonCacheStatus))
                .catch((e: Error) => setError(e instanceof ApiError ? e.detail || e.message : e.message))
            }}
            className="inline-flex items-center gap-0.5 hover:opacity-70 transition-opacity"
            style={{ color: '#ef4444' }}
            title="Delete cached data"
          >
            <Trash2 size={10} /> clear
          </button>
        </div>
      )}

      {!cache?.cached && dataPath && !building && (
        <div className="mt-1.5 text-[10px] px-1" style={{ color: '#f59e0b' }}>
          Not cached yet — click to flatten and cache as Parquet
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

// ─── ApiInputEditor ───────────────────────────────────────────────

export default function ApiInputEditor({
  config,
  onUpdate,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
}) {
  const currentPath = configField<string | undefined>(config, "path", undefined)
  const { schema, loading: loadingSchema, fetchForPath } = useSchemaFetch(currentPath)
  const showCacheButton = currentPath && (currentPath.endsWith(".json") || currentPath.endsWith(".jsonl"))

  return (
    <>
      <div className="px-4 py-3 space-y-3">
        <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs font-medium"
          style={{ background: 'rgba(34,197,94,.1)', border: '1px solid rgba(34,197,94,.3)', color: '#22c55e' }}
        >
          <Radio size={14} />
          <span>This node receives live API requests at deploy time</span>
        </div>

        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
            Preview Data
            <span className="ml-1.5 normal-case tracking-normal font-normal">.json or .jsonl</span>
          </label>
          <FileBrowser
            currentPath={currentPath}
            onSelect={(path) => {
              onUpdate("path", path)
              fetchForPath(path)
            }}
            extensions=".json,.jsonl"
          />
        </div>

        {showCacheButton && (
          <JsonCacheButton dataPath={currentPath} />
        )}

        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1 flex items-center gap-1.5" style={{ color: 'var(--text-muted)' }}>
            Row ID Column
            {!config.row_id_column && (
              <span className="inline-flex items-center gap-0.5 text-[10px] font-medium normal-case tracking-normal" style={{ color: '#f59e0b' }}>
                <AlertTriangle size={10} />
                Required for tracing
              </span>
            )}
          </label>
          <select
            value={configField(config, "row_id_column", "")}
            onChange={(e) => onUpdate("row_id_column", e.target.value || undefined)}
            className="w-full px-2.5 py-1.5 text-xs rounded-lg focus:outline-none focus:ring-2 appearance-none"
            style={{
              background: 'var(--bg-input)',
              border: config.row_id_column ? '1px solid var(--border)' : '1px solid rgba(245,158,11,.4)',
              color: 'var(--text-primary)',
            }}
            onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(59,130,246,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
            onBlur={(e) => { e.currentTarget.style.borderColor = config.row_id_column ? 'var(--border)' : 'rgba(245,158,11,.4)'; e.currentTarget.style.boxShadow = 'none' }}
          >
            <option value="">Select ID column...</option>
            {schema?.columns.map((col) => (
              <option key={col.name} value={col.name}>{col.name} ({col.dtype})</option>
            ))}
          </select>
          {Boolean(config.row_id_column) && (
            <div className="mt-1 text-[10px]" style={{ color: 'var(--text-muted)' }}>
              Traces will identify rows by <span className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{configField(config, "row_id_column", "")}</span>
            </div>
          )}
        </div>
      </div>

      {loadingSchema && (
        <div className="px-4 py-3" style={{ borderTop: '1px solid var(--border)' }}>
          <span className="text-xs" style={{ color: 'var(--text-muted)' }}>Loading schema...</span>
        </div>
      )}

      <SchemaPreview schema={schema} />
    </>
  )
}
