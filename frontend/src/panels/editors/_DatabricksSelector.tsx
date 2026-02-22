import { useState, useEffect, useRef } from "react"
import { Check, ChevronDown, Loader2, Trash2, HardDriveDownload } from "lucide-react"
import {
  getWarehouses,
  getCatalogs,
  getSchemas,
  getTables,
  getCacheStatus,
  getFetchProgress,
  fetchDatabricksData,
  deleteCache,
  ApiError,
} from "../../api/client"

// ─── WarehousePicker ──────────────────────────────────────────────

type Warehouse = {
  id: string
  name: string
  http_path: string
  state: string
  size: string
}

export function WarehousePicker({
  httpPath,
  onSelect,
}: {
  httpPath: string
  onSelect: (httpPath: string) => void
}) {
  const [warehouses, setWarehouses] = useState<Warehouse[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [open, setOpen] = useState(false)
  const fetched = useRef(false)

  const fetchWarehouses = () => {
    if (fetched.current) {
      setOpen(true)
      return
    }
    setLoading(true)
    setError(null)
    getWarehouses()
      .then((data) => {
        setWarehouses(data.warehouses || [])
        fetched.current = true
        setOpen(true)
        setLoading(false)
      })
      .catch((e: Error) => {
        setError(e instanceof ApiError ? e.detail || e.message : e.message)
        setLoading(false)
      })
  }

  const stateColor = (state: string) => {
    if (state === "RUNNING") return "#22c55e"
    if (state === "STOPPED" || state === "DELETED") return "#ef4444"
    return "#f59e0b"
  }

  return (
    <div>
      <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>SQL Warehouse</label>
      <div className="mt-1 flex gap-1.5">
        <input
          type="text"
          placeholder="/sql/1.0/warehouses/abc123"
          value={httpPath}
          onChange={(e) => onSelect(e.target.value)}
          className="flex-1 px-2.5 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
          style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
          onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(59,130,246,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
          onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
        />
        <button
          onClick={fetchWarehouses}
          disabled={loading}
          className="px-2 py-1.5 rounded-lg text-xs font-medium transition-colors flex items-center gap-1"
          style={{
            background: 'var(--bg-input)',
            border: '1px solid var(--border)',
            color: 'var(--text-secondary)',
          }}
          title="Fetch warehouses from Databricks"
        >
          {loading ? <Loader2 size={12} className="animate-spin" /> : <ChevronDown size={12} />}
          <span>{loading ? "" : "Browse"}</span>
        </button>
      </div>

      {error && (
        <div className="mt-1.5 text-[10px] px-2 py-1 rounded" style={{ background: 'rgba(239,68,68,.1)', color: '#ef4444' }}>
          {error}
        </div>
      )}

      {open && warehouses.length > 0 && (
        <div className="mt-1.5 rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)', background: 'var(--bg-input)' }}>
          {warehouses.map((wh) => (
            <button
              key={wh.id}
              onClick={() => {
                onSelect(wh.http_path)
                setOpen(false)
              }}
              className="w-full flex items-center gap-2 px-2.5 py-2 text-xs transition-colors hover:brightness-110"
              style={{
                background: httpPath === wh.http_path ? 'var(--accent-soft)' : 'transparent',
                borderBottom: '1px solid var(--border)',
                color: 'var(--text-primary)',
              }}
            >
              <span
                className="w-1.5 h-1.5 rounded-full flex-shrink-0"
                style={{ background: stateColor(wh.state) }}
                title={wh.state}
              />
              <span className="font-medium truncate">{wh.name}</span>
              {wh.size && (
                <span className="text-[10px] ml-auto flex-shrink-0" style={{ color: 'var(--text-muted)' }}>{wh.size}</span>
              )}
              {httpPath === wh.http_path && <Check size={12} style={{ color: 'var(--accent)' }} className="flex-shrink-0" />}
            </button>
          ))}
        </div>
      )}

      {open && warehouses.length === 0 && !loading && !error && (
        <div className="mt-1.5 text-[10px]" style={{ color: 'var(--text-muted)' }}>
          No SQL Warehouses found in this workspace
        </div>
      )}
    </div>
  )
}

// ─── CatalogTablePicker ───────────────────────────────────────────

type CatalogItem = { name: string; comment: string }
type SchemaItem = { name: string; comment: string }
type TableItem = { name: string; full_name: string; table_type: string; comment: string }

export function CatalogTablePicker({
  table,
  onSelect,
}: {
  table: string
  onSelect: (fullName: string) => void
}) {
  const parts = table ? table.split(".") : []
  const [catalog, setCatalog] = useState(parts[0] || "")
  const [dbSchema, setDbSchema] = useState(parts[1] || "")
  const [tableName, setTableName] = useState(parts[2] || "")

  const [catalogs, setCatalogs] = useState<CatalogItem[]>([])
  const [schemas, setSchemas] = useState<SchemaItem[]>([])
  const [tables, setTables] = useState<TableItem[]>([])

  const [loadingCatalogs, setLoadingCatalogs] = useState(false)
  const [loadingSchemas, setLoadingSchemas] = useState(false)
  const [loadingTables, setLoadingTables] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const errorMsg = (e: Error) => e instanceof ApiError ? e.detail || e.message : e.message

  const refreshCatalogs = () => {
    setLoadingCatalogs(true)
    setError(null)
    getCatalogs()
      .then((data) => { setCatalogs(data.catalogs || []); setLoadingCatalogs(false) })
      .catch((e: Error) => { setError(errorMsg(e)); setLoadingCatalogs(false) })
  }

  const refreshSchemas = (cat: string) => {
    setLoadingSchemas(true)
    setError(null)
    getSchemas(cat)
      .then((data) => { setSchemas(data.schemas || []); setLoadingSchemas(false) })
      .catch((e: Error) => { setError(errorMsg(e)); setLoadingSchemas(false) })
  }

  const refreshTables = (cat: string, sch: string) => {
    setLoadingTables(true)
    setError(null)
    getTables(cat, sch)
      .then((data) => { setTables(data.tables || []); setLoadingTables(false) })
      .catch((e: Error) => { setError(errorMsg(e)); setLoadingTables(false) })
  }

  const selectStyle = {
    background: 'var(--bg-input)',
    border: '1px solid var(--border)',
    color: 'var(--text-primary)',
  }

  return (
    <div>
      <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Table</label>

      <div className="mt-1 space-y-1.5">
        {/* Catalog */}
        <select
          value={catalog}
          onFocus={refreshCatalogs}
          onChange={(e) => {
            const cat = e.target.value
            setCatalog(cat)
            setDbSchema("")
            setTableName("")
            setSchemas([])
            setTables([])
            onSelect("")
            if (cat) refreshSchemas(cat)
          }}
          className="w-full px-2.5 py-1.5 text-xs rounded-lg focus:outline-none focus:ring-2"
          style={selectStyle}
        >
          <option value="">{loadingCatalogs ? "Loading..." : "Select catalog..."}</option>
          {catalog && catalogs.every((c) => c.name !== catalog) && (
            <option value={catalog}>{catalog}</option>
          )}
          {catalogs.map((c) => (
            <option key={c.name} value={c.name}>{c.name}{c.comment ? ` — ${c.comment}` : ""}</option>
          ))}
        </select>

        {/* Schema */}
        <select
          value={dbSchema}
          disabled={!catalog}
          onFocus={() => { if (catalog) refreshSchemas(catalog) }}
          onChange={(e) => {
            const sch = e.target.value
            setDbSchema(sch)
            setTableName("")
            setTables([])
            onSelect("")
            if (sch) refreshTables(catalog, sch)
          }}
          className="w-full px-2.5 py-1.5 text-xs rounded-lg focus:outline-none focus:ring-2 disabled:opacity-40"
          style={selectStyle}
        >
          <option value="">{loadingSchemas ? "Loading..." : catalog ? "Select schema..." : "Select catalog first"}</option>
          {dbSchema && schemas.every((s) => s.name !== dbSchema) && (
            <option value={dbSchema}>{dbSchema}</option>
          )}
          {schemas.map((s) => (
            <option key={s.name} value={s.name}>{s.name}{s.comment ? ` — ${s.comment}` : ""}</option>
          ))}
        </select>

        {/* Table */}
        <select
          value={tableName}
          disabled={!dbSchema}
          onFocus={() => { if (catalog && dbSchema) refreshTables(catalog, dbSchema) }}
          onChange={(e) => {
            const tbl = e.target.value
            setTableName(tbl)
            if (tbl) {
              onSelect(`${catalog}.${dbSchema}.${tbl}`)
            } else {
              onSelect("")
            }
          }}
          className="w-full px-2.5 py-1.5 text-xs rounded-lg focus:outline-none focus:ring-2 disabled:opacity-40"
          style={selectStyle}
        >
          <option value="">{loadingTables ? "Loading..." : dbSchema ? "Select table..." : "Select schema first"}</option>
          {tableName && tables.every((t) => t.name !== tableName) && (
            <option value={tableName}>{tableName}</option>
          )}
          {tables.map((t) => (
            <option key={t.name} value={t.name}>
              {t.name}{t.table_type ? ` (${t.table_type})` : ""}{t.comment ? ` — ${t.comment}` : ""}
            </option>
          ))}
        </select>
      </div>

      {table && (
        <div className="mt-1.5 text-[10px] font-mono px-1" style={{ color: 'var(--text-muted)' }}>
          {table}
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

// ─── DatabricksFetchButton ────────────────────────────────────────

type CacheStatus = {
  cached: boolean
  path?: string
  table: string
  row_count: number
  column_count: number
  size_bytes: number
  fetched_at: number
}

export function DatabricksFetchButton({
  table,
  httpPath,
  query,
  onFetched,
}: {
  table: string
  httpPath: string
  query: string
  onFetched?: (info: CacheStatus) => void
}) {
  const [cache, setCache] = useState<CacheStatus | null>(null)
  const [fetching, setFetching] = useState(false)
  const [progress, setProgress] = useState<{ rows: number; elapsed: number } | null>(null)
  const [error, setError] = useState("")

  useEffect(() => {
    if (!table) return
    getCacheStatus(table)
      .then((data) => {
        setCache(data)
        if (data.cached) onFetched?.(data)
      })
      .catch(() => setCache(null))
  }, [table])

  useEffect(() => {
    if (!fetching || !table) return
    const id = setInterval(() => {
      getFetchProgress(table)
        .then((data) => { if (data.active) setProgress({ rows: data.rows || 0, elapsed: data.elapsed || 0 }) })
        .catch(() => { /* polling retry on next interval */ })
    }, 1000)
    return () => { clearInterval(id); setProgress(null) }
  }, [fetching, table])

  const doFetch = () => {
    if (!table) return
    setFetching(true)
    setError("")
    fetchDatabricksData({
      table,
      http_path: httpPath || undefined,
      query: query || undefined,
    })
      .then((data) => {
        const info: CacheStatus = { cached: true, ...data } as CacheStatus
        setCache(info)
        setFetching(false)
        onFetched?.(info)
      })
      .catch((e: Error) => {
        setError(e instanceof ApiError ? e.detail || e.message : e.message)
        setFetching(false)
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
        onClick={doFetch}
        disabled={!table || fetching}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-colors disabled:opacity-40"
        style={{
          background: cache?.cached ? 'rgba(34,197,94,.1)' : 'var(--accent-soft)',
          border: cache?.cached ? '1px solid rgba(34,197,94,.3)' : '1px solid var(--accent)',
          color: cache?.cached ? '#22c55e' : 'var(--accent)',
        }}
      >
        {fetching ? (
          <><Loader2 size={14} className="animate-spin" /> {progress ? `${progress.rows.toLocaleString()} rows · ${progress.elapsed}s` : "Connecting..."}</>
        ) : cache?.cached ? (
          <><HardDriveDownload size={14} /> Refresh Data</>
        ) : (
          <><HardDriveDownload size={14} /> Fetch Data</>
        )}
      </button>

      {cache?.cached && (
        <div className="mt-1.5 flex items-center gap-2 text-[10px] px-1" style={{ color: 'var(--text-muted)' }}>
          <span>{cache.row_count.toLocaleString()} rows</span>
          <span>·</span>
          <span>{cache.column_count} cols</span>
          <span>·</span>
          <span>{formatBytes(cache.size_bytes)}</span>
          {cache.fetched_at > 0 && (
            <><span>·</span><span>{formatTime(cache.fetched_at)}</span></>
          )}
          <span>·</span>
          <button
            onClick={() => {
              deleteCache(table)
                .then((data) => setCache(data))
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

      {!cache?.cached && table && !fetching && (
        <div className="mt-1.5 text-[10px] px-1" style={{ color: '#f59e0b' }}>
          Not fetched yet — click to download from Databricks
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
