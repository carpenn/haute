import { useState, useEffect, useRef, useCallback, useMemo } from "react"
import { X, Folder, FileText, ChevronLeft, Check, Database, Table2, HardDriveDownload, Radio, AlertTriangle, Loader2, ChevronDown, Trash2, Package, Link2, ToggleLeft, SlidersHorizontal, Plus, Trash } from "lucide-react"
import { getDtypeColor } from "../utils/dtypeColors"
import { NODE_TYPES } from "../utils/nodeTypes"
import { sanitizeName } from "../utils/sanitizeName"
import ModellingConfig from "./ModellingConfig"

type FileItem = {
  name: string
  path: string
  type: "file" | "directory"
  size?: number
}

type SchemaColumn = {
  name: string
  dtype: string
}

type InputSource = {
  varName: string
  sourceLabel: string
  edgeId: string
}

type SchemaInfo = {
  path: string
  columns: SchemaColumn[]
  row_count: number
  column_count: number
  preview: Record<string, unknown>[]
} | null

export type SimpleNode = {
  id: string
  type?: string
  data: {
    label: string
    description: string
    nodeType: string
    config?: Record<string, unknown>
    [key: string]: unknown
  }
}

export type SimpleEdge = {
  id: string
  source: string
  target: string
}

type NodePanelProps = {
  node: SimpleNode | null
  edges: SimpleEdge[]
  allNodes: SimpleNode[]
  submodels?: Record<string, unknown>
  onClose: () => void
  onUpdateNode?: (id: string, data: Record<string, unknown>) => void
  onDeleteEdge?: (edgeId: string) => void
  onRefreshPreview?: () => void
}


function FileBrowser({ currentPath, onSelect, extensions }: { currentPath?: string; onSelect: (path: string) => void; extensions?: string }) {
  const [dir, setDir] = useState(".")
  const [items, setItems] = useState<FileItem[]>([])
  const [loading, setLoading] = useState(true)
  const [selectedPath, setSelectedPath] = useState<string | undefined>(currentPath)

  useEffect(() => {
    fetch(`/api/files?dir=${encodeURIComponent(dir)}${extensions ? `&extensions=${encodeURIComponent(extensions)}` : ``}`)
      .then((r) => r.json())
      .then((data) => {
        setItems(data.items || [])
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [dir, extensions])

  const goUp = () => {
    if (dir === ".") return
    const parts = dir.split("/")
    parts.pop()
    setLoading(true)
    setDir(parts.length > 0 ? parts.join("/") : ".")
  }

  const handleFileClick = (path: string) => {
    setSelectedPath(path)
    onSelect(path)
  }

  const formatSize = (bytes: number) => {
    if (bytes > 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
    return `${(bytes / 1024).toFixed(1)} KB`
  }

  return (
    <div>
      {selectedPath && (
        <div className="mb-2 px-2.5 py-2 rounded-lg flex items-center gap-2" style={{ background: 'rgba(34,197,94,.1)', border: '1px solid rgba(34,197,94,.2)' }}>
          <Check size={14} style={{ color: '#22c55e' }} className="shrink-0" />
          <span className="text-xs font-mono truncate" style={{ color: '#4ade80' }}>{selectedPath}</span>
        </div>
      )}

      <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)' }}>
        <div className="px-2 py-1.5 flex items-center gap-1.5" style={{ background: 'var(--bg-elevated)', borderBottom: '1px solid var(--border)' }}>
          <button
            onClick={goUp}
            disabled={dir === "."}
            className="p-0.5 rounded disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            style={{ color: 'var(--text-secondary)' }}
          >
            <ChevronLeft size={14} />
          </button>
          <span className="text-xs font-mono truncate" style={{ color: 'var(--text-muted)' }}>{dir === "." ? "/" : dir}</span>
        </div>

        <div className="max-h-40 overflow-y-auto" style={{ background: 'var(--bg-input)' }}>
          {loading ? (
            <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>Loading...</div>
          ) : items.length === 0 ? (
            <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>No matching files</div>
          ) : (
            items.map((item) => {
              const isSelected = item.type === "file" && item.path === selectedPath
              return (
                <button
                  key={item.path}
                  onClick={() => {
                    if (item.type === "directory") {
                      setLoading(true)
                      setDir(item.path)
                    } else {
                      handleFileClick(item.path)
                    }
                  }}
                  className="w-full px-3 py-2 flex items-center gap-2 text-left transition-colors"
                  style={{
                    borderBottom: '1px solid var(--border)',
                    background: isSelected ? 'var(--accent-soft)' : 'transparent',
                  }}
                  onMouseEnter={(e) => { if (!isSelected) e.currentTarget.style.background = 'var(--bg-hover)' }}
                  onMouseLeave={(e) => { if (!isSelected) e.currentTarget.style.background = 'transparent' }}
                >
                  {item.type === "directory" ? (
                    <Folder size={14} style={{ color: '#f59e0b' }} className="shrink-0" />
                  ) : isSelected ? (
                    <Check size={14} style={{ color: 'var(--accent)' }} className="shrink-0" />
                  ) : (
                    <FileText size={14} style={{ color: 'var(--text-muted)' }} className="shrink-0" />
                  )}
                  <span className="text-xs truncate" style={{ color: isSelected ? 'var(--accent)' : 'var(--text-secondary)', fontWeight: isSelected ? 500 : 400 }}>
                    {item.name}
                  </span>
                  {item.size !== undefined && (
                    <span className="text-[11px] ml-auto shrink-0" style={{ color: 'var(--text-muted)' }}>
                      {formatSize(item.size)}
                    </span>
                  )}
                </button>
              )
            })
          )}
        </div>
      </div>
    </div>
  )
}

function SchemaPreview({ schema }: { schema: SchemaInfo }) {
  const [showPreview, setShowPreview] = useState(false)

  if (!schema || !schema.columns) return null

  return (
    <div style={{ borderTop: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
      <div className="px-4 py-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Table2 size={14} style={{ color: 'var(--text-muted)' }} />
          <span className="text-xs font-semibold" style={{ color: 'var(--text-primary)' }}>Schema</span>
          <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
            {schema.column_count ?? 0} cols / {(schema.row_count ?? 0).toLocaleString()} rows
          </span>
        </div>
        <button
          onClick={() => setShowPreview(!showPreview)}
          className="text-[11px] font-medium" style={{ color: 'var(--accent)' }}
        >
          {showPreview ? "Hide preview" : "Show preview"}
        </button>
      </div>

      <div className="px-4 pb-3">
        <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)', background: 'var(--bg-input)' }}>
          <table className="w-full text-xs">
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
                <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Column</th>
                <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Type</th>
              </tr>
            </thead>
            <tbody>
              {schema.columns.map((col) => (
                <tr key={col.name} style={{ borderBottom: '1px solid var(--border)' }}>
                  <td className="px-2.5 py-1.5 font-mono" style={{ color: 'var(--text-primary)' }}>{col.name}</td>
                  <td className="px-2.5 py-1.5">
                    <span className={`text-[11px] font-medium ${getDtypeColor(col.dtype)}`}>
                      {col.dtype}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {showPreview && schema.preview.length > 0 && (
          <div className="mt-2 rounded-lg overflow-x-auto" style={{ border: '1px solid var(--border)', background: 'var(--bg-input)' }}>
            <table className="w-full text-[11px]">
              <thead>
                <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
                  {schema.columns.map((col) => (
                    <th key={col.name} className="text-left px-2 py-1 font-semibold whitespace-nowrap" style={{ color: 'var(--text-muted)' }}>
                      {col.name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {schema.preview.map((row, i) => (
                  <tr key={i} style={{ borderBottom: '1px solid var(--border)' }}>
                    {schema.columns.map((col) => (
                      <td key={col.name} className="px-2 py-1 font-mono whitespace-nowrap" style={{ color: 'var(--text-secondary)' }}>
                        {String(row[col.name] ?? "")}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

type Warehouse = {
  id: string
  name: string
  http_path: string
  state: string
  size: string
}

function WarehousePicker({
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
    fetch("/api/databricks/warehouses")
      .then((r) => {
        if (!r.ok) return r.json().then((d) => { throw new Error(d.detail || `HTTP ${r.status}`) })
        return r.json()
      })
      .then((data) => {
        setWarehouses(data.warehouses || [])
        fetched.current = true
        setOpen(true)
        setLoading(false)
      })
      .catch((e) => {
        setError(e.message)
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

type CatalogItem = { name: string; comment: string }
type SchemaItem = { name: string; comment: string }
type TableItem = { name: string; full_name: string; table_type: string; comment: string }

function CatalogTablePicker({
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

  const apiFetch = (url: string) =>
    fetch(url).then((r) => {
      if (!r.ok) return r.json().then((d) => { throw new Error(d.detail || `HTTP ${r.status}`) })
      return r.json()
    })

  const refreshCatalogs = () => {
    setLoadingCatalogs(true)
    setError(null)
    apiFetch("/api/databricks/catalogs")
      .then((data) => { setCatalogs(data.catalogs || []); setLoadingCatalogs(false) })
      .catch((e) => { setError(e.message); setLoadingCatalogs(false) })
  }

  const refreshSchemas = (cat: string) => {
    setLoadingSchemas(true)
    setError(null)
    apiFetch(`/api/databricks/schemas?catalog=${encodeURIComponent(cat)}`)
      .then((data) => { setSchemas(data.schemas || []); setLoadingSchemas(false) })
      .catch((e) => { setError(e.message); setLoadingSchemas(false) })
  }

  const refreshTables = (cat: string, sch: string) => {
    setLoadingTables(true)
    setError(null)
    apiFetch(`/api/databricks/tables?catalog=${encodeURIComponent(cat)}&schema=${encodeURIComponent(sch)}`)
      .then((data) => { setTables(data.tables || []); setLoadingTables(false) })
      .catch((e) => { setError(e.message); setLoadingTables(false) })
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

type CacheStatus = {
  cached: boolean
  path?: string
  table: string
  row_count: number
  column_count: number
  size_bytes: number
  fetched_at: number
}

function DatabricksFetchButton({
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
    fetch(`/api/databricks/cache?table=${encodeURIComponent(table)}`)
      .then((r) => r.json())
      .then((data) => {
        setCache(data)
        if (data.cached) onFetched?.(data)
      })
      .catch(() => setCache(null))
  }, [table])

  useEffect(() => {
    if (!fetching || !table) return
    const id = setInterval(() => {
      fetch(`/api/databricks/fetch/progress?table=${encodeURIComponent(table)}`)
        .then((r) => r.json())
        .then((data) => { if (data.active) setProgress({ rows: data.rows, elapsed: data.elapsed }) })
        .catch(() => { /* polling retry on next interval */ })
    }, 1000)
    return () => { clearInterval(id); setProgress(null) }
  }, [fetching, table])

  const doFetch = () => {
    if (!table) return
    setFetching(true)
    setError("")
    fetch("/api/databricks/fetch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        table,
        http_path: httpPath || undefined,
        query: query || undefined,
      }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => { throw new Error(d.detail || `HTTP ${r.status}`) })
        return r.json()
      })
      .then((data) => {
        const info: CacheStatus = { cached: true, ...data }
        setCache(info)
        setFetching(false)
        onFetched?.(info)
      })
      .catch((e) => { setError(e.message); setFetching(false) })
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
              fetch(`/api/databricks/cache?table=${encodeURIComponent(table)}`, { method: "DELETE" })
                .then((r) => {
                  if (!r.ok) return r.json().then((d) => { throw new Error(d.detail || `HTTP ${r.status}`) })
                  return r.json()
                })
                .then((data) => setCache(data))
                .catch((e) => setError(e.message))
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

function ApiInputConfig({
  config,
  onUpdate,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
}) {
  const [schema, setSchema] = useState<SchemaInfo>(null)
  const [loadingSchema, setLoadingSchema] = useState(!!config.path)

  const fetchSchema = (path: string) => {
    setLoadingSchema(true)
    fetch(`/api/schema?path=${encodeURIComponent(path)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data) => {
        setSchema(data)
        setLoadingSchema(false)
      })
      .catch(() => {
        setSchema(null)
        setLoadingSchema(false)
      })
  }

  useEffect(() => {
    if (!config.path) return
    fetch(`/api/schema?path=${encodeURIComponent(config.path as string)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data) => {
        setSchema(data)
        setLoadingSchema(false)
      })
      .catch(() => {
        setSchema(null)
        setLoadingSchema(false)
      })
  }, [config.path])

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
            currentPath={config.path as string | undefined}
            onSelect={(path) => {
              onUpdate("path", path)
              fetchSchema(path)
            }}
            extensions=".json,.jsonl"
          />
        </div>

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
            value={(config.row_id_column as string) || ""}
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
              Traces will identify rows by <span className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{config.row_id_column as string}</span>
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


function LiveSwitchConfig({
  config,
  onUpdate,
  inputSources,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  inputSources: InputSource[]
}) {
  const mode = (config.mode as string) || "live"
  const inputs = (config.inputs as string[]) || []
  const liveInput = inputs[0] || inputSources[0]?.varName || "live"

  const modeOptions: { value: string; label: string }[] = [
    { value: "live", label: `${liveInput} (live)` },
    ...inputSources.slice(1).map((s) => ({
      value: s.varName,
      label: s.sourceLabel,
    })),
  ]

  // If inputs list is populated from parser but inputSources doesn't match yet, use inputs
  if (modeOptions.length <= 1 && inputs.length > 1) {
    for (let i = 1; i < inputs.length; i++) {
      modeOptions.push({ value: inputs[i], label: inputs[i] })
    }
  }

  return (
    <div className="px-4 py-3 space-y-3">
      <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs font-medium"
        style={{ background: 'rgba(245,158,11,.1)', border: '1px solid rgba(245,158,11,.3)', color: '#f59e0b' }}
      >
        <ToggleLeft size={14} />
        <span>Routes live API or batch data into the pipeline</span>
      </div>

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1 block" style={{ color: 'var(--text-muted)' }}>
          Active Input
        </label>
        <select
          value={mode}
          onChange={(e) => onUpdate("mode", e.target.value)}
          className="w-full px-2.5 py-1.5 text-xs rounded-lg focus:outline-none focus:ring-2 appearance-none"
          style={{
            background: 'var(--bg-input)',
            border: '1px solid var(--border)',
            color: 'var(--text-primary)',
          }}
          onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(59,130,246,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
          onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
        >
          {modeOptions.map((opt) => (
            <option key={opt.value} value={opt.value}>{opt.label}</option>
          ))}
        </select>
        <div className="mt-1 text-[10px]" style={{ color: 'var(--text-muted)' }}>
          {mode === "live"
            ? "Using live API input — this is what runs in production"
            : `Using batch input "${mode}" — switch to live for deployment`
          }
        </div>
      </div>

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
          Connected Inputs ({inputSources.length})
        </label>
        <div className="space-y-1">
          {inputSources.map((src, i) => {
            const isActive = (i === 0 && mode === "live") || src.varName === mode
            return (
              <div
                key={src.varName}
                className="flex items-center gap-2 px-2 py-1.5 rounded-md text-xs"
                style={{
                  background: isActive ? 'rgba(245,158,11,.1)' : 'var(--bg-surface)',
                  border: isActive ? '1px solid rgba(245,158,11,.3)' : '1px solid var(--border)',
                }}
              >
                <span
                  className="w-1.5 h-1.5 rounded-full shrink-0"
                  style={{ background: isActive ? '#f59e0b' : 'var(--text-muted)' }}
                />
                <span className="font-mono truncate" style={{ color: 'var(--text-primary)' }}>
                  {src.sourceLabel}
                </span>
                {i === 0 && (
                  <span className="ml-auto text-[10px] font-medium px-1.5 py-0.5 rounded"
                    style={{ background: 'rgba(34,197,94,.15)', color: '#22c55e' }}
                  >
                    LIVE
                  </span>
                )}
                {isActive && (
                  <span className="ml-auto text-[10px] font-medium" style={{ color: '#f59e0b' }}>
                    active
                  </span>
                )}
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}


function DataSourceConfig({
  config,
  onUpdate,
  onRefreshPreview,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  onRefreshPreview?: () => void
}) {
  const [sourceType, setSourceType] = useState<string>((config.sourceType as string) || "flat_file")
  const [schema, setSchema] = useState<SchemaInfo>(null)
  const [loadingSchema, setLoadingSchema] = useState(!!config.path)

  const fetchSchema = (path: string) => {
    setLoadingSchema(true)
    fetch(`/api/schema?path=${encodeURIComponent(path)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data) => {
        setSchema(data)
        setLoadingSchema(false)
      })
      .catch(() => {
        setSchema(null)
        setLoadingSchema(false)
      })
  }

  useEffect(() => {
    if (!config.path) return
    fetch(`/api/schema?path=${encodeURIComponent(config.path as string)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data) => {
        setSchema(data)
        setLoadingSchema(false)
      })
      .catch(() => {
        setSchema(null)
        setLoadingSchema(false)
      })
  }, [config.path])

  return (
    <>
      <div className="px-4 py-3 space-y-3">
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Source Type</label>
          <div className="mt-1 flex gap-1.5">
            <button
              onClick={() => {
                setSourceType("flat_file")
                onUpdate("sourceType", "flat_file")
              }}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: sourceType === "flat_file" ? 'var(--accent-soft)' : 'var(--bg-input)',
                border: sourceType === "flat_file" ? '1px solid var(--accent)' : '1px solid var(--border)',
                color: sourceType === "flat_file" ? 'var(--accent)' : 'var(--text-secondary)',
              }}
            >
              <FileText size={12} />
              Flat File
            </button>
            <button
              onClick={() => {
                setSourceType("databricks")
                onUpdate("sourceType", "databricks")
              }}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: sourceType === "databricks" ? 'var(--accent-soft)' : 'var(--bg-input)',
                border: sourceType === "databricks" ? '1px solid var(--accent)' : '1px solid var(--border)',
                color: sourceType === "databricks" ? 'var(--accent)' : 'var(--text-secondary)',
              }}
            >
              <Database size={12} />
              Databricks
            </button>
          </div>
        </div>

        {sourceType === "flat_file" && (
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
              File
            </label>
            <FileBrowser
              currentPath={config.path as string | undefined}
              onSelect={(path) => {
                onUpdate("path", path)
                fetchSchema(path)
              }}
            />
          </div>
        )}

        {sourceType === "databricks" && (
          <div className="space-y-3">
            <WarehousePicker
              httpPath={(config.http_path as string) || ""}
              onSelect={(hp) => onUpdate("http_path", hp || undefined)}
            />
            <CatalogTablePicker
              table={(config.table as string) || ""}
              onSelect={(fullName) => onUpdate("table", fullName || undefined)}
            />
            <div>
              <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
                SQL Query
                <span className="ml-1.5 normal-case tracking-normal font-normal" style={{ color: 'var(--text-muted)' }}>(optional)</span>
              </label>
              <textarea
                placeholder={"SELECT *\nFROM catalog.schema.table\nWHERE status = 'active'"}
                defaultValue={(config.query as string) || "SELECT *"}
                onChange={(e) => onUpdate("query", e.target.value || undefined)}
                rows={3}
                className="mt-1 w-full px-2.5 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2 resize-y"
                style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(59,130,246,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
                onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
              />
              <div className="mt-1 text-[10px]" style={{ color: 'var(--text-muted)' }}>
                Combined with table above as: query FROM table
              </div>
            </div>
            <DatabricksFetchButton
              table={(config.table as string) || ""}
              httpPath={(config.http_path as string) || ""}
              query={(config.query as string) || ""}
              onFetched={() => {
                const tbl = (config.table as string) || ""
                if (tbl) {
                  setLoadingSchema(true)
                  fetch(`/api/schema/databricks?table=${encodeURIComponent(tbl)}`)
                    .then((r) => {
                      if (!r.ok) throw new Error(`HTTP ${r.status}`)
                      return r.json()
                    })
                    .then((data) => { setSchema(data); setLoadingSchema(false); onRefreshPreview?.() })
                    .catch(() => { setSchema(null); setLoadingSchema(false) })
                }
              }}
            />
          </div>
        )}
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

const PAIRS: Record<string, string> = { "(": ")", "[": "]", "{": "}", "'": "'", '"': '"' }
const CLOSE_CHARS = new Set([")", "]", "}"])

function CodeEditor({
  defaultValue,
  onChange,
  placeholder,
}: {
  defaultValue: string
  onChange: (value: string) => void
  placeholder?: string
}) {
  const [code, setCode] = useState(defaultValue)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const gutterRef = useRef<HTMLDivElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [focused, setFocused] = useState(false)

  const lineCount = Math.max((code || "").split("\n").length, 1)

  // Undo-safe text insertion - goes through the browser input pipeline
  // so Ctrl+Z / Ctrl+Shift+Z work natively.
  const insertText = useCallback((ta: HTMLTextAreaElement, text: string) => {
    ta.focus()
    document.execCommand("insertText", false, text)
    // Sync React state with the DOM value
    setCode(ta.value)
    onChange(ta.value)
  }, [onChange])

  // Replace a range and place cursor at `cursorPos`
  const replaceRange = useCallback((ta: HTMLTextAreaElement, start: number, end: number, text: string, cursorPos?: number) => {
    ta.focus()
    ta.setSelectionRange(start, end)
    document.execCommand("insertText", false, text)
    if (cursorPos !== undefined) {
      ta.setSelectionRange(cursorPos, cursorPos)
    }
    setCode(ta.value)
    onChange(ta.value)
  }, [onChange])

  // Replace a range and select the result
  const replaceRangeSelect = useCallback((ta: HTMLTextAreaElement, start: number, end: number, text: string, selStart: number, selEnd: number) => {
    ta.focus()
    ta.setSelectionRange(start, end)
    document.execCommand("insertText", false, text)
    ta.setSelectionRange(selStart, selEnd)
    setCode(ta.value)
    onChange(ta.value)
  }, [onChange])

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLTextAreaElement>) => {
      setCode(e.target.value)
      onChange(e.target.value)
    },
    [onChange],
  )

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      const ta = e.currentTarget
      const { selectionStart: start, selectionEnd: end, value: val } = ta
      const hasSelection = start !== end

      // --- Tab / Shift+Tab: multi-line indent/dedent ---
      if (e.key === "Tab") {
        e.preventDefault()
        if (hasSelection) {
          const lineStart = val.lastIndexOf("\n", start - 1) + 1
          const lineEnd = val.indexOf("\n", end - 1)
          const blockEnd = lineEnd === -1 ? val.length : lineEnd
          const block = val.substring(lineStart, blockEnd)
          const lines = block.split("\n")

          let newBlock: string
          if (e.shiftKey) {
            newBlock = lines.map((l) => l.startsWith("    ") ? l.slice(4) : l.replace(/^\t/, "")).join("\n")
          } else {
            newBlock = lines.map((l) => "    " + l).join("\n")
          }

          const delta = newBlock.length - block.length
          replaceRangeSelect(ta, lineStart, blockEnd, newBlock, lineStart, blockEnd + delta)
        } else {
          if (e.shiftKey) {
            const lineStart = val.lastIndexOf("\n", start - 1) + 1
            const lineText = val.substring(lineStart, start)
            if (lineText.endsWith("    ")) {
              replaceRange(ta, start - 4, start, "", start - 4)
            } else if (lineText.endsWith("\t")) {
              replaceRange(ta, start - 1, start, "", start - 1)
            }
          } else {
            insertText(ta, "    ")
          }
        }
        return
      }

      // --- Quote / bracket wrap selection or auto-close ---
      if (PAIRS[e.key]) {
        const open = e.key
        const close = PAIRS[e.key]
        if (hasSelection) {
          e.preventDefault()
          const selected = val.substring(start, end)
          const wrapped = open + selected + close
          replaceRangeSelect(ta, start, end, wrapped, start + 1, end + 1)
          return
        }
        // Auto-close pair (no selection)
        // For quotes, don't auto-close if the char before cursor is alphanumeric (mid-word)
        if (open === "'" || open === '"') {
          const charBefore = start > 0 ? val[start - 1] : ""
          if (/\w/.test(charBefore)) return // let browser handle normally
          // If cursor is right before the same closing quote, skip over it
          if (val[start] === open) {
            e.preventDefault()
            ta.setSelectionRange(start + 1, start + 1)
            return
          }
        }
        e.preventDefault()
        insertText(ta, open + close)
        ta.setSelectionRange(start + 1, start + 1)
        return
      }

      // --- Skip over closing bracket/quote if already there ---
      if (CLOSE_CHARS.has(e.key) && val[start] === e.key && !hasSelection) {
        e.preventDefault()
        ta.setSelectionRange(start + 1, start + 1)
        return
      }

      // --- Backspace: delete matching pair ---
      if (e.key === "Backspace" && !hasSelection && start > 0) {
        const before = val[start - 1]
        const after = val[start]
        if (PAIRS[before] && PAIRS[before] === after) {
          e.preventDefault()
          replaceRange(ta, start - 1, start + 1, "", start - 1)
          return
        }
      }

      // --- Enter: auto-indent + extra indent after colon ---
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault()
        const lineStart = val.lastIndexOf("\n", start - 1) + 1
        const currentLine = val.substring(lineStart, start)
        const indentMatch = currentLine.match(/^(\s*)/)
        let indent = indentMatch ? indentMatch[1] : ""
        const trimmedLine = currentLine.trimEnd()
        if (trimmedLine.endsWith(":")) {
          indent += "    "
        }
        insertText(ta, "\n" + indent)
        return
      }

      // --- Home: smart home (toggle between start-of-text and column 0) ---
      if (e.key === "Home" && !e.ctrlKey && !e.metaKey) {
        e.preventDefault()
        const lineStart = val.lastIndexOf("\n", start - 1) + 1
        const lineText = val.substring(lineStart)
        const textStart = lineStart + (lineText.match(/^\s*/)?.[0].length ?? 0)
        const target = start === textStart ? lineStart : textStart
        if (e.shiftKey) {
          ta.setSelectionRange(target, end)
        } else {
          ta.setSelectionRange(target, target)
        }
        return
      }

      // --- Ctrl+D: duplicate line or selection ---
      if (e.key === "d" && (e.ctrlKey || e.metaKey)) {
        e.preventDefault()
        if (hasSelection) {
          const selected = val.substring(start, end)
          replaceRangeSelect(ta, end, end, selected, end, end + selected.length)
        } else {
          const lineStart = val.lastIndexOf("\n", start - 1) + 1
          let lineEnd = val.indexOf("\n", start)
          if (lineEnd === -1) lineEnd = val.length
          const line = val.substring(lineStart, lineEnd)
          const offset = start - lineStart
          replaceRange(ta, lineEnd, lineEnd, "\n" + line, lineEnd + 1 + offset)
        }
        return
      }

      // --- Ctrl+/: toggle comment ---
      if (e.key === "/" && (e.ctrlKey || e.metaKey)) {
        e.preventDefault()
        const lineStart = val.lastIndexOf("\n", start - 1) + 1
        const lineEnd = hasSelection ? (val.indexOf("\n", end - 1) === -1 ? val.length : val.indexOf("\n", end - 1)) : (val.indexOf("\n", start) === -1 ? val.length : val.indexOf("\n", start))
        const block = val.substring(lineStart, lineEnd)
        const lines = block.split("\n")
        const allCommented = lines.every((l) => l.trimStart().startsWith("# ") || l.trim() === "")
        let newBlock: string
        if (allCommented) {
          newBlock = lines.map((l) => l.trim() === "" ? l : l.replace(/^(\s*)# /, "$1")).join("\n")
        } else {
          newBlock = lines.map((l) => l.trim() === "" ? l : l.replace(/^(\s*)/, "$1# ")).join("\n")
        }
        const delta = newBlock.length - block.length
        replaceRangeSelect(ta, lineStart, lineEnd, newBlock, lineStart, lineEnd + delta)
        return
      }

      // --- Ctrl+A: select all within editor ---
      if (e.key === "a" && (e.ctrlKey || e.metaKey)) {
        e.preventDefault()
        ta.setSelectionRange(0, val.length)
        return
      }
    },
    [insertText, replaceRange, replaceRangeSelect],
  )

  const handleScroll = useCallback(() => {
    if (textareaRef.current && gutterRef.current) {
      gutterRef.current.scrollTop = textareaRef.current.scrollTop
    }
  }, [])

  return (
    <div
      ref={containerRef}
      className="flex-1 min-h-[120px] rounded-lg overflow-hidden"
      style={{
        border: focused ? '1px solid rgba(59,130,246,.3)' : '1px solid var(--border)',
        boxShadow: focused ? '0 0 0 2px var(--accent-soft)' : 'none',
        background: 'var(--bg-input)',
      }}
    >
      <div className="flex h-full">
        <div
          ref={gutterRef}
          className="shrink-0 overflow-hidden select-none py-2.5"
          style={{
            background: 'var(--bg-elevated)',
            borderRight: '1px solid var(--border)',
            width: lineCount >= 100 ? 44 : 34,
          }}
        >
          {Array.from({ length: lineCount }, (_, i) => (
            <div
              key={i}
              className="text-right pr-2 font-mono"
              style={{
                color: 'var(--text-muted)',
                fontSize: '12px',
                lineHeight: '1.625',
                height: '19.5px',
              }}
            >
              {i + 1}
            </div>
          ))}
        </div>
        <textarea
          ref={textareaRef}
          defaultValue={defaultValue}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onScroll={handleScroll}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          spellCheck={false}
          placeholder={placeholder}
          className="flex-1 w-full h-full pl-2.5 pr-3 py-2.5 text-[12px] font-mono focus:outline-none resize-none"
          style={{
            background: 'transparent',
            color: '#a5f3fc',
            caretColor: 'var(--accent)',
            lineHeight: '1.625',
          }}
        />
      </div>
    </div>
  )
}

function InputSourcesBar({
  inputSources,
  onDeleteInput,
}: {
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
}) {
  if (inputSources.length === 0) return null
  return (
    <div className="rounded-lg px-3 py-1.5 shrink-0" style={{ background: 'var(--bg-input)', border: '1px solid var(--border)' }}>
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
          {inputSources.length > 1 ? "Inputs" : "Input"}
        </span>
        {inputSources.map((src) => (
          <span key={src.varName} className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded" style={{ background: 'var(--accent-soft)' }}>
            <code className="text-[11px] font-semibold" style={{ color: 'var(--accent)' }}>{src.varName}</code>
            {onDeleteInput && (
              <button
                onClick={() => onDeleteInput(src.edgeId)}
                className="p-0 rounded transition-colors"
                style={{ color: 'var(--text-muted)' }}
                onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}
                title={`Remove connection from ${src.sourceLabel}`}
              >
                <X size={10} />
              </button>
            )}
          </span>
        ))}
      </div>
    </div>
  )
}

type ContinuousRule = { op1: string; val1: string; op2: string; val2: string; assignment: string }
type CategoricalRule = { value: string; assignment: string }
type BandingFactor = {
  banding: string
  column: string
  outputColumn: string
  rules: (ContinuousRule | CategoricalRule)[]
  default?: string | null
}

function normaliseBandingFactors(config: Record<string, unknown>): BandingFactor[] {
  const raw = config.factors as BandingFactor[] | undefined
  if (Array.isArray(raw) && raw.length > 0) return raw
  return [{ banding: "continuous", column: "", outputColumn: "", rules: [], default: null }]
}

const EMPTY_CONTINUOUS: ContinuousRule = { op1: ">", val1: "", op2: "", val2: "", assignment: "" }
const EMPTY_CATEGORICAL: CategoricalRule = { value: "", assignment: "" }
const OPS = ["<", "<=", ">", ">=", "="]
const INPUT_STYLE = { background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }

function BandingRulesGrid({ factor, onUpdateFactor }: { factor: BandingFactor; onUpdateFactor: (patch: Partial<BandingFactor>) => void }) {
  const rules = factor.rules || []
  const bt = factor.banding || "continuous"

  const setRules = (r: (ContinuousRule | CategoricalRule)[]) => onUpdateFactor({ rules: r })
  const updateRule = (idx: number, field: string, value: string) => {
    const next = [...rules]; next[idx] = { ...next[idx], [field]: value }; setRules(next)
  }
  const removeRule = (idx: number) => setRules(rules.filter((_, i) => i !== idx))

  return (
    <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)', background: 'var(--bg-input)' }}>
      {bt === "continuous" ? (
        <table className="w-full text-[11px]">
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 52 }}>Op</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 60 }}>Value</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 52, opacity: 0.55 }}>Op</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 60, opacity: 0.55 }}>Value</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Band</th>
              <th style={{ width: 28 }}></th>
            </tr>
          </thead>
          <tbody>
            {rules.length === 0 ? (
              <tr><td colSpan={6} className="px-2 py-3 text-center" style={{ color: 'var(--text-muted)' }}>No rules yet</td></tr>
            ) : (rules as ContinuousRule[]).map((rule, i) => (
              <tr key={i} style={{ borderBottom: '1px solid var(--border)' }}>
                <td className="px-1 py-1">
                  <select value={rule.op1 || ""} onChange={(e) => updateRule(i, "op1", e.target.value)}
                    className="w-full px-1 py-1 rounded text-[11px] font-mono appearance-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}>
                    <option value="">—</option>
                    {OPS.map((o) => <option key={o} value={o}>{o}</option>)}
                  </select>
                </td>
                <td className="px-1 py-1">
                  <input type="text" value={rule.val1 ?? ""} onChange={(e) => updateRule(i, "val1", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }} placeholder="0" />
                </td>
                <td className="px-1 py-1">
                  <select value={rule.op2 || ""} onChange={(e) => updateRule(i, "op2", e.target.value)}
                    className="w-full px-1 py-1 rounded text-[11px] font-mono appearance-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}>
                    <option value="">—</option>
                    {OPS.map((o) => <option key={o} value={o}>{o}</option>)}
                  </select>
                </td>
                <td className="px-1 py-1">
                  <input type="text" value={rule.val2 ?? ""} onChange={(e) => updateRule(i, "val2", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }} placeholder="" />
                </td>
                <td className="px-1 py-1">
                  <input type="text" value={rule.assignment ?? ""} onChange={(e) => updateRule(i, "assignment", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono font-semibold focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: '#14b8a6' }} placeholder="band" />
                </td>
                <td className="px-1 py-1 text-center">
                  <button onClick={() => removeRule(i)} className="p-0.5 rounded transition-colors" style={{ color: 'var(--text-muted)' }}
                    onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                    onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}><Trash size={11} /></button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <table className="w-full text-[11px]">
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Value</th>
              <th className="text-left px-2 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Group</th>
              <th style={{ width: 28 }}></th>
            </tr>
          </thead>
          <tbody>
            {rules.length === 0 ? (
              <tr><td colSpan={3} className="px-2 py-3 text-center" style={{ color: 'var(--text-muted)' }}>No rules yet</td></tr>
            ) : (rules as CategoricalRule[]).map((rule, i) => (
              <tr key={i} style={{ borderBottom: '1px solid var(--border)' }}>
                <td className="px-1 py-1">
                  <input type="text" value={rule.value ?? ""} onChange={(e) => updateRule(i, "value", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-primary)' }} placeholder="Semi-detached House" />
                </td>
                <td className="px-1 py-1">
                  <input type="text" value={rule.assignment ?? ""} onChange={(e) => updateRule(i, "assignment", e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono font-semibold focus:outline-none"
                    style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: '#14b8a6' }} placeholder="House" />
                </td>
                <td className="px-1 py-1 text-center">
                  <button onClick={() => removeRule(i)} className="p-0.5 rounded transition-colors" style={{ color: 'var(--text-muted)' }}
                    onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                    onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}><Trash size={11} /></button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function BandingConfig({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
  upstreamColumns = [],
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  upstreamColumns?: { name: string; dtype: string }[]
}) {
  const factors = normaliseBandingFactors(config)
  const [activeIdx, setActiveIdx] = useState(0)
  const safeIdx = Math.min(activeIdx, factors.length - 1)
  const factor = factors[safeIdx]

  const colMap = Object.fromEntries(upstreamColumns.map(c => [c.name, c.dtype]))

  const isNumericDtype = (dtype: string) => {
    const d = dtype.toLowerCase()
    return d.startsWith("int") || d.startsWith("uint") || d.startsWith("float") || d === "f32" || d === "f64" || d === "i8" || d === "i16" || d === "i32" || d === "i64" || d === "u8" || d === "u16" || d === "u32" || d === "u64"
  }

  const inferBandingType = (colName: string): string | null => {
    const dtype = colMap[colName]
    if (!dtype) return null
    return isNumericDtype(dtype) ? "continuous" : "categorical"
  }

  const commitFactors = (next: BandingFactor[]) => {
    onUpdate("factors", next)
  }

  const updateFactor = (idx: number, patch: Partial<BandingFactor>) => {
    const next = factors.map((f, i) => i === idx ? { ...f, ...patch } : f)
    commitFactors(next)
  }

  const setColumnWithAutoDetect = (idx: number, colName: string) => {
    const patch: Partial<BandingFactor> = { column: colName }
    const detected = inferBandingType(colName)
    if (detected && detected !== factors[idx].banding) {
      patch.banding = detected
      patch.rules = []
    }
    updateFactor(idx, patch)
  }

  const addFactor = () => {
    const next = [...factors, { banding: "continuous" as const, column: "", outputColumn: "", rules: [] as (ContinuousRule | CategoricalRule)[], default: null }]
    commitFactors(next)
    setActiveIdx(next.length - 1)
  }

  const removeFactor = (idx: number) => {
    if (factors.length <= 1) return
    const next = factors.filter((_, i) => i !== idx)
    commitFactors(next)
    if (safeIdx >= next.length) setActiveIdx(next.length - 1)
  }

  const tabLabel = (f: BandingFactor, i: number) => {
    if (f.outputColumn) return f.outputColumn
    if (f.column) return f.column
    return `Factor ${i + 1}`
  }

  return (
    <div className="px-4 py-3 space-y-3 overflow-y-auto">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs font-medium"
        style={{ background: 'rgba(20,184,166,.1)', border: '1px solid rgba(20,184,166,.3)', color: '#14b8a6' }}>
        <SlidersHorizontal size={14} />
        <span>Group values into bands — {factors.length} factor{factors.length !== 1 ? 's' : ''}</span>
      </div>

      {/* Factor tabs */}
      <div>
        <div className="flex items-center gap-1 flex-wrap">
          {factors.map((f, i) => (
            <button
              key={i}
              onClick={() => setActiveIdx(i)}
              className="relative flex items-center gap-1 px-2.5 py-1.5 rounded-t-lg text-[11px] font-medium transition-colors"
              style={{
                background: i === safeIdx ? 'var(--bg-input)' : 'transparent',
                border: i === safeIdx ? '1px solid var(--border)' : '1px solid transparent',
                borderBottom: i === safeIdx ? '1px solid var(--bg-input)' : '1px solid var(--border)',
                color: i === safeIdx ? '#14b8a6' : 'var(--text-muted)',
              }}
            >
              <span className="font-mono truncate max-w-[100px]">{tabLabel(f, i)}</span>
              {factors.length > 1 && (
                <span
                  onClick={(e) => { e.stopPropagation(); removeFactor(i) }}
                  className="ml-0.5 p-0.5 rounded transition-colors cursor-pointer"
                  style={{ color: 'var(--text-muted)' }}
                  onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                  onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}
                >
                  <X size={9} />
                </span>
              )}
            </button>
          ))}
          <button
            onClick={addFactor}
            className="flex items-center gap-0.5 px-2 py-1.5 rounded-lg text-[11px] font-medium transition-colors"
            style={{ color: '#14b8a6' }}
            onMouseEnter={(e) => { e.currentTarget.style.background = 'rgba(20,184,166,.1)' }}
            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent' }}
          >
            <Plus size={11} />
          </button>
        </div>
        <div style={{ borderTop: '1px solid var(--border)', marginTop: -1 }} />
      </div>

      {/* Active factor config */}
      <div>
        <div className="flex items-center gap-1.5">
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Type</label>
          {factor.column && colMap[factor.column] && (
            <span className="text-[10px] font-medium" style={{ color: 'var(--text-muted)', opacity: 0.7 }}>
              auto: {colMap[factor.column]}
            </span>
          )}
        </div>
        <div className="mt-1 flex gap-1.5">
          {(["continuous", "categorical"] as const).map((bt) => (
            <button
              key={bt}
              onClick={() => updateFactor(safeIdx, { banding: bt, rules: [] })}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: factor.banding === bt ? 'rgba(20,184,166,.1)' : 'var(--bg-input)',
                border: factor.banding === bt ? '1px solid #14b8a6' : '1px solid var(--border)',
                color: factor.banding === bt ? '#14b8a6' : 'var(--text-secondary)',
              }}
            >
              {bt.charAt(0).toUpperCase() + bt.slice(1)}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Input Column</label>
          {upstreamColumns.length > 0 ? (
            <select
              key={`col-${safeIdx}`}
              value={factor.column}
              onChange={(e) => setColumnWithAutoDetect(safeIdx, e.target.value)}
              className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
              style={INPUT_STYLE}
            >
              <option value="">Select column...</option>
              {upstreamColumns.map(c => (
                <option key={c.name} value={c.name}>
                  {c.name} ({c.dtype})
                </option>
              ))}
            </select>
          ) : (
            <input
              key={`col-${safeIdx}`}
              type="text" placeholder="driver_age" defaultValue={factor.column}
              onChange={(e) => updateFactor(safeIdx, { column: e.target.value })}
              className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
              style={INPUT_STYLE} />
          )}
        </div>
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Output Column</label>
          <input
            key={`out-${safeIdx}`}
            type="text" placeholder="age_band" defaultValue={factor.outputColumn}
            onChange={(e) => updateFactor(safeIdx, { outputColumn: e.target.value })}
            className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
            style={INPUT_STYLE} />
        </div>
      </div>

      {/* Rules grid + add button */}
      <div>
        <div className="flex items-center justify-between mb-1.5">
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
            Rules ({(factor.rules || []).length})
          </label>
          <button
            onClick={() => {
              const empty = factor.banding === "continuous" ? { ...EMPTY_CONTINUOUS } : { ...EMPTY_CATEGORICAL }
              updateFactor(safeIdx, { rules: [...(factor.rules || []), empty] })
            }}
            className="flex items-center gap-1 px-2 py-1 rounded-md text-[11px] font-medium transition-colors"
            style={{ background: 'rgba(20,184,166,.1)', color: '#14b8a6', border: '1px solid rgba(20,184,166,.3)' }}
            onMouseEnter={(e) => { e.currentTarget.style.background = 'rgba(20,184,166,.2)' }}
            onMouseLeave={(e) => { e.currentTarget.style.background = 'rgba(20,184,166,.1)' }}
          >
            <Plus size={11} /> Add
          </button>
        </div>
        <BandingRulesGrid
          key={safeIdx}
          factor={factor}
          onUpdateFactor={(patch) => updateFactor(safeIdx, patch)}
        />
      </div>

      {/* Default value */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>
          Default <span className="ml-1.5 normal-case tracking-normal font-normal">(unmatched rows)</span>
        </label>
        <input
          key={`def-${safeIdx}`}
          type="text" placeholder="null" defaultValue={factor.default || ""}
          onChange={(e) => updateFactor(safeIdx, { default: e.target.value || null })}
          className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
          style={INPUT_STYLE} />
      </div>

      {/* Summary across all factors */}
      {factors.some(f => f.column && f.outputColumn && (f.rules || []).length > 0) && (
        <div className="rounded-lg px-3 py-2 space-y-1" style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border)' }}>
          {factors.map((f, i) => {
            if (!f.column || !f.outputColumn || !(f.rules || []).length) return null
            return (
              <div key={i} className="text-[10px] leading-relaxed" style={{ color: 'var(--text-muted)' }}>
                <span className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{f.column}</span>
                {' → '}
                <span className="font-mono font-medium" style={{ color: '#14b8a6' }}>{f.outputColumn}</span>
                {' · '}{f.rules.length} rule{f.rules.length !== 1 ? 's' : ''}
                {' · '}{f.banding}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ─── Rating Step ──────────────────────────────────────────────────
type RatingTable = {
  name: string
  factors: string[]
  outputColumn: string
  defaultValue: string | null
  entries: Record<string, string | number>[]
}

function normaliseRatingTables(config: Record<string, unknown>): RatingTable[] {
  const raw = config.tables as RatingTable[] | undefined
  if (Array.isArray(raw) && raw.length > 0) return raw
  return [{ name: "Table 1", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }]
}

function extractBandingLevels(allNodes: SimpleNode[]): Record<string, string[]> {
  const levelSets: Record<string, Set<string>> = {}
  for (const n of allNodes) {
    if (n.data.nodeType !== NODE_TYPES.BANDING) continue
    const cfg = (n.data.config || {}) as Record<string, unknown>
    const factors = cfg.factors as BandingFactor[] | undefined
    if (!Array.isArray(factors)) continue
    for (const f of factors) {
      if (!f.outputColumn) continue
      if (!levelSets[f.outputColumn]) levelSets[f.outputColumn] = new Set()
      for (const r of f.rules || []) {
        const a = (r as Record<string, string>).assignment
        if (a) levelSets[f.outputColumn].add(a)
      }
    }
  }
  const levels: Record<string, string[]> = {}
  for (const [col, s] of Object.entries(levelSets)) {
    if (s.size > 0) levels[col] = [...s]
  }
  return levels
}

/** Heatmap color for actuarial relativity values.
 *  Surcharges (>1) = warm orange/red, discounts (<1) = cool blue, 1.0 = neutral. */
function relativityColor(value: number): string {
  if (isNaN(value)) return 'transparent'
  const dev = value - 1.0
  const t = Math.min(Math.abs(dev) / 0.5, 1)           // saturates at ±0.5
  if (dev > 0.005)  return `rgba(239, 68, 68, ${(t * 0.22).toFixed(3)})`   // warm
  if (dev < -0.005) return `rgba(59, 130, 246, ${(t * 0.22).toFixed(3)})`  // cool
  return 'transparent'
}

function relativityTextColor(value: number): string {
  if (isNaN(value)) return 'var(--text-secondary)'
  const dev = value - 1.0
  if (dev > 0.005) return '#dc2626'
  if (dev < -0.005) return '#2563eb'
  return '#10b981'                                       // neutral green
}

function tableStats(entries: Record<string, string | number>[]): { min: number; max: number; avg: number; count: number } | null {
  const vals = entries.map(e => typeof e.value === 'number' ? e.value : parseFloat(String(e.value ?? ''))).filter(v => !isNaN(v))
  if (vals.length === 0) return null
  const min = Math.min(...vals)
  const max = Math.max(...vals)
  const avg = vals.reduce((s, v) => s + v, 0) / vals.length
  return { min, max, avg, count: vals.length }
}

function buildCartesianEntries(
  factors: string[],
  bandingLevels: Record<string, string[]>,
  existing: Record<string, string | number>[],
  defaultValue: string | null,
): Record<string, string | number>[] {
  if (factors.length === 0) return []
  const levelArrays = factors.map(f => bandingLevels[f] || [])
  if (levelArrays.some(a => a.length === 0)) return existing

  const existingLookup = new Map<string, number>()
  for (const e of existing) {
    const key = factors.map(f => String(e[f] ?? "")).join("|")
    const v = e.value
    if (v !== undefined && v !== null && v !== "") {
      existingLookup.set(key, typeof v === "number" ? v : parseFloat(String(v)))
    }
  }

  const defVal = defaultValue != null && String(defaultValue).trim() ? parseFloat(String(defaultValue)) : 1.0
  const entries: Record<string, string | number>[] = []

  function recurse(depth: number, current: Record<string, string>) {
    if (depth === factors.length) {
      const key = factors.map(f => current[f]).join("|")
      entries.push({ ...current, value: existingLookup.get(key) ?? defVal })
      return
    }
    for (const level of levelArrays[depth]) {
      recurse(depth + 1, { ...current, [factors[depth]]: level })
    }
  }
  recurse(0, {})
  return entries
}

function StatsFooter({ stats }: { stats: { min: number; max: number; avg: number; count: number } | null }) {
  if (!stats) return null
  return (
    <div className="flex items-center gap-3 px-2.5 py-1.5 text-[10px] font-mono rounded-b-lg"
      style={{ background: 'var(--bg-elevated)', borderTop: '1px solid var(--border)', color: 'var(--text-muted)' }}>
      <span>n={stats.count}</span>
      <span style={{ color: '#2563eb' }}>min {stats.min.toFixed(3)}</span>
      <span style={{ color: 'var(--text-secondary)' }}>avg {stats.avg.toFixed(3)}</span>
      <span style={{ color: '#dc2626' }}>max {stats.max.toFixed(3)}</span>
    </div>
  )
}

function OneWayEditor({ table, bandingLevels, onUpdateEntries }: {
  table: RatingTable
  bandingLevels: Record<string, string[]>
  onUpdateEntries: (entries: Record<string, string | number>[]) => void
}) {
  const factor = table.factors[0]
  const entries = useMemo(() => table.entries || [], [table.entries])
  const stats = useMemo(() => tableStats(entries), [entries])

  if (!factor) return null
  const levels = bandingLevels[factor] || []

  const lookup = new Map<string, number>()
  for (const e of entries) {
    const k = String(e[factor] ?? "")
    if (k) lookup.set(k, typeof e.value === "number" ? e.value : parseFloat(String(e.value ?? "1")))
  }
  const maxVal = stats ? Math.max(Math.abs(stats.max), Math.abs(stats.min), 1) : 1

  const updateCell = (level: string, val: string) => {
    const num = val === "" ? 0 : parseFloat(val)
    const next = entries.map(e => String(e[factor]) === level ? { ...e, value: isNaN(num) ? 0 : num } : e)
    if (!next.some(e => String(e[factor]) === level)) {
      next.push({ [factor]: level, value: isNaN(num) ? 0 : num })
    }
    onUpdateEntries(next)
  }

  return (
    <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)' }}>
      <table className="w-full text-[11px]" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
        <thead>
          <tr style={{ background: 'var(--bg-elevated)' }}>
            <th className="text-left px-2.5 py-2 font-bold uppercase tracking-[0.06em] text-[10px]"
              style={{ color: 'var(--text-muted)', borderBottom: '2px solid var(--border)' }}>{factor}</th>
            <th className="text-center px-2 py-2 font-bold uppercase tracking-[0.06em] text-[10px]"
              style={{ color: 'var(--text-muted)', borderBottom: '2px solid var(--border)', width: 80 }}>Relativity</th>
            <th className="px-2 py-2 text-[10px]"
              style={{ color: 'var(--text-muted)', borderBottom: '2px solid var(--border)', width: '40%' }}></th>
          </tr>
        </thead>
        <tbody>
          {levels.length === 0 ? (
            <tr><td colSpan={3} className="px-2 py-4 text-center" style={{ color: 'var(--text-muted)' }}>No banding levels found</td></tr>
          ) : levels.map((level, ri) => {
            const val = lookup.get(level) ?? 1
            const barWidth = Math.min((Math.abs(val) / maxVal) * 100, 100)
            return (
              <tr key={level} style={{
                borderBottom: '1px solid var(--border)',
                background: ri % 2 === 0 ? 'var(--bg-input)' : 'var(--bg-surface)',
              }}>
                <td className="px-2.5 py-1.5 font-mono text-[11px] font-medium"
                  style={{ color: 'var(--text-primary)', borderBottom: '1px solid var(--border)' }}>{level}</td>
                <td className="px-0.5 py-0.5" style={{ borderBottom: '1px solid var(--border)' }}>
                  <input type="number" step="0.01"
                    defaultValue={val}
                    onBlur={(e) => updateCell(level, e.target.value)}
                    className="w-full px-1.5 py-1 rounded text-[11px] font-mono text-center focus:outline-none focus:ring-1 focus:ring-emerald-500/40"
                    style={{
                      background: relativityColor(val),
                      border: '1px solid var(--border)',
                      color: relativityTextColor(val),
                      fontWeight: 600,
                    }} />
                </td>
                <td className="px-2 py-1.5" style={{ borderBottom: '1px solid var(--border)' }}>
                  <div className="relative h-3 rounded-full overflow-hidden" style={{ background: 'var(--bg-elevated)' }}>
                    <div className="absolute inset-y-0 left-0 rounded-full transition-all"
                      style={{
                        width: `${barWidth}%`,
                        background: val >= 1 ? 'rgba(239, 68, 68, 0.35)' : 'rgba(59, 130, 246, 0.35)',
                      }} />
                  </div>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
      <StatsFooter stats={stats} />
    </div>
  )
}

function TwoWayGrid({ table, bandingLevels, onUpdateEntries, factorOverrides }: {
  table: RatingTable
  bandingLevels: Record<string, string[]>
  onUpdateEntries: (entries: Record<string, string | number>[]) => void
  factorOverrides?: { factors: string[]; sliceKey?: Record<string, string> }
}) {
  const usedFactors = factorOverrides?.factors || table.factors.slice(0, 2)
  const sliceKey = factorOverrides?.sliceKey || {}
  const rowFactor = usedFactors[0]
  const colFactor = usedFactors[1]
  const entries = useMemo(() => table.entries || [], [table.entries])
  const stats = useMemo(() => tableStats(entries), [entries])

  if (!rowFactor || !colFactor) return null

  const rowLabels = bandingLevels[rowFactor] || []
  const colLabels = bandingLevels[colFactor] || []

  const lookup = new Map<string, number>()
  for (const e of entries) {
    const matchSlice = Object.entries(sliceKey).every(([k, v]) => String(e[k]) === v)
    if (!matchSlice) continue
    const key = `${e[rowFactor]}|${e[colFactor]}`
    lookup.set(key, typeof e.value === "number" ? e.value : parseFloat(String(e.value ?? "1")))
  }

  const updateCell = (row: string, col: string, val: string) => {
    const num = val === "" ? 0 : parseFloat(val)
    const numVal = isNaN(num) ? 0 : num
    const matchRow = (e: Record<string, string | number>) =>
      String(e[rowFactor]) === row && String(e[colFactor]) === col &&
      Object.entries(sliceKey).every(([k, v]) => String(e[k]) === v)

    let found = false
    const next = entries.map(e => {
      if (matchRow(e)) { found = true; return { ...e, value: numVal } }
      return e
    })
    if (!found) {
      next.push({ ...sliceKey, [rowFactor]: row, [colFactor]: col, value: numVal })
    }
    onUpdateEntries(next)
  }

  if (rowLabels.length === 0 || colLabels.length === 0) {
    return <div className="px-2 py-3 text-center text-[11px]" style={{ color: 'var(--text-muted)' }}>No banding levels found for selected factors</div>
  }

  return (
    <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)' }}>
      <div className="overflow-x-auto">
        <table className="w-full text-[11px]" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
          <thead>
            <tr style={{ background: 'var(--bg-elevated)' }}>
              <th className="text-left px-2.5 py-2 font-bold uppercase tracking-[0.06em] text-[10px] sticky left-0 z-10"
                style={{ color: 'var(--text-muted)', borderBottom: '2px solid var(--border)', background: 'var(--bg-elevated)' }}>
                <span style={{ color: 'var(--text-secondary)' }}>{rowFactor}</span>
                <span style={{ color: 'var(--text-muted)', margin: '0 4px' }}>↓</span>
                <span style={{ color: 'var(--text-secondary)' }}>{colFactor}</span>
                <span style={{ color: 'var(--text-muted)', margin: '0 2px' }}>→</span>
              </th>
              {colLabels.map(col => (
                <th key={col} className="text-center px-1 py-2 font-bold font-mono text-[10px] uppercase tracking-[0.04em]"
                  style={{ color: 'var(--text-muted)', minWidth: 64, borderBottom: '2px solid var(--border)' }}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rowLabels.map((row, ri) => (
              <tr key={row} style={{ background: ri % 2 === 0 ? 'var(--bg-input)' : 'var(--bg-surface)' }}>
                <td className="px-2.5 py-1 font-mono text-[11px] font-medium sticky left-0 z-10"
                  style={{
                    color: 'var(--text-primary)',
                    whiteSpace: 'nowrap',
                    borderBottom: '1px solid var(--border)',
                    background: ri % 2 === 0 ? 'var(--bg-input)' : 'var(--bg-surface)',
                  }}>{row}</td>
                {colLabels.map(col => {
                  const val = lookup.get(`${row}|${col}`) ?? 1
                  return (
                    <td key={col} className="px-0.5 py-0.5" style={{ borderBottom: '1px solid var(--border)' }}>
                      <input type="number" step="0.01"
                        defaultValue={val}
                        onBlur={(e) => updateCell(row, col, e.target.value)}
                        className="w-full px-1 py-1 rounded text-[11px] font-mono text-center focus:outline-none focus:ring-1 focus:ring-emerald-500/40"
                        style={{
                          background: relativityColor(val),
                          border: '1px solid var(--border)',
                          color: relativityTextColor(val),
                          fontWeight: 600,
                          minWidth: 56,
                        }} />
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <StatsFooter stats={stats} />
    </div>
  )
}

function RatingStepConfig({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
  allNodes,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  allNodes: SimpleNode[]
}) {
  const [activeTab, setActiveTab] = useState(0)
  const [sliceIdx, setSliceIdx] = useState(0)
  const tables = normaliseRatingTables(config)
  const bandingLevels = extractBandingLevels(allNodes)
  const operation = (config.operation as string) || "multiply"
  const combinedColumn = (config.combinedColumn as string) || ""

  const availableColumns = Object.keys(bandingLevels)
  const safeIdx = Math.min(activeTab, tables.length - 1)
  const table = tables[safeIdx] || { name: "Table 1", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }

  const commitTables = (next: RatingTable[]) => onUpdate("tables", next)

  const updateTable = (idx: number, patch: Partial<RatingTable>) => {
    const next = tables.map((t, i) => i === idx ? { ...t, ...patch } : t)
    commitTables(next)
  }

  const setFactors = (idx: number, newFactors: string[]) => {
    const t = tables[idx]
    const rebuilt = buildCartesianEntries(newFactors, bandingLevels, t.entries, t.defaultValue)
    updateTable(idx, { factors: newFactors, entries: rebuilt })
  }

  const addFactor = (idx: number, col: string) => {
    const t = tables[idx]
    if (t.factors.length >= 3 || t.factors.includes(col)) return
    setFactors(idx, [...t.factors, col])
  }

  const removeFactor = (idx: number, factorIdx: number) => {
    const t = tables[idx]
    const next = t.factors.filter((_, i) => i !== factorIdx)
    setFactors(idx, next)
  }

  const onUpdateEntries = (idx: number, entries: Record<string, string | number>[]) => {
    updateTable(idx, { entries })
  }

  const addTable = () => {
    commitTables([...tables, { name: `Table ${tables.length + 1}`, factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }])
    setActiveTab(tables.length)
  }

  const removeTable = (idx: number) => {
    if (tables.length <= 1) return
    const next = tables.filter((_, i) => i !== idx)
    commitTables(next)
    setActiveTab(Math.min(activeTab, next.length - 1))
  }

  const rebuildCurrentEntries = () => {
    const t = tables[safeIdx]
    const rebuilt = buildCartesianEntries(t.factors, bandingLevels, t.entries, t.defaultValue)
    updateTable(safeIdx, { entries: rebuilt })
  }

  const factorCount = table.factors.length

  // For 3-way: factor[2] is the slice dimension
  const sliceFactor = factorCount === 3 ? table.factors[2] : null
  const sliceLevels = sliceFactor ? (bandingLevels[sliceFactor] || []) : []
  const safeSliceIdx = Math.min(sliceIdx, Math.max(0, sliceLevels.length - 1))

  return (
    <div className="px-4 py-3 space-y-3 overflow-y-auto">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs font-medium"
        style={{ background: 'rgba(16,185,129,.1)', border: '1px solid rgba(16,185,129,.3)', color: '#10b981' }}>
        <Table2 size={13} />
        <span>Rating Tables · {tables.length} table{tables.length !== 1 ? 's' : ''}</span>
      </div>

      {/* Combination controls — shown when 2+ tables exist */}
      {tables.length >= 2 && (
        <div className="space-y-2 p-2.5 rounded-lg" style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)' }}>
          <div className="flex items-center gap-2">
            <label className="text-[11px] font-bold uppercase tracking-[0.08em] shrink-0" style={{ color: 'var(--text-muted)' }}>Combine</label>
            <select value={operation}
              onChange={(e) => onUpdate("operation", e.target.value)}
              className="flex-1 px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none"
              style={INPUT_STYLE}>
              <option value="multiply">× Multiply (relativities)</option>
              <option value="add">+ Add (loadings)</option>
              <option value="min">↓ Min</option>
              <option value="max">↑ Max</option>
            </select>
          </div>
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Combined Output Column</label>
            <input type="text" defaultValue={combinedColumn}
              onBlur={(e) => onUpdate("combinedColumn", e.target.value)}
              className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
              style={INPUT_STYLE} placeholder="combined_factor" />
          </div>
          {/* Formula summary */}
          {tables.some(t => t.outputColumn) && (() => {
            const cols = tables.filter(t => t.outputColumn).map(t => t.outputColumn)
            const lhs = combinedColumn || '?'
            let formula = ''
            if (operation === 'multiply') formula = cols.join(' × ')
            else if (operation === 'add') formula = cols.join(' + ')
            else if (operation === 'min') formula = `min(${cols.join(', ')})`
            else if (operation === 'max') formula = `max(${cols.join(', ')})`
            return (
              <div className="text-[10px] font-mono px-2 py-1.5 rounded flex items-center gap-1.5"
                style={{ background: 'var(--bg-elevated)', color: 'var(--text-secondary)', border: '1px solid var(--border)' }}>
                <span style={{ color: '#10b981', fontWeight: 600 }}>{lhs}</span>
                <span style={{ color: 'var(--text-muted)' }}>=</span>
                <span>{formula}</span>
                {!combinedColumn && <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}> — name the output above</span>}
              </div>
            )
          })()}
        </div>
      )}

      {/* Tab bar */}
      <div className="flex items-center gap-1 overflow-x-auto pb-1">
        {tables.map((t, i) => {
          const tStats = tableStats(t.entries || [])
          return (
            <button key={i} onClick={() => { setActiveTab(i); setSliceIdx(0) }}
              className="group flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[11px] font-medium whitespace-nowrap transition-colors"
              style={{
                background: i === safeIdx ? 'rgba(16,185,129,.12)' : 'var(--bg-surface)',
                border: i === safeIdx ? '1px solid rgba(16,185,129,.4)' : '1px solid var(--border)',
                color: i === safeIdx ? '#10b981' : 'var(--text-secondary)',
              }}>
              {t.name || `Table ${i + 1}`}
              {tStats && (
                <span className="text-[9px] font-mono px-1 py-0.5 rounded"
                  style={{ background: i === safeIdx ? 'rgba(16,185,129,.15)' : 'var(--bg-elevated)', color: 'var(--text-muted)' }}>
                  {tStats.count}
                </span>
              )}
              {tables.length > 1 && (
                <span onClick={(e) => { e.stopPropagation(); removeTable(i) }}
                  className="ml-0.5 opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer"
                  style={{ color: 'var(--text-muted)' }}><X size={10} /></span>
              )}
            </button>
          )
        })}
        <button onClick={addTable}
          className="p-1.5 rounded-lg transition-colors" style={{ color: 'var(--text-muted)', border: '1px dashed var(--border)' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#10b981'; e.currentTarget.style.color = '#10b981' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.color = 'var(--text-muted)' }}>
          <Plus size={12} />
        </button>
      </div>

      {/* Table name */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Table Name</label>
        <input key={`name-${safeIdx}`} type="text" defaultValue={table.name}
          onBlur={(e) => updateTable(safeIdx, { name: e.target.value })}
          className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
          style={INPUT_STYLE} placeholder="Age Factor" />
      </div>

      {/* Factor selection */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>
          Factors ({factorCount}/3)
        </label>
        <div className="space-y-1.5">
          {table.factors.map((f, fi) => (
            <div key={fi} className="flex items-center gap-1.5">
              <span className="text-[10px] font-bold w-4 text-center" style={{ color: 'var(--text-muted)' }}>{fi + 1}</span>
              <select key={`fsel-${safeIdx}-${fi}`} value={f}
                onChange={(e) => {
                  const next = [...table.factors]
                  next[fi] = e.target.value
                  setFactors(safeIdx, next)
                }}
                className="flex-1 px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none"
                style={INPUT_STYLE}>
                <option value="">Select column...</option>
                {availableColumns.map(c => (
                  <option key={c} value={c}>{c} ({(bandingLevels[c] || []).length} levels)</option>
                ))}
              </select>
              <button onClick={() => removeFactor(safeIdx, fi)}
                className="p-1 rounded transition-colors" style={{ color: 'var(--text-muted)' }}
                onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}>
                <X size={11} />
              </button>
            </div>
          ))}
          {factorCount < 3 && (
            <select value="" onChange={(e) => { if (e.target.value) addFactor(safeIdx, e.target.value) }}
              className="w-full px-2 py-1.5 text-xs rounded-lg focus:outline-none"
              style={{ ...INPUT_STYLE, color: 'var(--text-muted)' }}>
              <option value="">+ Add factor...</option>
              {availableColumns.filter(c => !table.factors.includes(c)).map(c => (
                <option key={c} value={c}>{c} ({(bandingLevels[c] || []).length} levels)</option>
              ))}
            </select>
          )}
        </div>
      </div>

      {/* Output column + default */}
      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Output Column</label>
          <input key={`out-${safeIdx}`} type="text" defaultValue={table.outputColumn}
            onBlur={(e) => updateTable(safeIdx, { outputColumn: e.target.value })}
            className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
            style={INPUT_STYLE} placeholder="age_factor" />
        </div>
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1" style={{ color: 'var(--text-muted)' }}>Default</label>
          <input key={`def-${safeIdx}`} type="number" step="0.01" defaultValue={table.defaultValue ?? "1.0"}
            onBlur={(e) => updateTable(safeIdx, { defaultValue: e.target.value })}
            className="w-full px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
            style={INPUT_STYLE} placeholder="1.0" />
        </div>
      </div>

      {/* Rebuild button */}
      {factorCount > 0 && (
        <button onClick={rebuildCurrentEntries}
          className="w-full px-2 py-1.5 text-[11px] font-medium rounded-lg transition-colors"
          style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#10b981'; e.currentTarget.style.color = '#10b981' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.color = 'var(--text-secondary)' }}>
          ↻ Rebuild from banding levels
        </button>
      )}

      {/* Table editor */}
      {factorCount === 0 && (
        <div className="px-2 py-4 text-center text-[11px]" style={{ color: 'var(--text-muted)' }}>
          Select at least one factor to populate the rating table
        </div>
      )}
      {factorCount === 1 && (
        <OneWayEditor table={table} bandingLevels={bandingLevels}
          onUpdateEntries={(e) => onUpdateEntries(safeIdx, e)} />
      )}
      {factorCount === 2 && (
        <TwoWayGrid table={table} bandingLevels={bandingLevels}
          onUpdateEntries={(e) => onUpdateEntries(safeIdx, e)} />
      )}
      {factorCount === 3 && (
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
              {sliceFactor}
            </label>
            <select value={safeSliceIdx} onChange={(e) => setSliceIdx(Number(e.target.value))}
              className="flex-1 px-2 py-1.5 text-xs font-mono rounded-lg focus:outline-none"
              style={INPUT_STYLE}>
              {sliceLevels.map((level, i) => (
                <option key={level} value={i}>{level}</option>
              ))}
            </select>
          </div>
          {sliceLevels.length > 0 && (
            <TwoWayGrid table={table} bandingLevels={bandingLevels}
              onUpdateEntries={(e) => onUpdateEntries(safeIdx, e)}
              factorOverrides={{
                factors: [table.factors[0], table.factors[1]],
                sliceKey: { [table.factors[2]]: sliceLevels[safeSliceIdx] },
              }} />
          )}
        </div>
      )}

      {/* Summary */}
      {table.entries.length > 0 && (() => {
        const s = tableStats(table.entries)
        return (
          <div className="flex items-center justify-between text-[10px] font-mono px-1"
            style={{ color: 'var(--text-muted)' }}>
            <span>{table.outputColumn ? <span style={{ color: 'var(--text-secondary)' }}>{table.outputColumn}</span> : 'untitled'}</span>
            <span>{table.entries.length} entries{s ? ` · range ${s.min.toFixed(2)}–${s.max.toFixed(2)}` : ''}</span>
          </div>
        )
      })()}
    </div>
  )
}

function TransformConfig({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
}) {
  const defaultCode = (config.code as string) || ""
  const isMultiInput = inputSources.length > 1
  const hasInput = inputSources.length > 0

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-2">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />
      <div className="flex items-center justify-between shrink-0">
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
          Polars Code
        </label>
        <span className="text-[11px] font-medium" style={{ color: 'var(--text-muted)' }}>
          {hasInput ? "use input names" : <>start with <code className="px-0.5 rounded" style={{ background: 'var(--bg-hover)' }}>.</code> to chain</>}
        </span>
      </div>
      <CodeEditor
        defaultValue={defaultCode}
        onChange={(val) => onUpdate("code", val)}
        placeholder={
          isMultiInput
            ? `${inputSources[0].varName}.join(${inputSources[1]?.varName || "other"}, on="key", how="left")`
            : hasInput
              ? `${inputSources[0].varName}\n.with_columns(\n    age=pl.col("YOA") - pl.col("DOB")\n)\n.select("age", "NCD")`
              : `.with_columns(\n    age=pl.col("YOA") - pl.col("DOB")\n)\n.select("age", "NCD")`
        }
      />
    </div>
  )
}

function ExternalFileConfig({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
}) {
  const [fileType, setFileType] = useState<string>((config.fileType as string) || "pickle")
  const [modelClass, setModelClass] = useState<string>((config.modelClass as string) || "classifier")
  const defaultCode = (config.code as string) || ""
  const hasInput = inputSources.length > 0

  const firstInput = inputSources.length > 0 ? inputSources[0].varName : "df"
  const placeholders: Record<string, string> = {
    pickle: hasInput
      ? `df = ${firstInput}.with_columns(\n    prediction=pl.Series(obj.predict(${firstInput}.to_numpy()))\n)`
      : `# obj is the loaded pickle\ndf = pl.DataFrame({"result": [obj]})`,
    json: hasInput
      ? `df = ${firstInput}.with_columns(\n    lookup=${firstInput}["key"].map_elements(lambda k: obj.get(k))\n)`
      : `# obj is the loaded JSON dict/list\ndf = pl.DataFrame(obj)`,
    joblib: hasInput
      ? `df = ${firstInput}.with_columns(\n    prediction=pl.Series(obj.predict(${firstInput}.to_numpy()))\n)`
      : `# obj is the loaded joblib object\ndf = pl.DataFrame({"result": [str(obj)]})`,
    catboost: hasInput
      ? `X = ${firstInput}.select(obj.feature_names_).collect().to_numpy()\npreds = obj.predict(X)\ndf = ${firstInput}.select("id").with_columns(prediction=pl.Series(preds))`
      : `# obj is the loaded CatBoost model\ndf = pl.DataFrame({"prediction": obj.predict([[1, 2, 3]])})`,
  }

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-2">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>File Type</label>
        <div className="mt-1 flex gap-1.5">
          {["pickle", "json", "joblib", "catboost"].map((ft) => (
            <button
              key={ft}
              onClick={() => {
                setFileType(ft)
                onUpdate("fileType", ft)
              }}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: fileType === ft ? 'rgba(236,72,153,.1)' : 'var(--bg-input)',
                border: fileType === ft ? '1px solid #ec4899' : '1px solid var(--border)',
                color: fileType === ft ? '#ec4899' : 'var(--text-secondary)',
              }}
            >
              {ft.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      {fileType === "catboost" && (
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Model Type</label>
          <div className="mt-1 flex gap-1.5">
            {["classifier", "regressor"].map((mc) => (
              <button
                key={mc}
                onClick={() => {
                  setModelClass(mc)
                  onUpdate("modelClass", mc)
                }}
                className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
                style={{
                  background: modelClass === mc ? 'rgba(236,72,153,.1)' : 'var(--bg-input)',
                  border: modelClass === mc ? '1px solid #ec4899' : '1px solid var(--border)',
                  color: modelClass === mc ? '#ec4899' : 'var(--text-secondary)',
                }}
              >
                {mc.charAt(0).toUpperCase() + mc.slice(1)}
              </button>
            ))}
          </div>
        </div>
      )}

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
          File Path
        </label>
        <FileBrowser
          currentPath={(config.path as string) || undefined}
          onSelect={(path) => onUpdate("path", path)}
          extensions=".pkl,.pickle,.json,.joblib,.cbm,.onnx,.pmml"
        />
      </div>

      <div className="flex items-center justify-between shrink-0">
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
          Code
        </label>
        <span className="text-[11px] font-medium" style={{ color: 'var(--text-muted)' }}>
          <code className="px-0.5 rounded" style={{ background: 'var(--bg-hover)' }}>obj</code> = loaded file, assign to <code className="px-0.5 rounded" style={{ background: 'var(--bg-hover)' }}>df</code>
        </span>
      </div>
      <CodeEditor
        defaultValue={defaultCode}
        onChange={(val) => onUpdate("code", val)}
        placeholder={placeholders[fileType] || placeholders.pickle}
      />
    </div>
  )
}

function DataSinkConfig({
  config,
  onUpdate,
  nodeId,
  allNodes,
  edges,
  submodels,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  nodeId: string
  allNodes: SimpleNode[]
  edges: SimpleEdge[]
  submodels?: Record<string, unknown>
}) {
  const [format, setFormat] = useState<string>((config.format as string) || "parquet")
  const [writing, setWriting] = useState(false)
  const [writeResult, setWriteResult] = useState<{ status: string; message: string } | null>(null)

  const hasPath = Boolean(config.path)

  const handleWrite = () => {
    if (!hasPath || writing) return
    setWriting(true)
    setWriteResult(null)

    const graph = {
      nodes: allNodes.map((n) => ({ id: n.id, type: n.type || n.data.nodeType, data: n.data, position: { x: 0, y: 0 } })),
      edges: edges,
      submodels: submodels,
    }

    fetch("/api/pipeline/sink", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ graph, nodeId }),
    })
      .then((r) => r.json())
      .then((data) => {
        setWriteResult({ status: data.status || "ok", message: data.message || "Written successfully" })
        setWriting(false)
      })
      .catch((err) => {
        setWriteResult({ status: "error", message: err.message })
        setWriting(false)
      })
  }

  return (
    <div className="px-4 py-3 space-y-3">
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Format</label>
        <div className="mt-1 flex gap-1.5">
          {["parquet", "csv"].map((fmt) => (
            <button
              key={fmt}
              onClick={() => {
                setFormat(fmt)
                onUpdate("format", fmt)
              }}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: format === fmt ? 'rgba(245,158,11,.1)' : 'var(--bg-input)',
                border: format === fmt ? '1px solid #f59e0b' : '1px solid var(--border)',
                color: format === fmt ? '#f59e0b' : 'var(--text-secondary)',
              }}
            >
              {fmt.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
          Output Path
        </label>
        <input
          type="text"
          placeholder={format === "csv" ? "output/results.csv" : "output/results.parquet"}
          defaultValue={(config.path as string) || ""}
          onChange={(e) => onUpdate("path", e.target.value)}
          className="w-full px-2.5 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
          style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
          onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(245,158,11,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px rgba(245,158,11,.1)' }}
          onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
        />
      </div>

      <button
        onClick={handleWrite}
        disabled={!hasPath || writing}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 text-[12px] font-semibold rounded-lg transition-colors disabled:opacity-40"
        style={{ background: '#f59e0b', color: '#000' }}
        onMouseEnter={(e) => { if (hasPath && !writing) e.currentTarget.style.background = '#fbbf24' }}
        onMouseLeave={(e) => { e.currentTarget.style.background = '#f59e0b' }}
      >
        <HardDriveDownload size={14} />
        {writing ? "Writing..." : "Write"}
      </button>

      {writeResult && (
        <div
          className="px-2.5 py-2 rounded-lg text-xs"
          style={{
            background: writeResult.status === "ok" ? 'rgba(34,197,94,.1)' : 'rgba(239,68,68,.1)',
            border: writeResult.status === "ok" ? '1px solid rgba(34,197,94,.2)' : '1px solid rgba(239,68,68,.2)',
            color: writeResult.status === "ok" ? '#4ade80' : '#f87171',
          }}
        >
          {writeResult.message}
        </div>
      )}
    </div>
  )
}

function OutputConfig({
  config,
  onUpdate,
  nodeId,
  allNodes,
  edges,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  nodeId: string
  allNodes: SimpleNode[]
  edges: SimpleEdge[]
}) {
  const fields = (config.fields as string[]) || []

  // Read cached columns from the upstream node (populated by preview/run)
  const incomingEdge = edges.find((e) => e.target === nodeId)
  const upstreamNode = incomingEdge ? allNodes.find((n) => n.id === incomingEdge.source) : null
  const upstreamColumns = ((upstreamNode?.data as Record<string, unknown>)?._columns as { name: string; dtype: string }[]) || []

  const toggleField = (col: string) => {
    const next = fields.includes(col) ? fields.filter((f) => f !== col) : [...fields, col]
    onUpdate("fields", next)
  }

  return (
    <div className="px-4 py-3 space-y-3">
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-2" style={{ color: 'var(--text-muted)' }}>Response Fields</label>

        {upstreamColumns.length === 0 ? (
          <div className="text-xs py-3" style={{ color: 'var(--text-muted)' }}>Preview or run the upstream node to see columns</div>
        ) : (
          <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)', background: 'var(--bg-input)' }}>
            <table className="w-full text-xs">
              <thead>
                <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
                  <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 28 }}></th>
                  <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Column</th>
                  <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Type</th>
                </tr>
              </thead>
              <tbody>
                {upstreamColumns.map((col) => {
                  const included = fields.includes(col.name)
                  return (
                    <tr key={col.name} style={{ borderBottom: '1px solid var(--border)' }}>
                      <td className="px-2.5 py-1.5 text-center">
                        <input
                          type="checkbox"
                          checked={included}
                          onChange={() => toggleField(col.name)}
                          className="accent-rose-500 rounded"
                        />
                      </td>
                      <td className="px-2.5 py-1.5 font-mono" style={{ color: included ? 'var(--text-primary)' : 'var(--text-muted)' }}>{col.name}</td>
                      <td className="px-2.5 py-1.5">
                        <span className={`text-[11px] font-medium ${getDtypeColor(col.dtype)}`}>{col.dtype}</span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {fields.length > 0 && (
        <div className="rounded-lg px-3 py-2" style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border)' }}>
          <div className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5" style={{ color: 'var(--text-muted)' }}>JSON Preview</div>
          <pre className="text-[11px] font-mono leading-relaxed" style={{ color: 'var(--text-secondary)' }}>
{`{\n${fields.map((f) => `  "${f}": ...`).join(",\n")}\n}`}
          </pre>
        </div>
      )}
    </div>
  )
}

function ModelScoreConfig({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
}: {
  config: Record<string, unknown>
  onUpdate: (keyOrUpdates: string | Record<string, unknown>, value?: unknown) => void
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
}) {
  const sourceType = (config.sourceType as string) || "registered"
  const task = (config.task as string) || "regression"
  const outputColumn = (config.output_column as string) || "prediction"
  const defaultCode = (config.code as string) || ""
  const selectedModel = (config.registered_model as string) || ""

  // MLflow connection status — checked once on mount
  const [mlflowStatus, setMlflowStatus] = useState<"loading" | "connected" | "error">("loading")
  const [mlflowBackend, setMlflowBackend] = useState("")

  const checkedMlflow = useRef(false)
  useEffect(() => {
    if (checkedMlflow.current) return
    checkedMlflow.current = true
    fetch("/api/modelling/mlflow/check")
      .then((r) => r.json())
      .then((data) => {
        if (data.mlflow_installed) {
          setMlflowStatus("connected")
          setMlflowBackend(data.backend || "local")
        } else {
          setMlflowStatus("error")
        }
      })
      .catch(() => { setMlflowStatus("error"); checkedMlflow.current = false })
  }, [])

  // Lazy-loaded dropdown data — fetched on focus only, like Databricks selects
  const [experiments, setExperiments] = useState<{ experiment_id: string; name: string }[]>([])
  const [runs, setRuns] = useState<{ run_id: string; run_name: string; metrics: Record<string, number>; artifacts: string[] }[]>([])
  const [models, setModels] = useState<{ name: string; latest_versions: { version: string; status: string; run_id: string }[] }[]>([])
  const [modelVersions, setModelVersions] = useState<{ version: string; run_id: string; status: string; description: string }[]>([])
  const [loadingExperiments, setLoadingExperiments] = useState(false)
  const [loadingRuns, setLoadingRuns] = useState(false)
  const [loadingModels, setLoadingModels] = useState(false)
  const [, setLoadingVersions] = useState(false)
  const [errorExperiments, setErrorExperiments] = useState("")
  const [errorRuns, setErrorRuns] = useState("")
  const [errorModels, setErrorModels] = useState("")
  const [errorVersions, setErrorVersions] = useState("")

  // Experiment ID persists in config so the panel can fetch runs on re-open without needing
  // the experiments list (the API requires the numeric ID, not the display name).
  const [browseExpId, setBrowseExpId] = useState((config.experiment_id as string) || "")

  // Fetch guards — only fetch once per mount, not on every focus
  const fetchedExperiments = useRef(false)
  const fetchedModels = useRef(false)
  const fetchedRunsFor = useRef("")
  const fetchedVersionsFor = useRef("")

  const apiFetch = (url: string) =>
    fetch(url).then((r) => {
      if (!r.ok) return r.json().then((d) => { throw new Error(d.detail || `HTTP ${r.status}`) })
      return r.json()
    })

  const refreshExperiments = () => {
    if (fetchedExperiments.current) return
    fetchedExperiments.current = true
    setLoadingExperiments(true)
    setErrorExperiments("")
    apiFetch("/api/mlflow/experiments")
      .then((data) => { setExperiments(Array.isArray(data) ? data : []); setLoadingExperiments(false) })
      .catch((e) => { setExperiments([]); setLoadingExperiments(false); setErrorExperiments(e.message || "Failed to load experiments"); fetchedExperiments.current = false })
  }

  const refreshRuns = (expId: string) => {
    if (!expId) return
    if (fetchedRunsFor.current === expId) return
    fetchedRunsFor.current = expId
    setLoadingRuns(true)
    setErrorRuns("")
    apiFetch(`/api/mlflow/runs?experiment_id=${encodeURIComponent(expId)}`)
      .then((data) => { setRuns(Array.isArray(data) ? data : []); setLoadingRuns(false) })
      .catch((e) => { setRuns([]); setLoadingRuns(false); setErrorRuns(e.message || "Failed to load runs"); fetchedRunsFor.current = "" })
  }

  const refreshModels = () => {
    if (fetchedModels.current) return
    fetchedModels.current = true
    setLoadingModels(true)
    setErrorModels("")
    apiFetch("/api/mlflow/models")
      .then((data) => { setModels(Array.isArray(data) ? data : []); setLoadingModels(false) })
      .catch((e) => { setModels([]); setLoadingModels(false); setErrorModels(e.message || "Failed to load models"); fetchedModels.current = false })
  }

  const refreshVersions = (modelName: string) => {
    if (!modelName) return
    if (fetchedVersionsFor.current === modelName) return
    fetchedVersionsFor.current = modelName
    setLoadingVersions(true)
    setErrorVersions("")
    apiFetch(`/api/mlflow/model-versions?model_name=${encodeURIComponent(modelName)}`)
      .then((data) => { setModelVersions(Array.isArray(data) ? data : []); setLoadingVersions(false) })
      .catch((e) => { setModelVersions([]); setLoadingVersions(false); setErrorVersions(e.message || "Failed to load versions"); fetchedVersionsFor.current = "" })
  }

  const selectStyle = {
    background: 'var(--bg-input)',
    border: '1px solid var(--border)',
    color: 'var(--text-primary)',
  }

  const [showCode, setShowCode] = useState(!!defaultCode)

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-3">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      {/* MLflow Status */}
      <div className="flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-[11px]" style={{
        background: mlflowStatus === "connected" ? "rgba(34,197,94,.06)" : mlflowStatus === "error" ? "rgba(239,68,68,.06)" : "var(--bg-surface)",
        border: `1px solid ${mlflowStatus === "connected" ? "rgba(34,197,94,.2)" : mlflowStatus === "error" ? "rgba(239,68,68,.2)" : "var(--border)"}`,
      }}>
        {mlflowStatus === "loading" ? (
          <><Loader2 size={11} className="animate-spin" style={{ color: "var(--text-muted)" }} /><span style={{ color: "var(--text-muted)" }}>Connecting to MLflow...</span></>
        ) : mlflowStatus === "connected" ? (
          <><Check size={11} style={{ color: "#22c55e" }} /><span style={{ color: "var(--text-secondary)" }}>MLflow ({mlflowBackend})</span></>
        ) : (
          <><AlertTriangle size={11} style={{ color: "#ef4444" }} /><span style={{ color: "#ef4444" }}>MLflow not available</span></>
        )}
      </div>

      {/* Source Type Toggle */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Model Source</label>
        <div className="mt-1 flex gap-1.5">
          {[
            { key: "registered", label: "Registered Model" },
            { key: "run", label: "Experiment Run" },
          ].map((opt) => (
            <button
              key={opt.key}
              onClick={() => onUpdate("sourceType", opt.key)}
              className="flex-1 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: sourceType === opt.key ? "rgba(139,92,246,.1)" : "var(--bg-input)",
                border: sourceType === opt.key ? "1px solid #8b5cf6" : "1px solid var(--border)",
                color: sourceType === opt.key ? "#8b5cf6" : "var(--text-secondary)",
              }}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* Registered Model Selection */}
      {sourceType === "registered" && (
        <div className="flex flex-col gap-2">
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Model Name</label>
            <select
              className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
              style={selectStyle}
              value={selectedModel}
              onFocus={refreshModels}
              onChange={(e) => onUpdate({ registered_model: e.target.value, version: "latest" })}
            >
              <option value="">{loadingModels ? "Loading..." : "Select a model..."}</option>
              {selectedModel && models.every((m) => m.name !== selectedModel) && (
                <option value={selectedModel}>{selectedModel}</option>
              )}
              {models.map((m) => (
                <option key={m.name} value={m.name}>{m.name}</option>
              ))}
            </select>
            {errorModels && <span className="text-[10px] mt-0.5" style={{ color: "#ef4444" }}>{errorModels}</span>}
          </div>
          {selectedModel && (
            <div>
              <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Version</label>
              <select
                className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
                style={selectStyle}
                value={(config.version as string) || "latest"}
                onFocus={() => refreshVersions(selectedModel)}
                onChange={(e) => onUpdate("version", e.target.value)}
              >
                <option value="latest">latest</option>
                {modelVersions.map((v) => (
                  <option key={v.version} value={v.version}>v{v.version} — {v.status}{v.description ? ` (${v.description})` : ""}</option>
                ))}
              </select>
              {errorVersions && <span className="text-[10px] mt-0.5" style={{ color: "#ef4444" }}>{errorVersions}</span>}
            </div>
          )}
        </div>
      )}

      {/* Run-based Selection */}
      {sourceType === "run" && (
        <div className="flex flex-col gap-2">
          {/* Experiment name persists in config so the panel remembers it on re-open */}
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Experiment</label>
            <select
              className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
              style={selectStyle}
              value={browseExpId}
              onFocus={refreshExperiments}
              onChange={(e) => {
                const eid = e.target.value
                const exp = experiments.find((x) => x.experiment_id === eid)
                setBrowseExpId(eid)
                onUpdate({ experiment_id: eid, experiment_name: exp?.name || eid })
                setRuns([])
                fetchedRunsFor.current = ""
                if (eid) refreshRuns(eid)
              }}
            >
              <option value="">{loadingExperiments ? "Loading..." : "Select an experiment..."}</option>
              {browseExpId && experiments.every((e) => e.experiment_id !== browseExpId) && (
                <option value={browseExpId}>{(config.experiment_name as string) || browseExpId}</option>
              )}
              {experiments.map((exp) => (
                <option key={exp.experiment_id} value={exp.experiment_id}>{exp.name}</option>
              ))}
            </select>
            {errorExperiments && <span className="text-[10px] mt-0.5" style={{ color: "#ef4444" }}>{errorExperiments}</span>}
          </div>
          {browseExpId && (
            <div>
              <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Run</label>
              <select
                className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
                style={selectStyle}
                value={(config.run_id as string) || ""}
                onFocus={() => refreshRuns(browseExpId)}
                onChange={(e) => {
                  const runId = e.target.value
                  const run = runs.find((r) => r.run_id === runId)
                  onUpdate({ run_id: runId, run_name: run?.run_name || "", artifact_path: run?.artifacts[0] || "" })
                }}
              >
                <option value="">{loadingRuns ? "Loading..." : "Select a run..."}</option>
                {(config.run_id as string) && runs.every((r) => r.run_id !== config.run_id) && (
                  <option value={config.run_id as string}>{(config.run_name as string) || (config.run_id as string).slice(0, 8) + "..."}</option>
                )}
                {runs.map((r) => (
                  <option key={r.run_id} value={r.run_id}>
                    {r.run_name || r.run_id.slice(0, 8)}
                    {Object.entries(r.metrics).slice(0, 2).map(([k, v]) => ` ${k}=${typeof v === "number" ? v.toFixed(4) : v}`).join("")}
                  </option>
                ))}
              </select>
              {errorRuns && <span className="text-[10px] mt-0.5" style={{ color: "#ef4444" }}>{errorRuns}</span>}
            </div>
          )}
          {/* Persisted values — always visible when set, editable directly */}
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Run ID</label>
            <input
              type="text"
              className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg font-mono focus:outline-none focus:ring-2"
              style={selectStyle}
              value={(config.run_id as string) || ""}
              onChange={(e) => onUpdate("run_id", e.target.value)}
              placeholder="e.g. a1b2c3d4e5f6..."
            />
          </div>
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Artifact Path</label>
            <input
              type="text"
              className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg font-mono focus:outline-none focus:ring-2"
              style={selectStyle}
              value={(config.artifact_path as string) || ""}
              onChange={(e) => onUpdate("artifact_path", e.target.value)}
              placeholder="e.g. model.cbm"
            />
          </div>
        </div>
      )}

      {/* Task and Output Column */}
      <div className="flex gap-2">
        <div className="flex-1">
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Task</label>
          <select
            className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
            style={selectStyle}
            value={task}
            onChange={(e) => onUpdate("task", e.target.value)}
          >
            <option value="regression">Regression</option>
            <option value="classification">Classification</option>
          </select>
        </div>
        <div className="flex-1">
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>Output Column</label>
          <input
            type="text"
            className="mt-1 w-full text-xs px-2.5 py-1.5 rounded-lg focus:outline-none focus:ring-2"
            style={selectStyle}
            value={outputColumn}
            onChange={(e) => onUpdate("output_column", e.target.value)}
            placeholder="prediction"
          />
        </div>
      </div>

      {task === "classification" && (
        <p className="text-[10px]" style={{ color: "var(--text-muted)" }}>
          Classification models also generate a <code className="px-0.5 rounded" style={{ background: "var(--bg-hover)" }}>{outputColumn}_proba</code> column.
        </p>
      )}

      {/* Optional Post-processing Code */}
      <div>
        <button
          onClick={() => setShowCode(!showCode)}
          className="flex items-center gap-1.5 text-[11px] font-medium transition-colors"
          style={{ color: "var(--text-muted)" }}
        >
          <ChevronDown size={11} style={{ transform: showCode ? "rotate(0deg)" : "rotate(-90deg)", transition: "transform 0.15s" }} />
          Post-processing Code (optional)
        </button>
      </div>
      {showCode && (
        <CodeEditor
          defaultValue={defaultCode}
          onChange={(val) => onUpdate("code", val)}
          placeholder={`# df has the prediction column already\n# model is the loaded CatBoost model\ndf = df.with_columns(\n    risk_band=pl.when(pl.col("${outputColumn}") > 0.5).then(pl.lit("high")).otherwise(pl.lit("low"))\n)`}
        />
      )}
    </div>
  )
}

function SubmodelConfig({
  config,
}: {
  config: Record<string, unknown>
}) {
  const file = (config.file as string) || ""
  const childNodeIds = (config.childNodeIds as string[]) || []
  const inputPorts = (config.inputPorts as string[]) || []
  const outputPorts = (config.outputPorts as string[]) || []

  return (
    <div className="px-4 py-3 space-y-3">
      <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg" style={{ background: 'rgba(249,115,22,.08)', border: '1px solid rgba(249,115,22,.2)' }}>
        <Package size={14} style={{ color: '#f97316' }} />
        <span className="text-xs font-medium" style={{ color: '#fb923c' }}>Submodel</span>
        <span className="ml-auto text-[11px] font-mono" style={{ color: 'var(--text-muted)' }}>{childNodeIds.length} nodes</span>
      </div>

      {file && (
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>File</label>
          <div className="mt-1 text-xs font-mono px-2.5 py-1.5 rounded-lg" style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}>
            {file}
          </div>
        </div>
      )}

      {inputPorts.length > 0 && (
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Inputs</label>
          <div className="mt-1 flex flex-wrap gap-1.5">
            {inputPorts.map((port) => (
              <span key={port} className="inline-flex items-center px-2 py-0.5 rounded text-[11px] font-mono" style={{ background: 'var(--accent-soft)', color: 'var(--accent)' }}>
                {port}
              </span>
            ))}
          </div>
        </div>
      )}

      {outputPorts.length > 0 && (
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Outputs</label>
          <div className="mt-1 flex flex-wrap gap-1.5">
            {outputPorts.map((port) => (
              <span key={port} className="inline-flex items-center px-2 py-0.5 rounded text-[11px] font-mono" style={{ background: 'rgba(249,115,22,.1)', color: '#fb923c' }}>
                {port}
              </span>
            ))}
          </div>
        </div>
      )}

      <div className="text-[11px] pt-1" style={{ color: 'var(--text-muted)' }}>
        Double-click to view internal nodes
      </div>
    </div>
  )
}

const MIN_PANEL_W = 320
const MAX_PANEL_W = 900
const DEFAULT_PANEL_W = 400

export default function NodePanel({ node, edges, allNodes, submodels, onClose, onUpdateNode, onDeleteEdge, onRefreshPreview }: NodePanelProps) {
  const [panelWidth, setPanelWidth] = useState(DEFAULT_PANEL_W)
  const isDragging = useRef(false)
  const startX = useRef(0)
  const startW = useRef(DEFAULT_PANEL_W)

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!isDragging.current) return
      const delta = startX.current - e.clientX
      const newW = Math.min(MAX_PANEL_W, Math.max(MIN_PANEL_W, startW.current + delta))
      setPanelWidth(newW)
    }
    const onMouseUp = () => {
      if (isDragging.current) {
        isDragging.current = false
        document.body.style.cursor = ''
        document.body.style.userSelect = ''
      }
    }
    window.addEventListener('mousemove', onMouseMove)
    window.addEventListener('mouseup', onMouseUp)
    return () => {
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('mouseup', onMouseUp)
    }
  }, [])

  const onDragStart = useCallback((e: React.MouseEvent) => {
    isDragging.current = true
    startX.current = e.clientX
    startW.current = panelWidth
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'
  }, [panelWidth])

  if (!node) return null

  const config = (node.data.config || {}) as Record<string, unknown>
  const isInstance = !!config.instanceOf
  const isApiInput = node.data.nodeType === NODE_TYPES.API_INPUT
  const isLiveSwitch = node.data.nodeType === NODE_TYPES.LIVE_SWITCH
  const isDataSource = node.data.nodeType === NODE_TYPES.DATA_SOURCE
  const isDataSink = node.data.nodeType === NODE_TYPES.DATA_SINK
  const isExternalFile = node.data.nodeType === NODE_TYPES.EXTERNAL_FILE
  const isOutput = node.data.nodeType === NODE_TYPES.OUTPUT
  const isBanding = node.data.nodeType === NODE_TYPES.BANDING
  const isRatingStep = node.data.nodeType === NODE_TYPES.RATING_STEP
  const isTransform = node.data.nodeType === NODE_TYPES.TRANSFORM
  const isModelScore = node.data.nodeType === NODE_TYPES.MODEL_SCORE
  const isModelling = node.data.nodeType === NODE_TYPES.MODELLING
  const isSubmodel = node.data.nodeType === NODE_TYPES.SUBMODEL

  // Compute input sources - variable name = sanitized source node label
  const nodeMap = Object.fromEntries(allNodes.map((n) => [n.id, n]))
  const inputSources: InputSource[] = edges
    .filter((e) => e.target === node.id)
    .map((e) => ({
      varName: sanitizeName(nodeMap[e.source]?.data.label || e.source),
      sourceLabel: nodeMap[e.source]?.data.label || e.source,
      edgeId: e.id,
    }))

  const handleConfigUpdate = (keyOrUpdates: string | Record<string, unknown>, value?: unknown) => {
    const newConfig = typeof keyOrUpdates === "string"
      ? { ...config, [keyOrUpdates]: value }
      : { ...config, ...keyOrUpdates }
    if (onUpdateNode) {
      onUpdateNode(node.id, { ...node.data, config: newConfig })
    }
  }

  return (
    <div key={node.id} className="h-full shrink-0 flex flex-row animate-slide-in" style={{ width: panelWidth, background: 'var(--bg-panel)' }}>
      {/* Drag handle */}
      <div
        onMouseDown={onDragStart}
        className="shrink-0 h-full w-1 cursor-col-resize transition-colors"
        style={{ background: 'transparent' }}
        onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--accent-soft)' }}
        onMouseLeave={(e) => { if (!isDragging.current) e.currentTarget.style.background = 'transparent' }}
      />
      <div className="flex-1 min-w-0 h-full overflow-y-auto flex flex-col">
      <div className="px-3 py-2.5 flex items-center gap-2 shrink-0" style={{ borderBottom: '1px solid var(--border)' }}>
        <input
          type="text"
          defaultValue={node.data.label}
          onChange={(e) => {
            if (onUpdateNode) {
              onUpdateNode(node.id, { ...node.data, label: e.target.value })
            }
          }}
          className="flex-1 min-w-0 px-2 py-1 text-[13px] font-semibold border border-transparent rounded-md focus:outline-none focus:ring-2 bg-transparent"
          style={{ color: 'var(--text-primary)', borderColor: 'transparent' }}
          onFocus={(e) => { e.currentTarget.style.borderColor = 'var(--accent)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
          onBlur={(e) => { e.currentTarget.style.borderColor = 'transparent'; e.currentTarget.style.boxShadow = 'none' }}
        />
        <span className="text-[11px] font-mono shrink-0" style={{ color: 'var(--text-muted)' }}>{node.id}</span>
        <button onClick={onClose} className="p-1 rounded shrink-0 transition-colors" style={{ color: 'var(--text-muted)' }}
          onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-hover)'}
          onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
        >
          <X size={14} />
        </button>
      </div>

      {isInstance ? (
        <div className="px-4 py-3 flex flex-col gap-3">
          <div className="flex items-center gap-2 px-3 py-2 rounded-lg" style={{ background: 'var(--accent-soft)', border: '1px solid rgba(96,165,250,.15)' }}>
            <Link2 size={13} style={{ color: 'var(--accent)' }} className="shrink-0" />
            <div className="min-w-0">
              <div className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--accent)' }}>Instance of</div>
              <div className="text-[13px] font-semibold truncate" style={{ color: 'var(--text-primary)' }}>
                {(() => {
                  const orig = allNodes.find((n) => n.id === config.instanceOf)
                  return orig ? orig.data.label : String(config.instanceOf)
                })()}
              </div>
            </div>
          </div>
          <p className="text-[11px] leading-relaxed" style={{ color: 'var(--text-muted)' }}>
            This node uses the same logic as the original. To edit the code or config, select the original node. Changes will automatically apply to all instances.
          </p>

          {/* ── Input Mapping ── */}
          {(() => {
            const origId = config.instanceOf as string
            // Original's upstream inputs (variable names the code expects)
            const origInputs = edges
              .filter((e) => e.target === origId)
              .map((e) => {
                const srcNode = nodeMap[e.source]
                return srcNode ? sanitizeName(srcNode.data.label) : e.source
              })
            // Instance's upstream inputs (what's actually connected)
            const instInputs = edges
              .filter((e) => e.target === node.id)
              .map((e) => {
                const srcNode = nodeMap[e.source]
                return {
                  varName: srcNode ? sanitizeName(srcNode.data.label) : e.source,
                  label: srcNode ? srcNode.data.label : e.source,
                }
              })

            if (origInputs.length === 0 && instInputs.length === 0) return null

            const currentMapping = (config.inputMapping || {}) as Record<string, string>

            // Auto-initialise mapping if empty or stale.
            // Mirrors build_instance_mapping() in graph_utils.py — keep in sync.
            const autoMap: Record<string, string> = {}
            const usedInst = new Set<string>()
            // Pass 1: exact
            for (const orig of origInputs) {
              const exact = instInputs.find((i) => i.varName === orig && !usedInst.has(i.varName))
              if (exact) { autoMap[orig] = exact.varName; usedInst.add(exact.varName) }
            }
            // Pass 2: substring
            for (const orig of origInputs) {
              if (autoMap[orig]) continue
              const sub = instInputs.find((i) => !usedInst.has(i.varName) && i.varName.includes(orig))
              if (sub) { autoMap[orig] = sub.varName; usedInst.add(sub.varName) }
            }
            // Pass 3: positional fallback
            const remaining = instInputs.filter((i) => !usedInst.has(i.varName))
            const unmapped = origInputs.filter((o) => !autoMap[o])
            unmapped.forEach((orig, idx) => {
              if (idx < remaining.length) autoMap[orig] = remaining[idx].varName
            })

            // Use saved mapping if it exists and all keys still valid, otherwise use auto
            const effectiveMap: Record<string, string> = {}
            const instVarNames = new Set(instInputs.map((i) => i.varName))
            for (const orig of origInputs) {
              if (currentMapping[orig] && instVarNames.has(currentMapping[orig])) {
                effectiveMap[orig] = currentMapping[orig]
              } else {
                effectiveMap[orig] = autoMap[orig] || ""
              }
            }

            const handleMappingChange = (origParam: string, instVar: string) => {
              const newMapping = { ...effectiveMap, [origParam]: instVar }
              handleConfigUpdate("inputMapping", newMapping)
            }

            return (
              <div className="flex flex-col gap-2">
                <div className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
                  Input Mapping
                </div>
                <p className="text-[10px] leading-relaxed" style={{ color: 'var(--text-muted)' }}>
                  Map each original input to a connected upstream node.
                </p>
                <div className="flex flex-col gap-1.5">
                  {origInputs.map((orig) => (
                    <div key={orig} className="flex items-center gap-2 px-2 py-1.5 rounded-md" style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)' }}>
                      <span className="text-[11px] font-mono shrink-0 min-w-[90px] truncate" style={{ color: 'var(--text-secondary)' }} title={orig}>
                        {orig}
                      </span>
                      <span className="text-[10px] shrink-0" style={{ color: 'var(--text-muted)' }}>→</span>
                      <select
                        className="flex-1 min-w-0 text-[11px] font-mono px-1.5 py-1 rounded border bg-transparent appearance-none cursor-pointer truncate"
                        style={{ color: 'var(--text-primary)', borderColor: 'var(--border)', background: 'var(--bg-panel)' }}
                        value={effectiveMap[orig] || ""}
                        onChange={(e) => handleMappingChange(orig, e.target.value)}
                      >
                        <option value="">— unmapped —</option>
                        {instInputs.map((i) => (
                          <option key={i.varName} value={i.varName}>{i.label}</option>
                        ))}
                      </select>
                    </div>
                  ))}
                </div>
              </div>
            )
          })()}

          {(() => {
            const warnings = (node.data._schemaWarnings as { column: string; status: string }[]) || []
            if (warnings.length === 0) return null
            return (
              <div className="flex flex-col gap-1.5 px-3 py-2 rounded-lg" style={{ background: 'rgba(245,158,11,.08)', border: '1px solid rgba(245,158,11,.2)' }}>
                <div className="flex items-center gap-1.5">
                  <AlertTriangle size={11} style={{ color: '#f59e0b' }} className="shrink-0" />
                  <span className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: '#f59e0b' }}>
                    Missing columns ({warnings.length})
                  </span>
                </div>
                <p className="text-[10px] leading-relaxed" style={{ color: 'var(--text-muted)' }}>
                  The original node receives columns that are not available at this instance&apos;s position:
                </p>
                <div className="flex flex-wrap gap-1 mt-0.5">
                  {warnings.map((w) => (
                    <span key={w.column} className="px-1.5 py-0.5 rounded text-[10px] font-mono" style={{ background: 'rgba(245,158,11,.12)', color: '#fbbf24' }}>
                      {w.column}
                    </span>
                  ))}
                </div>
              </div>
            )
          })()}
        </div>
      ) : isApiInput ? (
        <ApiInputConfig config={config} onUpdate={handleConfigUpdate} />
      ) : isLiveSwitch ? (
        <LiveSwitchConfig config={config} onUpdate={handleConfigUpdate} inputSources={inputSources} />
      ) : isDataSource ? (
        <DataSourceConfig config={config} onUpdate={handleConfigUpdate} onRefreshPreview={onRefreshPreview} />
      ) : isDataSink ? (
        <DataSinkConfig config={config} onUpdate={handleConfigUpdate} nodeId={node.id} allNodes={allNodes} edges={edges} submodels={submodels} />
      ) : isExternalFile ? (
        <ExternalFileConfig config={config} onUpdate={handleConfigUpdate} inputSources={inputSources} onDeleteInput={onDeleteEdge} />
      ) : isOutput ? (
        <OutputConfig config={config} onUpdate={handleConfigUpdate} nodeId={node.id} allNodes={allNodes} edges={edges} />
      ) : isBanding ? (
        <BandingConfig config={config} onUpdate={handleConfigUpdate} inputSources={inputSources} onDeleteInput={onDeleteEdge}
          upstreamColumns={(() => {
            const cols: { name: string; dtype: string }[] = []
            const seen = new Set<string>()
            edges.filter(e => e.target === node.id).forEach(e => {
              const src = nodeMap[e.source]
              const srcCols = (src?.data as Record<string, unknown>)?._columns as { name: string; dtype: string }[] | undefined
              if (srcCols) srcCols.forEach(c => { if (!seen.has(c.name)) { seen.add(c.name); cols.push(c) } })
            })
            return cols
          })()} />
      ) : isRatingStep ? (
        <RatingStepConfig config={config} onUpdate={handleConfigUpdate} inputSources={inputSources} onDeleteInput={onDeleteEdge} allNodes={allNodes} />
      ) : isModelScore ? (
        <ModelScoreConfig config={config} onUpdate={handleConfigUpdate} inputSources={inputSources} onDeleteInput={onDeleteEdge} />
      ) : isModelling ? (
        <ModellingConfig config={{...config, _nodeId: node.id}} onUpdate={handleConfigUpdate}
          allNodes={allNodes} edges={edges} submodels={submodels}
          upstreamColumns={(() => {
            const cols: { name: string; dtype: string }[] = []
            const seen = new Set<string>()
            edges.filter(e => e.target === node.id).forEach(e => {
              const src = nodeMap[e.source]
              const srcCols = (src?.data as Record<string, unknown>)?._columns as { name: string; dtype: string }[] | undefined
              if (srcCols) srcCols.forEach(c => { if (!seen.has(c.name)) { seen.add(c.name); cols.push(c) } })
            })
            // Modelling is a pass-through — its own _columns (set by preview) ARE the upstream columns
            if (cols.length === 0) {
              const ownCols = (node.data as Record<string, unknown>)?._columns as { name: string; dtype: string }[] | undefined
              if (ownCols) return ownCols
            }
            return cols
          })()} />
      ) : isTransform ? (
        <TransformConfig config={config} onUpdate={handleConfigUpdate} inputSources={inputSources} onDeleteInput={onDeleteEdge} />
      ) : isSubmodel ? (
        <SubmodelConfig config={config} />
      ) : (
        Object.keys(config).length > 0 && (
          <div className="px-4 py-3">
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Config</label>
            {Object.entries(config).map(([key, value]) => (
              <div key={key} className="mt-1.5 flex items-center gap-2">
                <span className="text-xs font-mono" style={{ color: 'var(--text-muted)' }}>{key}:</span>
                <span className="text-xs font-mono truncate" style={{ color: 'var(--text-primary)' }}>{String(value)}</span>
              </div>
            ))}
          </div>
        )
      )}
      </div>
    </div>
  )
}
