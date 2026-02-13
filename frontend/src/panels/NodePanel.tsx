import { useState, useEffect } from "react"
import { X, Folder, FileText, ChevronLeft, Check, Database, Table2 } from "lucide-react"
import { getDtypeColor } from "../utils/dtypeColors"

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
}

type SchemaInfo = {
  path: string
  columns: SchemaColumn[]
  row_count: number
  column_count: number
  preview: Record<string, unknown>[]
} | null

type SimpleNode = {
  id: string
  type?: string
  data: {
    label: string
    description: string
    nodeType: string
    config?: Record<string, unknown>
  }
}

type SimpleEdge = {
  id: string
  source: string
  target: string
}

type NodePanelProps = {
  node: SimpleNode | null
  edges: SimpleEdge[]
  allNodes: SimpleNode[]
  onClose: () => void
  onUpdateNode?: (id: string, data: Record<string, unknown>) => void
}


function FileBrowser({ currentPath, onSelect }: { currentPath?: string; onSelect: (path: string) => void }) {
  const [dir, setDir] = useState(".")
  const [items, setItems] = useState<FileItem[]>([])
  const [loading, setLoading] = useState(false)
  const [selectedPath, setSelectedPath] = useState<string | undefined>(currentPath)

  useEffect(() => {
    setLoading(true)
    fetch(`/api/files?dir=${encodeURIComponent(dir)}`)
      .then((r) => r.json())
      .then((data) => {
        setItems(data.items || [])
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [dir])

  const goUp = () => {
    if (dir === ".") return
    const parts = dir.split("/")
    parts.pop()
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
                    <span className="text-[10px] ml-auto shrink-0" style={{ color: 'var(--text-muted)' }}>
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

  if (!schema) return null

  return (
    <div style={{ borderTop: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
      <div className="px-4 py-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Table2 size={14} style={{ color: 'var(--text-muted)' }} />
          <span className="text-xs font-semibold" style={{ color: 'var(--text-primary)' }}>Schema</span>
          <span className="text-[10px]" style={{ color: 'var(--text-muted)' }}>
            {schema.column_count} cols / {schema.row_count.toLocaleString()} rows
          </span>
        </div>
        <button
          onClick={() => setShowPreview(!showPreview)}
          className="text-[10px] font-medium" style={{ color: 'var(--accent)' }}
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
                    <span className={`text-[10px] font-medium ${getDtypeColor(col.dtype)}`}>
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
            <table className="w-full text-[10px]">
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

function DataSourceConfig({
  config,
  onUpdate,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
}) {
  const [sourceType, setSourceType] = useState<string>((config.sourceType as string) || "flat_file")
  const [schema, setSchema] = useState<SchemaInfo>(null)
  const [loadingSchema, setLoadingSchema] = useState(false)

  const fetchSchema = (path: string) => {
    setLoadingSchema(true)
    fetch(`/api/schema?path=${encodeURIComponent(path)}`)
      .then((r) => r.json())
      .then((data) => {
        setSchema(data)
        setLoadingSchema(false)
      })
      .catch(() => setLoadingSchema(false))
  }

  useEffect(() => {
    if (config.path) {
      fetchSchema(config.path as string)
    }
  }, [])

  return (
    <>
      <div className="px-4 py-3 space-y-3">
        <div>
          <label className="text-[9px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Source Type</label>
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
            <label className="text-[9px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
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
          <div>
            <label className="text-[9px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Table</label>
            <input
              type="text"
              placeholder="catalog.schema.table"
              defaultValue={(config.table as string) || ""}
              onChange={(e) => onUpdate("table", e.target.value)}
              className="mt-1 w-full px-2.5 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
              style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
              onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(59,130,246,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
              onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
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

function TransformConfig({
  config,
  onUpdate,
  inputSources,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  inputSources: InputSource[]
}) {
  const defaultCode = (config.code as string) || ""
  const isMultiInput = inputSources.length > 1
  const hasInput = inputSources.length > 0

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-2">
      {hasInput && (
        <div className="rounded-lg px-3 py-1.5 shrink-0" style={{ background: 'var(--bg-input)', border: '1px solid var(--border)' }}>
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-[9px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
              {isMultiInput ? "Inputs" : "Input"}
            </span>
            {inputSources.map((src) => (
              <code key={src.varName} className="text-[11px] px-1.5 py-0.5 rounded font-semibold" style={{ color: 'var(--accent)', background: 'var(--accent-soft)' }}>{src.varName}</code>
            ))}
          </div>
        </div>
      )}
      <div className="flex items-center justify-between shrink-0">
        <label className="text-[9px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
          Polars Code
        </label>
        <span className="text-[9px] font-medium" style={{ color: 'var(--text-muted)' }}>
          {hasInput ? "use input names" : <>start with <code className="px-0.5 rounded" style={{ background: 'var(--bg-hover)' }}>.</code> to chain</>}
        </span>
      </div>
      <div className="flex-1 min-h-[120px]">
        <textarea
          defaultValue={defaultCode}
          onChange={(e) => onUpdate("code", e.target.value)}
          spellCheck={false}
          placeholder={
            isMultiInput
              ? `${inputSources[0].varName}.join(${inputSources[1]?.varName || "other"}, on="key", how="left")`
              : hasInput
                ? `${inputSources[0].varName}\n.with_columns(\n    age=pl.col("YOA") - pl.col("DOB")\n)\n.select("age", "NCD")`
                : `.with_columns(\n    age=pl.col("YOA") - pl.col("DOB")\n)\n.select("age", "NCD")`
          }
          className="w-full h-full px-3 py-2.5 text-[12px] font-mono rounded-lg focus:outline-none focus:ring-2 resize-none leading-relaxed"
          style={{ background: 'var(--bg-input)', color: '#a5f3fc', border: '1px solid var(--border)', caretColor: 'var(--accent)' }}
          onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(59,130,246,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
          onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
        />
      </div>
    </div>
  )
}

function sanitizeName(label: string): string {
  let name = label.trim().replace(/[\s-]/g, "_")
  name = name.replace(/[^a-zA-Z0-9_]/g, "")
  if (name && /^[0-9]/.test(name)) name = `node_${name}`
  return name || "unnamed_node"
}

export default function NodePanel({ node, edges, allNodes, onClose, onUpdateNode }: NodePanelProps) {
  if (!node) return null

  const config = (node.data.config || {}) as Record<string, unknown>
  const isDataSource = node.data.nodeType === "dataSource"
  const isTransform = node.data.nodeType === "transform" || node.data.nodeType === "output"

  // Compute input sources — variable name = sanitized source node label
  const nodeMap = Object.fromEntries(allNodes.map((n) => [n.id, n]))
  const inputSources: InputSource[] = edges
    .filter((e) => e.target === node.id)
    .map((e) => ({
      varName: sanitizeName(nodeMap[e.source]?.data.label || e.source),
      sourceLabel: nodeMap[e.source]?.data.label || e.source,
    }))

  const handleConfigUpdate = (key: string, value: unknown) => {
    const newConfig = { ...config, [key]: value }
    if (onUpdateNode) {
      onUpdateNode(node.id, { ...node.data, config: newConfig })
    }
  }

  return (
    <div key={node.id} className="w-[400px] h-full overflow-y-auto shrink-0 flex flex-col" style={{ background: 'var(--bg-panel)', borderLeft: '1px solid var(--border)' }}>
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
        <span className="text-[9px] font-mono shrink-0" style={{ color: 'var(--text-muted)' }}>{node.id}</span>
        <button onClick={onClose} className="p-1 rounded shrink-0 transition-colors" style={{ color: 'var(--text-muted)' }}
          onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-hover)'}
          onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
        >
          <X size={14} />
        </button>
      </div>

      {isDataSource ? (
        <DataSourceConfig config={config} onUpdate={handleConfigUpdate} />
      ) : isTransform ? (
        <TransformConfig config={config} onUpdate={handleConfigUpdate} inputSources={inputSources} />
      ) : (
        Object.keys(config).length > 0 && (
          <div className="px-4 py-3">
            <label className="text-[9px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Config</label>
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
  )
}
