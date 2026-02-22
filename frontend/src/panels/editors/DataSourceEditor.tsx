import { useState, useEffect } from "react"
import { FileText, Database } from "lucide-react"
import { FileBrowser, SchemaPreview } from "./_shared"
import type { SchemaInfo } from "./_shared"
import { WarehousePicker, CatalogTablePicker, DatabricksFetchButton } from "./_DatabricksSelector"

export default function DataSourceEditor({
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
