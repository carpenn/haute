import { useState } from "react"
import { FileText, Database } from "lucide-react"
import { FileBrowser, SchemaPreview } from "./_shared"
import type { OnUpdateConfig } from "./_shared"
import { useSchemaFetch } from "../../hooks/useSchemaFetch"
import { fetchDatabricksSchema } from "../../api/client"
import { WarehousePicker, CatalogTablePicker, DatabricksFetchButton } from "./_DatabricksSelector"
import { configField } from "../../utils/configField"

export default function DataSourceEditor({
  config,
  onUpdate,
  onRefreshPreview,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  onRefreshPreview?: () => void
}) {
  const [sourceType, setSourceType] = useState<string>(configField(config, "sourceType", "flat_file"))
  const { schema, setSchema, loading: loadingSchema, fetchForPath } = useSchemaFetch(configField<string | undefined>(config, "path", undefined))

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
              currentPath={configField<string | undefined>(config, "path", undefined)}
              onSelect={(path) => {
                onUpdate("path", path)
                fetchForPath(path)
              }}
            />
          </div>
        )}

        {sourceType === "databricks" && (
          <div className="space-y-3">
            <WarehousePicker
              httpPath={configField(config, "http_path", "")}
              onSelect={(hp) => onUpdate("http_path", hp || undefined)}
            />
            <CatalogTablePicker
              table={configField(config, "table", "")}
              onSelect={(fullName) => onUpdate("table", fullName || undefined)}
            />
            <div>
              <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
                SQL Query
                <span className="ml-1.5 normal-case tracking-normal font-normal" style={{ color: 'var(--text-muted)' }}>(optional)</span>
              </label>
              <textarea
                placeholder={"SELECT *\nFROM catalog.schema.table\nWHERE status = 'active'"}
                defaultValue={configField(config, "query", "") || "SELECT *"}
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
              table={configField(config, "table", "")}
              httpPath={configField(config, "http_path", "")}
              query={configField(config, "query", "")}
              onFetched={() => {
                const tbl = configField(config, "table", "")
                if (tbl) {
                  fetchDatabricksSchema(tbl)
                    .then((data) => { setSchema(data); onRefreshPreview?.() })
                    .catch((err: unknown) => { console.warn("Databricks schema fetch failed:", err); setSchema(null) })
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
