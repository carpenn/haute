import { Radio, AlertTriangle } from "lucide-react"
import { FileBrowser, SchemaPreview } from "./_shared"
import { useSchemaFetch } from "../../hooks/useSchemaFetch"

export default function ApiInputEditor({
  config,
  onUpdate,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
}) {
  const { schema, loading: loadingSchema, fetchForPath } = useSchemaFetch(config.path as string | undefined)

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
              fetchForPath(path)
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
