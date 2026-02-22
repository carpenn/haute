import { getDtypeColor } from "../../utils/dtypeColors"
import type { SimpleNode, SimpleEdge } from "./_shared"

export default function OutputEditor({
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
