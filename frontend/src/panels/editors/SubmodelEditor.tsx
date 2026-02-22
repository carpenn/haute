import { Package } from "lucide-react"

export default function SubmodelEditor({
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
