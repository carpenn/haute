import { ToggleLeft } from "lucide-react"
import type { InputSource } from "./_shared"

export default function LiveSwitchEditor({
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
