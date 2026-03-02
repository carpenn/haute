import { ToggleLeft } from "lucide-react"
import type { InputSource, OnUpdateConfig } from "./_shared"
import { configField } from "../../utils/configField"
import useUIStore from "../../stores/useUIStore"

export default function LiveSwitchEditor({
  config,
  onUpdate,
  inputSources,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  inputSources: InputSource[]
}) {
  const scenarios = useUIStore((s) => s.scenarios)
  const activeScenario = useUIStore((s) => s.activeScenario)
  const inputScenarioMap = configField<Record<string, string>>(config, "input_scenario_map", {})

  /** Update the mapping for a single input. */
  const setMapping = (inputName: string, scenario: string) => {
    onUpdate("input_scenario_map", { ...inputScenarioMap, [inputName]: scenario })
  }

  return (
    <div className="px-4 py-3 space-y-3">
      <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs font-medium"
        style={{ background: 'rgba(245,158,11,.1)', border: '1px solid rgba(245,158,11,.3)', color: '#f59e0b' }}
      >
        <ToggleLeft size={14} />
        <span>Routes inputs based on the active scenario</span>
      </div>

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1 block" style={{ color: 'var(--text-muted)' }}>
          Active Scenario
        </label>
        <div className="px-2.5 py-1.5 text-xs rounded-lg font-mono"
          style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
        >
          {activeScenario === "live" ? "● live" : activeScenario}
        </div>
        <div className="mt-1 text-[10px]" style={{ color: 'var(--text-muted)' }}>
          Change the active scenario in the toolbar dropdown
        </div>
      </div>

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
          Input → Scenario Mapping ({inputSources.length})
        </label>
        <div className="space-y-1.5">
          {inputSources.map((src) => {
            const mappedScenario = inputScenarioMap[src.varName] || ""
            const isActive = mappedScenario === activeScenario
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
                <span className="font-mono truncate min-w-0 flex-1" style={{ color: 'var(--text-primary)' }}>
                  {src.sourceLabel}
                </span>
                <select
                  value={mappedScenario}
                  onChange={(e) => setMapping(src.varName, e.target.value)}
                  className="px-1.5 py-0.5 text-[11px] font-mono rounded focus:outline-none"
                  style={{
                    background: 'var(--bg-input)',
                    border: '1px solid var(--border)',
                    color: 'var(--text-primary)',
                  }}
                >
                  <option value="">—</option>
                  {scenarios.map((s) => (
                    <option key={s} value={s}>{s}</option>
                  ))}
                </select>
                {isActive && (
                  <span className="text-[10px] font-medium shrink-0" style={{ color: '#f59e0b' }}>
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
