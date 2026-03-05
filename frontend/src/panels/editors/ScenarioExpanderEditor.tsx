import { InputSourcesBar, INPUT_STYLE } from "./_shared"
import type { InputSource, OnUpdateConfig } from "./_shared"
import { configField } from "../../utils/configField"

export default function ScenarioExpanderEditor({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
  upstreamColumns,
  accentColor,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  upstreamColumns: { name: string; dtype: string }[]
  accentColor: string
}) {
  const quoteId = configField(config, "quote_id", "")
  const columnName = configField(config, "column_name", "scenario_value")
  const minValue = configField(config, "min_value", 0.8)
  const maxValue = configField(config, "max_value", 1.2)
  const steps = configField(config, "steps", 21)
  const stepColumn = configField(config, "step_column", "scenario_index")

  return (
    <div className="px-4 py-3 space-y-4">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      {/* Quote ID column */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1.5" style={{ color: 'var(--text-muted)' }}>
          Quote ID Column
        </label>
        {upstreamColumns.length > 0 ? (
          <select
            className="w-full px-2.5 py-1.5 rounded-md text-[12px] font-mono appearance-none cursor-pointer"
            style={INPUT_STYLE}
            value={quoteId}
            onChange={(e) => onUpdate("quote_id", e.target.value)}
          >
            <option value="">-- select column --</option>
            {upstreamColumns.map((c) => (
              <option key={c.name} value={c.name}>{c.name}</option>
            ))}
          </select>
        ) : (
          <input
            type="text"
            className="w-full px-2.5 py-1.5 rounded-md text-[12px] font-mono"
            style={INPUT_STYLE}
            value={quoteId}
            onChange={(e) => onUpdate("quote_id", e.target.value)}
            placeholder="quote_id"
          />
        )}
        <p className="text-[10px] mt-1" style={{ color: 'var(--text-muted)' }}>
          Column that identifies each quote / row-group
        </p>
      </div>

      {/* New column name */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1.5" style={{ color: 'var(--text-muted)' }}>
          Value Column
        </label>
        <input
          type="text"
          className="w-full px-2.5 py-1.5 rounded-md text-[12px] font-mono"
          style={INPUT_STYLE}
          value={columnName}
          onChange={(e) => onUpdate("column_name", e.target.value)}
          placeholder="scenario_value"
        />
        <p className="text-[10px] mt-1" style={{ color: 'var(--text-muted)' }}>
          Name of the new column with generated values
        </p>
      </div>

      {/* Range: Min / Max / Steps */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1.5" style={{ color: 'var(--text-muted)' }}>
          Range
        </label>
        <div className="grid grid-cols-3 gap-2">
          <div>
            <label className="text-[10px] block mb-0.5" style={{ color: 'var(--text-muted)' }}>Min</label>
            <input
              type="number"
              step="any"
              className="w-full px-2 py-1.5 rounded-md text-[12px] font-mono"
              style={INPUT_STYLE}
              value={minValue}
              onChange={(e) => onUpdate("min_value", parseFloat(e.target.value) || 0)}
            />
          </div>
          <div>
            <label className="text-[10px] block mb-0.5" style={{ color: 'var(--text-muted)' }}>Max</label>
            <input
              type="number"
              step="any"
              className="w-full px-2 py-1.5 rounded-md text-[12px] font-mono"
              style={INPUT_STYLE}
              value={maxValue}
              onChange={(e) => onUpdate("max_value", parseFloat(e.target.value) || 0)}
            />
          </div>
          <div>
            <label className="text-[10px] block mb-0.5" style={{ color: 'var(--text-muted)' }}>Steps</label>
            <input
              type="number"
              min={1}
              className="w-full px-2 py-1.5 rounded-md text-[12px] font-mono"
              style={INPUT_STYLE}
              value={steps}
              onChange={(e) => onUpdate("steps", Math.max(1, parseInt(e.target.value) || 1))}
            />
          </div>
        </div>
      </div>

      {/* Step column name */}
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-1.5" style={{ color: 'var(--text-muted)' }}>
          Step Column
        </label>
        <input
          type="text"
          className="w-full px-2.5 py-1.5 rounded-md text-[12px] font-mono"
          style={INPUT_STYLE}
          value={stepColumn}
          onChange={(e) => onUpdate("step_column", e.target.value)}
          placeholder="scenario_index"
        />
        <p className="text-[10px] mt-1" style={{ color: 'var(--text-muted)' }}>
          Name of the 0-based step index column
        </p>
      </div>

      {/* Preview line */}
      <div className="rounded-lg px-3 py-2" style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border)' }}>
        <div className="text-[11px] font-mono" style={{ color: 'var(--text-secondary)' }}>
          Each input row &rarr; <span style={{ color: accentColor, fontWeight: 600 }}>{steps}</span> output rows
        </div>
        <div className="text-[10px] font-mono mt-0.5" style={{ color: 'var(--text-muted)' }}>
          {columnName || "value"}: {minValue} &rarr; {maxValue} ({steps} steps)
        </div>
      </div>
    </div>
  )
}
