import { InputSourcesBar, INPUT_STYLE } from "./_shared"
import type { InputSource, OnUpdateConfig } from "./_shared"
import { configField } from "../../utils/configField"

// Date-like Polars dtypes (origin / development period columns must be date fields)
const DATE_DTYPES = new Set(["Date", "Datetime", "Duration"])

// Non-string numeric Polars dtypes (value field must be numeric)
const STRING_DTYPES = new Set(["Utf8", "String", "Categorical", "Enum", "Boolean"])

type FieldSpec = {
  key: string
  label: string
  description: string
  filterFn: (dtype: string) => boolean
}

const FIELDS: FieldSpec[] = [
  {
    key: "originField",
    label: "Origin Period",
    description: "row dimension — date field",
    filterFn: (dtype) => DATE_DTYPES.has(dtype),
  },
  {
    key: "developmentField",
    label: "Development Period",
    description: "column dimension — date field",
    filterFn: (dtype) => DATE_DTYPES.has(dtype),
  },
  {
    key: "valueField",
    label: "Value",
    description: "summed in each cell — numeric field",
    filterFn: (dtype) => !STRING_DTYPES.has(dtype),
  },
]

export default function TriangleViewerEditor({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
  upstreamColumns = [],
  accentColor,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  upstreamColumns?: { name: string; dtype: string }[]
  accentColor: string
}) {
  const noInput = inputSources.length === 0
  const noColumns = !noInput && upstreamColumns.length === 0

  return (
    <div className="px-4 py-3 space-y-3">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      {noInput && (
        <div
          className="text-[12px] rounded-lg px-3 py-2.5"
          style={{
            background: "rgba(245,158,11,.08)",
            border: "1px solid rgba(245,158,11,.2)",
            color: "#f59e0b",
          }}
        >
          Connect a Data Source node to map fields.
        </div>
      )}

      {noColumns && (
        <div
          className="text-[12px] rounded-lg px-3 py-2.5"
          style={{
            background: "rgba(100,116,139,.1)",
            border: "1px solid rgba(100,116,139,.2)",
            color: "var(--text-muted)",
          }}
        >
          No columns available yet — refresh the upstream node&apos;s preview first.
        </div>
      )}

      {FIELDS.map(({ key, label, description, filterFn }) => {
        const value = configField(config, key, "")
        const filteredColumns = upstreamColumns.filter((c) => filterFn(c.dtype))
        return (
          <div key={key}>
            <label
              className="text-[11px] font-bold uppercase tracking-[0.08em] flex items-center gap-1.5"
              style={{ color: "var(--text-muted)" }}
            >
              {label}
              <span className="normal-case tracking-normal font-normal text-[10px]">
                ({description})
              </span>
            </label>

            {upstreamColumns.length > 0 ? (
              <select
                className="mt-1 w-full px-2.5 py-1.5 rounded-md text-[12px] font-mono appearance-none cursor-pointer"
                style={{ ...INPUT_STYLE, color: value ? "var(--text-primary)" : "var(--text-muted)" }}
                value={value}
                onChange={(e) => onUpdate(key, e.target.value)}
                aria-label={label}
              >
                <option value="">— select column —</option>
                {filteredColumns.map((c) => (
                  <option key={c.name} value={c.name}>
                    {c.name}
                  </option>
                ))}
                {filteredColumns.length === 0 && (
                  <option value="" disabled>
                    No matching columns
                  </option>
                )}
              </select>
            ) : (
              <input
                type="text"
                className="mt-1 w-full px-2.5 py-1.5 rounded-md text-[12px] font-mono"
                style={INPUT_STYLE}
                placeholder="column name"
                value={value}
                onChange={(e) => onUpdate(key, e.target.value)}
                aria-label={label}
              />
            )}
          </div>
        )
      })}

      {!noInput && (
        <div
          className="text-[11px] px-3 py-2 rounded-lg"
          style={{
            background: `rgba(8,145,178,.08)`,
            border: `1px solid rgba(8,145,178,.2)`,
            color: accentColor,
          }}
        >
          Map all three fields then refresh preview to see the triangle table.
        </div>
      )}
    </div>
  )
}

