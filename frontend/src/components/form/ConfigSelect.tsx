import { useId } from "react"
import EditorLabel from "./EditorLabel"

interface ConfigSelectProps {
  value: string;
  onChange: (value: string) => void;
  options: Array<{ value: string; label: string }> | string[];
  label?: string;
  disabled?: boolean;
  id?: string;
}

const SELECT_STYLE = {
  background: "var(--bg-input)",
  border: "1px solid var(--border)",
  color: "var(--text-primary)",
} as const

/** Normalize options to { value, label } pairs regardless of input format. */
function normalizeOptions(
  options: Array<{ value: string; label: string }> | string[],
): Array<{ value: string; label: string }> {
  if (options.length === 0) return []
  if (typeof options[0] === "string") {
    return (options as string[]).map((o) => ({ value: o, label: o }))
  }
  return options as Array<{ value: string; label: string }>
}

export default function ConfigSelect({
  value,
  onChange,
  options,
  label,
  disabled = false,
  id: externalId,
}: ConfigSelectProps) {
  const generatedId = useId()
  const selectId = externalId ?? generatedId
  const normalized = normalizeOptions(options)

  return (
    <div>
      {label && (
        <EditorLabel htmlFor={selectId} className="mb-1 block">
          {label}
        </EditorLabel>
      )}
      <select
        id={selectId}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
        aria-label={label}
        className="w-full px-2.5 py-1.5 text-xs rounded-lg focus:outline-none focus:ring-2 disabled:opacity-50 disabled:cursor-not-allowed"
        style={SELECT_STYLE}
        onFocus={(e) => {
          e.currentTarget.style.borderColor = "rgba(59,130,246,.3)"
          e.currentTarget.style.boxShadow = "0 0 0 2px var(--accent-soft)"
        }}
        onBlur={(e) => {
          e.currentTarget.style.borderColor = "var(--border)"
          e.currentTarget.style.boxShadow = "none"
        }}
      >
        {normalized.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </div>
  )
}
