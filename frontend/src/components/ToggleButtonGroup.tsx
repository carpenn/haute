import { withAlpha } from "../utils/color"

interface ToggleButtonGroupProps<T extends string> {
  value: T
  onChange: (value: T) => void
  options: { key: T; label: string; icon?: React.ReactNode }[]
  accentColor: string
}

export default function ToggleButtonGroup<T extends string>({
  value,
  onChange,
  options,
  accentColor,
}: ToggleButtonGroupProps<T>) {
  return (
    <div className="flex gap-1.5">
      {options.map((opt) => {
        const active = value === opt.key
        return (
          <button
            key={opt.key}
            onClick={() => onChange(opt.key)}
            className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
            style={{
              background: active ? withAlpha(accentColor, 0.1) : "var(--bg-input)",
              border: active ? `1px solid ${accentColor}` : "1px solid var(--border)",
              color: active ? accentColor : "var(--text-secondary)",
            }}
          >
            {opt.icon}
            {opt.label}
          </button>
        )
      })}
    </div>
  )
}
