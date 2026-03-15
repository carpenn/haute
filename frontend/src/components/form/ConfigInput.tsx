import { useId } from "react"
import EditorLabel from "./EditorLabel"

interface ConfigInputProps {
  value: string;
  onChange: (value: string) => void;
  label?: string;
  placeholder?: string;
  type?: "text" | "number";
  disabled?: boolean;
  id?: string;
}

const INPUT_STYLE = {
  background: "var(--bg-input)",
  border: "1px solid var(--border)",
  color: "var(--text-primary)",
} as const

export default function ConfigInput({
  value,
  onChange,
  label,
  placeholder,
  type = "text",
  disabled = false,
  id: externalId,
}: ConfigInputProps) {
  const generatedId = useId()
  const inputId = externalId ?? generatedId

  return (
    <div>
      {label && (
        <EditorLabel htmlFor={inputId} className="mb-1 block">
          {label}
        </EditorLabel>
      )}
      <input
        id={inputId}
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        disabled={disabled}
        aria-label={label}
        className="w-full px-2.5 py-1.5 text-xs rounded-lg focus:outline-none focus:ring-2 disabled:opacity-50 disabled:cursor-not-allowed"
        style={INPUT_STYLE}
        onFocus={(e) => {
          e.currentTarget.style.borderColor = "rgba(59,130,246,.3)"
          e.currentTarget.style.boxShadow = "0 0 0 2px var(--accent-soft)"
        }}
        onBlur={(e) => {
          e.currentTarget.style.borderColor = "var(--border)"
          e.currentTarget.style.boxShadow = "none"
        }}
      />
    </div>
  )
}
