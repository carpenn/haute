import { useState, useEffect } from "react"

/** Controlled number input that syncs from prop and commits on blur. */
export function ControlledNumberCell({ val, onCommit, ...rest }: {
  val: number
  onCommit: (v: string) => void
} & Omit<React.InputHTMLAttributes<HTMLInputElement>, "value" | "onChange" | "onBlur">) {
  const [local, setLocal] = useState(String(val))
  useEffect(() => setLocal(String(val)), [val])
  return (
    <input type="number" step="0.01"
      value={local}
      onChange={(e) => setLocal(e.target.value)}
      onBlur={() => onCommit(local)}
      {...rest}
    />
  )
}
