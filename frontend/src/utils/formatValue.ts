export function formatValue(v: unknown, maxFractionDigits = 4): string {
  if (v === null || v === undefined) return "null"
  if (typeof v === "number") {
    if (Number.isInteger(v)) return v.toLocaleString()
    return v.toLocaleString(undefined, { maximumFractionDigits: maxFractionDigits })
  }
  return String(v)
}

export function formatValueCompact(v: unknown): string {
  const s = formatValue(v)
  return s.length > 20 ? s.slice(0, 18) + "\u2026" : s
}
