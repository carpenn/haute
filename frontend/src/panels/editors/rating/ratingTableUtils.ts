// ─── Rating Table Types & Pure Utilities ──────────────────────────

export type RatingTable = {
  name: string
  factors: string[]
  outputColumn: string
  defaultValue: string | null
  entries: Record<string, string | number>[]
}

export function normaliseRatingTables(config: Record<string, unknown>): RatingTable[] {
  const raw = config.tables as RatingTable[] | undefined
  if (Array.isArray(raw) && raw.length > 0) return raw
  return [{ name: "Table 1", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }]
}

/** Heatmap color for actuarial relativity values. */
export function relativityColor(value: number): string {
  if (isNaN(value)) return 'transparent'
  const dev = value - 1.0
  const t = Math.min(Math.abs(dev) / 0.5, 1)
  if (dev > 0.005)  return `rgba(239, 68, 68, ${(t * 0.22).toFixed(3)})`
  if (dev < -0.005) return `rgba(59, 130, 246, ${(t * 0.22).toFixed(3)})`
  return 'transparent'
}

export function relativityTextColor(value: number): string {
  if (isNaN(value)) return 'var(--text-secondary)'
  const dev = value - 1.0
  if (dev > 0.005) return '#dc2626'
  if (dev < -0.005) return '#2563eb'
  return '#10b981'
}

export function tableStats(entries: Record<string, string | number>[]): { min: number; max: number; avg: number; count: number } | null {
  const vals = entries.map(e => typeof e.value === 'number' ? e.value : parseFloat(String(e.value ?? ''))).filter(v => !isNaN(v))
  if (vals.length === 0) return null
  const min = Math.min(...vals)
  const max = Math.max(...vals)
  const avg = vals.reduce((s, v) => s + v, 0) / vals.length
  return { min, max, avg, count: vals.length }
}

export function buildCartesianEntries(
  factors: string[],
  bandingLevels: Record<string, string[]>,
  existing: Record<string, string | number>[],
  defaultValue: string | null,
): Record<string, string | number>[] {
  if (factors.length === 0) return []
  const levelArrays = factors.map(f => bandingLevels[f] || [])
  if (levelArrays.some(a => a.length === 0)) return existing

  const existingLookup = new Map<string, number>()
  for (const e of existing) {
    const key = factors.map(f => String(e[f] ?? "")).join("|")
    const v = e.value
    if (v !== undefined && v !== null && v !== "") {
      existingLookup.set(key, typeof v === "number" ? v : parseFloat(String(v)))
    }
  }

  const defVal = defaultValue != null && String(defaultValue).trim() ? parseFloat(String(defaultValue)) : 1.0
  const entries: Record<string, string | number>[] = []

  function recurse(depth: number, current: Record<string, string>) {
    if (depth === factors.length) {
      const key = factors.map(f => current[f]).join("|")
      entries.push({ ...current, value: existingLookup.get(key) ?? defVal })
      return
    }
    for (const level of levelArrays[depth]) {
      recurse(depth + 1, { ...current, [factors[depth]]: level })
    }
  }
  recurse(0, {})
  return entries
}
