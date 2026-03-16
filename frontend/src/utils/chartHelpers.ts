/**
 * Shared chart formatting helpers used by OptimiserPreview and OptimiserDataPreview.
 */

/** Format a numeric axis tick label with K/M suffixes or exponential notation. */
export function formatAxisLabel(v: number): string {
  if (Math.abs(v) >= 1_000_000) return (v / 1_000_000).toFixed(1) + "M"
  if (Math.abs(v) >= 1_000) return (v / 1_000).toFixed(1) + "K"
  if (Math.abs(v) < 0.01 && v !== 0) return v.toExponential(1)
  if (Number.isInteger(v)) return String(v)
  return v.toFixed(2)
}

/** Generate evenly-spaced tick values between min and max (inclusive). */
export function yTicks(min: number, max: number, count = 4): number[] {
  if (min === max) return [min]
  const step = (max - min) / count
  const ticks: number[] = []
  for (let i = 0; i <= count; i++) ticks.push(min + step * i)
  return ticks
}
