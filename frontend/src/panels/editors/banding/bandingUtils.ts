import type { BandingFactor } from "../../../types/banding"

export function normaliseBandingFactors(config: Record<string, unknown>): BandingFactor[] {
  const raw = config.factors as BandingFactor[] | undefined
  if (Array.isArray(raw) && raw.length > 0) return raw
  return [{ banding: "continuous", column: "", outputColumn: "", rules: [], default: null }]
}

export function isNumericDtype(dtype: string): boolean {
  const d = dtype.toLowerCase()
  return d.startsWith("int") || d.startsWith("uint") || d.startsWith("float") || d === "f32" || d === "f64" || d === "i8" || d === "i16" || d === "i32" || d === "i64" || d === "u8" || d === "u16" || d === "u32" || d === "u64"
}

export function inferBandingType(colName: string, colMap: Record<string, string>): string | null {
  const dtype = colMap[colName]
  if (!dtype) return null
  return isNumericDtype(dtype) ? "continuous" : "categorical"
}
