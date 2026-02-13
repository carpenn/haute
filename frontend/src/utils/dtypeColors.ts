const dtypeColorMap: Record<string, string> = {
  Int64: "text-blue-400",
  Int32: "text-blue-400",
  Float64: "text-emerald-400",
  Float32: "text-emerald-400",
  String: "text-amber-400",
  Utf8: "text-amber-400",
  Boolean: "text-purple-400",
  Date: "text-rose-400",
  Datetime: "text-rose-400",
}

export function getDtypeColor(dtype: string): string {
  for (const [key, color] of Object.entries(dtypeColorMap)) {
    if (dtype.includes(key)) return color
  }
  return "text-slate-400"
}
