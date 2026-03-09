/**
 * Coefficients table for GLM results.
 *
 * Sortable table showing each term's coefficient, standard error, z-value,
 * p-value, and significance stars. Significance coloring on the p-value
 * column makes it easy to spot which terms matter.
 */
import { useState, useMemo } from "react"
import type { TrainResult } from "../../stores/useNodeResultsStore"
import { formatFixed } from "../../utils/formatValue"

interface GLMCoefficientsTabProps {
  result: TrainResult
}

type SortKey = "feature" | "coefficient" | "std_error" | "z_value" | "p_value"
type SortDir = "asc" | "desc"

const SIGNIF_COLORS: Record<string, string> = {
  "***": "var(--signif-high)",
  "**": "var(--signif-med)",
  "*": "var(--signif-low)",
  ".": "var(--signif-marginal)",
  "": "var(--text-muted)",
}

function pColor(p: number): string {
  if (!Number.isFinite(p)) return "var(--text-muted)"
  if (p < 0.001) return "var(--signif-high)"
  if (p < 0.01) return "var(--signif-med)"
  if (p < 0.05) return "var(--signif-low)"
  if (p < 0.1) return "var(--signif-marginal)"
  return "var(--text-muted)"
}

export function GLMCoefficientsTab({ result }: GLMCoefficientsTabProps) {
  const rows = result.glm_coefficients
  const [sortKey, setSortKey] = useState<SortKey>("p_value")
  const [sortDir, setSortDir] = useState<SortDir>("asc")

  const sorted = useMemo(() => {
    if (!rows || rows.length === 0) return []
    return [...rows].sort((a, b) => {
      const av = a[sortKey]
      const bv = b[sortKey]
      if (typeof av === "string" && typeof bv === "string") {
        return sortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av)
      }
      const na = Number(av)
      const nb = Number(bv)
      return sortDir === "asc" ? na - nb : nb - na
    })
  }, [rows, sortKey, sortDir])

  if (!rows || rows.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-xs" style={{ color: "var(--text-muted)" }}>
        No coefficient data available
      </div>
    )
  }

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === "asc" ? "desc" : "asc")
    } else {
      setSortKey(key)
      setSortDir(key === "feature" ? "asc" : "asc")
    }
  }

  const sortIndicator = (key: SortKey) =>
    sortKey === key ? (sortDir === "asc" ? " ▲" : " ▼") : ""

  const columns: { key: SortKey; label: string; align: string }[] = [
    { key: "feature", label: "Term", align: "text-left" },
    { key: "coefficient", label: "Estimate", align: "text-right" },
    { key: "std_error", label: "Std. Error", align: "text-right" },
    { key: "z_value", label: "z", align: "text-right" },
    { key: "p_value", label: "Pr(>|z|)", align: "text-right" },
  ]

  return (
    <div className="space-y-2">
      <div className="overflow-auto" style={{ maxHeight: 480 }}>
        <table className="w-full text-xs font-mono" style={{ borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid var(--border)" }}>
              {columns.map(col => (
                <th
                  key={col.key}
                  onClick={() => handleSort(col.key)}
                  className={`px-2 py-1.5 font-semibold cursor-pointer select-none ${col.align}`}
                  style={{ color: "var(--text-muted)", whiteSpace: "nowrap" }}
                >
                  {col.label}{sortIndicator(col.key)}
                </th>
              ))}
              <th className="px-2 py-1.5 text-center font-semibold" style={{ color: "var(--text-muted)" }}>Sig.</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((row, i) => (
              <tr
                key={row.feature}
                style={{
                  borderBottom: "1px solid var(--border)",
                  background: i % 2 === 0 ? "transparent" : "rgba(255,255,255,.02)",
                }}
              >
                <td className="px-2 py-1 text-left truncate" style={{ color: "var(--text-primary)", maxWidth: 200 }} title={row.feature}>
                  {row.feature}
                </td>
                <td className="px-2 py-1 text-right" style={{ color: "var(--text-primary)" }}>
                  {formatFixed(row.coefficient, 6)}
                </td>
                <td className="px-2 py-1 text-right" style={{ color: "var(--text-secondary)" }}>
                  {formatFixed(row.std_error, 6)}
                </td>
                <td className="px-2 py-1 text-right" style={{ color: "var(--text-secondary)" }}>
                  {formatFixed(row.z_value, 3)}
                </td>
                <td className="px-2 py-1 text-right" style={{ color: pColor(row.p_value) }}>
                  {typeof row.p_value === 'number' && Number.isFinite(row.p_value)
                    ? (row.p_value < 0.0001 ? row.p_value.toExponential(2) : row.p_value.toFixed(4))
                    : 'N/A'}
                </td>
                <td className="px-2 py-1 text-center font-bold" style={{ color: SIGNIF_COLORS[row.significance] || "var(--text-muted)" }}>
                  {row.significance || ""}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Legend */}
      <div className="flex gap-3 text-[10px]" style={{ color: "var(--text-muted)" }}>
        <span>Signif. codes:</span>
        <span style={{ color: "var(--signif-high)" }}>*** &lt; 0.001</span>
        <span style={{ color: "var(--signif-med)" }}>** &lt; 0.01</span>
        <span style={{ color: "var(--signif-low)" }}>* &lt; 0.05</span>
        <span style={{ color: "var(--signif-marginal)" }}>. &lt; 0.1</span>
        <span>{rows.length} term{rows.length !== 1 ? "s" : ""}</span>
      </div>
    </div>
  )
}
