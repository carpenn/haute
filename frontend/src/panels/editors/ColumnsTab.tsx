import { useState, useMemo } from "react"
import { Search } from "lucide-react"
import { getDtypeColor } from "../../utils/dtypeColors"
import { configField } from "../../utils/configField"
import type { OnUpdateConfig } from "./_shared"

interface ColumnsTabProps {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  /** Full column set before selected_columns filtering */
  availableColumns: { name: string; dtype: string }[]
  /** Current output columns (post-filter) */
  columns: { name: string; dtype: string }[]
}

export default function ColumnsTab({ config, onUpdate, availableColumns, columns }: ColumnsTabProps) {
  const [search, setSearch] = useState("")

  const selectedColumns = configField<string[]>(config, "selected_columns", [])

  // Use available columns if we have them, else fall back to current columns
  const allColumns = availableColumns.length > 0 ? availableColumns : columns

  const filtered = useMemo(() => {
    if (!search) return allColumns
    const q = search.toLowerCase()
    return allColumns.filter((c) => c.name.toLowerCase().includes(q))
  }, [allColumns, search])

  // When selected_columns is empty, all columns are kept
  const isAllSelected = selectedColumns.length === 0

  const isSelected = (col: string) => isAllSelected || selectedColumns.includes(col)

  const toggleColumn = (col: string) => {
    if (isAllSelected) {
      // Switching from "all" to explicit: keep everything except the toggled column
      const next = allColumns.map((c) => c.name).filter((c) => c !== col)
      onUpdate("selected_columns", next)
    } else if (selectedColumns.includes(col)) {
      const next = selectedColumns.filter((c) => c !== col)
      // If nothing selected, revert to "all"
      onUpdate("selected_columns", next.length > 0 ? next : [])
    } else {
      const next = [...selectedColumns, col]
      // If all columns are now selected, revert to "all" (empty = all)
      if (next.length >= allColumns.length) {
        onUpdate("selected_columns", [])
      } else {
        onUpdate("selected_columns", next)
      }
    }
  }

  const selectAll = () => onUpdate("selected_columns", [])

  const selectNone = () => {
    // Keep at least one column to avoid empty DataFrames
    if (allColumns.length > 0) {
      onUpdate("selected_columns", [allColumns[0].name])
    }
  }

  const selectedCount = isAllSelected ? allColumns.length : selectedColumns.length

  if (allColumns.length === 0) {
    return (
      <div className="px-4 py-6 text-center">
        <p className="text-xs" style={{ color: "var(--text-muted)" }}>
          Preview or run this node to see its output columns
        </p>
      </div>
    )
  }

  return (
    <div className="px-4 py-3 flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <span className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: "var(--text-muted)" }}>
          Output Columns
        </span>
        <span className="text-[11px]" style={{ color: "var(--text-muted)" }}>
          {selectedCount} / {allColumns.length}
        </span>
      </div>

      {/* Search + select all / none */}
      <div className="flex items-center gap-2">
        <div className="flex-1 relative">
          <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2" style={{ color: "var(--text-muted)" }} />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Filter columns..."
            className="w-full pl-7 pr-2 py-1 text-xs rounded-md border bg-transparent focus:outline-none focus:ring-1"
            style={{ color: "var(--text-primary)", borderColor: "var(--border)", background: "var(--bg-input)" }}
          />
        </div>
        <button
          onClick={selectAll}
          className="text-[10px] font-medium px-1.5 py-0.5 rounded"
          style={{ color: isAllSelected ? "var(--text-muted)" : "var(--accent)" }}
          disabled={isAllSelected}
        >
          All
        </button>
        <button
          onClick={selectNone}
          className="text-[10px] font-medium px-1.5 py-0.5 rounded"
          style={{ color: "var(--accent)" }}
        >
          None
        </button>
      </div>

      {/* Column table */}
      <div className="rounded-lg overflow-hidden max-h-[400px] overflow-y-auto" style={{ border: "1px solid var(--border)", background: "var(--bg-input)" }}>
        <table className="w-full text-xs">
          <thead>
            <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-elevated)" }}>
              <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: "var(--text-muted)", width: 28 }}></th>
              <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: "var(--text-muted)" }}>Column</th>
              <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: "var(--text-muted)" }}>Type</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((col) => {
              const checked = isSelected(col.name)
              return (
                <tr
                  key={col.name}
                  className="cursor-pointer transition-colors"
                  style={{ borderBottom: "1px solid var(--border)" }}
                  onClick={() => toggleColumn(col.name)}
                  onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg-hover)" }}
                  onMouseLeave={(e) => { e.currentTarget.style.background = "transparent" }}
                >
                  <td className="px-2.5 py-1.5 text-center">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleColumn(col.name)}
                      onClick={(e) => e.stopPropagation()}
                      className="accent-blue-500 rounded"
                    />
                  </td>
                  <td className="px-2.5 py-1.5 font-mono" style={{ color: checked ? "var(--text-primary)" : "var(--text-muted)" }}>
                    {col.name}
                  </td>
                  <td className="px-2.5 py-1.5">
                    <span className={`text-[11px] font-medium ${getDtypeColor(col.dtype)}`}>{col.dtype}</span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {!isAllSelected && (
        <p className="text-[10px] leading-relaxed" style={{ color: "var(--text-muted)" }}>
          Deselected columns will be dropped via <code className="font-mono">.select()</code> on this node's output.
        </p>
      )}
    </div>
  )
}
