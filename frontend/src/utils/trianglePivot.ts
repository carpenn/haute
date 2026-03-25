/**
 * Triangle pivot utilities — pure functions for building actuarial-style
 * loss-development triangle (cross-tab) data from flat row records.
 */

export type PivotResult = {
  /** Sorted unique Origin Period values (row headers). */
  origins: string[]
  /** Sorted unique Development Period values (column headers). */
  developments: string[]
  /**
   * Sparse map: cells.get(origin)?.get(development) → sum of Value field.
   * Missing combinations are absent (treat as 0 / blank in UI).
   */
  cells: Map<string, Map<string, number>>
}

/**
 * Sort an array of string keys:
 * – numeric ascending when every key is parseable as a finite number;
 * – lexicographic ascending otherwise.
 */
function sortKeys(keys: string[]): string[] {
  const allNumeric = keys.every((k) => k.trim() !== "" && isFinite(Number(k)))
  if (allNumeric) {
    return [...keys].sort((a, b) => Number(a) - Number(b))
  }
  return [...keys].sort((a, b) => a.localeCompare(b))
}

/**
 * Build a pivot table from flat row records.
 *
 * @param rows             - Flat row objects (from the upstream DataSource preview).
 * @param originField      - Field to use as row dimension (Origin Period).
 * @param developmentField - Field to use as column dimension (Development Period).
 * @param valueField       - Field to sum into each cell (Value).
 * @returns PivotResult with sorted origins/developments and a sparse cell map.
 */
export function buildTrianglePivot(
  rows: Record<string, unknown>[],
  originField: string,
  developmentField: string,
  valueField: string,
): PivotResult {
  const cells = new Map<string, Map<string, number>>()
  const originSet = new Set<string>()
  const devSet = new Set<string>()

  for (const row of rows) {
    const origin = row[originField] != null ? String(row[originField]) : ""
    const dev = row[developmentField] != null ? String(row[developmentField]) : ""
    const rawVal = row[valueField]

    let val = 0
    if (rawVal != null) {
      const n = Number(rawVal)
      if (!isNaN(n)) {
        val = n
      } else {
        console.warn(`[Triangle_Viewer] Non-numeric value in "${valueField}":`, rawVal)
      }
    }

    originSet.add(origin)
    devSet.add(dev)

    if (!cells.has(origin)) cells.set(origin, new Map())
    const devMap = cells.get(origin)!
    devMap.set(dev, (devMap.get(dev) ?? 0) + val)
  }

  return {
    origins: sortKeys([...originSet]),
    developments: sortKeys([...devSet]),
    cells,
  }
}
