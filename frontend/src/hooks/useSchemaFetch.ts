import { useState, useEffect, useCallback } from "react"
import { fetchSchema } from "../api/client"
import type { SchemaInfo } from "../panels/editors/_shared"

/**
 * Shared hook for fetching file schema (columns, preview, row count).
 *
 * Used by DataSourceEditor and ApiInputEditor to avoid duplicating
 * the same fetch-schema-on-mount + fetch-on-select pattern.
 */
export function useSchemaFetch(initialPath?: string) {
  const [schema, setSchema] = useState<SchemaInfo>(null)
  const [loading, setLoading] = useState(!!initialPath)
  const [error, setError] = useState<string | null>(null)

  const fetchForPath = useCallback((path: string) => {
    setLoading(true)
    setError(null)
    fetchSchema(path)
      .then((data) => {
        setSchema(data)
        setLoading(false)
      })
      .catch((err: unknown) => {
        setSchema(null)
        setError(err instanceof Error ? err.message : String(err))
        setLoading(false)
      })
  }, [])

  // Auto-fetch on mount when path exists
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- fetch-on-mount: fetchForPath sets state asynchronously via .then()
    if (initialPath) fetchForPath(initialPath)
  }, [initialPath, fetchForPath])

  return { schema, setSchema, loading, error, fetchForPath }
}
