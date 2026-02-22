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

  const fetchForPath = useCallback((path: string) => {
    setLoading(true)
    fetchSchema(path)
      .then((data) => {
        setSchema(data)
        setLoading(false)
      })
      .catch(() => {
        setSchema(null)
        setLoading(false)
      })
  }, [])

  // Auto-fetch on mount when path exists
  useEffect(() => {
    if (initialPath) fetchForPath(initialPath)
  }, [initialPath, fetchForPath])

  return { schema, setSchema, loading, fetchForPath }
}
