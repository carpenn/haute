import { Radio } from "lucide-react"
import { FileBrowser, SchemaPreview } from "./_shared"
import type { OnUpdateConfig } from "./_shared"
import { useSchemaFetch } from "../../hooks/useSchemaFetch"
import { configField } from "../../utils/configField"
import { withAlpha } from "../../utils/color"
import { CacheFetchButton } from "../../components/CacheFetchButton"
import {
  buildJsonCache,
  getJsonCacheProgress,
  getJsonCacheStatus,
  deleteJsonCache,
} from "../../api/client"

// ─── JsonCacheButton ──────────────────────────────────────────────

type JsonCacheStatus = {
  cached: boolean
  path?: string
  data_path: string
  row_count: number
  column_count: number
  size_bytes: number
  cached_at: number
}

function JsonCacheButton({ dataPath }: { dataPath: string }) {
  return (
    <CacheFetchButton<JsonCacheStatus>
      resourceKey={dataPath}
      getStatus={(key) => getJsonCacheStatus(key)}
      startFetch={(key) =>
        buildJsonCache({ path: key }).then(
          (data) => ({ cached: true, ...data }) as JsonCacheStatus,
        )
      }
      getProgress={(key) => getJsonCacheProgress(key)}
      deleteCache={(key) => deleteJsonCache(key) as Promise<JsonCacheStatus>}
      timestampField="cached_at"
      labels={{
        fetchLabel: "Cache as Parquet",
        refreshLabel: "Refresh Cache",
        notCachedHint: "Not cached yet \u2014 click to flatten and cache as Parquet",
        pendingLabel: "Processing...",
      }}
    />
  )
}

// ─── ApiInputEditor ───────────────────────────────────────────────

export default function ApiInputEditor({
  config,
  onUpdate,
  accentColor,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  accentColor: string
}) {
  const currentPath = configField<string | undefined>(config, "path", undefined)
  const { schema, loading: loadingSchema, fetchForPath } = useSchemaFetch(currentPath)
  const showCacheButton = currentPath && (currentPath.endsWith(".json") || currentPath.endsWith(".jsonl"))

  return (
    <>
      <div className="px-4 py-3 space-y-3">
        <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs font-medium"
          style={{ background: withAlpha(accentColor, 0.1), border: `1px solid ${withAlpha(accentColor, 0.3)}`, color: accentColor }}
        >
          <Radio size={14} />
          <span>This node receives live API requests at deploy time</span>
        </div>

        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
            Preview Data
            <span className="ml-1.5 normal-case tracking-normal font-normal">.json or .jsonl</span>
          </label>
          <FileBrowser
            currentPath={currentPath}
            onSelect={(path) => {
              onUpdate("path", path)
              fetchForPath(path)
            }}
            extensions=".json,.jsonl"
          />
        </div>

        {showCacheButton && (
          <JsonCacheButton dataPath={currentPath} />
        )}

      </div>

      {loadingSchema && (
        <div className="px-4 py-3" style={{ borderTop: '1px solid var(--border)' }}>
          <span className="text-xs" style={{ color: 'var(--text-muted)' }}>Loading schema...</span>
        </div>
      )}

      <SchemaPreview schema={schema} />
    </>
  )
}
