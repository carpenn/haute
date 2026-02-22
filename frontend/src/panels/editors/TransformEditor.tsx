import { InputSourcesBar, CodeEditor } from "./_shared"
import type { InputSource } from "./_shared"

export default function TransformEditor({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
}) {
  const defaultCode = (config.code as string) || ""
  const isMultiInput = inputSources.length > 1
  const hasInput = inputSources.length > 0

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-2">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />
      <div className="flex items-center justify-between shrink-0">
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
          Polars Code
        </label>
        <span className="text-[11px] font-medium" style={{ color: 'var(--text-muted)' }}>
          {hasInput ? "use input names" : <>start with <code className="px-0.5 rounded" style={{ background: 'var(--bg-hover)' }}>.</code> to chain</>}
        </span>
      </div>
      <CodeEditor
        defaultValue={defaultCode}
        onChange={(val) => onUpdate("code", val)}
        placeholder={
          isMultiInput
            ? `${inputSources[0].varName}.join(${inputSources[1]?.varName || "other"}, on="key", how="left")`
            : hasInput
              ? `${inputSources[0].varName}\n.with_columns(\n    age=pl.col("YOA") - pl.col("DOB")\n)\n.select("age", "NCD")`
              : `.with_columns(\n    age=pl.col("YOA") - pl.col("DOB")\n)\n.select("age", "NCD")`
        }
      />
    </div>
  )
}
