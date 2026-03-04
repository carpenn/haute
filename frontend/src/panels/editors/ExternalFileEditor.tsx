import { useState } from "react"
import { InputSourcesBar, FileBrowser, CodeEditor } from "./_shared"
import type { InputSource, OnUpdateConfig } from "./_shared"
import { configField } from "../../utils/configField"

export default function ExternalFileEditor({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
  errorLine,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  errorLine?: number | null
}) {
  const [fileType, setFileType] = useState<string>(configField(config, "fileType", "pickle"))
  const [modelClass, setModelClass] = useState<string>(configField(config, "modelClass", "classifier"))
  const defaultCode = configField(config, "code", "")
  const hasInput = inputSources.length > 0

  const firstInput = inputSources.length > 0 ? inputSources[0].varName : "df"
  const placeholders: Record<string, string> = {
    pickle: hasInput
      ? `df = ${firstInput}.with_columns(\n    prediction=pl.Series(obj.predict(${firstInput}.to_numpy()))\n)`
      : `# obj is the loaded pickle\ndf = pl.DataFrame({"result": [obj]})`,
    json: hasInput
      ? `df = ${firstInput}.with_columns(\n    lookup=${firstInput}["key"].map_elements(lambda k: obj.get(k))\n)`
      : `# obj is the loaded JSON dict/list\ndf = pl.DataFrame(obj)`,
    joblib: hasInput
      ? `df = ${firstInput}.with_columns(\n    prediction=pl.Series(obj.predict(${firstInput}.to_numpy()))\n)`
      : `# obj is the loaded joblib object\ndf = pl.DataFrame({"result": [str(obj)]})`,
    catboost: hasInput
      ? `X = ${firstInput}.select(obj.feature_names_).collect().to_numpy()\npreds = obj.predict(X)\ndf = ${firstInput}.select("id").with_columns(prediction=pl.Series(preds))`
      : `# obj is the loaded CatBoost model\ndf = pl.DataFrame({"prediction": obj.predict([[1, 2, 3]])})`,
  }

  return (
    <div className="flex-1 flex flex-col min-h-0 px-3 py-2 gap-2">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>File Type</label>
        <div className="mt-1 flex gap-1.5">
          {["pickle", "json", "joblib", "catboost"].map((ft) => (
            <button
              key={ft}
              onClick={() => {
                setFileType(ft)
                onUpdate("fileType", ft)
              }}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: fileType === ft ? 'rgba(236,72,153,.1)' : 'var(--bg-input)',
                border: fileType === ft ? '1px solid #ec4899' : '1px solid var(--border)',
                color: fileType === ft ? '#ec4899' : 'var(--text-secondary)',
              }}
            >
              {ft.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      {fileType === "catboost" && (
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Model Type</label>
          <div className="mt-1 flex gap-1.5">
            {["classifier", "regressor"].map((mc) => (
              <button
                key={mc}
                onClick={() => {
                  setModelClass(mc)
                  onUpdate("modelClass", mc)
                }}
                className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
                style={{
                  background: modelClass === mc ? 'rgba(236,72,153,.1)' : 'var(--bg-input)',
                  border: modelClass === mc ? '1px solid #ec4899' : '1px solid var(--border)',
                  color: modelClass === mc ? '#ec4899' : 'var(--text-secondary)',
                }}
              >
                {mc.charAt(0).toUpperCase() + mc.slice(1)}
              </button>
            ))}
          </div>
        </div>
      )}

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
          File Path
        </label>
        <FileBrowser
          currentPath={configField(config, "path", "") || undefined}
          onSelect={(path) => onUpdate("path", path)}
          extensions=".pkl,.pickle,.json,.joblib,.cbm,.onnx,.pmml"
        />
      </div>

      <div className="flex items-center justify-between shrink-0">
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
          Code
        </label>
        <span className="text-[11px] font-medium" style={{ color: 'var(--text-muted)' }}>
          <code className="px-0.5 rounded" style={{ background: 'var(--bg-hover)' }}>obj</code> = loaded file, assign to <code className="px-0.5 rounded" style={{ background: 'var(--bg-hover)' }}>df</code>
        </span>
      </div>
      <CodeEditor
        defaultValue={defaultCode}
        onChange={(val) => onUpdate("code", val)}
        errorLine={errorLine}
        placeholder={placeholders[fileType] || placeholders.pickle}
      />
    </div>
  )
}
