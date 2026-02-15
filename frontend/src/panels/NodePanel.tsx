import { useState, useEffect, useRef, useCallback } from "react"
import { X, Folder, FileText, ChevronLeft, Check, Database, Table2, HardDriveDownload, Radio } from "lucide-react"
import { getDtypeColor } from "../utils/dtypeColors"
import { sanitizeName } from "../utils/sanitizeName"

type FileItem = {
  name: string
  path: string
  type: "file" | "directory"
  size?: number
}

type SchemaColumn = {
  name: string
  dtype: string
}

type InputSource = {
  varName: string
  sourceLabel: string
  edgeId: string
}

type SchemaInfo = {
  path: string
  columns: SchemaColumn[]
  row_count: number
  column_count: number
  preview: Record<string, unknown>[]
} | null

type SimpleNode = {
  id: string
  type?: string
  data: {
    label: string
    description: string
    nodeType: string
    config?: Record<string, unknown>
  }
}

type SimpleEdge = {
  id: string
  source: string
  target: string
}

type NodePanelProps = {
  node: SimpleNode | null
  edges: SimpleEdge[]
  allNodes: SimpleNode[]
  onClose: () => void
  onUpdateNode?: (id: string, data: Record<string, unknown>) => void
  onDeleteEdge?: (edgeId: string) => void
}


function FileBrowser({ currentPath, onSelect, extensions }: { currentPath?: string; onSelect: (path: string) => void; extensions?: string }) {
  const [dir, setDir] = useState(".")
  const [items, setItems] = useState<FileItem[]>([])
  const [loading, setLoading] = useState(false)
  const [selectedPath, setSelectedPath] = useState<string | undefined>(currentPath)

  useEffect(() => {
    setLoading(true)
    fetch(`/api/files?dir=${encodeURIComponent(dir)}${extensions ? `&extensions=${encodeURIComponent(extensions)}` : ``}`)
      .then((r) => r.json())
      .then((data) => {
        setItems(data.items || [])
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [dir, extensions])

  const goUp = () => {
    if (dir === ".") return
    const parts = dir.split("/")
    parts.pop()
    setDir(parts.length > 0 ? parts.join("/") : ".")
  }

  const handleFileClick = (path: string) => {
    setSelectedPath(path)
    onSelect(path)
  }

  const formatSize = (bytes: number) => {
    if (bytes > 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
    return `${(bytes / 1024).toFixed(1)} KB`
  }

  return (
    <div>
      {selectedPath && (
        <div className="mb-2 px-2.5 py-2 rounded-lg flex items-center gap-2" style={{ background: 'rgba(34,197,94,.1)', border: '1px solid rgba(34,197,94,.2)' }}>
          <Check size={14} style={{ color: '#22c55e' }} className="shrink-0" />
          <span className="text-xs font-mono truncate" style={{ color: '#4ade80' }}>{selectedPath}</span>
        </div>
      )}

      <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)' }}>
        <div className="px-2 py-1.5 flex items-center gap-1.5" style={{ background: 'var(--bg-elevated)', borderBottom: '1px solid var(--border)' }}>
          <button
            onClick={goUp}
            disabled={dir === "."}
            className="p-0.5 rounded disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            style={{ color: 'var(--text-secondary)' }}
          >
            <ChevronLeft size={14} />
          </button>
          <span className="text-xs font-mono truncate" style={{ color: 'var(--text-muted)' }}>{dir === "." ? "/" : dir}</span>
        </div>

        <div className="max-h-40 overflow-y-auto" style={{ background: 'var(--bg-input)' }}>
          {loading ? (
            <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>Loading...</div>
          ) : items.length === 0 ? (
            <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>No matching files</div>
          ) : (
            items.map((item) => {
              const isSelected = item.type === "file" && item.path === selectedPath
              return (
                <button
                  key={item.path}
                  onClick={() => {
                    if (item.type === "directory") {
                      setDir(item.path)
                    } else {
                      handleFileClick(item.path)
                    }
                  }}
                  className="w-full px-3 py-2 flex items-center gap-2 text-left transition-colors"
                  style={{
                    borderBottom: '1px solid var(--border)',
                    background: isSelected ? 'var(--accent-soft)' : 'transparent',
                  }}
                  onMouseEnter={(e) => { if (!isSelected) e.currentTarget.style.background = 'var(--bg-hover)' }}
                  onMouseLeave={(e) => { if (!isSelected) e.currentTarget.style.background = 'transparent' }}
                >
                  {item.type === "directory" ? (
                    <Folder size={14} style={{ color: '#f59e0b' }} className="shrink-0" />
                  ) : isSelected ? (
                    <Check size={14} style={{ color: 'var(--accent)' }} className="shrink-0" />
                  ) : (
                    <FileText size={14} style={{ color: 'var(--text-muted)' }} className="shrink-0" />
                  )}
                  <span className="text-xs truncate" style={{ color: isSelected ? 'var(--accent)' : 'var(--text-secondary)', fontWeight: isSelected ? 500 : 400 }}>
                    {item.name}
                  </span>
                  {item.size !== undefined && (
                    <span className="text-[11px] ml-auto shrink-0" style={{ color: 'var(--text-muted)' }}>
                      {formatSize(item.size)}
                    </span>
                  )}
                </button>
              )
            })
          )}
        </div>
      </div>
    </div>
  )
}

function SchemaPreview({ schema }: { schema: SchemaInfo }) {
  const [showPreview, setShowPreview] = useState(false)

  if (!schema) return null

  return (
    <div style={{ borderTop: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
      <div className="px-4 py-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Table2 size={14} style={{ color: 'var(--text-muted)' }} />
          <span className="text-xs font-semibold" style={{ color: 'var(--text-primary)' }}>Schema</span>
          <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
            {schema.column_count} cols / {schema.row_count.toLocaleString()} rows
          </span>
        </div>
        <button
          onClick={() => setShowPreview(!showPreview)}
          className="text-[11px] font-medium" style={{ color: 'var(--accent)' }}
        >
          {showPreview ? "Hide preview" : "Show preview"}
        </button>
      </div>

      <div className="px-4 pb-3">
        <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)', background: 'var(--bg-input)' }}>
          <table className="w-full text-xs">
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
                <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Column</th>
                <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Type</th>
              </tr>
            </thead>
            <tbody>
              {schema.columns.map((col) => (
                <tr key={col.name} style={{ borderBottom: '1px solid var(--border)' }}>
                  <td className="px-2.5 py-1.5 font-mono" style={{ color: 'var(--text-primary)' }}>{col.name}</td>
                  <td className="px-2.5 py-1.5">
                    <span className={`text-[11px] font-medium ${getDtypeColor(col.dtype)}`}>
                      {col.dtype}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {showPreview && schema.preview.length > 0 && (
          <div className="mt-2 rounded-lg overflow-x-auto" style={{ border: '1px solid var(--border)', background: 'var(--bg-input)' }}>
            <table className="w-full text-[11px]">
              <thead>
                <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
                  {schema.columns.map((col) => (
                    <th key={col.name} className="text-left px-2 py-1 font-semibold whitespace-nowrap" style={{ color: 'var(--text-muted)' }}>
                      {col.name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {schema.preview.map((row, i) => (
                  <tr key={i} style={{ borderBottom: '1px solid var(--border)' }}>
                    {schema.columns.map((col) => (
                      <td key={col.name} className="px-2 py-1 font-mono whitespace-nowrap" style={{ color: 'var(--text-secondary)' }}>
                        {String(row[col.name] ?? "")}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

function DataSourceConfig({
  config,
  onUpdate,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
}) {
  const [sourceType, setSourceType] = useState<string>((config.sourceType as string) || "flat_file")
  const [schema, setSchema] = useState<SchemaInfo>(null)
  const [loadingSchema, setLoadingSchema] = useState(false)

  const fetchSchema = (path: string) => {
    setLoadingSchema(true)
    fetch(`/api/schema?path=${encodeURIComponent(path)}`)
      .then((r) => r.json())
      .then((data) => {
        setSchema(data)
        setLoadingSchema(false)
      })
      .catch(() => setLoadingSchema(false))
  }

  useEffect(() => {
    if (config.path) {
      fetchSchema(config.path as string)
    }
  }, [])

  return (
    <>
      <div className="px-4 py-3 space-y-3">
        <div>
          <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Source Type</label>
          <div className="mt-1 flex gap-1.5">
            <button
              onClick={() => {
                setSourceType("flat_file")
                onUpdate("sourceType", "flat_file")
              }}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: sourceType === "flat_file" ? 'var(--accent-soft)' : 'var(--bg-input)',
                border: sourceType === "flat_file" ? '1px solid var(--accent)' : '1px solid var(--border)',
                color: sourceType === "flat_file" ? 'var(--accent)' : 'var(--text-secondary)',
              }}
            >
              <FileText size={12} />
              Flat File
            </button>
            <button
              onClick={() => {
                setSourceType("databricks")
                onUpdate("sourceType", "databricks")
              }}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: sourceType === "databricks" ? 'var(--accent-soft)' : 'var(--bg-input)',
                border: sourceType === "databricks" ? '1px solid var(--accent)' : '1px solid var(--border)',
                color: sourceType === "databricks" ? 'var(--accent)' : 'var(--text-secondary)',
              }}
            >
              <Database size={12} />
              Databricks
            </button>
          </div>
        </div>

        {/* API Input toggle */}
        <div>
          <button
            onClick={() => {
              const next = !config.deploy_input
              onUpdate("deploy_input", next || undefined)
            }}
            className="w-full flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs font-medium transition-colors"
            style={{
              background: config.deploy_input ? 'rgba(34,197,94,.1)' : 'var(--bg-input)',
              border: config.deploy_input ? '1px solid rgba(34,197,94,.3)' : '1px solid var(--border)',
              color: config.deploy_input ? '#22c55e' : 'var(--text-secondary)',
            }}
          >
            <Radio size={14} />
            <span>API Input</span>
            <span className="ml-auto text-[11px]" style={{ color: 'var(--text-muted)' }}>
              {config.deploy_input ? 'This source receives live requests' : 'Mark as live API input'}
            </span>
          </button>
        </div>

        {sourceType === "flat_file" && (
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
              File
            </label>
            <FileBrowser
              currentPath={config.path as string | undefined}
              onSelect={(path) => {
                onUpdate("path", path)
                fetchSchema(path)
              }}
            />
          </div>
        )}

        {sourceType === "databricks" && (
          <div>
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Table</label>
            <input
              type="text"
              placeholder="catalog.schema.table"
              defaultValue={(config.table as string) || ""}
              onChange={(e) => onUpdate("table", e.target.value)}
              className="mt-1 w-full px-2.5 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
              style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
              onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(59,130,246,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
              onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
            />
          </div>
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

const PAIRS: Record<string, string> = { "(": ")", "[": "]", "{": "}", "'": "'", '"': '"' }
const CLOSE_CHARS = new Set([")", "]", "}"])

function CodeEditor({
  defaultValue,
  onChange,
  placeholder,
}: {
  defaultValue: string
  onChange: (value: string) => void
  placeholder?: string
}) {
  const [code, setCode] = useState(defaultValue)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const gutterRef = useRef<HTMLDivElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [focused, setFocused] = useState(false)

  const lineCount = Math.max((code || "").split("\n").length, 1)

  // Undo-safe text insertion — goes through the browser input pipeline
  // so Ctrl+Z / Ctrl+Shift+Z work natively.
  const insertText = useCallback((ta: HTMLTextAreaElement, text: string) => {
    ta.focus()
    document.execCommand("insertText", false, text)
    // Sync React state with the DOM value
    setCode(ta.value)
    onChange(ta.value)
  }, [onChange])

  // Replace a range and place cursor at `cursorPos`
  const replaceRange = useCallback((ta: HTMLTextAreaElement, start: number, end: number, text: string, cursorPos?: number) => {
    ta.focus()
    ta.setSelectionRange(start, end)
    document.execCommand("insertText", false, text)
    if (cursorPos !== undefined) {
      ta.setSelectionRange(cursorPos, cursorPos)
    }
    setCode(ta.value)
    onChange(ta.value)
  }, [onChange])

  // Replace a range and select the result
  const replaceRangeSelect = useCallback((ta: HTMLTextAreaElement, start: number, end: number, text: string, selStart: number, selEnd: number) => {
    ta.focus()
    ta.setSelectionRange(start, end)
    document.execCommand("insertText", false, text)
    ta.setSelectionRange(selStart, selEnd)
    setCode(ta.value)
    onChange(ta.value)
  }, [onChange])

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLTextAreaElement>) => {
      setCode(e.target.value)
      onChange(e.target.value)
    },
    [onChange],
  )

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      const ta = e.currentTarget
      const { selectionStart: start, selectionEnd: end, value: val } = ta
      const hasSelection = start !== end

      // --- Tab / Shift+Tab: multi-line indent/dedent ---
      if (e.key === "Tab") {
        e.preventDefault()
        if (hasSelection) {
          const lineStart = val.lastIndexOf("\n", start - 1) + 1
          const lineEnd = val.indexOf("\n", end - 1)
          const blockEnd = lineEnd === -1 ? val.length : lineEnd
          const block = val.substring(lineStart, blockEnd)
          const lines = block.split("\n")

          let newBlock: string
          if (e.shiftKey) {
            newBlock = lines.map((l) => l.startsWith("    ") ? l.slice(4) : l.replace(/^\t/, "")).join("\n")
          } else {
            newBlock = lines.map((l) => "    " + l).join("\n")
          }

          const delta = newBlock.length - block.length
          replaceRangeSelect(ta, lineStart, blockEnd, newBlock, lineStart, blockEnd + delta)
        } else {
          if (e.shiftKey) {
            const lineStart = val.lastIndexOf("\n", start - 1) + 1
            const lineText = val.substring(lineStart, start)
            if (lineText.endsWith("    ")) {
              replaceRange(ta, start - 4, start, "", start - 4)
            } else if (lineText.endsWith("\t")) {
              replaceRange(ta, start - 1, start, "", start - 1)
            }
          } else {
            insertText(ta, "    ")
          }
        }
        return
      }

      // --- Quote / bracket wrap selection or auto-close ---
      if (PAIRS[e.key]) {
        const open = e.key
        const close = PAIRS[e.key]
        if (hasSelection) {
          e.preventDefault()
          const selected = val.substring(start, end)
          const wrapped = open + selected + close
          replaceRangeSelect(ta, start, end, wrapped, start + 1, end + 1)
          return
        }
        // Auto-close pair (no selection)
        // For quotes, don't auto-close if the char before cursor is alphanumeric (mid-word)
        if (open === "'" || open === '"') {
          const charBefore = start > 0 ? val[start - 1] : ""
          if (/\w/.test(charBefore)) return // let browser handle normally
          // If cursor is right before the same closing quote, skip over it
          if (val[start] === open) {
            e.preventDefault()
            ta.setSelectionRange(start + 1, start + 1)
            return
          }
        }
        e.preventDefault()
        insertText(ta, open + close)
        ta.setSelectionRange(start + 1, start + 1)
        return
      }

      // --- Skip over closing bracket/quote if already there ---
      if (CLOSE_CHARS.has(e.key) && val[start] === e.key && !hasSelection) {
        e.preventDefault()
        ta.setSelectionRange(start + 1, start + 1)
        return
      }

      // --- Backspace: delete matching pair ---
      if (e.key === "Backspace" && !hasSelection && start > 0) {
        const before = val[start - 1]
        const after = val[start]
        if (PAIRS[before] && PAIRS[before] === after) {
          e.preventDefault()
          replaceRange(ta, start - 1, start + 1, "", start - 1)
          return
        }
      }

      // --- Enter: auto-indent + extra indent after colon ---
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault()
        const lineStart = val.lastIndexOf("\n", start - 1) + 1
        const currentLine = val.substring(lineStart, start)
        const indentMatch = currentLine.match(/^(\s*)/)
        let indent = indentMatch ? indentMatch[1] : ""
        const trimmedLine = currentLine.trimEnd()
        if (trimmedLine.endsWith(":")) {
          indent += "    "
        }
        insertText(ta, "\n" + indent)
        return
      }

      // --- Home: smart home (toggle between start-of-text and column 0) ---
      if (e.key === "Home" && !e.ctrlKey && !e.metaKey) {
        e.preventDefault()
        const lineStart = val.lastIndexOf("\n", start - 1) + 1
        const lineText = val.substring(lineStart)
        const textStart = lineStart + (lineText.match(/^\s*/)?.[0].length ?? 0)
        const target = start === textStart ? lineStart : textStart
        if (e.shiftKey) {
          ta.setSelectionRange(target, end)
        } else {
          ta.setSelectionRange(target, target)
        }
        return
      }

      // --- Ctrl+D: duplicate line or selection ---
      if (e.key === "d" && (e.ctrlKey || e.metaKey)) {
        e.preventDefault()
        if (hasSelection) {
          const selected = val.substring(start, end)
          replaceRangeSelect(ta, end, end, selected, end, end + selected.length)
        } else {
          const lineStart = val.lastIndexOf("\n", start - 1) + 1
          let lineEnd = val.indexOf("\n", start)
          if (lineEnd === -1) lineEnd = val.length
          const line = val.substring(lineStart, lineEnd)
          const offset = start - lineStart
          replaceRange(ta, lineEnd, lineEnd, "\n" + line, lineEnd + 1 + offset)
        }
        return
      }

      // --- Ctrl+/: toggle comment ---
      if (e.key === "/" && (e.ctrlKey || e.metaKey)) {
        e.preventDefault()
        const lineStart = val.lastIndexOf("\n", start - 1) + 1
        const lineEnd = hasSelection ? (val.indexOf("\n", end - 1) === -1 ? val.length : val.indexOf("\n", end - 1)) : (val.indexOf("\n", start) === -1 ? val.length : val.indexOf("\n", start))
        const block = val.substring(lineStart, lineEnd)
        const lines = block.split("\n")
        const allCommented = lines.every((l) => l.trimStart().startsWith("# ") || l.trim() === "")
        let newBlock: string
        if (allCommented) {
          newBlock = lines.map((l) => l.trim() === "" ? l : l.replace(/^(\s*)# /, "$1")).join("\n")
        } else {
          newBlock = lines.map((l) => l.trim() === "" ? l : l.replace(/^(\s*)/, "$1# ")).join("\n")
        }
        const delta = newBlock.length - block.length
        replaceRangeSelect(ta, lineStart, lineEnd, newBlock, lineStart, lineEnd + delta)
        return
      }

      // --- Ctrl+A: select all within editor ---
      if (e.key === "a" && (e.ctrlKey || e.metaKey)) {
        e.preventDefault()
        ta.setSelectionRange(0, val.length)
        return
      }
    },
    [insertText, replaceRange, replaceRangeSelect],
  )

  const handleScroll = useCallback(() => {
    if (textareaRef.current && gutterRef.current) {
      gutterRef.current.scrollTop = textareaRef.current.scrollTop
    }
  }, [])

  return (
    <div
      ref={containerRef}
      className="flex-1 min-h-[120px] rounded-lg overflow-hidden"
      style={{
        border: focused ? '1px solid rgba(59,130,246,.3)' : '1px solid var(--border)',
        boxShadow: focused ? '0 0 0 2px var(--accent-soft)' : 'none',
        background: 'var(--bg-input)',
      }}
    >
      <div className="flex h-full">
        <div
          ref={gutterRef}
          className="shrink-0 overflow-hidden select-none py-2.5"
          style={{
            background: 'var(--bg-elevated)',
            borderRight: '1px solid var(--border)',
            width: lineCount >= 100 ? 44 : 34,
          }}
        >
          {Array.from({ length: lineCount }, (_, i) => (
            <div
              key={i}
              className="text-right pr-2 font-mono"
              style={{
                color: 'var(--text-muted)',
                fontSize: '12px',
                lineHeight: '1.625',
                height: '19.5px',
              }}
            >
              {i + 1}
            </div>
          ))}
        </div>
        <textarea
          ref={textareaRef}
          defaultValue={defaultValue}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onScroll={handleScroll}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          spellCheck={false}
          placeholder={placeholder}
          className="flex-1 w-full h-full pl-2.5 pr-3 py-2.5 text-[12px] font-mono focus:outline-none resize-none"
          style={{
            background: 'transparent',
            color: '#a5f3fc',
            caretColor: 'var(--accent)',
            lineHeight: '1.625',
          }}
        />
      </div>
    </div>
  )
}

function InputSourcesBar({
  inputSources,
  onDeleteInput,
}: {
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
}) {
  if (inputSources.length === 0) return null
  return (
    <div className="rounded-lg px-3 py-1.5 shrink-0" style={{ background: 'var(--bg-input)', border: '1px solid var(--border)' }}>
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>
          {inputSources.length > 1 ? "Inputs" : "Input"}
        </span>
        {inputSources.map((src) => (
          <span key={src.varName} className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded" style={{ background: 'var(--accent-soft)' }}>
            <code className="text-[11px] font-semibold" style={{ color: 'var(--accent)' }}>{src.varName}</code>
            {onDeleteInput && (
              <button
                onClick={() => onDeleteInput(src.edgeId)}
                className="p-0 rounded transition-colors"
                style={{ color: 'var(--text-muted)' }}
                onMouseEnter={(e) => { e.currentTarget.style.color = '#ef4444' }}
                onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)' }}
                title={`Remove connection from ${src.sourceLabel}`}
              >
                <X size={10} />
              </button>
            )}
          </span>
        ))}
      </div>
    </div>
  )
}

function TransformConfig({
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

function ExternalFileConfig({
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
  const [fileType, setFileType] = useState<string>((config.fileType as string) || "pickle")
  const [modelClass, setModelClass] = useState<string>((config.modelClass as string) || "classifier")
  const defaultCode = (config.code as string) || ""
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
          currentPath={(config.path as string) || undefined}
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
        placeholder={placeholders[fileType] || placeholders.pickle}
      />
    </div>
  )
}

function DataSinkConfig({
  config,
  onUpdate,
  nodeId,
  allNodes,
  edges,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  nodeId: string
  allNodes: SimpleNode[]
  edges: SimpleEdge[]
}) {
  const [format, setFormat] = useState<string>((config.format as string) || "parquet")
  const [writing, setWriting] = useState(false)
  const [writeResult, setWriteResult] = useState<{ status: string; message: string } | null>(null)

  const hasPath = Boolean(config.path)

  const handleWrite = () => {
    if (!hasPath || writing) return
    setWriting(true)
    setWriteResult(null)

    const graph = {
      nodes: allNodes.map((n) => ({ id: n.id, type: n.type || n.data.nodeType, data: n.data, position: { x: 0, y: 0 } })),
      edges: edges,
    }

    fetch("/api/pipeline/sink", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ graph, nodeId }),
    })
      .then((r) => r.json())
      .then((data) => {
        setWriteResult({ status: data.status || "ok", message: data.message || "Written successfully" })
        setWriting(false)
      })
      .catch((err) => {
        setWriteResult({ status: "error", message: err.message })
        setWriting(false)
      })
  }

  return (
    <div className="px-4 py-3 space-y-3">
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Format</label>
        <div className="mt-1 flex gap-1.5">
          {["parquet", "csv"].map((fmt) => (
            <button
              key={fmt}
              onClick={() => {
                setFormat(fmt)
                onUpdate("format", fmt)
              }}
              className="flex-1 flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg text-xs font-medium transition-colors"
              style={{
                background: format === fmt ? 'rgba(245,158,11,.1)' : 'var(--bg-input)',
                border: format === fmt ? '1px solid #f59e0b' : '1px solid var(--border)',
                color: format === fmt ? '#f59e0b' : 'var(--text-secondary)',
              }}
            >
              {fmt.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5 block" style={{ color: 'var(--text-muted)' }}>
          Output Path
        </label>
        <input
          type="text"
          placeholder={format === "csv" ? "output/results.csv" : "output/results.parquet"}
          defaultValue={(config.path as string) || ""}
          onChange={(e) => onUpdate("path", e.target.value)}
          className="w-full px-2.5 py-1.5 text-xs font-mono rounded-lg focus:outline-none focus:ring-2"
          style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
          onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(245,158,11,.3)'; e.currentTarget.style.boxShadow = '0 0 0 2px rgba(245,158,11,.1)' }}
          onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.boxShadow = 'none' }}
        />
      </div>

      <button
        onClick={handleWrite}
        disabled={!hasPath || writing}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 text-[12px] font-semibold rounded-lg transition-colors disabled:opacity-40"
        style={{ background: '#f59e0b', color: '#000' }}
        onMouseEnter={(e) => { if (hasPath && !writing) e.currentTarget.style.background = '#fbbf24' }}
        onMouseLeave={(e) => { e.currentTarget.style.background = '#f59e0b' }}
      >
        <HardDriveDownload size={14} />
        {writing ? "Writing..." : "Write"}
      </button>

      {writeResult && (
        <div
          className="px-2.5 py-2 rounded-lg text-xs"
          style={{
            background: writeResult.status === "ok" ? 'rgba(34,197,94,.1)' : 'rgba(239,68,68,.1)',
            border: writeResult.status === "ok" ? '1px solid rgba(34,197,94,.2)' : '1px solid rgba(239,68,68,.2)',
            color: writeResult.status === "ok" ? '#4ade80' : '#f87171',
          }}
        >
          {writeResult.message}
        </div>
      )}
    </div>
  )
}

function OutputConfig({
  config,
  onUpdate,
  nodeId,
  allNodes,
  edges,
}: {
  config: Record<string, unknown>
  onUpdate: (key: string, value: unknown) => void
  nodeId: string
  allNodes: SimpleNode[]
  edges: SimpleEdge[]
}) {
  const fields = (config.fields as string[]) || []

  // Read cached columns from the upstream node (populated by preview/run)
  const incomingEdge = edges.find((e) => e.target === nodeId)
  const upstreamNode = incomingEdge ? allNodes.find((n) => n.id === incomingEdge.source) : null
  const upstreamColumns = ((upstreamNode?.data as Record<string, unknown>)?._columns as { name: string; dtype: string }[]) || []

  const toggleField = (col: string) => {
    const next = fields.includes(col) ? fields.filter((f) => f !== col) : [...fields, col]
    onUpdate("fields", next)
  }

  return (
    <div className="px-4 py-3 space-y-3">
      <div>
        <label className="text-[11px] font-bold uppercase tracking-[0.08em] block mb-2" style={{ color: 'var(--text-muted)' }}>Response Fields</label>

        {upstreamColumns.length === 0 ? (
          <div className="text-xs py-3" style={{ color: 'var(--text-muted)' }}>Preview or run the upstream node to see columns</div>
        ) : (
          <div className="rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)', background: 'var(--bg-input)' }}>
            <table className="w-full text-xs">
              <thead>
                <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
                  <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)', width: 28 }}></th>
                  <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Column</th>
                  <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: 'var(--text-muted)' }}>Type</th>
                </tr>
              </thead>
              <tbody>
                {upstreamColumns.map((col) => {
                  const included = fields.includes(col.name)
                  return (
                    <tr key={col.name} style={{ borderBottom: '1px solid var(--border)' }}>
                      <td className="px-2.5 py-1.5 text-center">
                        <input
                          type="checkbox"
                          checked={included}
                          onChange={() => toggleField(col.name)}
                          className="accent-rose-500 rounded"
                        />
                      </td>
                      <td className="px-2.5 py-1.5 font-mono" style={{ color: included ? 'var(--text-primary)' : 'var(--text-muted)' }}>{col.name}</td>
                      <td className="px-2.5 py-1.5">
                        <span className={`text-[11px] font-medium ${getDtypeColor(col.dtype)}`}>{col.dtype}</span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {fields.length > 0 && (
        <div className="rounded-lg px-3 py-2" style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border)' }}>
          <div className="text-[11px] font-bold uppercase tracking-[0.08em] mb-1.5" style={{ color: 'var(--text-muted)' }}>JSON Preview</div>
          <pre className="text-[11px] font-mono leading-relaxed" style={{ color: 'var(--text-secondary)' }}>
{`{\n${fields.map((f) => `  "${f}": ...`).join(",\n")}\n}`}
          </pre>
        </div>
      )}
    </div>
  )
}

export default function NodePanel({ node, edges, allNodes, onClose, onUpdateNode, onDeleteEdge }: NodePanelProps) {
  if (!node) return null

  const config = (node.data.config || {}) as Record<string, unknown>
  const isDataSource = node.data.nodeType === "dataSource"
  const isDataSink = node.data.nodeType === "dataSink"
  const isExternalFile = node.data.nodeType === "externalFile"
  const isOutput = node.data.nodeType === "output"
  const isTransform = node.data.nodeType === "transform"

  // Compute input sources — variable name = sanitized source node label
  const nodeMap = Object.fromEntries(allNodes.map((n) => [n.id, n]))
  const inputSources: InputSource[] = edges
    .filter((e) => e.target === node.id)
    .map((e) => ({
      varName: sanitizeName(nodeMap[e.source]?.data.label || e.source),
      sourceLabel: nodeMap[e.source]?.data.label || e.source,
      edgeId: e.id,
    }))

  const handleConfigUpdate = (key: string, value: unknown) => {
    const newConfig = { ...config, [key]: value }
    if (onUpdateNode) {
      onUpdateNode(node.id, { ...node.data, config: newConfig })
    }
  }

  return (
    <div key={node.id} className="w-[min(400px,40vw)] h-full overflow-y-auto shrink-0 flex flex-col animate-slide-in" style={{ background: 'var(--bg-panel)', borderLeft: '1px solid var(--border)' }}>
      <div className="px-3 py-2.5 flex items-center gap-2 shrink-0" style={{ borderBottom: '1px solid var(--border)' }}>
        <input
          type="text"
          defaultValue={node.data.label}
          onChange={(e) => {
            if (onUpdateNode) {
              onUpdateNode(node.id, { ...node.data, label: e.target.value })
            }
          }}
          className="flex-1 min-w-0 px-2 py-1 text-[13px] font-semibold border border-transparent rounded-md focus:outline-none focus:ring-2 bg-transparent"
          style={{ color: 'var(--text-primary)', borderColor: 'transparent' }}
          onFocus={(e) => { e.currentTarget.style.borderColor = 'var(--accent)'; e.currentTarget.style.boxShadow = '0 0 0 2px var(--accent-soft)' }}
          onBlur={(e) => { e.currentTarget.style.borderColor = 'transparent'; e.currentTarget.style.boxShadow = 'none' }}
        />
        <span className="text-[11px] font-mono shrink-0" style={{ color: 'var(--text-muted)' }}>{node.id}</span>
        <button onClick={onClose} className="p-1 rounded shrink-0 transition-colors" style={{ color: 'var(--text-muted)' }}
          onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-hover)'}
          onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
        >
          <X size={14} />
        </button>
      </div>

      {isDataSource ? (
        <DataSourceConfig config={config} onUpdate={handleConfigUpdate} />
      ) : isDataSink ? (
        <DataSinkConfig config={config} onUpdate={handleConfigUpdate} nodeId={node.id} allNodes={allNodes} edges={edges} />
      ) : isExternalFile ? (
        <ExternalFileConfig config={config} onUpdate={handleConfigUpdate} inputSources={inputSources} onDeleteInput={onDeleteEdge} />
      ) : isOutput ? (
        <OutputConfig config={config} onUpdate={handleConfigUpdate} nodeId={node.id} allNodes={allNodes} edges={edges} />
      ) : isTransform ? (
        <TransformConfig config={config} onUpdate={handleConfigUpdate} inputSources={inputSources} onDeleteInput={onDeleteEdge} />
      ) : (
        Object.keys(config).length > 0 && (
          <div className="px-4 py-3">
            <label className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}>Config</label>
            {Object.entries(config).map(([key, value]) => (
              <div key={key} className="mt-1.5 flex items-center gap-2">
                <span className="text-xs font-mono" style={{ color: 'var(--text-muted)' }}>{key}:</span>
                <span className="text-xs font-mono truncate" style={{ color: 'var(--text-primary)' }}>{String(value)}</span>
              </div>
            ))}
          </div>
        )
      )}
    </div>
  )
}
