import { useState, useEffect, useRef, useCallback } from "react"
import { X, Folder, FileText, ChevronLeft, Check, Table2 } from "lucide-react"
import { getDtypeColor } from "../../utils/dtypeColors"
import type { ColumnInfo } from "../../types/node"
import { listFiles } from "../../api/client"

// ─── Shared Types ─────────────────────────────────────────────────

export type FileItem = {
  name: string
  path: string
  type: "file" | "directory"
  size?: number
}

export type InputSource = {
  varName: string
  sourceLabel: string
  edgeId: string
}

export type SchemaInfo = {
  path: string
  columns: ColumnInfo[]
  row_count: number
  column_count: number
  preview: Record<string, unknown>[]
} | null

export type SimpleNode = {
  id: string
  type?: string
  data: {
    label: string
    description: string
    nodeType: string
    config?: Record<string, unknown>
    [key: string]: unknown
  }
}

export type SimpleEdge = {
  id: string
  source: string
  target: string
}

// ─── FileBrowser ──────────────────────────────────────────────────

export function FileBrowser({ currentPath, onSelect, extensions }: { currentPath?: string; onSelect: (path: string) => void; extensions?: string }) {
  const [dir, setDir] = useState(".")
  const [items, setItems] = useState<FileItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedPath, setSelectedPath] = useState<string | undefined>(currentPath)

  useEffect(() => {
    setError(null)
    listFiles(dir, extensions)
      .then((data) => {
        setItems(data.items || [])
        setLoading(false)
      })
      .catch((e: unknown) => {
        setError(e instanceof Error ? e.message : "Failed to load files")
        setItems([])
        setLoading(false)
      })
  }, [dir, extensions])

  const goUp = () => {
    if (dir === ".") return
    const parts = dir.split("/")
    parts.pop()
    setLoading(true)
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
          ) : error ? (
            <div className="px-3 py-2 text-xs" style={{ color: '#f87171' }}>{error}</div>
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
                      setLoading(true)
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

// ─── SchemaPreview ────────────────────────────────────────────────

export function SchemaPreview({ schema }: { schema: SchemaInfo }) {
  const [showPreview, setShowPreview] = useState(false)

  if (!schema || !schema.columns) return null

  return (
    <div style={{ borderTop: '1px solid var(--border)', background: 'var(--bg-elevated)' }}>
      <div className="px-4 py-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Table2 size={14} style={{ color: 'var(--text-muted)' }} />
          <span className="text-xs font-semibold" style={{ color: 'var(--text-primary)' }}>Schema</span>
          <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
            {schema.column_count ?? 0} cols / {(schema.row_count ?? 0).toLocaleString()} rows
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

// ─── CodeEditor ───────────────────────────────────────────────────

const PAIRS: Record<string, string> = { "(": ")", "[": "]", "{": "}", "'": "'", '"': '"' }
const CLOSE_CHARS = new Set([")", "]", "}"])

export function CodeEditor({
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

  // Debounce parent onChange — local state updates instantly, parent
  // update is deferred by 150ms to avoid re-render storms on fast typing.
  const debounceRef = useRef<ReturnType<typeof setTimeout>>()
  useEffect(() => {
    debounceRef.current = setTimeout(() => onChange(code), 150)
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current) }
  }, [code, onChange])

  const insertText = useCallback((ta: HTMLTextAreaElement, text: string) => {
    ta.focus()
    document.execCommand("insertText", false, text)
    setCode(ta.value)
  }, [])

  const replaceRange = useCallback((ta: HTMLTextAreaElement, start: number, end: number, text: string, cursorPos?: number) => {
    ta.focus()
    ta.setSelectionRange(start, end)
    document.execCommand("insertText", false, text)
    if (cursorPos !== undefined) {
      ta.setSelectionRange(cursorPos, cursorPos)
    }
    setCode(ta.value)
  }, [])

  const replaceRangeSelect = useCallback((ta: HTMLTextAreaElement, start: number, end: number, text: string, selStart: number, selEnd: number) => {
    ta.focus()
    ta.setSelectionRange(start, end)
    document.execCommand("insertText", false, text)
    ta.setSelectionRange(selStart, selEnd)
    setCode(ta.value)
  }, [])

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLTextAreaElement>) => {
      setCode(e.target.value)
    },
    [],
  )

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      const ta = e.currentTarget
      const { selectionStart: start, selectionEnd: end, value: val } = ta
      const hasSelection = start !== end

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
        if (open === "'" || open === '"') {
          const charBefore = start > 0 ? val[start - 1] : ""
          if (/\w/.test(charBefore)) return
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

      if (CLOSE_CHARS.has(e.key) && val[start] === e.key && !hasSelection) {
        e.preventDefault()
        ta.setSelectionRange(start + 1, start + 1)
        return
      }

      if (e.key === "Backspace" && !hasSelection && start > 0) {
        const before = val[start - 1]
        const after = val[start]
        if (PAIRS[before] && PAIRS[before] === after) {
          e.preventDefault()
          replaceRange(ta, start - 1, start + 1, "", start - 1)
          return
        }
      }

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

// ─── InputSourcesBar ──────────────────────────────────────────────

export function InputSourcesBar({
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
