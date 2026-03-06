import { X, Package } from "lucide-react"
import { CodeEditor } from "./editors"
import PanelShell from "./PanelShell"

interface ImportsPanelProps {
  preamble: string
  onPreambleChange: (value: string) => void
  onClose: () => void
}

export default function ImportsPanel({ preamble, onPreambleChange, onClose }: ImportsPanelProps) {
  return (
    <PanelShell>
      {/* Header */}
      <div className="px-3 py-2.5 flex items-center gap-2 shrink-0" style={{ borderBottom: '1px solid var(--border)' }}>
        <Package size={14} style={{ color: 'var(--accent)' }} />
        <div className="flex-1 min-w-0">
          <span className="text-[13px] font-semibold block" style={{ color: 'var(--text-primary)' }}>Pipeline Imports</span>
          <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
            Import statements for utility modules and third-party libraries.
          </span>
        </div>
        <button onClick={onClose} className="p-1 rounded shrink-0 transition-colors" style={{ color: 'var(--text-muted)' }}
          onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-hover)'}
          onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
          title="Close"
        >
          <X size={14} />
        </button>
      </div>

      {/* Info */}
      <div className="px-3 py-2 shrink-0" style={{ borderBottom: '1px solid var(--border)' }}>
        <p className="text-[11px] font-mono" style={{ color: 'var(--text-muted)' }}>
          <span style={{ color: 'rgba(96,165,250,.5)' }}>import polars as pl</span> and <span style={{ color: 'rgba(96,165,250,.5)' }}>import haute</span> are always included
        </p>
      </div>

      {/* Editor */}
      <div className="flex-1 min-h-0">
        <CodeEditor
          defaultValue={preamble}
          onChange={onPreambleChange}
          placeholder={"from utility.features import *\nimport numpy as np\nfrom catboost import CatBoostRegressor"}
        />
      </div>
    </PanelShell>
  )
}
