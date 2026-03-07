interface SubmodelDialogProps {
  nodeCount: number
  onClose: () => void
  onSubmit: (name: string) => void
}

export default function SubmodelDialog({ nodeCount, onClose, onSubmit }: SubmodelDialogProps) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      role="dialog"
      aria-modal="true"
      aria-label="Create submodel"
      style={{ background: 'rgba(0,0,0,.5)' }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose() }}
    >
      <div className="w-[400px] flex flex-col rounded-xl overflow-hidden shadow-2xl" style={{ background: 'var(--bg-panel)', border: '1px solid var(--border)' }}>
        <div className="px-4 py-3" style={{ borderBottom: '1px solid var(--border)' }}>
          <h2 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>Create Submodel</h2>
          <p className="text-[11px] mt-0.5" style={{ color: 'var(--text-muted)' }}>
            Group {nodeCount} selected nodes into a submodel
          </p>
        </div>
        <form
          className="p-4 flex flex-col gap-3"
          onSubmit={(e) => {
            e.preventDefault()
            const formData = new FormData(e.currentTarget)
            const name = (formData.get("name") as string || "").trim()
            if (name) onSubmit(name)
          }}
        >
          <div>
            <label className="text-[11px] font-medium block mb-1" style={{ color: 'var(--text-muted)' }}>Submodel name</label>
            <input
              name="name"
              type="text"
              autoFocus
              placeholder="e.g. model_scoring"
              className="w-full px-3 py-1.5 text-[13px] rounded-md focus:outline-none focus:ring-2"
              style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', color: 'var(--text-primary)', caretColor: 'var(--accent)' }}
            />
          </div>
          <div className="flex justify-end gap-2">
            <button
              type="button"
              onClick={onClose}
              className="px-3 py-1.5 text-[12px] font-medium rounded-md transition-colors"
              style={{ color: 'var(--text-secondary)' }}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-1.5 text-[12px] font-semibold text-white rounded-md transition-colors"
              style={{ background: '#64748b' }}
              onMouseEnter={(e) => e.currentTarget.style.background = '#94a3b8'}
              onMouseLeave={(e) => e.currentTarget.style.background = '#64748b'}
            >
              Create
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
