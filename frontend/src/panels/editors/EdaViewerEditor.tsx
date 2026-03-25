import { INPUT_STYLE } from "./_shared"
import type { InputSource, OnUpdateConfig } from "./_shared"
import { configField } from "../../utils/configField"

const ROLE_OPTIONS = [
  "policy_key",
  "claim_key",
  "underwriting_date",
  "accident_date",
  "reporting_date",
  "transaction_date",
  "exposure",
  "target",
  "covariate",
  "fold",
] as const

type FieldRole = typeof ROLE_OPTIONS[number] | ""

export default function EdaViewerEditor({
  config,
  onUpdate,
  inputSources,
  onDeleteInput,
  upstreamColumns = [],
  accentColor,
}: {
  config: Record<string, unknown>
  onUpdate: OnUpdateConfig
  inputSources: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  upstreamColumns?: { name: string; dtype: string }[]
  accentColor: string
}) {
  const noInput = inputSources.length === 0
  const noColumns = !noInput && upstreamColumns.length === 0

  // fieldRoles is stored as {fieldName: role} in the config
  const fieldRoles = (configField(config, "fieldRoles", {}) ?? {}) as Record<string, FieldRole>

  function handleRoleChange(fieldName: string, role: string) {
    const updated = { ...fieldRoles }
    if (role === "") {
      delete updated[fieldName]
    } else {
      updated[fieldName] = role as FieldRole
    }
    onUpdate("fieldRoles", updated)
  }

  return (
    <div className="px-4 py-3 space-y-3">
      {/* Input sources */}
      {inputSources.length > 0 && (
        <div className="space-y-1">
          {inputSources.map((src) => (
            <div key={src.edgeId} className="flex items-center justify-between px-2 py-1 rounded text-[11px]" style={{ background: "rgba(124,58,237,.08)", border: "1px solid rgba(124,58,237,.2)" }}>
              <span style={{ color: accentColor }}>{src.sourceLabel}</span>
              {onDeleteInput && (
                <button
                  onClick={() => onDeleteInput(src.edgeId)}
                  className="ml-2 text-[10px] opacity-60 hover:opacity-100"
                  style={{ color: "var(--text-muted)" }}
                  aria-label="Remove input"
                >✕</button>
              )}
            </div>
          ))}
        </div>
      )}

      {noInput && (
        <div
          className="text-[12px] rounded-lg px-3 py-2.5"
          style={{
            background: "rgba(245,158,11,.08)",
            border: "1px solid rgba(245,158,11,.2)",
            color: "#f59e0b",
          }}
        >
          Connect a Data Source node to assign field roles.
        </div>
      )}

      {noColumns && (
        <div
          className="text-[12px] rounded-lg px-3 py-2.5"
          style={{
            background: "rgba(100,116,139,.1)",
            border: "1px solid rgba(100,116,139,.2)",
            color: "var(--text-muted)",
          }}
        >
          No columns available yet — refresh the upstream node&apos;s preview first.
        </div>
      )}

      {upstreamColumns.length > 0 && (
        <>
          <div
            className="text-[11px] font-bold uppercase tracking-[0.08em]"
            style={{ color: "var(--text-muted)" }}
          >
            Field Roles
          </div>

          {/* Table header */}
          <div
            className="grid text-[10px] font-bold uppercase tracking-[0.06em] px-2 py-1 rounded"
            style={{
              gridTemplateColumns: "1fr 80px 140px",
              color: "var(--text-muted)",
              background: "var(--bg-input)",
              border: "1px solid var(--border)",
            }}
          >
            <span>Feature</span>
            <span>Dtype</span>
            <span>Role</span>
          </div>

          {/* Table rows */}
          <div className="space-y-1">
            {upstreamColumns.map(({ name, dtype }) => {
              const currentRole = fieldRoles[name] ?? ""
              return (
                <div
                  key={name}
                  className="grid items-center px-2 py-1 rounded text-[11px]"
                  style={{
                    gridTemplateColumns: "1fr 80px 140px",
                    background: currentRole ? "rgba(124,58,237,.06)" : "transparent",
                    border: `1px solid ${currentRole ? "rgba(124,58,237,.2)" : "var(--border)"}`,
                  }}
                >
                  <span
                    className="font-mono truncate pr-1"
                    style={{ color: "var(--text-primary)" }}
                    title={name}
                  >
                    {name}
                  </span>
                  <span
                    className="font-mono text-[10px] truncate pr-1"
                    style={{ color: "var(--text-muted)" }}
                    title={dtype}
                  >
                    {dtype}
                  </span>
                  <select
                    className="text-[11px] font-mono rounded px-1 py-0.5 appearance-none cursor-pointer"
                    style={{
                      ...INPUT_STYLE,
                      color: currentRole ? "var(--text-primary)" : "var(--text-muted)",
                    }}
                    value={currentRole}
                    onChange={(e) => handleRoleChange(name, e.target.value)}
                    aria-label={`Role for ${name}`}
                  >
                    <option value="">— none —</option>
                    {ROLE_OPTIONS.map((role) => (
                      <option key={role} value={role}>{role}</option>
                    ))}
                  </select>
                </div>
              )
            })}
          </div>

          {Object.keys(fieldRoles).length > 0 && (
            <div
              className="text-[11px] px-3 py-2 rounded-lg"
              style={{
                background: `rgba(124,58,237,.08)`,
                border: `1px solid rgba(124,58,237,.2)`,
                color: accentColor,
              }}
            >
              {Object.keys(fieldRoles).length} field{Object.keys(fieldRoles).length !== 1 ? "s" : ""} assigned — run preview to see EDA results.
            </div>
          )}
        </>
      )}
    </div>
  )
}
