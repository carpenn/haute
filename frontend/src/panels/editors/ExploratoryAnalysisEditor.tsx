import { InputSourcesBar, INPUT_STYLE } from "./_shared"
import type { InputSource, OnUpdateConfig } from "./_shared"

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

export default function ExploratoryAnalysisEditor({
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
  const fieldRoles = ((config.fieldRoles as Record<string, string> | undefined) ?? {})
  const usedRoles = new Set(Object.values(fieldRoles).filter(Boolean))
  const noInput = inputSources.length === 0
  const noColumns = !noInput && upstreamColumns.length === 0

  const handleRoleChange = (field: string, role: string) => {
    const nextRoles = { ...fieldRoles }
    if (!role) {
      delete nextRoles[field]
    } else {
      Object.keys(nextRoles).forEach((key) => {
        if (key !== field && nextRoles[key] === role) delete nextRoles[key]
      })
      nextRoles[field] = role
    }
    onUpdate("fieldRoles", nextRoles)
  }

  return (
    <div className="px-4 py-3 space-y-3">
      <InputSourcesBar inputSources={inputSources} onDeleteInput={onDeleteInput} />

      {noInput && (
        <div
          className="text-[12px] rounded-lg px-3 py-2.5"
          style={{
            background: "rgba(245,158,11,.08)",
            border: "1px solid rgba(245,158,11,.2)",
            color: "#f59e0b",
          }}
        >
          Connect a Data Source node to assign feature roles.
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
        <div className="rounded-lg overflow-hidden" style={{ border: "1px solid var(--border)", background: "var(--bg-input)" }}>
          <table className="w-full text-xs">
            <thead>
              <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg-elevated)" }}>
                <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: "var(--text-muted)" }}>Feature</th>
                <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: "var(--text-muted)" }}>Datatype</th>
                <th className="text-left px-2.5 py-1.5 font-semibold" style={{ color: "var(--text-muted)" }}>Nature</th>
              </tr>
            </thead>
            <tbody>
              {upstreamColumns.map((column) => {
                const currentRole = fieldRoles[column.name] ?? ""
                return (
                  <tr key={column.name} style={{ borderBottom: "1px solid var(--border)" }}>
                    <td className="px-2.5 py-1.5 font-mono" style={{ color: "var(--text-primary)" }}>{column.name}</td>
                    <td className="px-2.5 py-1.5" style={{ color: "var(--text-secondary)" }}>{column.dtype}</td>
                    <td className="px-2.5 py-1.5">
                      <select
                        aria-label={`${column.name} role`}
                        value={currentRole}
                        onChange={(e) => handleRoleChange(column.name, e.target.value)}
                        className="w-full px-2.5 py-1.5 rounded-md text-[12px] font-mono appearance-none cursor-pointer"
                        style={{ ...INPUT_STYLE, color: currentRole ? "var(--text-primary)" : "var(--text-muted)" }}
                      >
                        <option value="">— unassigned —</option>
                        {ROLE_OPTIONS
                          .filter((role) => role === currentRole || !usedRoles.has(role))
                          .map((role) => (
                            <option key={role} value={role}>{role}</option>
                          ))}
                      </select>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {!noInput && (
        <div
          className="text-[11px] px-3 py-2 rounded-lg"
          style={{
            background: "rgba(15,118,110,.08)",
            border: "1px solid rgba(15,118,110,.2)",
            color: accentColor,
          }}
        >
          Assign roles to the fields you want analysed. Each role can be used once.
        </div>
      )}
    </div>
  )
}
