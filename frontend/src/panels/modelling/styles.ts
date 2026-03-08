/** Style helper for the purple selected/unselected toggle buttons used across modelling config. */
export function toggleButtonStyle(selected: boolean): React.CSSProperties {
  return {
    background: selected ? "rgba(168,85,247,.15)" : "var(--chrome-hover)",
    color: selected ? "#a855f7" : "var(--text-muted)",
    border: `1px solid ${selected ? "rgba(168,85,247,.3)" : "transparent"}`,
  }
}
