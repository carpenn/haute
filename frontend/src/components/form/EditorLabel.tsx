import type { ReactNode } from "react"

interface EditorLabelProps {
  children: ReactNode
  /** Override text color. Defaults to `var(--text-muted)`. */
  color?: string
  /** Extra CSS class names appended after the base label classes. */
  className?: string
  /** HTML `for` attribute linking to a form control. */
  htmlFor?: string
  /** Render as `<span>` instead of `<label>`. Useful when wrapping non-form content. */
  as?: "label" | "span" | "div"
}

/**
 * Consistent editor label with the standard uppercase micro-text style
 * used across all node editors and panels.
 *
 * Replaces the repeated pattern:
 * `className="text-[11px] font-bold uppercase tracking-[0.08em]" style={{ color: 'var(--text-muted)' }}`
 */
export default function EditorLabel({
  children,
  color = "var(--text-muted)",
  className = "",
  htmlFor,
  as: Tag = "label",
}: EditorLabelProps) {
  return (
    <Tag
      htmlFor={Tag === "label" ? htmlFor : undefined}
      className={`text-[11px] font-bold uppercase tracking-[0.08em] ${className}`.trim()}
      style={{ color }}
    >
      {children}
    </Tag>
  )
}
