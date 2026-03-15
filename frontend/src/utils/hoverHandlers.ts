/**
 * Shared hover handler factories to eliminate repeated inline
 * onMouseEnter / onMouseLeave style-toggling across the codebase.
 *
 * For simple chrome-style hovers (transparent → chrome-hover bg,
 * text-secondary → text-primary color), prefer the `.hover-chrome`
 * CSS class defined in index.css instead.
 */
import type { MouseEvent } from "react"

type HoverStylePair = {
  onMouseEnter: (e: MouseEvent<HTMLElement>) => void
  onMouseLeave: (e: MouseEvent<HTMLElement>) => void
}

/**
 * Returns `{ onMouseEnter, onMouseLeave }` handlers that toggle
 * inline styles on hover.
 *
 * @param hoverBg    - background on hover (default: `var(--chrome-hover)`)
 * @param hoverColor - color on hover (default: `var(--text-primary)`)
 * @param restBg     - background at rest (default: `transparent`)
 * @param restColor  - color at rest (default: `var(--text-secondary)`)
 */
export function hoverHandlers(
  hoverBg = "var(--chrome-hover)",
  hoverColor = "var(--text-primary)",
  restBg = "transparent",
  restColor = "var(--text-secondary)",
): HoverStylePair {
  return {
    onMouseEnter: (e: MouseEvent<HTMLElement>) => {
      e.currentTarget.style.background = hoverBg
      e.currentTarget.style.color = hoverColor
    },
    onMouseLeave: (e: MouseEvent<HTMLElement>) => {
      e.currentTarget.style.background = restBg
      e.currentTarget.style.color = restColor
    },
  }
}

/**
 * Hover that only toggles background (no color change).
 */
export function hoverBg(
  hoverBg = "var(--bg-hover)",
  restBg = "transparent",
): HoverStylePair {
  return {
    onMouseEnter: (e: MouseEvent<HTMLElement>) => {
      e.currentTarget.style.background = hoverBg
    },
    onMouseLeave: (e: MouseEvent<HTMLElement>) => {
      e.currentTarget.style.background = restBg
    },
  }
}
