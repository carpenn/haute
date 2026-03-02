import { useEffect, type RefObject } from "react"

/**
 * Close a dropdown/popover when the user clicks outside its container.
 * Only attaches a listener when `active` is true.
 */
export default function useClickOutside(
  ref: RefObject<HTMLElement | null>,
  onClose: () => void,
  active: boolean,
) {
  useEffect(() => {
    if (!active) return
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as HTMLElement)) {
        onClose()
      }
    }
    document.addEventListener("mousedown", handler)
    return () => document.removeEventListener("mousedown", handler)
  }, [active, ref, onClose])
}
