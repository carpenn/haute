import { describe, it, expect, beforeEach } from "vitest"
import useToastStore from "../useToastStore"

function reset() {
  useToastStore.setState({
    toasts: [],
    _toastCounter: 0,
  })
}

describe("useToastStore", () => {
  beforeEach(reset)

  describe("addToast / dismissToast", () => {
    it("adds a toast with incrementing id", () => {
      useToastStore.getState().addToast("info", "Hello")
      const { toasts, _toastCounter } = useToastStore.getState()
      expect(toasts).toHaveLength(1)
      expect(toasts[0]).toEqual({ id: "1", type: "info", text: "Hello" })
      expect(_toastCounter).toBe(1)
    })

    it("accumulates multiple toasts", () => {
      const { addToast } = useToastStore.getState()
      addToast("info", "First")
      addToast("error", "Second")
      addToast("success", "Third")
      const { toasts } = useToastStore.getState()
      expect(toasts).toHaveLength(3)
      expect(toasts.map((t) => t.type)).toEqual(["info", "error", "success"])
      expect(toasts.map((t) => t.id)).toEqual(["1", "2", "3"])
    })

    it("dismisses a toast by id", () => {
      const { addToast } = useToastStore.getState()
      addToast("info", "Keep")
      addToast("error", "Remove")
      useToastStore.getState().dismissToast("2")
      const { toasts } = useToastStore.getState()
      expect(toasts).toHaveLength(1)
      expect(toasts[0].text).toBe("Keep")
    })

    it("dismissing non-existent id is a no-op", () => {
      useToastStore.getState().addToast("info", "Only")
      useToastStore.getState().dismissToast("999")
      expect(useToastStore.getState().toasts).toHaveLength(1)
    })

    it("counter keeps incrementing after dismiss", () => {
      const { addToast } = useToastStore.getState()
      addToast("info", "A")
      useToastStore.getState().dismissToast("1")
      addToast("info", "B")
      expect(useToastStore.getState().toasts[0].id).toBe("2")
    })
  })
})
