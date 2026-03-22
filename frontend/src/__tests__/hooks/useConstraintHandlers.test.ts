/**
 * Tests for useConstraintHandlers hook.
 *
 * Tests constraint add, remove, column rename, and value change operations.
 */
import { describe, it, expect, vi } from "vitest"
import { renderHook, act } from "@testing-library/react"
import { useConstraintHandlers } from "../../hooks/useConstraintHandlers"

const COLUMNS = [
  { name: "volume", dtype: "float64" },
  { name: "loss_ratio", dtype: "float64" },
  { name: "premium", dtype: "float64" },
]

describe("useConstraintHandlers", () => {
  // ─── handleAddConstraint ────────────────────────────────────────

  describe("handleAddConstraint", () => {
    it("adds first available column not already constrained", () => {
      const onUpdate = vi.fn()
      const { result } = renderHook(() =>
        useConstraintHandlers({}, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleAddConstraint())

      expect(onUpdate).toHaveBeenCalledWith("constraints", {
        volume: { min: 0.9 },
      })
    })

    it("skips the objective column", () => {
      const onUpdate = vi.fn()
      const { result } = renderHook(() =>
        useConstraintHandlers({}, "volume", COLUMNS, onUpdate),
      )

      act(() => result.current.handleAddConstraint())

      // Should skip "volume" (objective) and pick "loss_ratio"
      expect(onUpdate).toHaveBeenCalledWith("constraints", {
        loss_ratio: { min: 0.9 },
      })
    })

    it("skips already-constrained columns", () => {
      const onUpdate = vi.fn()
      const existing = { volume: { min: 0.9 } }
      const { result } = renderHook(() =>
        useConstraintHandlers(existing, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleAddConstraint())

      expect(onUpdate).toHaveBeenCalledWith("constraints", {
        volume: { min: 0.9 },
        loss_ratio: { min: 0.9 },
      })
    })

    it("generates synthetic name when all columns are used", () => {
      const onUpdate = vi.fn()
      const existing = {
        volume: { min: 0.9 },
        loss_ratio: { max: 1.1 },
      }
      const { result } = renderHook(() =>
        useConstraintHandlers(existing, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleAddConstraint())

      const call = onUpdate.mock.calls[0]
      expect(call[0]).toBe("constraints")
      expect(call[1]).toHaveProperty("constraint_3")
    })
  })

  // ─── handleRemoveConstraint ─────────────────────────────────────

  describe("handleRemoveConstraint", () => {
    it("removes the specified constraint", () => {
      const onUpdate = vi.fn()
      const existing = {
        volume: { min: 0.9 },
        loss_ratio: { max: 1.1 },
      }
      const { result } = renderHook(() =>
        useConstraintHandlers(existing, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleRemoveConstraint("volume"))

      expect(onUpdate).toHaveBeenCalledWith("constraints", {
        loss_ratio: { max: 1.1 },
      })
    })

    it("produces empty constraints when removing last one", () => {
      const onUpdate = vi.fn()
      const existing = { volume: { min: 0.9 } }
      const { result } = renderHook(() =>
        useConstraintHandlers(existing, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleRemoveConstraint("volume"))

      expect(onUpdate).toHaveBeenCalledWith("constraints", {})
    })
  })

  // ─── handleConstraintColumnChange ───────────────────────────────

  describe("handleConstraintColumnChange", () => {
    it("renames a constraint column", () => {
      const onUpdate = vi.fn()
      const existing = {
        volume: { min: 0.9 },
        loss_ratio: { max: 1.1 },
      }
      const { result } = renderHook(() =>
        useConstraintHandlers(existing, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleConstraintColumnChange("volume", "premium"))

      expect(onUpdate).toHaveBeenCalledWith("constraints", {
        premium: { min: 0.9 },
        loss_ratio: { max: 1.1 },
      })
    })

    it("does nothing when old and new names are the same", () => {
      const onUpdate = vi.fn()
      const existing = { volume: { min: 0.9 } }
      const { result } = renderHook(() =>
        useConstraintHandlers(existing, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleConstraintColumnChange("volume", "volume"))

      expect(onUpdate).not.toHaveBeenCalled()
    })

    it("preserves order of other constraints", () => {
      const onUpdate = vi.fn()
      const existing = {
        a: { min: 0.8 },
        b: { max: 1.2 },
        c: { min: 0.95 },
      }
      const { result } = renderHook(() =>
        useConstraintHandlers(existing, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleConstraintColumnChange("b", "renamed_b"))

      const newConstraints = onUpdate.mock.calls[0][1]
      expect(Object.keys(newConstraints)).toEqual(["a", "renamed_b", "c"])
    })
  })

  // ─── handleConstraintValueChange ────────────────────────────────

  describe("handleConstraintValueChange", () => {
    it("updates constraint type and value, preserving siblings", () => {
      const onUpdate = vi.fn()
      const existing = { volume: { min: 0.9 } }
      const { result } = renderHook(() =>
        useConstraintHandlers(existing, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleConstraintValueChange("volume", "max", 1.1))

      // Both min and max are preserved
      expect(onUpdate).toHaveBeenCalledWith("constraints", {
        volume: { min: 0.9, max: 1.1 },
      })
    })

    it("adds new constraint type without destroying existing ones", () => {
      const onUpdate = vi.fn()
      const existing = { volume: { min: 0.9 } }
      const { result } = renderHook(() =>
        useConstraintHandlers(existing, "premium", COLUMNS, onUpdate),
      )

      act(() => result.current.handleConstraintValueChange("volume", "max_abs", 500000))

      // The old {min: 0.9} is preserved alongside the new max_abs
      expect(onUpdate).toHaveBeenCalledWith("constraints", {
        volume: { min: 0.9, max_abs: 500000 },
      })
    })
  })
})
