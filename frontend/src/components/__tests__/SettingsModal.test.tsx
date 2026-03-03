import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import SettingsModal from "../SettingsModal"

function renderModal(overrides: Partial<Parameters<typeof SettingsModal>[0]> = {}) {
  const props = {
    preamble: "import numpy as np",
    onPreambleChange: vi.fn(),
    onClose: vi.fn(),
    ...overrides,
  }
  return { ...render(<SettingsModal {...props} />), props }
}

describe("SettingsModal", () => {
  afterEach(cleanup)
  it("renders title and description", () => {
    renderModal()
    expect(screen.getByText("Pipeline Imports & Helpers")).toBeInTheDocument()
    expect(screen.getByText(/Extra imports, constants, and helper functions/)).toBeInTheDocument()
  })

  it("close button calls onClose", () => {
    const { props } = renderModal()
    fireEvent.click(screen.getByLabelText("Close settings"))
    expect(props.onClose).toHaveBeenCalledTimes(1)
  })

  it("done button calls onClose", () => {
    const { props } = renderModal()
    fireEvent.click(screen.getByText("Done"))
    expect(props.onClose).toHaveBeenCalledTimes(1)
  })

  it("backdrop click calls onClose", () => {
    const { props } = renderModal()
    const overlay = screen.getByRole("dialog")
    fireEvent.click(overlay)
    expect(props.onClose).toHaveBeenCalledTimes(1)
  })

  it("textarea calls onPreambleChange on input", () => {
    const { props } = renderModal()
    const textarea = screen.getByRole("textbox")
    fireEvent.change(textarea, { target: { value: "import pandas" } })
    expect(props.onPreambleChange).toHaveBeenCalledWith("import pandas")
  })

  it("textarea shows preamble value as default", () => {
    renderModal({ preamble: "import scipy" })
    const textarea = screen.getByRole("textbox") as HTMLTextAreaElement
    expect(textarea.defaultValue).toBe("import scipy")
  })

  it("textarea shows placeholder text when preamble is empty", () => {
    renderModal({ preamble: "" })
    const textarea = screen.getByRole("textbox") as HTMLTextAreaElement
    expect(textarea.placeholder).toContain("import numpy as np")
  })

  it("clicking inner panel does not close modal", () => {
    const { props } = renderModal()
    // Click on the inner panel content area, not the overlay
    const heading = screen.getByText("Pipeline Imports & Helpers")
    fireEvent.click(heading)
    expect(props.onClose).not.toHaveBeenCalled()
  })

  it("renders informational text about default imports", () => {
    renderModal()
    expect(screen.getByText(/import polars as pl/)).toBeInTheDocument()
    expect(screen.getByText(/import haute/)).toBeInTheDocument()
  })

  it("has correct aria attributes for accessibility", () => {
    renderModal()
    const dialog = screen.getByRole("dialog")
    expect(dialog).toHaveAttribute("aria-modal", "true")
    expect(dialog).toHaveAttribute("aria-label", "Pipeline imports and helpers")
  })
})
