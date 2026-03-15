import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import SubmodelDialog from "../SubmodelDialog"

function renderDialog(overrides: Partial<Parameters<typeof SubmodelDialog>[0]> = {}) {
  const props = {
    nodeCount: 5,
    onClose: vi.fn(),
    onSubmit: vi.fn(),
    ...overrides,
  }
  return { ...render(<SubmodelDialog {...props} />), props }
}

describe("SubmodelDialog", () => {
  afterEach(cleanup)
  it("renders node count in description", () => {
    renderDialog({ nodeCount: 3 })
    expect(screen.getByText(/3 selected nodes/)).toBeInTheDocument()
  })

  it("cancel button calls onClose", () => {
    const { props } = renderDialog()
    fireEvent.click(screen.getByText("Cancel"))
    expect(props.onClose).toHaveBeenCalledTimes(1)
  })

  it("backdrop click calls onClose", () => {
    const { props } = renderDialog()
    const overlay = screen.getByRole("dialog")
    fireEvent.click(overlay)
    expect(props.onClose).toHaveBeenCalledTimes(1)
  })

  it("empty name submission does NOT call onSubmit", () => {
    const { props } = renderDialog()
    fireEvent.click(screen.getByText("Create"))
    expect(props.onSubmit).not.toHaveBeenCalled()
  })

  it("valid name submission calls onSubmit with trimmed name", () => {
    const { props } = renderDialog()
    const input = screen.getByPlaceholderText("e.g. model_scoring")
    fireEvent.change(input, { target: { value: "  my_submodel  " } })
    fireEvent.click(screen.getByText("Create"))
    expect(props.onSubmit).toHaveBeenCalledWith("my_submodel")
  })

  it("Escape key calls onClose", () => {
    const { props } = renderDialog()
    fireEvent.keyDown(document, { key: "Escape" })
    expect(props.onClose).toHaveBeenCalledTimes(1)
  })

  it("non-Escape keys do NOT call onClose", () => {
    const { props } = renderDialog()
    fireEvent.keyDown(document, { key: "Enter" })
    fireEvent.keyDown(document, { key: "a" })
    expect(props.onClose).not.toHaveBeenCalled()
  })

  it("Escape handler is cleaned up on unmount", () => {
    const { props, unmount } = renderDialog()
    unmount()
    fireEvent.keyDown(document, { key: "Escape" })
    expect(props.onClose).not.toHaveBeenCalled()
  })
})
