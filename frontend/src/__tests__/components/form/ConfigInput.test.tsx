import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import ConfigInput from "../../../components/form/ConfigInput"

afterEach(cleanup)

describe("ConfigInput", () => {
  it("renders with value", () => {
    render(<ConfigInput value="hello" onChange={vi.fn()} />)
    expect(screen.getByDisplayValue("hello")).toBeTruthy()
  })

  it("renders with label", () => {
    render(<ConfigInput value="" onChange={vi.fn()} label="Name" />)
    expect(screen.getByText("Name")).toBeTruthy()
    expect(screen.getByLabelText("Name")).toBeTruthy()
  })

  it("no label element when label prop omitted", () => {
    const { container } = render(<ConfigInput value="" onChange={vi.fn()} />)
    expect(container.querySelector("label")).toBeNull()
  })

  it("calls onChange with new value on input change", () => {
    const onChange = vi.fn()
    render(<ConfigInput value="" onChange={onChange} label="Field" />)
    fireEvent.change(screen.getByRole("textbox"), { target: { value: "new" } })
    expect(onChange).toHaveBeenCalledWith("new")
  })

  it("renders placeholder", () => {
    render(<ConfigInput value="" onChange={vi.fn()} placeholder="Type here..." />)
    expect(screen.getByPlaceholderText("Type here...")).toBeTruthy()
  })

  it("disabled input has disabled attribute", () => {
    render(<ConfigInput value="" onChange={vi.fn()} disabled />)
    const input = screen.getByRole("textbox") as HTMLInputElement
    expect(input.disabled).toBe(true)
  })

  it("uses type='number' when specified", () => {
    render(<ConfigInput value="42" onChange={vi.fn()} type="number" />)
    const input = screen.getByRole("spinbutton") as HTMLInputElement
    expect(input.type).toBe("number")
    expect(input.value).toBe("42")
  })
})
