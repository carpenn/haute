import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import ColumnTable from "../ColumnTable"

afterEach(cleanup)

const COLUMNS = [
  { name: "premium", dtype: "Float64" },
  { name: "area", dtype: "String" },
  { name: "power", dtype: "Int64" },
]

describe("ColumnTable", () => {
  it("renders column names and dtypes", () => {
    render(<ColumnTable columns={COLUMNS} />)
    expect(screen.getByText("premium")).toBeTruthy()
    expect(screen.getByText("Float64")).toBeTruthy()
    expect(screen.getByText("area")).toBeTruthy()
    expect(screen.getByText("String")).toBeTruthy()
    expect(screen.getByText("power")).toBeTruthy()
    expect(screen.getByText("Int64")).toBeTruthy()
  })

  it("renders Column and Type headers", () => {
    render(<ColumnTable columns={COLUMNS} />)
    expect(screen.getByText("Column")).toBeTruthy()
    expect(screen.getByText("Type")).toBeTruthy()
  })

  it("renders no checkboxes when checkbox prop is omitted", () => {
    render(<ColumnTable columns={COLUMNS} />)
    expect(screen.queryAllByRole("checkbox")).toHaveLength(0)
  })

  it("renders checkboxes when checkbox prop is provided", () => {
    render(
      <ColumnTable
        columns={COLUMNS}
        checkbox={{
          isChecked: (name) => name === "premium",
          onToggle: vi.fn(),
        }}
      />,
    )
    const boxes = screen.getAllByRole("checkbox")
    expect(boxes).toHaveLength(3)
    expect((boxes[0] as HTMLInputElement).checked).toBe(true)
    expect((boxes[1] as HTMLInputElement).checked).toBe(false)
    expect((boxes[2] as HTMLInputElement).checked).toBe(false)
  })

  it("calls onToggle when checkbox is clicked", () => {
    const onToggle = vi.fn()
    render(
      <ColumnTable
        columns={COLUMNS}
        checkbox={{
          isChecked: () => false,
          onToggle,
        }}
      />,
    )
    fireEvent.click(screen.getAllByRole("checkbox")[1])
    expect(onToggle).toHaveBeenCalledWith("area")
  })

  it("calls onToggle when interactive row is clicked", () => {
    const onToggle = vi.fn()
    render(
      <ColumnTable
        columns={COLUMNS}
        checkbox={{
          isChecked: () => false,
          onToggle,
        }}
        interactiveRows
      />,
    )
    fireEvent.click(screen.getByText("power"))
    expect(onToggle).toHaveBeenCalledWith("power")
  })

  it("applies custom nameColor", () => {
    const { container } = render(
      <ColumnTable
        columns={[{ name: "x", dtype: "Int64" }]}
        nameColor={() => "rgb(255, 0, 0)"}
      />,
    )
    const cell = container.querySelector("td.font-mono") as HTMLElement
    expect(cell.style.color).toBe("rgb(255, 0, 0)")
  })

  it("applies custom accentClass to checkboxes", () => {
    render(
      <ColumnTable
        columns={[{ name: "x", dtype: "Int64" }]}
        checkbox={{
          isChecked: () => true,
          onToggle: vi.fn(),
          accentClass: "accent-rose-500",
        }}
      />,
    )
    const cb = screen.getByRole("checkbox")
    expect(cb.className).toContain("accent-rose-500")
  })

  it("applies extra className to wrapper", () => {
    const { container } = render(
      <ColumnTable columns={COLUMNS} className="max-h-[400px]" />,
    )
    expect(container.firstElementChild!.className).toContain("max-h-[400px]")
  })

  it("renders empty table for empty columns", () => {
    render(<ColumnTable columns={[]} />)
    expect(screen.getByText("Column")).toBeTruthy()
    expect(screen.getByText("Type")).toBeTruthy()
    // No data rows
    const rows = screen.getAllByRole("row")
    expect(rows).toHaveLength(1) // header only
  })

  it("calls onToggle only once when clicking checkbox on interactive row", () => {
    const onToggle = vi.fn()
    render(
      <ColumnTable
        columns={COLUMNS}
        checkbox={{
          isChecked: () => false,
          onToggle,
        }}
        interactiveRows
      />,
    )
    // Click the checkbox directly — stopPropagation should prevent the row onClick from also firing
    fireEvent.click(screen.getAllByRole("checkbox")[0])
    expect(onToggle).toHaveBeenCalledTimes(1)
    expect(onToggle).toHaveBeenCalledWith("premium")
  })

  it("uses default nameColor when not provided", () => {
    const { container } = render(
      <ColumnTable columns={[{ name: "x", dtype: "Int64" }]} />,
    )
    const cell = container.querySelector("td.font-mono") as HTMLElement
    expect(cell.style.color).toBe("var(--text-primary)")
  })
})
