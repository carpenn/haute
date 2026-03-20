import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import TracePanel from "../TracePanel"
import type { TraceResult, TraceStep } from "../../types/trace"

function makeStep(overrides: Partial<TraceStep> = {}): TraceStep {
  return {
    node_id: "n1",
    node_name: "Transform 1",
    node_type: "polars",
    schema_diff: {
      columns_added: [],
      columns_removed: [],
      columns_modified: [],
      columns_passed: ["age"],
    },
    input_values: { age: 25 },
    output_values: { age: 25, premium: 100 },
    column_relevant: true,
    execution_ms: 5.2,
    ...overrides,
  }
}

function makeTrace(overrides: Partial<TraceResult> = {}): TraceResult {
  return {
    target_node_id: "n2",
    row_index: 0,
    column: "premium",
    output_value: 42.5,
    steps: [
      makeStep({ node_id: "n1", node_name: "Source" }),
      makeStep({
        node_id: "n2",
        node_name: "Calc",
        schema_diff: { columns_added: ["premium"], columns_removed: [], columns_modified: [], columns_passed: ["age"] },
        output_values: { age: 25, premium: 42.5 },
      }),
    ],
    row_id_column: "quote_id",
    row_id_value: "Q001",
    total_nodes_in_pipeline: 5,
    nodes_in_trace: 2,
    execution_ms: 12.3,
    ...overrides,
  }
}

describe("TracePanel", () => {
  afterEach(cleanup)

  it("renders the Trace header with column name", () => {
    render(<TracePanel trace={makeTrace()} onClose={vi.fn()} />)
    expect(screen.getByText(/Trace.*premium/)).toBeInTheDocument()
  })

  it("renders the output value", () => {
    render(<TracePanel trace={makeTrace()} onClose={vi.fn()} />)
    expect(screen.getByText("42.5")).toBeInTheDocument()
  })

  it("renders execution time", () => {
    render(<TracePanel trace={makeTrace()} onClose={vi.fn()} />)
    expect(screen.getByText("12.3ms")).toBeInTheDocument()
  })

  it("renders step count", () => {
    render(<TracePanel trace={makeTrace()} onClose={vi.fn()} />)
    expect(screen.getByText("2 steps")).toBeInTheDocument()
  })

  it("renders row_id info when available", () => {
    render(<TracePanel trace={makeTrace()} onClose={vi.fn()} />)
    expect(screen.getByText("quote_id")).toBeInTheDocument()
    expect(screen.getByText("Q001")).toBeInTheDocument()
  })

  it("renders Row N when no row_id_column", () => {
    render(<TracePanel trace={makeTrace({ row_id_column: null, row_id_value: null, row_index: 3 })} onClose={vi.fn()} />)
    expect(screen.getByText(/Row 3/)).toBeInTheDocument()
  })

  it("renders nodes in trace count", () => {
    render(<TracePanel trace={makeTrace()} onClose={vi.fn()} />)
    expect(screen.getByText(/2 of 5 nodes/)).toBeInTheDocument()
  })

  it("close button calls onClose", () => {
    const onClose = vi.fn()
    render(<TracePanel trace={makeTrace()} onClose={onClose} />)
    const closeButtons = screen.getAllByRole("button")
    // The close button is in the header
    const closeBtn = closeButtons[0]
    fireEvent.click(closeBtn)
    expect(onClose).toHaveBeenCalledOnce()
  })

  it("renders step names in order", () => {
    render(<TracePanel trace={makeTrace()} onClose={vi.fn()} />)
    const source = screen.getByText("Source")
    const calc = screen.getByText("Calc")
    expect(source).toBeInTheDocument()
    expect(calc).toBeInTheDocument()
    // Source should appear before Calc in DOM order
    const body = document.body
    const sourcePos = Array.from(body.querySelectorAll("*")).indexOf(source)
    const calcPos = Array.from(body.querySelectorAll("*")).indexOf(calc)
    expect(sourcePos).toBeLessThan(calcPos)
  })

  it("renders step indexes starting from 1", () => {
    render(<TracePanel trace={makeTrace()} onClose={vi.fn()} />)
    expect(screen.getByText("1")).toBeInTheDocument()
    expect(screen.getByText("2")).toBeInTheDocument()
  })

  it("renders per-step execution time", () => {
    render(
      <TracePanel
        trace={makeTrace({
          steps: [
            makeStep({ node_id: "n1", node_name: "Only Step", execution_ms: 7.3 }),
          ],
        })}
        onClose={vi.fn()}
      />,
    )
    expect(screen.getByText("7.3ms")).toBeInTheDocument()
  })

  it("step card expands on click to show column details", () => {
    render(
      <TracePanel
        trace={makeTrace({
          steps: [
            makeStep({
              node_id: "n1",
              node_name: "Source",
              schema_diff: {
                columns_added: ["premium"],
                columns_removed: [],
                columns_modified: [],
                columns_passed: ["age"],
              },
              output_values: { age: 25, premium: 100 },
            }),
          ],
        })}
        onClose={vi.fn()}
      />,
    )
    // Before expanding: key entries are shown but not the full column list
    const stepButton = screen.getByText("Source").closest("button") as HTMLElement
    fireEvent.click(stepButton)
    // After expanding: should show schema diff summary
    expect(screen.getByText(/1 added/)).toBeInTheDocument()
    expect(screen.getByText(/1 passed through/)).toBeInTheDocument()
  })

  it("step card collapses on second click", () => {
    render(
      <TracePanel
        trace={makeTrace({
          steps: [
            makeStep({
              node_id: "n1",
              node_name: "Source",
              schema_diff: {
                columns_added: ["new_col"],
                columns_removed: [],
                columns_modified: [],
                columns_passed: ["age"],
              },
              output_values: { age: 25, new_col: 42 },
            }),
          ],
        })}
        onClose={vi.fn()}
      />,
    )
    const stepButton = screen.getByText("Source").closest("button") as HTMLElement
    // Expand
    fireEvent.click(stepButton)
    expect(screen.getByText(/1 added/)).toBeInTheDocument()
    // Collapse
    fireEvent.click(stepButton)
    // Schema diff summary should be gone after collapse
    expect(screen.queryByText(/1 added/)).not.toBeInTheDocument()
  })

  it("renders schema diff with added columns highlighted", () => {
    render(
      <TracePanel
        trace={makeTrace({
          steps: [
            makeStep({
              node_id: "n1",
              node_name: "Source",
              schema_diff: {
                columns_added: ["premium"],
                columns_removed: ["old_col"],
                columns_modified: ["age"],
                columns_passed: [],
              },
              output_values: { age: 30, premium: 100 },
              input_values: { age: 25 },
            }),
          ],
        })}
        onClose={vi.fn()}
      />,
    )
    // Expand the step
    const stepButton = screen.getByText("Source").closest("button") as HTMLElement
    fireEvent.click(stepButton)
    expect(screen.getByText(/1 added/)).toBeInTheDocument()
    expect(screen.getByText(/1 removed/)).toBeInTheDocument()
    expect(screen.getByText(/1 modified/)).toBeInTheDocument()
  })

  it("shows key entries (collapsed) with tag badges for traced column", () => {
    render(
      <TracePanel
        trace={makeTrace({
          column: "score",
          steps: [
            makeStep({
              node_id: "n1",
              node_name: "Scorer",
              schema_diff: {
                columns_added: ["score"],
                columns_removed: [],
                columns_modified: [],
                columns_passed: ["age"],
              },
              output_values: { age: 25, score: 88.5 },
            }),
          ],
        })}
        onClose={vi.fn()}
      />,
    )
    // Should show the traced column with its value as a key entry (collapsed key badge)
    expect(screen.getByText(/score.*88.5/)).toBeInTheDocument()
  })

  it("renders header with no column name when column is null", () => {
    render(
      <TracePanel
        trace={makeTrace({ column: null })}
        onClose={vi.fn()}
      />,
    )
    // Should render "Trace" without a column suffix
    const header = screen.getByText("Trace")
    expect(header).toBeInTheDocument()
  })

  it("renders Result badge with output value", () => {
    render(<TracePanel trace={makeTrace({ output_value: 99.9 })} onClose={vi.fn()} />)
    expect(screen.getByText("Result")).toBeInTheDocument()
    expect(screen.getByText("99.9")).toBeInTheDocument()
  })

  it("non-relevant steps have reduced opacity", () => {
    const { container } = render(
      <TracePanel
        trace={makeTrace({
          steps: [
            makeStep({ node_id: "n1", node_name: "Irrelevant", column_relevant: false }),
          ],
        })}
        onClose={vi.fn()}
      />,
    )
    // The step card should have opacity 0.55 for non-relevant steps
    const card = container.querySelector("[style*='opacity: 0.55']")
    expect(card).toBeTruthy()
  })
})
