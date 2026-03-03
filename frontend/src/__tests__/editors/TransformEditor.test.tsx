/**
 * Render tests for TransformEditor.
 *
 * Tests: label, hint text for empty/present input sources, input sources bar.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, cleanup } from "@testing-library/react"
import TransformEditor from "../../panels/editors/TransformEditor"

vi.mock("../../panels/editors/_shared", async () => {
  const actual = await vi.importActual("../../panels/editors/_shared")
  return {
    ...(actual as Record<string, unknown>),
    CodeEditor: ({ defaultValue, onChange, placeholder }: { defaultValue: string; onChange?: (v: string) => void; placeholder?: string }) => (
      <textarea
        data-testid="code-editor"
        defaultValue={defaultValue}
        onChange={(e) => onChange?.(e.target.value)}
        placeholder={placeholder}
      />
    ),
  }
})

afterEach(cleanup)

describe("TransformEditor", () => {
  it("renders Polars Code label", () => {
    render(
      <TransformEditor config={{}} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("Polars Code")).toBeTruthy()
  })

  it('shows "start with" hint when no input sources', () => {
    render(
      <TransformEditor config={{}} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText(/start with/)).toBeTruthy()
    expect(screen.getByText(/to chain/)).toBeTruthy()
  })

  it('shows "use input names" hint when input sources present', () => {
    const inputs = [
      { varName: "claims", sourceLabel: "Claims Data", edgeId: "e1" },
    ]
    render(
      <TransformEditor config={{}} onUpdate={vi.fn()} inputSources={inputs} />,
    )
    expect(screen.getByText("use input names")).toBeTruthy()
  })

  it("renders input sources bar showing connected variable names", () => {
    const inputs = [
      { varName: "claims", sourceLabel: "Claims Data", edgeId: "e1" },
      { varName: "policies", sourceLabel: "Policy Data", edgeId: "e2" },
    ]
    render(
      <TransformEditor config={{}} onUpdate={vi.fn()} inputSources={inputs} />,
    )
    expect(screen.getByText("claims")).toBeTruthy()
    expect(screen.getByText("policies")).toBeTruthy()
    // Multiple inputs should show "Inputs" (plural)
    expect(screen.getByText("Inputs")).toBeTruthy()
  })
})
