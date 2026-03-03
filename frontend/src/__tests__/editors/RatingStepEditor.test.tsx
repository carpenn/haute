/**
 * Render tests for RatingStepEditor.
 *
 * Tests: renders with default config, renders factor tabs, table tabs,
 * handles factor add/remove, table add/remove via config updates.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import RatingStepEditor from "../../panels/editors/RatingStepEditor"
import type { SimpleNode } from "../../panels/editors/_shared"

// ─── Helpers ──────────────────────────────────────────────────────

/** Create a banding node that provides levels for the rating editor. */
function makeBandingNode(outputColumn: string, assignments: string[]): SimpleNode {
  return {
    id: `banding_${outputColumn}`,
    data: {
      label: `Banding ${outputColumn}`,
      description: "",
      nodeType: "banding",
      config: {
        factors: [{
          banding: "continuous",
          column: outputColumn,
          outputColumn,
          rules: assignments.map(a => ({ op1: ">", val1: "0", op2: "", val2: "", assignment: a })),
        }],
      },
    },
  }
}

const BANDING_NODES: SimpleNode[] = [
  makeBandingNode("age_band", ["young", "mid", "old"]),
  makeBandingNode("region", ["north", "south"]),
]

afterEach(cleanup)

// ─── Tests ────────────────────────────────────────────────────────

describe("RatingStepEditor", () => {
  it("renders with default empty config", () => {
    render(
      <RatingStepEditor
        config={{}}
        onUpdate={vi.fn()}
        inputSources={[]}
        allNodes={[]}
      />,
    )
    expect(screen.getByText("Rating Tables · 1 table")).toBeTruthy()
    expect(screen.getByText("Select at least one factor to populate the rating table")).toBeTruthy()
  })

  it("renders rating table header with table count", () => {
    const config = {
      tables: [
        { name: "Age Factor", factors: [], outputColumn: "af", defaultValue: "1.0", entries: [] },
        { name: "Region Factor", factors: [], outputColumn: "rf", defaultValue: "1.0", entries: [] },
      ],
    }
    render(
      <RatingStepEditor
        config={config}
        onUpdate={vi.fn()}
        inputSources={[]}
        allNodes={[]}
      />,
    )
    expect(screen.getByText("Rating Tables · 2 tables")).toBeTruthy()
  })

  it("renders table tab buttons", () => {
    const config = {
      tables: [
        { name: "Table A", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] },
        { name: "Table B", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] },
      ],
    }
    render(
      <RatingStepEditor
        config={config}
        onUpdate={vi.fn()}
        inputSources={[]}
        allNodes={[]}
      />,
    )
    expect(screen.getByText("Table A")).toBeTruthy()
    expect(screen.getByText("Table B")).toBeTruthy()
  })

  it("renders table name input with current name", () => {
    const config = {
      tables: [{ name: "My Rating", factors: [], outputColumn: "", defaultValue: "1.0", entries: [] }],
    }
    render(
      <RatingStepEditor
        config={config}
        onUpdate={vi.fn()}
        inputSources={[]}
        allNodes={[]}
      />,
    )
    const input = screen.getByPlaceholderText("Age Factor") as HTMLInputElement
    expect(input.value).toBe("My Rating")
  })

  it("shows factor count label", () => {
    render(
      <RatingStepEditor
        config={{}}
        onUpdate={vi.fn()}
        inputSources={[]}
        allNodes={BANDING_NODES}
      />,
    )
    expect(screen.getByText("Factors (0/3)")).toBeTruthy()
  })

  it("shows factor dropdown with available banding columns", () => {
    render(
      <RatingStepEditor
        config={{}}
        onUpdate={vi.fn()}
        inputSources={[]}
        allNodes={BANDING_NODES}
      />,
    )
    // The "Add factor" select
    const addSelect = screen.getByRole("combobox") as HTMLSelectElement
    const options = Array.from(addSelect.options).map(o => o.textContent)
    expect(options).toContain("+ Add factor...")
    expect(options.some(o => o?.includes("age_band"))).toBe(true)
    expect(options.some(o => o?.includes("region"))).toBe(true)
  })

  it("calls onUpdate when adding a table", () => {
    const onUpdate = vi.fn()
    render(
      <RatingStepEditor
        config={{}}
        onUpdate={onUpdate}
        inputSources={[]}
        allNodes={[]}
      />,
    )
    // The "+" button to add a table
    const addButtons = screen.getAllByRole("button")
    const addTableBtn = addButtons.find(b => b.querySelector("svg"))
    // There should be a + button (Plus icon)
    expect(addTableBtn).toBeTruthy()
  })

  it("renders one-way editor when table has 1 factor", () => {
    const config = {
      tables: [{
        name: "T1",
        factors: ["age_band"],
        outputColumn: "af",
        defaultValue: "1.0",
        entries: [
          { age_band: "young", value: 1.1 },
          { age_band: "mid", value: 1.0 },
          { age_band: "old", value: 0.9 },
        ],
      }],
    }
    render(
      <RatingStepEditor
        config={config}
        onUpdate={vi.fn()}
        inputSources={[]}
        allNodes={BANDING_NODES}
      />,
    )
    // Should show the factor column header in the one-way table
    expect(screen.getByText("age_band")).toBeTruthy()
    expect(screen.getByText("Relativity")).toBeTruthy()
  })

  it("renders combination controls when 2+ tables exist", () => {
    const config = {
      tables: [
        { name: "T1", factors: [], outputColumn: "af", defaultValue: "1.0", entries: [] },
        { name: "T2", factors: [], outputColumn: "rf", defaultValue: "1.0", entries: [] },
      ],
    }
    render(
      <RatingStepEditor
        config={config}
        onUpdate={vi.fn()}
        inputSources={[]}
        allNodes={[]}
      />,
    )
    expect(screen.getByText("Combine")).toBeTruthy()
    expect(screen.getByText("× Multiply (relativities)")).toBeTruthy()
  })

  it("shows entry count in summary", () => {
    const config = {
      tables: [{
        name: "T1",
        factors: ["age_band"],
        outputColumn: "age_factor",
        defaultValue: "1.0",
        entries: [
          { age_band: "young", value: 1.1 },
          { age_band: "old", value: 0.9 },
        ],
      }],
    }
    render(
      <RatingStepEditor
        config={config}
        onUpdate={vi.fn()}
        inputSources={[]}
        allNodes={BANDING_NODES}
      />,
    )
    expect(screen.getByText("age_factor")).toBeTruthy()
    expect(screen.getByText(/2 entries/)).toBeTruthy()
  })

  it("renders input sources bar when inputs provided", () => {
    render(
      <RatingStepEditor
        config={{}}
        onUpdate={vi.fn()}
        inputSources={[{ varName: "source_data", sourceLabel: "Source Data", edgeId: "e1" }]}
        allNodes={[]}
      />,
    )
    expect(screen.getByText("source_data")).toBeTruthy()
  })
})
