import { describe, expect, it, vi } from "vitest"
import { fireEvent, render, screen } from "@testing-library/react"
import ExploratoryAnalysisEditor from "../../panels/editors/ExploratoryAnalysisEditor"

describe("ExploratoryAnalysisEditor", () => {
  it("renders one row per upstream column", () => {
    render(
      <ExploratoryAnalysisEditor
        config={{ fieldRoles: {} }}
        onUpdate={vi.fn()}
        inputSources={[{ varName: "source", sourceLabel: "Source", edgeId: "e1" }]}
        upstreamColumns={[
          { name: "claim_id", dtype: "String" },
          { name: "target_value", dtype: "Float64" },
        ]}
        accentColor="#0f766e"
      />,
    )

    expect(screen.getByText("claim_id")).toBeInTheDocument()
    expect(screen.getByText("target_value")).toBeInTheDocument()
  })

  it("updates fieldRoles when a dropdown changes", () => {
    const onUpdate = vi.fn()
    render(
      <ExploratoryAnalysisEditor
        config={{ fieldRoles: {} }}
        onUpdate={onUpdate}
        inputSources={[{ varName: "source", sourceLabel: "Source", edgeId: "e1" }]}
        upstreamColumns={[{ name: "claim_id", dtype: "String" }]}
        accentColor="#0f766e"
      />,
    )

    fireEvent.change(screen.getByLabelText("claim_id role"), { target: { value: "claim_key" } })

    expect(onUpdate).toHaveBeenCalledWith("fieldRoles", { claim_id: "claim_key" })
  })
})
