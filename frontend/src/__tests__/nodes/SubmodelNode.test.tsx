/**
 * Tests for SubmodelNode component.
 *
 * Tests: label rendering, SUBMODEL badge, child count, file path display,
 * output port labels, per-port handles, opacity when dimmed,
 * border style (dashed vs solid).
 */
import { describe, it, expect, afterEach } from "vitest"
import { render, screen, cleanup } from "@testing-library/react"
import { ReactFlowProvider } from "@xyflow/react"
import SubmodelNode, { type SubmodelNodeData } from "../../nodes/SubmodelNode"

afterEach(cleanup)

// ── Helpers ─────────────────────────────────────────────────────

function makeProps(
  data: Partial<SubmodelNodeData> & { label: string },
  overrides: { selected?: boolean } = {},
) {
  const fullData: SubmodelNodeData = {
    description: "",
    nodeType: "submodel",
    ...data,
  }

  // NodeProps shape expected by ReactFlow node components
  return {
    id: "test-node",
    type: "submodel",
    data: fullData as Record<string, unknown>,
    selected: overrides.selected ?? false,
    isConnectable: true,
    positionAbsoluteX: 0,
    positionAbsoluteY: 0,
    zIndex: 0,
    dragging: false,
    dragHandle: undefined,
    parentId: undefined,
    sourcePosition: undefined,
    targetPosition: undefined,
    width: 240,
    height: 70,
  }
}

function renderNode(
  data: Partial<SubmodelNodeData> & { label: string },
  opts: { selected?: boolean } = {},
) {
  const props = makeProps(data, opts)
  return render(
    <ReactFlowProvider>
      <SubmodelNode {...(props as any)} />
    </ReactFlowProvider>,
  )
}

// ── Tests ───────────────────────────────────────────────────────

describe("SubmodelNode", () => {
  it("renders the label text", () => {
    renderNode({ label: "My Submodel" })
    expect(screen.getByText("My Submodel")).toBeTruthy()
  })

  it('renders the "SUBMODEL" badge', () => {
    renderNode({ label: "Test" })
    expect(screen.getByText("SUBMODEL")).toBeTruthy()
  })

  it("renders the child node count", () => {
    renderNode({
      label: "Test",
      config: { childNodeIds: ["a", "b", "c"] },
    })
    expect(screen.getByText("3 nodes")).toBeTruthy()
  })

  it("renders 0 nodes when no childNodeIds", () => {
    renderNode({ label: "Test" })
    expect(screen.getByText("0 nodes")).toBeTruthy()
  })

  it("renders file path when config.file is set", () => {
    renderNode({
      label: "Test",
      config: { file: "submodels/pricing.py" },
    })
    expect(screen.getByText("submodels/pricing.py")).toBeTruthy()
  })

  it("does not render file path when config.file is not set", () => {
    renderNode({ label: "Test" })
    // Only the port labels use --text-muted; without a file, there should be none
    // in the header area. We verify by ensuring the specific text is absent.
    expect(screen.queryByText("submodels/pricing.py")).toBeNull()
  })

  it("renders output port labels", () => {
    renderNode({
      label: "Test",
      config: { outputPorts: ["premium", "discount"] },
    })
    expect(screen.getByText(/premium/)).toBeTruthy()
    expect(screen.getByText(/discount/)).toBeTruthy()
  })

  it("renders per-port input handles for each inputPort", () => {
    const { container } = renderNode({
      label: "Test",
      config: { inputPorts: ["base_rate", "claims"] },
    })
    // Hidden per-port handles have ids like "in__base_rate"
    const handle1 = container.querySelector('[data-handleid="in__base_rate"]')
    const handle2 = container.querySelector('[data-handleid="in__claims"]')
    expect(handle1).toBeTruthy()
    expect(handle2).toBeTruthy()
  })

  it("renders per-port output handles for each outputPort", () => {
    const { container } = renderNode({
      label: "Test",
      config: { outputPorts: ["result_a", "result_b"] },
    })
    const handle1 = container.querySelector('[data-handleid="out__result_a"]')
    const handle2 = container.querySelector('[data-handleid="out__result_b"]')
    expect(handle1).toBeTruthy()
    expect(handle2).toBeTruthy()
  })

  it("sets opacity to 0.3 when _traceDimmed is true", () => {
    const { container } = renderNode({
      label: "Dimmed",
      _traceDimmed: true,
    })
    const wrapper = container.querySelector(".rounded-xl") as HTMLElement
    expect(wrapper.style.opacity).toBe("0.3")
  })

  it("sets full opacity when _traceDimmed is false", () => {
    const { container } = renderNode({
      label: "Bright",
      _traceDimmed: false,
    })
    const wrapper = container.querySelector(".rounded-xl") as HTMLElement
    expect(wrapper.style.opacity).toBe("1")
  })

  it("uses dashed border when not selected and not traceActive", () => {
    const { container } = renderNode(
      { label: "Default" },
      { selected: false },
    )
    const wrapper = container.querySelector(".rounded-xl") as HTMLElement
    expect(wrapper.style.border).toContain("dashed")
  })

  it("uses solid border when selected", () => {
    const { container } = renderNode(
      { label: "Selected" },
      { selected: true },
    )
    const wrapper = container.querySelector(".rounded-xl") as HTMLElement
    expect(wrapper.style.border).toContain("solid")
    expect(wrapper.style.border).not.toContain("dashed")
  })

  it("uses solid border when _traceActive is true", () => {
    const { container } = renderNode({
      label: "Active",
      _traceActive: true,
    })
    const wrapper = container.querySelector(".rounded-xl") as HTMLElement
    expect(wrapper.style.border).toContain("solid")
    expect(wrapper.style.border).not.toContain("dashed")
  })
})
