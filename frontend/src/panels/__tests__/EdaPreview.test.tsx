import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, waitFor, fireEvent, cleanup } from "@testing-library/react"
import EdaPreview from "../EdaPreview"
import type { EdaOneWayResponse, EdaResponse, GraphPayload } from "../../api/types"

const fetchEda = vi.fn<(...args: unknown[]) => Promise<EdaResponse>>()
const fetchEdaOneWay = vi.fn<(...args: unknown[]) => Promise<EdaOneWayResponse>>()

class MockResizeObserver {
  static instances: MockResizeObserver[] = []

  callback: ResizeObserverCallback

  constructor(callback: ResizeObserverCallback) {
    this.callback = callback
    MockResizeObserver.instances.push(this)
  }

  observe() {}
  unobserve() {}
  disconnect() {}
}

vi.mock("../../api/client", () => ({
  fetchEda: (...args: unknown[]) => fetchEda(...args),
  fetchEdaOneWay: (...args: unknown[]) => fetchEdaOneWay(...args),
}))

vi.mock("../../hooks/useDragResize", () => ({
  useDragResize: () => ({
    height: 360,
    containerRef: { current: null },
    onDragStart: vi.fn(),
  }),
}))

const graph: GraphPayload = {
  nodes: [],
  edges: [],
  preamble: "",
}

const edaResponse: EdaResponse = {
  status: "ok",
  descriptive: [],
  outliers: [],
  disguised_missings: [],
  correlations: {
    fields: [],
    pearson: [],
    spearman: [],
    cramer: [],
  },
}

const oneWayResponse: EdaOneWayResponse = {
  status: "ok",
  x_field: "policy_month",
  x_labels: ["Jan", "Feb", "Mar"],
  claim_counts: [10, 14, 9],
  target_sums: [100, 150, 120],
}

describe("EdaPreview", () => {
  beforeEach(() => {
    globalThis.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver
    MockResizeObserver.instances = []
    fetchEda.mockResolvedValue(edaResponse)
    fetchEdaOneWay.mockResolvedValue(oneWayResponse)
  })

  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
  })

  it("fills the one-way chart panel and resizes the svg with the pane", async () => {
    const { container } = render(
      <EdaPreview
        data={null}
        config={{ fieldRoles: { policy_month: "covariate" } }}
        graph={graph}
        nodeId="eda-1"
      />,
    )

    await waitFor(() => expect(fetchEda).toHaveBeenCalled())

    fireEvent.click(screen.getByRole("button", { name: "One-way Charts" }))

    await waitFor(() => expect(fetchEdaOneWay).toHaveBeenCalled())

    const chartHost = container.querySelector(".min-h-\\[280px\\] > div") as HTMLDivElement
    expect(chartHost).not.toBeNull()

    Object.defineProperty(chartHost, "clientWidth", { configurable: true, value: 720 })
    Object.defineProperty(chartHost, "clientHeight", { configurable: true, value: 420 })

    MockResizeObserver.instances.at(-1)?.callback([
      {
        contentRect: { width: 720, height: 420 } as DOMRectReadOnly,
      } as ResizeObserverEntry,
    ], {} as ResizeObserver)

    const svg = await screen.findByLabelText("One-way chart")
    expect(svg).toHaveAttribute("width", "720")
    expect(svg).toHaveAttribute("height", "420")

    MockResizeObserver.instances.at(-1)?.callback([
      {
        contentRect: { width: 960, height: 520 } as DOMRectReadOnly,
      } as ResizeObserverEntry,
    ], {} as ResizeObserver)

    await waitFor(() => {
      expect(svg).toHaveAttribute("width", "960")
      expect(svg).toHaveAttribute("height", "520")
    })
  })
})
