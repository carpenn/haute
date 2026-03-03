import { describe, it, expect, afterEach } from "vitest"
import { render, cleanup } from "@testing-library/react"
import { StatsFooter } from "../StatsFooter"

describe("StatsFooter", () => {
  afterEach(cleanup)
  it("returns nothing when stats is null", () => {
    const { container } = render(<StatsFooter stats={null} />)
    expect(container.innerHTML).toBe("")
  })

  it("renders all stats formatted to 3 decimal places", () => {
    const stats = { min: 1.23456, max: 9.87654, avg: 5.55555, count: 42 }
    const { container } = render(<StatsFooter stats={stats} />)
    expect(container.textContent).toContain("n=42")
    expect(container.textContent).toContain("min 1.235")
    expect(container.textContent).toContain("avg 5.556")
    expect(container.textContent).toContain("max 9.877")
  })
})
