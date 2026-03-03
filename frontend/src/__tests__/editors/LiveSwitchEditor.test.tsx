/**
 * Render tests for LiveSwitchEditor.
 *
 * Tests: renders scenario info, input mapping, active indicator.
 */
import { describe, it, expect, vi, afterEach, beforeEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import LiveSwitchEditor from "../../panels/editors/LiveSwitchEditor"
import useSettingsStore from "../../stores/useSettingsStore"

afterEach(cleanup)

// Reset store before each test
beforeEach(() => {
  useSettingsStore.setState({
    scenarios: ["live", "backtest"],
    activeScenario: "live",
  })
})

describe("LiveSwitchEditor", () => {
  it("renders scenario routing description", () => {
    render(
      <LiveSwitchEditor config={{}} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("Routes inputs based on the active scenario")).toBeTruthy()
  })

  it("shows active scenario", () => {
    render(
      <LiveSwitchEditor config={{}} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("● live")).toBeTruthy()
  })

  it("renders input mapping section with count", () => {
    const inputs = [
      { varName: "live_data", sourceLabel: "Live Data", edgeId: "e1" },
      { varName: "backtest_data", sourceLabel: "Backtest Data", edgeId: "e2" },
    ]
    render(
      <LiveSwitchEditor config={{}} onUpdate={vi.fn()} inputSources={inputs} />,
    )
    expect(screen.getByText("Input → Scenario Mapping (2)")).toBeTruthy()
    expect(screen.getByText("Live Data")).toBeTruthy()
    expect(screen.getByText("Backtest Data")).toBeTruthy()
  })

  it("renders scenario dropdowns for each input", () => {
    const inputs = [
      { varName: "live_data", sourceLabel: "Live Data", edgeId: "e1" },
    ]
    render(
      <LiveSwitchEditor config={{}} onUpdate={vi.fn()} inputSources={inputs} />,
    )
    const selects = screen.getAllByRole("combobox")
    expect(selects.length).toBeGreaterThanOrEqual(1)
    // Should have scenario options
    const options = Array.from((selects[0] as HTMLSelectElement).options).map(o => o.value)
    expect(options).toContain("live")
    expect(options).toContain("backtest")
  })

  it("shows active indicator when input is mapped to active scenario", () => {
    const inputs = [
      { varName: "live_data", sourceLabel: "Live Data", edgeId: "e1" },
    ]
    const config = {
      input_scenario_map: { live_data: "live" },
    }
    render(
      <LiveSwitchEditor config={config} onUpdate={vi.fn()} inputSources={inputs} />,
    )
    expect(screen.getByText("active")).toBeTruthy()
  })

  it("does not show active indicator when mapped to non-active scenario", () => {
    const inputs = [
      { varName: "backtest_data", sourceLabel: "Backtest Data", edgeId: "e2" },
    ]
    const config = {
      input_scenario_map: { backtest_data: "backtest" },
    }
    render(
      <LiveSwitchEditor config={config} onUpdate={vi.fn()} inputSources={inputs} />,
    )
    expect(screen.queryByText("active")).toBeNull()
  })

  it("calls onUpdate when selecting a scenario for an input", () => {
    const onUpdate = vi.fn()
    const inputs = [
      { varName: "live_data", sourceLabel: "Live Data", edgeId: "e1" },
    ]
    render(
      <LiveSwitchEditor config={{}} onUpdate={onUpdate} inputSources={inputs} />,
    )
    const select = screen.getAllByRole("combobox")[0] as HTMLSelectElement
    fireEvent.change(select, { target: { value: "backtest" } })
    expect(onUpdate).toHaveBeenCalledWith("input_scenario_map", { live_data: "backtest" })
  })

  it("renders with non-live active scenario", () => {
    useSettingsStore.setState({ activeScenario: "backtest" })
    render(
      <LiveSwitchEditor config={{}} onUpdate={vi.fn()} inputSources={[]} />,
    )
    expect(screen.getByText("backtest")).toBeTruthy()
  })
})
