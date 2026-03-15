import { describe, it, expect } from "vitest"
import { hoverHandlers, hoverBg } from "../hoverHandlers"
import type { MouseEvent } from "react"

/** Create a minimal mock event with a currentTarget that has a style object. */
function mockEvent(): MouseEvent<HTMLElement> {
  const el = document.createElement("div")
  return { currentTarget: el } as unknown as MouseEvent<HTMLElement>
}

// ---------------------------------------------------------------------------
// hoverHandlers
// ---------------------------------------------------------------------------

describe("hoverHandlers", () => {
  it("returns onMouseEnter and onMouseLeave functions", () => {
    const handlers = hoverHandlers()
    expect(typeof handlers.onMouseEnter).toBe("function")
    expect(typeof handlers.onMouseLeave).toBe("function")
  })

  it("onMouseEnter sets hover background and color", () => {
    const handlers = hoverHandlers("red", "blue")
    const event = mockEvent()
    handlers.onMouseEnter(event)
    expect(event.currentTarget.style.background).toBe("red")
    expect(event.currentTarget.style.color).toBe("blue")
  })

  it("onMouseLeave restores rest background and color", () => {
    const handlers = hoverHandlers("red", "blue", "green", "yellow")
    const event = mockEvent()
    handlers.onMouseLeave(event)
    expect(event.currentTarget.style.background).toBe("green")
    expect(event.currentTarget.style.color).toBe("yellow")
  })

  it("uses sensible defaults when no args are provided", () => {
    const handlers = hoverHandlers()
    const enterEvent = mockEvent()
    handlers.onMouseEnter(enterEvent)
    expect(enterEvent.currentTarget.style.background).toBe("var(--chrome-hover)")
    expect(enterEvent.currentTarget.style.color).toBe("var(--text-primary)")

    const leaveEvent = mockEvent()
    handlers.onMouseLeave(leaveEvent)
    expect(leaveEvent.currentTarget.style.background).toBe("transparent")
    expect(leaveEvent.currentTarget.style.color).toBe("var(--text-secondary)")
  })
})

// ---------------------------------------------------------------------------
// hoverBg
// ---------------------------------------------------------------------------

describe("hoverBg", () => {
  it("returns onMouseEnter and onMouseLeave functions", () => {
    const handlers = hoverBg()
    expect(typeof handlers.onMouseEnter).toBe("function")
    expect(typeof handlers.onMouseLeave).toBe("function")
  })

  it("onMouseEnter sets only background", () => {
    const handlers = hoverBg("purple")
    const event = mockEvent()
    handlers.onMouseEnter(event)
    expect(event.currentTarget.style.background).toBe("purple")
  })

  it("onMouseLeave restores rest background", () => {
    const handlers = hoverBg("purple", "orange")
    const event = mockEvent()
    handlers.onMouseLeave(event)
    expect(event.currentTarget.style.background).toBe("orange")
  })

  it("uses sensible defaults when no args are provided", () => {
    const handlers = hoverBg()
    const enterEvent = mockEvent()
    handlers.onMouseEnter(enterEvent)
    expect(enterEvent.currentTarget.style.background).toBe("var(--bg-hover)")

    const leaveEvent = mockEvent()
    handlers.onMouseLeave(leaveEvent)
    expect(leaveEvent.currentTarget.style.background).toBe("transparent")
  })
})
