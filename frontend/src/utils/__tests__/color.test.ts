import { describe, it, expect } from "vitest"
import { withAlpha } from "../color"

describe("withAlpha", () => {
  it("converts 6-digit hex to rgba", () => {
    expect(withAlpha("#ff0000", 0.5)).toBe("rgba(255,0,0,0.5)")
  })

  it("converts 3-digit shorthand hex to rgba", () => {
    expect(withAlpha("#f00", 0.5)).toBe("rgba(255,0,0,0.5)")
  })

  it("works without # prefix", () => {
    expect(withAlpha("00ff00", 1)).toBe("rgba(0,255,0,1)")
  })

  it("handles alpha = 0", () => {
    expect(withAlpha("#000000", 0)).toBe("rgba(0,0,0,0)")
  })

  it("handles alpha = 1", () => {
    expect(withAlpha("#ffffff", 1)).toBe("rgba(255,255,255,1)")
  })

  it("handles mixed hex values", () => {
    expect(withAlpha("#1a2b3c", 0.75)).toBe("rgba(26,43,60,0.75)")
  })

  it("handles 3-digit shorthand correctly (e.g. #abc)", () => {
    // #abc → #aabbcc → rgb(170, 187, 204)
    expect(withAlpha("#abc", 0.1)).toBe("rgba(170,187,204,0.1)")
  })

  it("preserves fractional alpha precision", () => {
    expect(withAlpha("#000000", 0.094)).toBe("rgba(0,0,0,0.094)")
  })
})
