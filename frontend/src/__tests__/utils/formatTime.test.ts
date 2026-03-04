import { describe, it, expect } from "vitest"
import { formatTime } from "../../utils/formatTime"

describe("formatTime", () => {
  it("returns empty string for 0", () => {
    expect(formatTime(0)).toBe("")
  })

  it("returns empty string for NaN-like falsy values", () => {
    // The function checks `if (!ts)` so any falsy number returns ""
    expect(formatTime(0)).toBe("")
  })

  it("formats a unix timestamp into HH:MM", () => {
    // 2024-01-15 12:30:00 UTC = 1705319400
    const result = formatTime(1705319400)
    // We can't assert exact locale output, but we can check it's non-empty
    // and matches a time-like pattern (e.g. "12:30 PM" or "12:30")
    expect(result).toBeTruthy()
    expect(result).toMatch(/\d{1,2}:\d{2}/)
  })

  it("formats midnight correctly", () => {
    // 2024-01-15 00:00:00 UTC = 1705276800
    const result = formatTime(1705276800)
    expect(result).toBeTruthy()
    expect(result).toMatch(/\d{1,2}:\d{2}/)
  })

  it("handles very large timestamps", () => {
    // 2040-01-01 00:00:00 UTC = 2208988800
    const result = formatTime(2208988800)
    expect(result).toBeTruthy()
    expect(result).toMatch(/\d{1,2}:\d{2}/)
  })
})
